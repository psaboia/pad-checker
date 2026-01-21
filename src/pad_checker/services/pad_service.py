"""Service for querying PAD Analytics data."""

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import pandas as pd

import pad_analytics as pad


PAD_BASE_URL = "https://pad.crc.nd.edu"


@dataclass
class NotesInfo:
    """Parsed notes information from PAD card."""

    phone_id: Optional[str] = None
    user: Optional[str] = None
    app_type: Optional[str] = None
    build: Optional[int] = None
    neural_net: Optional[str] = None
    predicted_drug: Optional[str] = None
    prediction_score: Optional[float] = None
    safe_status: Optional[str] = None
    quantity_nn: Optional[float] = None
    quantity_pls: Optional[float] = None
    pls_used: Optional[bool] = None
    notes_text: Optional[str] = None
    raw: Optional[str] = None  # Original JSON string if parsing fails


@dataclass
class CardInfo:
    """Information about a PAD card."""

    id: int
    sample_id: Optional[int]
    sample_name: str
    project_name: str
    user_name: str
    date_of_creation: str
    quantity: Optional[float]
    notes: Optional[NotesInfo]
    image_url: Optional[str]
    camera_type: Optional[str]


class PADService:
    """Service for querying PAD data."""

    def __init__(self):
        self._projects_cache: Optional[pd.DataFrame] = None
        self._users_cache: Optional[list[str]] = None

    def get_projects(self) -> pd.DataFrame:
        """Get all projects, with caching."""
        if self._projects_cache is None:
            self._projects_cache = pad.get_projects()
        return self._projects_cache

    def get_users(self) -> list[str]:
        """Get list of unique users from all projects."""
        if self._users_cache is None:
            projects = self.get_projects()
            if "user_name" in projects.columns:
                self._users_cache = sorted(
                    projects["user_name"].dropna().unique().tolist()
                )
            else:
                self._users_cache = []
        return self._users_cache

    def get_latest_card_by_user(
        self,
        username: str,
        project_name: Optional[str] = None
    ) -> Optional[CardInfo]:
        """Get the most recent card submitted by a user.

        Args:
            username: The username to search for.
            project_name: Optional project name to filter by.

        Returns:
            CardInfo with the latest card details, or None if not found.
        """
        try:
            if project_name:
                cards = pad.get_project_cards(project_name)
            else:
                projects = self.get_projects()
                project_ids = projects["id"].tolist()

                all_cards = []
                for pid in project_ids:
                    try:
                        project_cards = pad.get_project_cards(pid)
                        if not project_cards.empty:
                            all_cards.append(project_cards)
                    except Exception:
                        continue

                if not all_cards:
                    return None
                cards = pd.concat(all_cards, ignore_index=True)

            if cards.empty:
                return None

            user_col = self._find_column(cards, ["user_name", "user"])
            if not user_col:
                return None

            user_cards = cards[
                cards[user_col].str.lower() == username.lower()
            ]

            if user_cards.empty:
                return None

            date_col = self._find_column(
                user_cards,
                ["date_of_creation", "created_at", "date"]
            )

            if date_col:
                user_cards = user_cards.sort_values(
                    by=date_col,
                    ascending=False
                )

            latest = user_cards.iloc[0]

            return self._row_to_card_info(latest)

        except Exception as e:
            print(f"Error fetching cards: {e}")
            return None

    def get_card_by_id(self, card_id: int) -> Optional[CardInfo]:
        """Get a specific card by its ID.

        Args:
            card_id: The card ID to fetch.

        Returns:
            CardInfo or None if not found.
        """
        try:
            card_df = pad.get_card(card_id=card_id)

            if card_df is None or card_df.empty:
                return None

            return self._row_to_card_info(card_df.iloc[0])

        except Exception as e:
            print(f"Error fetching card {card_id}: {e}")
            return None

    def get_recent_cards_in_project(
        self,
        project_name: str,
        limit: int = 3
    ) -> list[CardInfo]:
        """Get the most recent cards in a specific project.

        Args:
            project_name: The project name to search in.
            limit: Number of cards to return.

        Returns:
            List of CardInfo with the latest cards.
        """
        try:
            cards = pad.get_project_cards(project_name)

            if cards.empty:
                return []

            date_col = self._find_column(
                cards,
                ["date_of_creation", "created_at", "date"]
            )

            if date_col:
                cards = cards.sort_values(by=date_col, ascending=False)

            recent = cards.head(limit)
            return [self._row_to_card_info(row) for _, row in recent.iterrows()]

        except Exception as e:
            print(f"Error fetching recent cards: {e}")
            return []

    def get_latest_card_in_project(
        self,
        project_name: str
    ) -> Optional[CardInfo]:
        """Get the most recent card in a specific project.

        Args:
            project_name: The project name to search in.

        Returns:
            CardInfo with the latest card details, or None if not found.
        """
        try:
            cards = pad.get_project_cards(project_name)

            if cards.empty:
                return None

            date_col = self._find_column(
                cards,
                ["date_of_creation", "created_at", "date"]
            )

            if date_col:
                cards = cards.sort_values(by=date_col, ascending=False)

            latest = cards.iloc[0]
            return self._row_to_card_info(latest)

        except Exception as e:
            print(f"Error fetching latest card in project: {e}")
            return None

    def _find_column(
        self,
        df: pd.DataFrame,
        candidates: list[str]
    ) -> Optional[str]:
        """Find the first matching column name from candidates."""
        for col in candidates:
            if col in df.columns:
                return col
        return None

    def _safe_get(self, row: pd.Series, candidates: list[str], default=None):
        """Safely get a value from row using candidate column names."""
        for col in candidates:
            if col in row.index:
                val = row[col]
                if pd.notna(val):
                    return val
        return default

    def _format_datetime(self, dt_str: Optional[str]) -> str:
        """Format ISO datetime string to readable format."""
        if not dt_str:
            return ""
        try:
            dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
            return dt.strftime("%d/%m/%Y %I:%M %p")
        except (ValueError, AttributeError):
            return str(dt_str)

    def _convert_image_path_to_url(self, path: Optional[str]) -> Optional[str]:
        """Convert local server path to public URL."""
        if not path:
            return None
        # Remove /var/www/html prefix if present
        if path.startswith("/var/www/html"):
            path = path[len("/var/www/html"):]
        # Ensure path starts with /
        if not path.startswith("/"):
            path = "/" + path
        return f"{PAD_BASE_URL}{path}"

    def _parse_notes(self, notes_str: Optional[str]) -> Optional[NotesInfo]:
        """Parse JSON notes string into NotesInfo object."""
        if not notes_str:
            return None

        try:
            data = json.loads(notes_str)
            return NotesInfo(
                phone_id=data.get("Phone ID"),
                user=data.get("User") or None,
                app_type=data.get("App type"),
                build=data.get("Build"),
                neural_net=data.get("Neural net"),
                predicted_drug=data.get("Predicted drug"),
                prediction_score=data.get("Prediction score"),
                safe_status=data.get("Safe"),
                quantity_nn=data.get("Quantity NN"),
                quantity_pls=data.get("Quantity PLS"),
                pls_used=data.get("PLS used"),
                notes_text=data.get("Notes") or None,
            )
        except (json.JSONDecodeError, TypeError):
            # If parsing fails, return raw string
            return NotesInfo(raw=notes_str)

    def _row_to_card_info(self, row: pd.Series) -> CardInfo:
        """Convert a DataFrame row to CardInfo."""
        raw_image_path = self._safe_get(
            row,
            ["processed_file_location", "raw_file_location", "url", "image_url"]
        )
        raw_notes = self._safe_get(row, ["notes", "note"])
        return CardInfo(
            id=int(self._safe_get(row, ["id", "card_id"], 0)),
            sample_id=self._safe_get(row, ["sample_id"]),
            sample_name=str(self._safe_get(
                row,
                ["sample_name", "sample_name.name", "drug_name"],
                "Unknown"
            )),
            project_name=str(self._safe_get(
                row,
                ["project.project_name", "project_name", "project.name"],
                "Unknown"
            )),
            user_name=str(self._safe_get(
                row,
                ["user_name", "user_name.name", "user"],
                "Unknown"
            )),
            date_of_creation=self._format_datetime(self._safe_get(
                row,
                ["date_of_creation", "created_at", "date"],
                ""
            )),
            quantity=self._safe_get(row, ["quantity", "concentration"]),
            notes=self._parse_notes(raw_notes),
            image_url=self._convert_image_path_to_url(raw_image_path),
            camera_type=self._safe_get(row, ["camera_type_1", "camera_type"]),
        )

    def clear_cache(self):
        """Clear all cached data."""
        self._projects_cache = None
        self._users_cache = None
