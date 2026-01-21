"""PAD Checker - FastAPI application."""

from pathlib import Path

from typing import Optional

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

import uvicorn

from .services import PADService

app = FastAPI(title="PAD Checker", version="0.1.0")

BASE_DIR = Path(__file__).parent
templates = Jinja2Templates(directory=BASE_DIR / "templates")

pad_service = PADService()


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Render the main page."""
    users = pad_service.get_users()
    projects_df = pad_service.get_projects()
    # Sort by ID descending (most recent first)
    projects_df = projects_df.sort_values("id", ascending=False)
    projects = projects_df["project_name"].tolist()
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "users": users, "projects": projects}
    )


@app.post("/search", response_class=HTMLResponse)
async def search(
    request: Request,
    project: str = Form(...),
    username: Optional[str] = Form(None)
):
    """Search for the latest card in a project, optionally filtered by username."""
    username = username.strip() if username else None

    if username:
        card = pad_service.get_latest_card_by_user(username, project_name=project)
        error_msg = f"No cards found for user '{username}' in project '{project}'"
    else:
        card = pad_service.get_latest_card_in_project(project)
        error_msg = f"No cards found in project '{project}'"

    recent_cards = pad_service.get_recent_cards_in_project(project, limit=3)

    return templates.TemplateResponse(
        "partials/card_result.html",
        {
            "request": request,
            "card": card,
            "username": username,
            "error": None if card else error_msg,
            "recent_cards": recent_cards
        }
    )


@app.get("/card/{card_id}", response_class=HTMLResponse)
async def get_card_by_id(
    request: Request,
    card_id: int
):
    """Get a specific card by ID."""
    card = pad_service.get_card_by_id(card_id)

    if not card:
        return templates.TemplateResponse(
            "partials/card_result.html",
            {
                "request": request,
                "card": None,
                "error": f"Card {card_id} not found",
                "recent_cards": []
            }
        )

    recent_cards = pad_service.get_recent_cards_in_project(card.project_name, limit=3)

    return templates.TemplateResponse(
        "partials/card_result.html",
        {
            "request": request,
            "card": card,
            "username": None,
            "error": None,
            "recent_cards": recent_cards
        }
    )


@app.get("/check-newer", response_class=HTMLResponse)
async def check_newer(
    request: Request,
    project: str,
    current_id: int
):
    """Check if there's a newer card in the project."""
    latest = pad_service.get_latest_card_in_project(project)

    if latest and latest.id != current_id:
        return templates.TemplateResponse(
            "partials/newer_alert.html",
            {
                "request": request,
                "project": project,
                "new_id": latest.id
            }
        )
    return ""


@app.post("/refresh-cache")
async def refresh_cache():
    """Clear the service cache to fetch fresh data."""
    pad_service.clear_cache()
    return {"status": "ok", "message": "Cache cleared"}


def run():
    """Run the application."""
    uvicorn.run(
        "pad_checker.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )


if __name__ == "__main__":
    run()
