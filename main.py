"""Allow `uvicorn main:app` from the project root."""
from app.api import create_app

app = create_app()
