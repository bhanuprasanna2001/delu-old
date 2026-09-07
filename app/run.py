"""Start the website and API on Render, Databricks Apps, or locally."""

import os

import uvicorn

if __name__ == "__main__":
    uvicorn.run(
        "delu_app.api:app",
        host="0.0.0.0",
        port=int(os.environ.get("PORT", os.environ.get("DATABRICKS_APP_PORT", "8000"))),
    )
