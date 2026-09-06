"""Start the website and API on the port assigned by Databricks Apps."""

import os

import uvicorn

if __name__ == "__main__":
    uvicorn.run(
        "delu_app.api:app",
        host="0.0.0.0",
        port=int(os.environ.get("DATABRICKS_APP_PORT", "8000")),
    )
