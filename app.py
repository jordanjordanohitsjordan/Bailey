import os
from fastapi import FastAPI, HTTPException
from dotenv import load_dotenv

load_dotenv()  # loads IG_WEBHOOK_VERIFY_TOKEN from .env

VERIFY_TOKEN = os.getenv("IG_WEBHOOK_VERIFY_TOKEN", "my_verify_token")

app = FastAPI()

@app.get("/webhook")
async def verify_webhook(
    mode: str = None,
    verify_token: str = None,
    challenge: str = None,
):
    if mode == "subscribe" and verify_token == VERIFY_TOKEN:
        return int(challenge)
    raise HTTPException(status_code=403, detail="Verification token mismatch")