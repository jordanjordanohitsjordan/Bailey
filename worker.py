#!/usr/bin/env python3
# worker.py
# Background worker to pull jobs from SQS, download videos, upload to S3,
# extract frames (once), decide meal vs non-meal via OpenAI Vision+Reasoning,
# send an acknowledgment DM, and record frames for later transcription & recipe generation.

import os
import json
import tempfile
import subprocess
import logging
from pathlib import Path

import boto3
import requests
import openai
import instagram_api  # your existing IG DM wrapper
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Table, Column, String, Integer, MetaData, Text

# Load environment variables
load_dotenv()
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL")
S3_BUCKET      = os.getenv("S3_BUCKET_NAME")
DATABASE_URL   = os.getenv("DATABASE_URL")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
AWS_REGION     = os.getenv("AWS_REGION", "us-east-1")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("worker")

# AWS clients
sqs = boto3.client("sqs", region_name=AWS_REGION)
s3  = boto3.client("s3", region_name=AWS_REGION)

# OpenAI client key
openai.api_key = OPENAI_API_KEY

# Database setup (async SQLAlchemy)
engine        = create_async_engine(DATABASE_URL, echo=False)
async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
metadata      = MetaData()

# Define tables
jobs_table = Table(
    "video_jobs", metadata,
    Column("id",         Integer, primary_key=True),
    Column("video_id",   String(255), unique=True),
    Column("video_url",  Text),
    Column("raw_s3_key", String(255)),
    Column("sender_id",  String(255)),
    Column("status",     String(50)),
)
frames_table = Table(
    "video_frames", metadata,
    Column("id",           Integer, primary_key=True),
    Column("video_id",     String(255)),
    Column("frame_s3_key", String(255)),
    Column("frame_number", Integer),
)

# System prompt for meal detection + acknowledgment
MEAL_ACK_PROMPT = '''
You are a vision-and-language chef assistant.
Given images from a cooking reel, decide if it shows a true multi-ingredient “meal”
vs only snacking, reheating, or single-ingredient treatment.

Then compose a friendly DM:
- If "is_meal" is true, set "ack_message" to something like:
  "Thanks for sharing your [dish name]! 🍽️ Your recipe is on its way."
- If "is_meal" is false, set "ack_message" to:
  "Looks like this wasn't a full recipe, but thanks for sharing! 😊"

Respond using the following strict JSON schema:
{
  "type": "object",
  "properties": {
    "is_meal": { "type": "boolean" },
    "ack_message": { "type": "string" }
  },
  "required": ["is_meal","ack_message"],
  "additionalProperties": false
}
'''

# JSON-schema wrapper for structured outputs
detection_schema = {
    "type": "json_schema",
    "json_schema": {
        "name": "meal_ack",
        "strict": True,
        "schema": {
            "type": "object",
            "properties": {
                "is_meal": {"type": "boolean"},
                "ack_message": {"type": "string"}
            },
            "required": ["is_meal","ack_message"],
            "additionalProperties": False
        }
    }
}

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(metadata.create_all)


def download_video(url: str, dest: Path):
    logger.info(f"Downloading video from {url}")
    resp = requests.get(url, stream=True, timeout=30)
    resp.raise_for_status()
    with open(dest, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)


def upload_to_s3(local_path: Path, s3_key: str) -> str:
    logger.info(f"Uploading {local_path.name} to s3://{S3_BUCKET}/{s3_key}")
    s3.upload_file(
        Filename=str(local_path),
        Bucket=S3_BUCKET,
        Key=s3_key
    )
    return f"https://{S3_BUCKET}.s3.{AWS_REGION}.amazonaws.com/{s3_key}"


def extract_frames(video_path: Path, frames_dir: Path):
    if any(frames_dir.glob("*.jpg")):
        logger.info("Frames already extracted; skipping")
    else:
        logger.info(f"Extracting frames from {video_path.name}")
        frames_dir.mkdir(parents=True, exist_ok=True)
        subprocess.run([
            "ffmpeg", "-i", str(video_path),
            "-vf", "fps=1",
            str(frames_dir / "%04d.jpg")
        ], check=True)
    return sorted(frames_dir.glob("*.jpg"))


def detect_and_ack(frames_urls: list[str]) -> tuple[bool,str]:
    """
    Call OpenAI with structured outputs to both detect meal and generate DM text.
    Returns (is_meal, ack_message).
    """
    logger.info(f"Detecting meal vs non-meal over {len(frames_urls)} frames + composing DM")
    messages = [{"role": "system", "content": MEAL_ACK_PROMPT}]
    for url in frames_urls:
        messages.append({
            "role": "user",
            "content": [
                {"type": "text", "text": "frame"},
                {"type": "image_url", "image_url": {"url": url, "detail": "low"}}
            ]
        })

    resp = openai.chat.completions.create(
        model="gpt-4o-mini",
        messages=messages,
        response_format=detection_schema,
        max_tokens=128
    )

    data = json.loads(resp.choices[0].message.content)
    return bool(data["is_meal"]), data["ack_message"]


async def process_job(job_body: dict, receipt_handle: str):
    video_url  = job_body["video_url"]
    sender_id  = job_body["sender_id"]
    message_id = job_body["message_id"]
    video_id   = message_id.replace(":", "_")

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp        = Path(tmpdir)
            video_file = tmp / f"{video_id}.mp4"

            # 1. Download video
            download_video(video_url, video_file)

            # 2. Upload raw video
            raw_key = f"raw/{video_id}.mp4"
            upload_to_s3(video_file, raw_key)

            # 3. Extract frames
            frames = extract_frames(video_file, tmp / "frames")

            # 4. Upload frames & collect URLs
            frame_urls = []
            for frame in frames:
                key = f"frames/{video_id}/{frame.name}"
                frame_urls.append(upload_to_s3(frame, key))

            # 5. Detect meal & compose DM
            is_meal, dm_text = detect_and_ack(frame_urls)
            instagram_api.send_direct_message(to=sender_id, text=dm_text)

            if not is_meal:
                return

            # 6. Record to database
            async with async_session() as session:
                await session.execute(
                    jobs_table.insert().values(
                        video_id=video_id,
                        video_url=video_url,
                        raw_s3_key=raw_key,
                        sender_id=sender_id,
                        status="meal_detected"
                    )
                )
                for idx, url in enumerate(frame_urls, start=1):
                    await session.execute(
                        frames_table.insert().values(
                            video_id=video_id,
                            frame_s3_key=url,
                            frame_number=idx
                        )
                    )
                await session.commit()
            logger.info("Meal frames recorded; ready for transcription & recipe.")

    except Exception:
        logger.exception("Error processing job %s", video_id)

    finally:
        try:
            sqs.delete_message(
                QueueUrl=SQS_QUEUE_URL,
                ReceiptHandle=receipt_handle
            )
        except Exception:
            logger.warning("Failed to delete SQS message: %s", receipt_handle)


if __name__ == "__main__":
    import asyncio

    async def main_loop():
        await init_db()
        while True:
            resp = sqs.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=5,
                WaitTimeSeconds=20
            )
            for msg in resp.get("Messages", []):
                await process_job(
                    job_body=json.loads(msg["Body"]),
                    receipt_handle=msg["ReceiptHandle"]
                )
            await asyncio.sleep(1)

    asyncio.run(main_loop())