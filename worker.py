# worker.py
# Background worker to pull jobs from SQS, download videos, upload to S3,
# extract frames, upload frames, and record metadata to MySQL.

import os
import json
import time
import tempfile
import subprocess
import logging
from pathlib import Path

import boto3
import requests
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Table, Column, String, Integer, MetaData, Text

# Load environment variables
load_dotenv()
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL")
S3_BUCKET = os.getenv("S3_BUCKET_NAME")
DATABASE_URL = os.getenv("DATABASE_URL")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("worker")

# AWS clients
sqs = boto3.client("sqs")
s3 = boto3.client("s3")

# Database setup (async SQLAlchemy)
engine = create_async_engine(DATABASE_URL, echo=False)
async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
metadata = MetaData()

# Define tables (reflect or define your schema here)
jobs_table = Table(
    "video_jobs",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("video_id", String(255), unique=True),
    Column("video_url", Text),
    Column("raw_s3_key", String(255)),
    Column("sender_id", String(255)),
    Column("status", String(50)),
)
frames_table = Table(
    "video_frames",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("video_id", String(255)),
    Column("frame_s3_key", String(255)),
    Column("frame_number", Integer),
)

# Ensure tables exist (run once)
async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(metadata.create_all)


def download_video(url: str, dest_path: Path):
    logger.info(f"Downloading video from {url}")
    resp = requests.get(url, stream=True, timeout=30)
    resp.raise_for_status()
    with open(dest_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)


def upload_to_s3(local_path: Path, s3_key: str):
    logger.info(f"Uploading {local_path} to s3://{S3_BUCKET}/{s3_key}")
    s3.upload_file(str(local_path), S3_BUCKET, s3_key)
    return f"s3://{S3_BUCKET}/{s3_key}"


def extract_frames(video_path: Path, frames_dir: Path):
    logger.info(f"Extracting frames from {video_path} to {frames_dir}")
    frames_dir.mkdir(parents=True, exist_ok=True)
    subprocess.run([
        "ffmpeg", "-i", str(video_path), "-vf", "fps=1", str(frames_dir / "%04d.jpg")
    ], check=True)
    return sorted(frames_dir.glob("*.jpg"))


async def process_job(job_body: dict):
    video_url = job_body["video_url"]
    sender_id = job_body["sender_id"]
    message_id = job_body["message_id"]
    video_id = message_id.replace(':', '_')

    # Temporary workspace
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        video_file = tmpdir / f"{video_id}.mp4"

        # 1. Download video
        download_video(video_url, video_file)

        # 2. Upload raw video to S3
        raw_key = f"raw/{video_id}.mp4"
        raw_s3_url = upload_to_s3(video_file, raw_key)

        # 3. Extract frames
        frames_dir = tmpdir / "frames"
        frames = extract_frames(video_file, frames_dir)

        async with async_session() as session:
            # Insert job record
            await session.execute(
                jobs_table.insert().values(
                    video_id=video_id,
                    video_url=video_url,
                    raw_s3_key=raw_key,
                    sender_id=sender_id,
                    status="frames_extracted"
                )
            )
            # Insert frame records
            for idx, frame_path in enumerate(frames, start=1):
                frame_key = f"frames/{video_id}/{frame_path.name}"
                upload_to_s3(frame_path, frame_key)
                await session.execute(
                    frames_table.insert().values(
                        video_id=video_id,
                        frame_s3_key=frame_key,
                        frame_number=idx
                    )
                )
            await session.commit()


if __name__ == "__main__":
    import asyncio

    async def main_loop():
        await init_db()
        while True:
            # Poll SQS for messages
            resp = sqs.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=5,
                WaitTimeSeconds=20
            )
            messages = resp.get("Messages", [])
            if not messages:
                continue
            for msg in messages:
                try:
                    job = json.loads(msg["Body"])
                    await process_job(job)
                    # Delete message on success
                    sqs.delete_message(
                        QueueUrl=SQS_QUEUE_URL,
                        ReceiptHandle=msg["ReceiptHandle"]
                    )
                except Exception:
                    logger.exception("Failed to process job: %s", msg)

    asyncio.run(main_loop())