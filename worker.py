#!/usr/bin/env python3
# worker.py
# Background worker:
#   1. Pull SQS jobs
#   2. Download video → upload to S3
#   3. Extract 1 fps frames → upload to S3
#   4. Decide meal vs non-meal (Vision + Structured Outputs)
#   5. Send dynamic ACK DM
#   6. If meal → extract ingredients & recipe, then DM the recipe
#   7. Persist basic metadata (recipe JSON can be added later)
#   8. Delete SQS message

import os
import time
import json
import tempfile
import subprocess
import logging
from pathlib import Path

import boto3
import requests
import openai
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Table, Column, String, Integer, MetaData, Text

# ─────────────────── ENV & LOGGING ─────────────────────────────
load_dotenv()
SQS_QUEUE_URL     = os.getenv("SQS_QUEUE_URL")
S3_BUCKET         = os.getenv("S3_BUCKET_NAME")
DATABASE_URL      = os.getenv("DATABASE_URL")
OPENAI_API_KEY    = os.getenv("OPENAI_API_KEY")
AWS_REGION        = os.getenv("AWS_REGION", "us-east-1")
PAGE_ACCESS_TOKEN = os.getenv("PAGE_ACCESS_TOKEN")
IG_API_VERSION    = os.getenv("GRAPH_API_VERSION", "v23.0")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("worker")
logger.info("### bailey-worker recipe build ###")

# ─────────────────── AWS & OPENAI CLIENTS ─────────────────────
sqs = boto3.client("sqs", region_name=AWS_REGION)
s3  = boto3.client("s3", region_name=AWS_REGION)

openai.api_key = OPENAI_API_KEY

# ─────────────────── DATABASE SETUP ───────────────────────────
engine        = create_async_engine(DATABASE_URL, echo=False)
async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
metadata      = MetaData()

jobs_table = Table(
    "video_jobs", metadata,
    Column("id",         Integer, primary_key=True),
    Column("video_id",   String(255), unique=True),
    Column("video_url",  Text),
    Column("raw_s3_key", String(255)),
    Column("sender_id",  String(255)),
    Column("status",     String(50)),
    # recipe_json column can be added later
)

frames_table = Table(
    "video_frames", metadata,
    Column("id",           Integer, primary_key=True),
    Column("video_id",     String(255)),
    Column("frame_s3_key", String(255)),
    Column("frame_number", Integer),
)

BYTE_LIMIT = 950

# ─────────────────── PROMPTS & SCHEMAS ────────────────────────
MEAL_DETECTION_PROMPT = """
You are a vision-and-language chef assistant.
Given images from a cooking reel, decide if it shows a true multi-ingredient “meal”
vs only snacking, reheating, or single-ingredient treatment.
Respond *only* with JSON matching this schema:
{
  "type": "object",
  "properties": { "is_meal": { "type": "boolean" } },
  "required": ["is_meal"],
  "additionalProperties": false
}
"""

MEAL_SCHEMA = {
    "type": "json_schema",
    "json_schema": {
        "name": "meal_detection",
        "strict": True,
        "schema": {
            "type": "object",
            "properties": { "is_meal": {"type": "boolean"} },
            "required": ["is_meal"],
            "additionalProperties": False
        }
    }
}

RECIPE_SYSTEM_PROMPT = (
    "You are a senior recipe developer for a premium meal-kit company. "
    "You have full vision context (all frames) and the Instagram caption.\n\n"

    "Return JSON in exactly this form — nothing else:\n"
    "{\n"
    "  \"ingredients\": [\"…\"],   # distinct store-bought or raw items\n"
    "  \"recipe_steps\": [\"…\"]   # numbered cooking method strings\n"
    "}\n\n"

    "━━━━━━━━  INGREDIENT RULES  ━━━━━━━━\n"
    "• One entry per raw or packaged item a home cook must buy (e.g. “salmon fillet”, “plain flour”, “corn tortilla”).\n"
    "• Do NOT combine items into umbrella terms (“marinated steak” ❌). Break into their parts.\n"
    "• No quantities. Keep original order of first appearance.\n\n"

    "━━━━━━━━  STEP RULES  ━━━━━━━━\n"
    "• Write **14-22 steps** so a first-time cook can reproduce the dish from scratch.\n"
    "• Start each step with ONE fitting emoji, then ≤40 words, present tense.\n"
    "• Cover every visible or logically required action, including (where relevant):\n"
    "  – Trimming, butterflying, pounding, or filleting proteins to even thickness.\n"
    "  – Marinades, dry rubs, or seasoning mixes shown or implied.\n"
    "  – Complete coating processes (flour → egg → crumbs, batter mixes, etc.).\n"
    "  – All cooking phases: exact oil or oven temps, times, colour & sensory cues.\n"
    "  – Resting, draining, chilling, or tempering steps.\n"
    "  – Heating breads/wraps, reducing sauces, chopping garnishes.\n"
    "  – Concurrency hints: “While X cooks, prep Y …”.\n"
    "• Finish with final assembly & serving suggestion.\n\n"

    "━━━━━━━━  STYLE  ━━━━━━━━\n"
    "• Use clear sensory cues (deep-gold, fragrant, juices run clear) plus temps/timers.\n"
    "• Never split a single technique across two steps.\n"
    "• Respond ONLY with valid JSON matching the schema."
)


RECIPE_SCHEMA = {
    "type": "json_schema",
    "json_schema": {
        "name": "recipe_extraction",
        "strict": True,
        "schema": {
            "type": "object",
            "properties": {
                "ingredients": {
                    "type": "array",
                    "items": {"type": "string"}
                },
                "recipe_steps": {
                    "type": "array",
                    "items": {"type": "string"}
                }
            },
            "required": ["ingredients", "recipe_steps"],
            "additionalProperties": False
        }
    }
}

# ─────────────────── IG HELPER ────────────────────────────────
def send_ig_message(recipient_id: str, text: str):
    url = f"https://graph.facebook.com/{IG_API_VERSION}/me/messages"
    resp = requests.post(
        url,
        params={"access_token": PAGE_ACCESS_TOKEN},
        data={
            "recipient": json.dumps({"id": recipient_id}),
            "message":   json.dumps({"text": text}),
        },
        timeout=10
    )
    resp.raise_for_status()
    return resp.json()

# ─────────────────── ACKNOWLEDGEMENT TEXT ─────────────────────
def generate_ack_text(is_meal: bool, caption: str, frame_urls: list[str]) -> str:
    system = (
        "You are a friendly, energetic chef’s assistant DM’ing a friend in colloquial UK text-speak, "
        "never cringe, max 20 words, no em dashes. "
        "• Reference one concrete detail you saw/read. "
        "• Use ONE relevant iOS emoji. "
        "• End exactly with:\n"
        + (
            "“Your recipe is on the way!”" if is_meal else
            "“There’s no meal here, but send me a tasty food Reel anytime and I’ll be happy to share the recipe!”"
        )
    )

    examples = [
        {
            "user": (
                "Caption: “The perfect burger !! Stacked G.F.C. spicy kimchi marinated chicken…”\n"
                "Frames: melted cheese dripping; hash brown patty; sesame-seed bun\n"
                "Write an acknowledgement:"
            ),
            "bot": (
                "WOAH! When Gordon Ramsay says it's a perfect burger, melted cheese never lies 🍔 Your recipe is on the way!"
            )
        },
        {
            "user": (
                "Caption: “Just a cute pig shredding on a skateboard!”\n"
                "Frames: pink pig in sunglasses; skate ramp; blue sky\n"
                "Write an acknowledgement:"
            ),
            "bot": (
                "That pig absolutely rips 🛹 There’s no meal here, but send me a tasty food Reel anytime and I’ll be happy to share the recipe!"
            )
        }
    ]

    messages = [{"role": "system", "content": system}]
    for ex in examples:
        messages.append({"role": "user",      "content": ex["user"]})
        messages.append({"role": "assistant", "content": ex["bot"]})

    live_user = (
        f"Caption: “{caption or '(no caption)'}”\n"
        "Frames:\n" + "\n".join(f"- {url}" for url in frame_urls[:3]) +
        "\nWrite an acknowledgement:"
    )
    messages.append({"role": "user", "content": live_user})

    resp = openai.chat.completions.create(
        model="gpt-4.1",
        messages=messages,
    )
    return resp.choices[0].message.content.strip()

# ─────────────────── RECIPE → TEXT DM ─────────────────────────
def format_recipe_text(data: dict) -> str:
    """Convert recipe JSON into a user-friendly DM."""
    ingredients = "\n".join(f"• {item}" for item in data["ingredients"])
    steps       = "\n".join(f"{i+1}. {step}" for i, step in enumerate(data["recipe_steps"]))
    return (
        "Here’s your recipe! 👩‍🍳\n\n"
        "Ingredients:\n"
        f"{ingredients}\n\n"
        "Steps:\n"
        f"{steps}"
    )

def send_recipe_dm(recipient_id: str, data: dict):
    """
    Sends the recipe in IG DMs, ensuring every payload ≤ BYTE_LIMIT UTF-8 bytes
    and never splitting a numbered step.
    """
    header = "Here’s your recipe! 👩‍🍳\n\nIngredients:\n"
    ingredients_block = "\n".join(f"• {item}" for item in data["ingredients"])
    steps_lines = [f"{i+1}. {step}" for i, step in enumerate(data["recipe_steps"])]

    def _bytes(s: str) -> int:
        return len(s.encode("utf-8"))

    # ---------- First chunk: header + ingredients ----------
    first_chunk = f"{header}{ingredients_block}\n\nSteps:\n"
    if _bytes(first_chunk) > BYTE_LIMIT:
        # Very long ingredient list — send it alone
        send_ig_message(recipient_id, first_chunk.rstrip())
        time.sleep(0.4)
        current_chunk = ""          # start fresh for steps
    else:
        current_chunk = first_chunk

    # ---------- Add steps, splitting at boundaries ----------
    for line in steps_lines:
        prospective = current_chunk + line + "\n"
        if _bytes(prospective) > BYTE_LIMIT:
            # flush current chunk
            send_ig_message(recipient_id, current_chunk.rstrip())
            time.sleep(0.4)
            current_chunk = line + "\n"
        else:
            current_chunk = prospective

    if current_chunk.strip():
        send_ig_message(recipient_id, current_chunk.rstrip())

# ─────────────────── DB INIT ──────────────────────────────────
async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(metadata.create_all)

# ─────────────────── VIDEO / IMAGE UTILS ──────────────────────
def download_video(url: str, dest: Path):
    logger.info("Downloading video from %s", url)
    resp = requests.get(url, stream=True, timeout=30)
    resp.raise_for_status()
    with open(dest, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)

def upload_to_s3(local_path: Path, s3_key: str) -> str:
    logger.info("Uploading %s → s3://%s/%s", local_path.name, S3_BUCKET, s3_key)
    s3.upload_file(Filename=str(local_path), Bucket=S3_BUCKET, Key=s3_key)
    return f"https://{S3_BUCKET}.s3.{AWS_REGION}.amazonaws.com/{s3_key}"

def extract_frames(video_path: Path, frames_dir: Path):
    if any(frames_dir.glob("*.jpg")):
        logger.info("Frames already extracted; skipping")
    else:
        frames_dir.mkdir(parents=True, exist_ok=True)
        subprocess.run([
            "ffmpeg", "-i", str(video_path),
            "-vf", "fps=1",
            str(frames_dir / "%04d.jpg")
        ], check=True)
    return sorted(frames_dir.glob("*.jpg"))

# ─────────────────── MEAL VS NON-MEAL ─────────────────────────
def detect_meal(frame_urls: list[str]) -> bool:
    logger.info("Detecting meal vs non-meal over %d frames", len(frame_urls))
    messages = [{"role": "system", "content": MEAL_DETECTION_PROMPT}]
    for url in frame_urls:
        messages.append({
            "role": "user",
            "content": [
                {"type": "text", "text": "frame"},
                {"type": "image_url", "image_url": {"url": url, "detail": "high"}}
            ]
        })
    resp   = openai.chat.completions.create(
        model="gpt-4.1",
        messages=messages,
        response_format=MEAL_SCHEMA
    )
    choice = resp.choices[0].message
    if getattr(choice, "refusal", None):
        logger.error("Meal detection refusal: %s", choice.refusal)
        return False
    return json.loads(choice.content)["is_meal"]

# ─────────────────── RECIPE EXTRACTION ────────────────────────
def extract_recipe(caption: str, frame_urls: list[str]) -> dict:
    logger.info("Extracting recipe from %d frames", len(frame_urls))
    messages = [{"role": "system", "content": RECIPE_SYSTEM_PROMPT}]
    messages.append({"role": "user", "content": f"CAPTION:\n{caption or '(no caption)'}"})
    for url in frame_urls:
        messages.append({
            "role": "user",
            "content": [
                {"type": "text", "text": "frame"},
                {"type": "image_url", "image_url": {"url": url, "detail": "high"}}
            ]
        })
    resp   = openai.chat.completions.create(
        model="gpt-4.1",
        messages=messages,
        response_format=RECIPE_SCHEMA
    )
    choice = resp.choices[0].message
    if getattr(choice, "refusal", None):
        raise RuntimeError(f"Recipe extraction refusal: {choice.refusal}")
    return json.loads(choice.content)

# ─────────────────── MAIN JOB PROCESSOR ───────────────────────
async def process_job(job_body: dict, receipt_handle: str):
    logger.info("Processing job: %s", job_body)

    video_url  = job_body["video_url"]
    sender_id  = job_body["sender_id"]
    message_id = job_body["message_id"]
    caption    = job_body.get("caption", "")
    video_id   = message_id.replace(":", "_")

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            video_file = tmp / f"{video_id}.mp4"

            # 1. Download video
            download_video(video_url, video_file)

            # 2. Upload raw video
            raw_key = f"raw/{video_id}.mp4"
            upload_to_s3(video_file, raw_key)

            # 3. Extract frames → upload → get URLs
            frame_paths = extract_frames(video_file, tmp / "frames")
            frame_urls  = [
                upload_to_s3(p, f"frames/{video_id}/{p.name}") for p in frame_paths
            ]

            # 4. Detect meal
            is_meal = detect_meal(frame_urls)

            # 5. ACK
            try:
                ack = generate_ack_text(is_meal, caption, frame_urls)
                send_ig_message(sender_id, ack)
                logger.info("Sent ACK to %s: %s", sender_id, ack)
            except Exception as e:
                logger.error("ACK send failed: %s", e)

            # 6. If meal → extract recipe & DM it
            recipe_data = None
            if is_meal:
                try:
                    recipe_data = extract_recipe(caption, frame_urls)
                    logger.info("Recipe extracted: %s", recipe_data)
                    try:
                        send_recipe_dm(sender_id, recipe_data)
                        logger.info("Sent recipe DM to %s", sender_id)
                    except Exception as e:
                        logger.error("Failed to DM recipe: %s", e)
                except Exception as e:
                    logger.error("Recipe extraction failed: %s", e)

            # 7. If non-meal, we’re done
            if not is_meal:
                logger.info("Non-meal reel; skipping DB write")
                return

            # 8. Persist minimal metadata (extend later with recipe_json)
            async with async_session() as session:
                await session.execute(
                    jobs_table.insert().values(
                        video_id=video_id,
                        video_url=video_url,
                        raw_s3_key=raw_key,
                        sender_id=sender_id,
                        status="meal_detected",
                        # recipe_json=json.dumps(recipe_data) if recipe_data else None,
                    )
                )
                for idx, url in enumerate(frame_urls, 1):
                    await session.execute(
                        frames_table.insert().values(
                            video_id=video_id,
                            frame_s3_key=url,
                            frame_number=idx
                        )
                    )
                await session.commit()

            logger.info("DB write complete; job done.")

    except Exception:
        logger.exception("Fatal error processing job %s", video_id)

    finally:
        try:
            sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
        except Exception:
            logger.warning("Failed to delete SQS message %s", receipt_handle)

# ─────────────────── RUNNER LOOP ──────────────────────────────
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