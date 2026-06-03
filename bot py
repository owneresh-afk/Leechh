"""
AnimeTadka Leech Bot
Built with Pyrogram (MTProto) — no file size limits
"""

import asyncio
import aiohttp
import os
import json
import psutil
import time
import zipfile
import gzip
import shutil
import math
from pathlib import Path
from pyrogram import Client, filters
from pyrogram.types import Message
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
#  CONFIG — fill these in or set via env vars
# ─────────────────────────────────────────────
API_ID   = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")

# ─────────────────────────────────────────────
#  CONSTANTS
# ─────────────────────────────────────────────
SETTINGS_FILE       = "leech_settings.json"
DOWNLOADS_DIR       = "/tmp/animetadka_leech"
MAX_CONCURRENT      = 12
COMPRESS_THRESHOLD  = 2 * 1024 * 1024 * 1024   # 2 GB
PROGRESS_INTERVAL   = 3                          # seconds between edits

Path(DOWNLOADS_DIR).mkdir(parents=True, exist_ok=True)

# ─────────────────────────────────────────────
#  GLOBALS
# ─────────────────────────────────────────────
active_tasks: dict = {}
task_counter: int  = 0

DEFAULT_SETTINGS = {
    "prefix":            "[AnimeTadka]- ",
    "remove_patterns":   [],
    "token_pickle_path": "",
    "temp_folder":       DOWNLOADS_DIR,
}

# ─────────────────────────────────────────────
#  SETTINGS HELPERS
# ─────────────────────────────────────────────
def load_settings() -> dict:
    if os.path.exists(SETTINGS_FILE):
        with open(SETTINGS_FILE) as f:
            return json.load(f)
    return DEFAULT_SETTINGS.copy()

def save_settings(s: dict):
    with open(SETTINGS_FILE, "w") as f:
        json.dump(s, f, indent=2)

# ─────────────────────────────────────────────
#  FILENAME PROCESSING
# ─────────────────────────────────────────────
def clean_filename(name: str, settings: dict) -> str:
    for pat in settings.get("remove_patterns", []):
        name = name.replace(pat, "")
    name = ".".join(name.split())          # collapse whitespace
    prefix = settings.get("prefix", "[AnimeTadka]- ")
    if not name.startswith(prefix):
        name = prefix + name
    return name

# ─────────────────────────────────────────────
#  FORMAT HELPERS
# ─────────────────────────────────────────────
def fmt_bytes(n: float) -> str:
    if n == 0:
        return "0B"
    units = ["B","KB","MB","GB","TB"]
    i = int(math.floor(math.log(max(n,1), 1024)))
    i = min(i, len(units)-1)
    return f"{n / (1024**i):.2f}{units[i]}"

def fmt_time(s: float) -> str:
    s = max(0, int(s))
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    if h:
        return f"{h}h {m}m {sec}s"
    if m:
        return f"{m}m {sec}s"
    return f"{sec}s"

def progress_bar(pct: float, length: int = 10) -> str:
    filled = int(length * pct / 100)
    return "●" * filled + "○" * (length - filled)

def sys_stats() -> dict:
    cpu  = psutil.cpu_percent(interval=0.1)
    mem  = psutil.virtual_memory()
    disk = psutil.disk_usage(DOWNLOADS_DIR)
    net  = psutil.net_io_counters()
    return {
        "cpu":  cpu,
        "ram":  mem.percent,
        "free": disk.free,
        "in":   net.bytes_recv,
        "out":  net.bytes_sent,
    }

def build_progress_msg(task_id: int, filename: str, task: dict) -> str:
    p    = task.get("progress", {})
    st   = task.get("status", "downloading")
    stats = sys_stats()

    dl    = p.get("downloaded", 0)
    total = p.get("total", 0)
    spd   = p.get("speed", 0)
    eta   = p.get("eta", 0)
    elaps = p.get("elapsed", 0)
    pct   = (dl / total * 100) if total > 0 else 0

    bar = progress_bar(pct)

    return (
        f"{task_id}. {filename}\n"
        f"   {st.capitalize()}...\n"
        f"   {bar}\n"
        f"- Progress: {pct:.2f}%\n"
        f"- Processed: {fmt_bytes(dl)}\n"
        f"- Total Size: {fmt_bytes(total)}\n"
        f"- Speed: {fmt_bytes(spd)}/s\n"
        f"- ETA: {fmt_time(eta)}\n"
        f"- Elapsed: {fmt_time(elaps)}\n"
        f"- Engine: HTTP/Pyrogram\n"
        f"- Action: #leech\n\n"
        f"CPU: {stats['cpu']:.1f}% | RAM: {stats['ram']:.1f}% | FREE: {fmt_bytes(stats['free'])}\n"
        f"IN: {fmt_bytes(stats['in'])} | OUT: {fmt_bytes(stats['out'])}"
    )

# ─────────────────────────────────────────────
#  DOWNLOADER
# ─────────────────────────────────────────────
async def download_http(url: str, filepath: str, task_id: int):
    async with aiohttp.ClientSession() as session:
        async with session.get(url, allow_redirects=True,
                               timeout=aiohttp.ClientTimeout(total=None)) as resp:
            if resp.status != 200:
                raise RuntimeError(f"HTTP {resp.status}")

            total     = int(resp.headers.get("content-length", 0))
            downloaded = 0
            start     = time.time()

            with open(filepath, "wb") as f:
                async for chunk in resp.content.iter_chunked(256 * 1024):
                    f.write(chunk)
                    downloaded += len(chunk)
                    elapsed = time.time() - start
                    speed   = downloaded / elapsed if elapsed > 0 else 0
                    eta     = (total - downloaded) / speed if speed > 0 else 0

                    if task_id in active_tasks:
                        active_tasks[task_id]["progress"] = {
                            "downloaded": downloaded,
                            "total":      total,
                            "speed":      speed,
                            "elapsed":    elapsed,
                            "eta":        eta,
                        }
                    await asyncio.sleep(0)

# ─────────────────────────────────────────────
#  UNZIP (recursive)
# ─────────────────────────────────────────────
def unzip_recursive(zip_path: str, out_dir: str, settings: dict):
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(out_dir)
    os.remove(zip_path)

    for root, _, files in os.walk(out_dir):
        for fname in files:
            fpath = os.path.join(root, fname)
            if fname.lower().endswith(".zip"):
                unzip_recursive(fpath, root, settings)
            else:
                new_name = clean_filename(fname, settings)
                if new_name != fname:
                    os.rename(fpath, os.path.join(root, new_name))

# ─────────────────────────────────────────────
#  COMPRESS > 2GB
# ─────────────────────────────────────────────
def maybe_compress(filepath: str) -> str:
    if os.path.getsize(filepath) > COMPRESS_THRESHOLD:
        out = filepath + ".gz"
        with open(filepath, "rb") as f_in, gzip.open(out, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        os.remove(filepath)
        return out
    return filepath

# ─────────────────────────────────────────────
#  PROGRESS UPDATER (background loop)
# ─────────────────────────────────────────────
async def progress_loop(client: Client, chat_id: int, msg_id: int, task_id: int, filename: str):
    while task_id in active_tasks:
        task = active_tasks[task_id]
        try:
            text = build_progress_msg(task_id, filename, task)
            await client.edit_message_text(chat_id, msg_id, text)
        except Exception:
            pass
        await asyncio.sleep(PROGRESS_INTERVAL)

# ─────────────────────────────────────────────
#  MAIN TASK RUNNER
# ─────────────────────────────────────────────
async def run_task(client: Client, chat_id: int, task_id: int, url: str,
                   do_unzip: bool, settings: dict):
    global active_tasks

    raw_name  = url.split("/")[-1].split("?")[0] or f"file_{task_id}"
    filename  = clean_filename(raw_name, settings)
    filepath  = os.path.join(DOWNLOADS_DIR, f"{task_id}_{filename}")

    active_tasks[task_id] = {
        "url":      url,
        "filename": filename,
        "status":   "downloading",
        "progress": {},
        "start":    time.time(),
    }

    # Send initial progress message
    init_msg = await client.send_message(chat_id,
        f"{task_id}. {filename}\n   Downloading...\n   ○○○○○○○○○○\n- Progress: 0%")
    msg_id = init_msg.id

    # Start progress updater
    loop_task = asyncio.create_task(
        progress_loop(client, chat_id, msg_id, task_id, filename)
    )

    try:
        # ── Download ──
        await download_http(url, filepath, task_id)

        # ── Unzip ──
        if do_unzip and zipfile.is_zipfile(filepath):
            active_tasks[task_id]["status"] = "unzipping"
            extract_dir = filepath + "_extracted"
            Path(extract_dir).mkdir(parents=True, exist_ok=True)
            unzip_recursive(filepath, extract_dir, settings)
            upload_targets = [
                os.path.join(extract_dir, f)
                for f in os.listdir(extract_dir)
                if os.path.isfile(os.path.join(extract_dir, f))
            ]
        else:
            upload_targets = [filepath]

        # ── Compress & Upload ──
        active_tasks[task_id]["status"] = "uploading"
        for fpath in upload_targets:
            fpath = maybe_compress(fpath)
            fname = os.path.basename(fpath)
            await client.send_document(
                chat_id,
                fpath,
                file_name=fname,
                caption=f"{fname}"
            )
            os.remove(fpath)

        # Cleanup extracted dir if exists
        ext_dir = filepath + "_extracted"
        if os.path.isdir(ext_dir):
            shutil.rmtree(ext_dir)

        # Final status
        elapsed = time.time() - active_tasks[task_id]["start"]
        await client.edit_message_text(
            chat_id, msg_id,
            f"{task_id}. {filename}\n"
            f"   Upload Has Been Completed!\n"
            f"- Elapsed: {fmt_time(elapsed)}\n"
            f"- Action: #leech"
        )

    except Exception as e:
        await client.edit_message_text(
            chat_id, msg_id,
            f"{task_id}. {filename}\n"
            f"   Download Has Been Stopped!\n"
            f"- Due to: {str(e)}"
        )
        # Cleanup
        if os.path.exists(filepath):
            os.remove(filepath)
    finally:
        loop_task.cancel()
        active_tasks.pop(task_id, None)

# ─────────────────────────────────────────────
#  BOT CLIENT
# ─────────────────────────────────────────────
app = Client(
    "animetadka_leech",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
)

# ─────────────────────────────────────────────
#  HANDLERS
# ─────────────────────────────────────────────
@app.on_message(filters.command("start"))
async def cmd_start(client: Client, msg: Message):
    await msg.reply_text(
        "AnimeTadka Leech Bot\n\n"
        "Commands:\n"
        "/l <url>      - Leech file\n"
        "/l -e <url>   - Leech + unzip\n"
        "/user         - View settings\n"
        "/user set prefix <val>\n"
        "/user set patterns <p1>,<p2>\n"
        "/user set token <path>\n"
        "/status       - Active tasks\n"
        "/cancel <id>  - Cancel task"
    )

@app.on_message(filters.command("l"))
async def cmd_leech(client: Client, msg: Message):
    global task_counter

    args = msg.command[1:]  # skip "l"

    do_unzip = False
    url = None

    if args and args[0] == "-e":
        do_unzip = True
        args = args[1:]

    if not args or not args[0].startswith("http"):
        await msg.reply_text("Usage: /l <url> or /l -e <url>")
        return

    url = args[0]

    if len(active_tasks) >= MAX_CONCURRENT:
        await msg.reply_text(f"Max {MAX_CONCURRENT} concurrent tasks running. Try later.")
        return

    settings = load_settings()
    task_counter += 1
    tid = task_counter

    asyncio.create_task(
        run_task(client, msg.chat.id, tid, url, do_unzip, settings)
    )

@app.on_message(filters.command("status"))
async def cmd_status(client: Client, msg: Message):
    if not active_tasks:
        await msg.reply_text("No active tasks.")
        return

    lines = []
    for tid, task in active_tasks.items():
        p   = task.get("progress", {})
        dl  = p.get("downloaded", 0)
        tot = p.get("total", 0)
        pct = (dl / tot * 100) if tot > 0 else 0
        lines.append(
            f"[{tid}] {task['filename']}\n"
            f"   {progress_bar(pct)} {pct:.1f}% | {task['status']}"
        )

    await msg.reply_text("\n\n".join(lines))

@app.on_message(filters.command("cancel"))
async def cmd_cancel(client: Client, msg: Message):
    args = msg.command[1:]
    if not args or not args[0].isdigit():
        await msg.reply_text("Usage: /cancel <task_id>")
        return

    tid = int(args[0])
    if tid in active_tasks:
        active_tasks.pop(tid)
        await msg.reply_text(f"Task {tid} cancelled.")
    else:
        await msg.reply_text(f"No task with ID {tid}.")

@app.on_message(filters.command("user"))
async def cmd_user(client: Client, msg: Message):
    settings = load_settings()
    args = msg.command[1:]

    if not args:
        await msg.reply_text(
            f"Current Settings:\n"
            f"Prefix: {settings['prefix']}\n"
            f"Remove Patterns: {', '.join(settings['remove_patterns']) or 'None'}\n"
            f"Token Pickle: {settings['token_pickle_path'] or 'Not set'}\n"
            f"Temp Folder: {settings['temp_folder']}\n\n"
            f"Change with:\n"
            f"/user set prefix [val]\n"
            f"/user set patterns [p1],[p2]\n"
            f"/user set token [path]"
        )
        return

    if args[0] == "set" and len(args) >= 3:
        key = args[1]
        val = " ".join(args[2:])

        if key == "prefix":
            settings["prefix"] = val
            save_settings(settings)
            await msg.reply_text(f"Prefix updated: {val}")

        elif key == "patterns":
            settings["remove_patterns"] = [p.strip() for p in val.split(",")]
            save_settings(settings)
            await msg.reply_text(f"Patterns updated: {settings['remove_patterns']}")

        elif key == "token":
            settings["token_pickle_path"] = val
            save_settings(settings)
            await msg.reply_text(f"Token pickle path set: {val}")

        else:
            await msg.reply_text(f"Unknown setting: {key}")
    else:
        await msg.reply_text("Usage: /user set <key> <value>")

# ─────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    print("Starting AnimeTadka Leech Bot...")
    app.run()
    
