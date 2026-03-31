import os
import time
import math
import shutil
import random
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()
import asyncio
import threading
import httpx
import json
import m3u8
from urllib.parse import urljoin
from flask import Flask
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.enums import MessageEntityType, ParseMode
from contextlib import asynccontextmanager
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials

from cache import init_supabase, get_cached_video, save_to_cache, CACHE_CHANNEL_ID


# =================== Configuration ===================
API_ID = os.environ.get("API_ID")
API_HASH = os.environ.get("API_HASH")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
ALLOWED_CHAT_ID = int(os.environ.get("ALLOWED_CHAT_ID", "-100"))

ADMINS_FILE = "admins.json"


def load_admins():
    """Load admin IDs from file and environment."""
    env_admins = [int(i.strip()) for i in os.environ.get("ADMIN_IDS", "").split(",") if i.strip()]
    dynamic_admins = []
    if os.path.exists(ADMINS_FILE):
        try:
            with open(ADMINS_FILE, "r") as f:
                dynamic_admins = json.load(f)
        except:
            pass
    return env_admins, dynamic_admins

def save_admins(dynamic_admins):
    """Save dynamic admin IDs to file."""
    try:
        with open(ADMINS_FILE, "w") as f:
            json.dump(dynamic_admins, f)
    except Exception as e:
        print(f"Error saving admins: {e}")

SUPER_ADMINS, DYNAMIC_ADMINS = load_admins()

def is_admin(user_id):
    """Check if a user is an admin or super admin."""
    return user_id in SUPER_ADMINS or user_id in DYNAMIC_ADMINS

def is_super_admin(user_id):
    """Check if a user is a fixed super admin from .env."""
    return user_id in SUPER_ADMINS
# =================== Global State ===================
global_semaphore = asyncio.Semaphore(5)
user_active_downloads = {} # {id: bool}
user_download_counts = {} # {id: {'count': int, 'cooldown_until': datetime}}
queue_waitlist = [] # List of message objects or IDs just to track total waiters

# Cancellation tracking
active_downloads = {} # {status_msg_id: subprocess.Process}
task_cancel_flags = {} # {original_msg_id: bool}

app = Client("toydownbot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, max_concurrent_transmissions=1)
flask_app = Flask(__name__)
# =================== Flask ===================
@flask_app.route("/")
def home():
    return "✅ Instagram & YouTube Downloader Bot (Pyrogram) is running!"

@flask_app.route("/status")
def status():
    return "📡 Bot Status: Running"

@flask_app.route("/health", methods=['GET'])
def health_check():
    return "OK", 200

def run_flask():
    port = int(os.environ.get("PORT", 8000))
    try:
        flask_app.run(host="0.0.0.0", port=port)
    except Exception as e:
        print(f"⚠️ Flask could not start on port {port}: {e}")

# =================== Progress Tracking ===================
class DownloadQueue:
    def __init__(self, limit):
        self.semaphore = asyncio.Semaphore(limit)
        self.waitlist = []
        self.user_active = {}
        self.user_stats = {} # {id: {'count': 0, 'cooldown_until': datetime}}

    async def can_start(self, user_id, message):
        """Check if user is allowed to start based on per-user rules."""
        if is_admin(user_id):
            return True # Admins skip all local limits

        now = datetime.now()
        
        # 1. One at a time per user
        if self.user_active.get(user_id):
            await message.reply_text("<emoji id=5210952531676504517>❌</emoji> <b>Access Denied!</b>\n\nYou already have an active download. Please wait until it's finished.", parse_mode=ParseMode.HTML)
            return False

        # 2. Rate limiting (2 downloads -> 10-14m wait)
        stat = self.user_stats.get(user_id, {'count': 0, 'cooldown_until': None})
        if stat['cooldown_until'] and now < stat['cooldown_until']:
            rem = stat['cooldown_until'] - now
            mins, secs = int(rem.total_seconds() // 60), int(rem.total_seconds() % 60)
            await message.reply_text(f"⏳ <b>Cooldown Active!</b>\n\nYou've reached your 2-video limit. Please wait <b>{mins}m {secs}s</b> for your next window.", parse_mode=ParseMode.HTML)
            return False
            
        return True

    @asynccontextmanager
    async def acquire_global(self, user_id, message):
        """Acquire a slot in the global queue."""
        if self.semaphore.locked():
            self.waitlist.append(user_id)
            serial = len(self.waitlist)
            status_msg = await message.reply_text(f"📡 <b>Server Busy! (Queue: #{serial})</b>\n\nGlobal limit of 5 concurrent downloads is reached. Please wait for your turn...", parse_mode=ParseMode.HTML)
            
            async with self.semaphore:
                if user_id in self.waitlist: self.waitlist.remove(user_id)
                await status_msg.edit_text("✅ <b>It's your turn!</b> Initializing download...", parse_mode=ParseMode.HTML)
                self.user_active[user_id] = True
                try:
                    yield
                finally:
                    self.release(user_id)
        else:
            async with self.semaphore:
                self.user_active[user_id] = True
                try:
                    yield
                finally:
                    self.release(user_id)

    def release(self, user_id):
        """Handle end of download tracking."""
        self.user_active[user_id] = False

    def on_success(self, user_id):
        """Record a successful download and handle cooldown."""
        # Admins don't get cooldowns
        if is_admin(user_id):
            return

        stat = self.user_stats.get(user_id, {'count': 0, 'cooldown_until': None})
        stat['count'] += 1
        
        if stat['count'] >= 2:
            stat['count'] = 0
            wait_time = random.randint(10, 14)
            stat['cooldown_until'] = datetime.now() + timedelta(minutes=wait_time)
            
        self.user_stats[user_id] = stat

# Instance
dl_queue = DownloadQueue(5)

def progress_bar(percent):
    done = int(percent / 5)
    return "▓" * done + "░" * (20 - done)

async def upload_progress(current, total, client, message, start_time):
    if total == 0:
        return
    percent = current * 100 / total
    bar = progress_bar(percent)
    elapsed_time = time.time() - start_time
    speed = current / (1024 * 1024 * elapsed_time + 1e-6)

    status_text = f"""
📥 Upload Progress 📥

{bar}

🚧 PC: {percent:.2f}%
⚡️ Speed: {speed:.2f} MB/s
📶 Status: {current / (1024 * 1024):.1f} MB of {total / (1024 * 1024):.1f} MB
"""
    # throttled update to avoid flood limits
    if not hasattr(upload_progress, "last_update"):
        upload_progress.last_update = 0
    
    if time.time() - upload_progress.last_update > 3:
        try:
            await message.edit_text(status_text)
            upload_progress.last_update = time.time()
        except Exception:
            pass

async def get_video_metadata(filepath):
    """Extract width, height, and duration from video file using ffprobe."""
    try:
        cmd = [
            "ffprobe", "-v", "quiet", "-print_format", "json",
            "-show_streams", "-show_format", filepath
        ]
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            return 0, 0, 0
        
        data = json.loads(stdout)
        width = height = duration = 0
        
        for stream in data.get("streams", []):
            if stream.get("codec_type") == "video":
                w = stream.get("width")
                h = stream.get("height")
                if w and h:
                    width = int(w)
                    height = int(h)
                break
                
        duration_val = data.get("format", {}).get("duration")
        if duration_val:
            duration = int(float(duration_val))
        
        return width, height, duration
    except FileNotFoundError:
        print("DEBUG: ffprobe not found in system path.")
        return 0, 0, 0
    except Exception as e:
        print(f"Error in get_video_metadata for {filepath}: {e}")
        return 0, 0, 0

async def get_duration_with_ffprobe(filepath):
    """A direct ffprobe call to get duration as fallback."""
    try:
        cmd = ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", filepath]
        process = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, _ = await process.communicate()
        if stdout:
            return float(stdout.decode().strip())
    except:
        pass
    return 0

async def send_video_with_fallback(client, chat_id, filepath, thumb, caption, duration, width, height, reply_to_id=None, progress=None, progress_args=None):
    """
    Tries to send a video to Telegram, falling back to document if it fails.
    Automatically splits files larger than 2GB (2000MB) into 1800MB parts.
    """
    file_size = os.path.getsize(filepath)
    limit_2gb = 2000 * 1024 * 1024 # 2000MB
    part_size_target = 1800 * 1024 * 1024 # 1800MB

    # Pyrogram requires duration/width/height to be integers (not None) for some backends
    final_duration = int(duration) if duration else 0
    final_width = int(width) if width else 0
    final_height = int(height) if height else 0

    if file_size > limit_2gb:
        print(f"File size {file_size} exceeds 2GB. Attempting to split...")
        try:
            # Ensure we have a valid duration
            v_duration = float(duration) if duration else 0
            if v_duration <= 0:
                v_duration = await get_duration_with_ffprobe(filepath)
                if v_duration <= 0:
                    print("STILL no duration. Trying metadata helper.")
                    _, _, v_duration = await get_video_metadata(filepath)

            if v_duration <= 0:
                print("Splitting Error: No duration found.")
                if progress_args and len(progress_args) >= 2:
                    await progress_args[1].edit_text("<emoji id=5210952531676504517>❌</emoji> <b>Splitting Error:</b> Could not detect video duration.\nTrying whole document...", parse_mode=ParseMode.HTML)
            else:
                num_parts = int(math.ceil(file_size / part_size_target))
                seconds_per_part = v_duration / num_parts
                
                if progress_args and len(progress_args) >= 2:
                    status_msg = progress_args[1]
                    await status_msg.edit_text(f"<emoji id=5271604874419647061>📏</emoji>   <b>File > 2GB!</b> Splitting into {num_parts} parts...", parse_mode=ParseMode.HTML)

                success_count = 0
                sent_parts = []
                ffmpeg_path = shutil.which("ffmpeg") or "ffmpeg"
                
                for i in range(num_parts):
                    start_time = i * seconds_per_part
                    part_filename = f"{os.path.splitext(filepath)[0]}_part{i+1}.mp4"
                    
                    split_cmd = [
                        ffmpeg_path, "-y",
                        "-i", filepath,
                        "-ss", str(start_time),
                        "-t", str(seconds_per_part),
                        "-c", "copy",
                        "-map", "0",
                        "-ignore_unknown",
                        part_filename
                    ]
                    
                    process = await asyncio.create_subprocess_exec(
                        *split_cmd,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE
                    )
                    _, stderr = await process.communicate()
                    
                    if os.path.exists(part_filename) and os.path.getsize(part_filename) > 50000:
                        success_count += 1
                        part_caption = f"{caption}\n\n <emoji id=5213157963023273778>💔</emoji> <b>Part {i+1} of {num_parts}</b>"
                        
                        part_msg = await send_video_with_fallback(
                            client=client, chat_id=chat_id, filepath=part_filename,
                            thumb=thumb, caption=part_caption, duration=int(seconds_per_part),
                            width=final_width, height=final_height, reply_to_id=reply_to_id,
                            progress=progress, progress_args=progress_args
                        )
                        if part_msg:
                            if isinstance(part_msg, list):
                                sent_parts.extend(part_msg)
                            else:
                                sent_parts.append(part_msg)
                                
                        if os.path.exists(part_filename): os.remove(part_filename)
                    else:
                        print(f"Part {i+1} failed: {stderr.decode()[:100]}")

                if success_count > 0:
                    return sent_parts # Successfully uploaded parts
                    
        except Exception as split_err:
            print(f"Fatal error in split logic: {split_err}")
            if progress_args and len(progress_args) >= 2:
                await progress_args[1].edit_text(f"⚠️ <b>Split Crash:</b> `{split_err}`\nRetrying standard...", parse_mode=ParseMode.HTML)

    # Standard upload logic
    try:
        return await client.send_video(
            chat_id=chat_id,
            video=filepath,
            thumb=thumb,
            caption=caption,
            duration=final_duration,
            width=final_width,
            height=final_height,
            supports_streaming=True,
            parse_mode=ParseMode.HTML,
            reply_to_message_id=reply_to_id,
            progress=progress,
            progress_args=progress_args
        )
    except Exception as e:
        print(f"send_video failed: {e}. Falling back to send_document.")
        
        # Update status message if available
        if progress_args and len(progress_args) >= 2:
            try:
                status_msg = progress_args[1]
                await status_msg.edit_text(f"⚠️ <b>Video upload failed!</b> Retrying as document...", parse_mode=ParseMode.HTML)
            except: pass

        try:
            # First try sending document with thumbnail
            return await client.send_document(
                chat_id=chat_id,
                document=filepath,
                thumb=thumb,
                caption=caption,
                parse_mode=ParseMode.HTML,
                reply_to_message_id=reply_to_id,
                progress=progress,
                progress_args=progress_args
            )
        except Exception as doc_error:
            print(f"send_document WITH thumb failed: {doc_error}. Retrying WITHOUT thumb.")
            # Final attempt: Send document without any extra attributes that might cause invalidity
            return await client.send_document(
                chat_id=chat_id,
                document=filepath,
                caption=caption,
                parse_mode=ParseMode.HTML,
                reply_to_message_id=reply_to_id,
                progress=progress,
                progress_args=progress_args
            )

def parse_yt_dlp_progress(line):
    import re
    if "[download]" not in line:
        return None
        
    percent_match = re.search(r"(\d+\.\d+)%", line)
    if not percent_match:
        return None
        
    percent = float(percent_match.group(1))
    size_match = re.search(r"of\s+~?([\d\.\w]+)", line)
    speed_match = re.search(r"at\s+([\d\.\w/s]+)", line)
    eta_match = re.search(r"ETA\s+([\d:]+)", line)
    
    return {
        "percent": percent,
        "size": size_match.group(1) if size_match else "Unknown",
        "speed": speed_match.group(1) if speed_match else "N/A",
        "eta": eta_match.group(1) if eta_match else "N/A"
    }

async def download_with_progress(cmd, message, status_msg):
    if os.path.exists("cookies.txt"):
        cmd.extend(["--cookies", "cookies.txt"])
    
    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    
    # Register for cancellation
    active_downloads[status_msg.id] = process
    
    try:
        last_update_time = 0
        buffer = ""
        
        # Read stdout in chunks to avoid LimitOverrunError (Separator not found)
        while True:
            chunk = await process.stdout.read(4096)
            if not chunk:
                break
                
            buffer += chunk.decode(errors="ignore")
            
            # yt-dlp uses \r for progress updates on the same line
            while "\n" in buffer or "\r" in buffer:
                n_pos = buffer.find("\n")
                r_pos = buffer.find("\r")
                
                if n_pos != -1 and r_pos != -1:
                    pos = min(n_pos, r_pos)
                else:
                    pos = max(n_pos, r_pos)
                    
                line = buffer[:pos]
                buffer = buffer[pos+1:]
                    
                line = line.strip()
                if not line:
                    continue
                    
                progress = parse_yt_dlp_progress(line)
                if progress and time.time() - last_update_time > 4:
                    percent = progress["percent"]
                    bar = progress_bar(percent)
                    
                    size_text = f"📶 <b>Size:</b> {progress['size']}\n" if progress['size'] != "Unknown" else ""
                    
                    status_text = (
                        f"<b>📥 Downloading...</b>\n\n"
                        f"{bar}\n\n"
                        f"🚧 <b>Progress:</b> {percent:.1f}%\n"
                        f"⚡️ <b>Speed:</b> {progress['speed']}\n"
                        f"⏳ <b>ETA:</b> {progress['eta']}\n"
                        f"{size_text}"
                    )
                    try:
                        await status_msg.edit_text(status_text, parse_mode=ParseMode.HTML)
                        last_update_time = time.time()
                    except Exception:
                        pass
    finally:
        # Cleanup
        if status_msg.id in active_downloads:
            del active_downloads[status_msg.id]

    await process.wait()
    return process.returncode, await process.stderr.read()

async def get_bunny_m3u8(url):
    try:
        # Use yt-dlp to get the direct URL
        cmd = ["yt-dlp", "--get-url", "--format", "best", url]
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()
        if process.returncode == 0:
            return stdout.decode().strip()
        else:
            return None
    except Exception:
        return None

# =================== YouTube Uploader Logic ===================
SCOPES = [
    'https://www.googleapis.com/auth/youtube.upload',
    'https://www.googleapis.com/auth/youtube.readonly'
]

def get_youtube_service():
    creds = None
    
    # 1. Check if token exists in environment variable (Prioritize this for GitHub/Prod)
    token_env = os.environ.get('YOUTUBE_TOKEN_JSON')
    if token_env:
        try:
            creds_data = json.loads(token_env)
            creds = Credentials.from_authorized_user_info(creds_data, SCOPES)
            print("✅ YouTube Token loaded from ENV.")
        except Exception as e:
            print(f"❌ Error loading YOUTUBE_TOKEN_JSON from ENV: {e}")

    # 2. Fallback to file storage if not in ENV
    if not creds and os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
        print("📁 YouTube Token loaded from token.json file.")

    # 3. Handle login if no credentials
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            # Check for client secrets in ENV or File
            client_secrets_env = os.environ.get('GOOGLE_CLIENT_SECRETS_JSON')
            if client_secrets_env:
                client_config = json.loads(client_secrets_env)
                flow = InstalledAppFlow.from_client_config(client_config, SCOPES)
            elif os.path.exists('client_secrets.json'):
                flow = InstalledAppFlow.from_client_secrets_file('client_secrets.json', SCOPES)
            else:
                raise FileNotFoundError("GOOGLE_CLIENT_SECRETS_JSON not found in ENV or client_secrets.json not found.")

            # Headless login for VPS
            creds = flow.run_local_server(port=0, open_browser=False)
            
        # 4. Save credentials (only to file if we don't have it in ENV)
        if not os.environ.get('YOUTUBE_TOKEN_JSON'):
            with open('token.json', 'w') as token_file:
                token_file.write(creds.to_json())
            print("💾 New credentials saved to token.json. Copy this into your .env as YOUTUBE_TOKEN_JSON!")

    return build('youtube', 'v3', credentials=creds)

def upload_to_youtube(file_path, title, description, category_id="27", privacy_status="unlisted"):
    """
    Uploads a video to YouTube. (Synchronous)
    category_id "27" is Education.
    """
    youtube = get_youtube_service()

    # Get channel info to show user where it's being uploaded
    channel_response = youtube.channels().list(mine=True, part='snippet').execute()
    channel_title = "Unknown"
    if channel_response.get('items'):
        channel_title = channel_response['items'][0]['snippet']['title']

    body = {
        'snippet': {
            'title': title,
            'description': description,
            'categoryId': category_id
        },
        'status': {
            'privacyStatus': privacy_status,
            'selfDeclaredMadeForKids': False
        }
    }

    # Call the API's videos().insert method to create and upload the video.
    insert_request = youtube.videos().insert(
        part=','.join(body.keys()),
        body=body,
        media_body=MediaFileUpload(file_path, chunksize=-1, resumable=True)
    )

    response = None
    while response is None:
        status, response = insert_request.next_chunk()
        if status:
            print(f"Uploaded {int(status.progress() * 100)}%")

    video_id = response.get('id')
    yt_link = f"https://www.youtube.com/watch?v={video_id}"
    
    return yt_link, channel_title

# =================== Cache Helpers ===================
async def check_and_serve_cache(client, message: Message, url: str, status_msg: Message) -> bool:
    cached = get_cached_video(url)
    if cached:
        user_name = message.from_user.first_name or "User"
        
        # Multi-part cached video (list of entries)
        if isinstance(cached, list):
            title = cached[0].get('title', 'Video')
            total_parts = len(cached)
            rich_caption = (
                f"<emoji id=5463107823946717464>🎬</emoji> <b>Title:</b> <code>{title}</code>\n"
                f"<emoji id=5251203410396458957>👤</emoji> <b>Fetched by:</b> <a href='tg://user?id={message.from_user.id}'>{user_name}</a>"
            )
            try:
                for i, part in enumerate(cached, 1):
                    part_caption = rich_caption + f"\n\n <emoji id=5213157963023273778>💔</emoji> <b>Part {i} of {total_parts}</b>"
                    fid = part.get("file_id")
                    ftype = part.get("file_type", "video")
                    if fid:
                        if ftype == "document":
                            await client.send_document(chat_id=message.chat.id, document=fid, caption=part_caption, parse_mode=ParseMode.HTML, reply_to_message_id=message.id)
                        else:
                            await client.send_video(chat_id=message.chat.id, video=fid, caption=part_caption, parse_mode=ParseMode.HTML, reply_to_message_id=message.id)
                await status_msg.delete()
                dl_queue.on_success(message.from_user.id)
                return True
            except Exception as e:
                print(f"Cache send fail (multi-part): {e}")
        else:
            # Single video cached
            title = cached.get('title', 'Video')
            rich_caption = (
                f"<emoji id=5463107823946717464>🎬</emoji> <b>Title:</b> <code>{title}</code>\n"
                f"<emoji id=5251203410396458957>👤</emoji> <b>Fetched by:</b> <a href='tg://user?id={message.from_user.id}'>{user_name}</a>"
            )
            try:
                if cached.get("file_type") == "document":
                    await client.send_document(chat_id=message.chat.id, document=cached["file_id"], caption=rich_caption, parse_mode=ParseMode.HTML, reply_to_message_id=message.id)
                else:
                    await client.send_video(chat_id=message.chat.id, video=cached["file_id"], caption=rich_caption, parse_mode=ParseMode.HTML, reply_to_message_id=message.id)
                await status_msg.delete()
                dl_queue.on_success(message.from_user.id)
                return True
            except Exception as e:
                print(f"Cache send fail: {e}")
    return False

async def cached_upload(client, chat_id, url, filename, thumb_name, title, rich_caption, duration, width, height, message, status_msg, start_upload, command_type):
    upload_chat_id = CACHE_CHANNEL_ID if CACHE_CHANNEL_ID != 0 else chat_id
    
    sent_msgs = await send_video_with_fallback(
        client=client,
        chat_id=upload_chat_id,
        filepath=filename,
        thumb=thumb_name,
        caption=rich_caption if upload_chat_id == chat_id else f"📦 Cache: {title}",
        duration=duration,
        width=width,
        height=height,
        reply_to_id=None if upload_chat_id == CACHE_CHANNEL_ID else message.id,
        progress=upload_progress,
        progress_args=(client, status_msg, start_upload)
    )
    
    if upload_chat_id == CACHE_CHANNEL_ID and sent_msgs:
        if not isinstance(sent_msgs, list):
            sent_msgs = [sent_msgs]
        
        # Filter out None entries
        sent_msgs = [m for m in sent_msgs if m]
        total_parts = len(sent_msgs)
            
        if total_parts == 1:
            sent_msg = sent_msgs[0]
            fid = None
            ftype = "video"
            if sent_msg.video:
                fid = sent_msg.video.file_id
            elif sent_msg.document:
                fid = sent_msg.document.file_id
                ftype = "document"
                
            if fid:
                save_to_cache(url, fid, ftype, title, duration, width, height, os.path.getsize(filename), CACHE_CHANNEL_ID, sent_msg.id, command_type, part_number=1, total_parts=1)
                
            # Send to user
            try:
                if ftype == "video":
                    await client.send_video(chat_id=chat_id, video=fid, caption=rich_caption, parse_mode=ParseMode.HTML, reply_to_message_id=message.id)
                else:
                    await client.send_document(chat_id=chat_id, document=fid, caption=rich_caption, parse_mode=ParseMode.HTML, reply_to_message_id=message.id)
            except Exception as e:
                print(f"Failed to forward cached video: {e}")
        else:
            # Multiple parts: save ALL parts to DB, then forward to user
            for idx, m in enumerate(sent_msgs, 1):
                fid = None
                ftype = "video"
                if m.video:
                    fid = m.video.file_id
                elif m.document:
                    fid = m.document.file_id
                    ftype = "document"
                    
                if fid:
                    # Save each part to cache DB
                    save_to_cache(
                        source_url=url,
                        file_id=fid,
                        file_type=ftype,
                        title=title,
                        duration=duration,
                        width=width,
                        height=height,
                        file_size=0,  # individual part size not easily available
                        cache_chat_id=CACHE_CHANNEL_ID,
                        cache_message_id=m.id,
                        command_type=command_type,
                        part_number=idx,
                        total_parts=total_parts
                    )
                    
                    # Forward to user
                    try:
                        part_caption = rich_caption + f"\n\n <emoji id=5213157963023273778>💔</emoji> <b>Part {idx} of {total_parts}</b>"
                        if ftype == "video":
                            await client.send_video(chat_id=chat_id, video=fid, caption=part_caption, parse_mode=ParseMode.HTML, reply_to_message_id=message.id)
                        else:
                            await client.send_document(chat_id=chat_id, document=fid, caption=part_caption, parse_mode=ParseMode.HTML, reply_to_message_id=message.id)
                    except Exception as e:
                        print(f"Failed to forward multi-part video to user: {e}")

# =================== Handlers ===================
@app.on_message(filters.command("start"))
async def start_handler(client, message: Message):
    print(f"DEBUG: Received /start from user: {message.from_user.id} in chat: {message.chat.id}")
    if message.chat.id != ALLOWED_CHAT_ID and not is_admin(message.from_user.id):
        print("DEBUG: Access Denied sending reply...")
        try:
            await message.reply_text(
                "<emoji id=5210952531676504517>❌</emoji> <b>Access Denied!</b>\n\nThis bot only works in the authorized group.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔗 Join Authorized Group", url="https://t.me/navigatesupport")]])
            )
            print("DEBUG: Reply sent successfully!")
        except Exception as e:
            print(f"DEBUG: Error sending reply: {e}")
        return
    user_id = message.from_user.id
    print(f"DEBUG: User {user_id} is recognized as Admin or in allowed chat.")
    bot_info = await client.get_me()

    bot_name = bot_info.first_name
    welcome_text = (
        f"<emoji id=5220195537520711716>⚡️</emoji> <b>Welcome to {bot_name}!</b>\n\n"
        f"I'm your ultimate companion for high-speed, high-quality video downloads. <emoji id=5217880283860194582>🚀</emoji> Whatever you need, I capture it with precision! <emoji id=5222044641200720562>🌸</emoji>\n\n"
        f"<b>Available Services:</b>\n"
        f" <emoji id=5206607081334906820>✔️</emoji> <b>RM Downloader:</b> <code>/rm [link]</code>\n"
        f" <emoji id=5206607081334906820>✔️</emoji> <b>RM JSON:</b> <code>/rmd (reply to JSON)</code>\n"
        f" <emoji id=5206607081334906820>✔️</emoji> <b>Full Website:</b> <code>/rmall (reply to ALL JSON)</code>\n"
        f" <emoji id=5206607081334906820>✔️</emoji> <b>Shikho:</b> <code>/shikho [link]</code>\n"
        f" <emoji id=5206607081334906820>✔️</emoji> <b>Udvash:</b> <code>/udvash [link]</code>\n"
        f" <emoji id=5206607081334906820>✔️</emoji> <b>Hulkstain Downloader:</b> <code>/hk [link]</code>\n"
        f" <emoji id=5206607081334906820>✔️</emoji> <b>Biology Adda:</b> <code>/ba [link]</code>\n"
        f" <emoji id=5206607081334906820>✔️</emoji> <b>AFS Downloader:</b> <code>/afs [link]</code>\n"
        f" <emoji id=5206607081334906820>✔️</emoji> <b>Facebook:</b> <code>/fb [link]</code>\n"
        f" <emoji id=5206607081334906820>✔️</emoji> <b>Instagram:</b> <code>/ig [link]</code>\n"
        f" <emoji id=5206607081334906820>✔️</emoji> <b>TikTok:</b> <code>/tik [link]</code>\n"
        f" <emoji id=5206607081334906820>✔️</emoji> <b>RM to YouTube:</b> <code>/rmu [link]</code>\n"
        f" <emoji id=5206607081334906820>✔️</emoji> <b>Bulk YouTube:</b> <code>/rmallu (reply to JSON)</code>\n"
        f" <emoji id=5206607081334906820>✔️</emoji> <b>YouTube Upload:</b> <code>/up (reply to video)</code>\n\n"
        f"<i>Just send me a link and let the magic happen!</i> <emoji id=5220166546491459639>🔥</emoji>"
    )
    await message.reply_text(welcome_text, parse_mode=ParseMode.HTML)

@app.on_message(filters.command("getid"))
async def get_id_handler(client, message: Message):
    if not is_admin(message.from_user.id):
        return
    await message.reply_text(f"ID is: <code>{message.chat.id}</code>", parse_mode=ParseMode.HTML)

@app.on_message(filters.command("afs"))
async def afs_link_handler(client, message: Message):
    if message.chat.id != ALLOWED_CHAT_ID and not is_admin(message.from_user.id):
        await message.reply_text(
            "<emoji id=5210952531676504517>❌</emoji> <b>Access Denied!</b>\n\nThis bot only works in the authorized group.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔗 Join Authorized Group", url="https://t.me/navigatesupport")]])
        )
        return
    user_id = message.from_user.id
    
    # Queue Check
    if not await dl_queue.can_start(user_id, message): return

    async with dl_queue.acquire_global(user_id, message):
        parts = message.text.split()
    url = None
    referer = "https://iframe.mediadelivery.net"
    
    if len(parts) >= 2:
        url = parts[1]
        
    if not url and message.reply_to_message and message.reply_to_message.text:
        url = message.reply_to_message.text.strip()
        
    if not url:
        await message.reply_text("<emoji id=5274099962655816924>❗</emoji> Please provide an AFS URL.\nUsage: /afs <URL>", parse_mode=ParseMode.HTML)
        return

    # URL Validation
    allowed_domains = ["iframe.mediadelivery.net"]
    if not any(domain in url for domain in allowed_domains):
        await message.reply_text(
            "<emoji id=5274099962655816924>❌</emoji> <b>Invalid URL!</b>\n\nOnly AFS URLs are allowed for this command.",
            parse_mode=ParseMode.HTML
        )
        return

    status_msg = await message.reply_text("<emoji id=5231012545799666522>🔍</emoji> Processing AFS video...", parse_mode=ParseMode.HTML)

    if await check_and_serve_cache(client, message, url, status_msg):
        return

    filename = f"afs_video_{user_id}_{int(time.time())}.mp4"
    thumb_name = None
    title = "AFS Video"
    
    try:
        # Fetch metadata using yt-dlp
        metadata_cmd = [
            "yt-dlp",
            "--dump-json",
            "--referer", referer,
            "--user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "--add-header", "Origin: https://iframe.mediadelivery.net",
            "--add-header", "Accept: */*",
            "--add-header", "Accept-Language: en-US,en;q=0.9",
            "--add-header", "Sec-Fetch-Site: cross-site",
            "--add-header", "Sec-Fetch-Mode: cors",
            "--no-check-certificate",
            url
        ]
        
        process_meta = await asyncio.create_subprocess_exec(
            *metadata_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout_meta, stderr_meta = await process_meta.communicate()
        
        if process_meta.returncode == 0:
            try:
                metadata = json.loads(stdout_meta.decode())
                title = metadata.get("title", "AFS Video")
                thumbnail_url = metadata.get("thumbnail")
                
                if thumbnail_url:
                    thumb_name = f"afs_thumb_{user_id}_{int(time.time())}.jpg"
                    async with httpx.AsyncClient(timeout=20) as client_dl:
                        r_thumb = await client_dl.get(thumbnail_url)
                        if r_thumb.status_code == 200:
                            with open(thumb_name, "wb") as f:
                                f.write(r_thumb.content)
                        else:
                            thumb_name = None
            except Exception:
                pass

        # Construct yt-dlp command with dedicated referer and user-agent flags
        cmd = [
            "yt-dlp",
            "-f", "bestvideo[height<=1080]+bestaudio/best[height<=1080]/best",
            "-o", filename,
            "--no-playlist",
            "--merge-output-format", "mp4",
            "--referer", referer,
            "--user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "--add-header", "Origin: https://iframe.mediadelivery.net",
            "--add-header", "Accept: */*",
            "--add-header", "Accept-Language: en-US,en;q=0.9",
            "--add-header", "Sec-Fetch-Site: cross-site",
            "--add-header", "Sec-Fetch-Mode: cors",
            "--no-check-certificate",
            "--downloader-args", "ffmpeg:-allowed_segment_extensions ALL",
            "--concurrent-fragments", "10"
        ]
        cmd.append(url)
        
        await status_msg.edit_text("<emoji id=5429381339851796035>✅</emoji> Found! Downloading to server...", parse_mode=ParseMode.HTML)
        
        returncode, stderr = await download_with_progress(cmd, message, status_msg)
        
        if returncode != 0 or not os.path.exists(filename):
            await status_msg.edit_text(f"<emoji id=5274099962655816924>❌</emoji> <b>Download failed!</b>\n\nThe video might be restricted or inaccessible.", parse_mode=ParseMode.HTML)
            return

        await status_msg.edit_text("<emoji id=5449683594425410231>📤</emoji> Uploading to Telegram...", parse_mode=ParseMode.HTML)
        
        width, height, duration = await get_video_metadata(filename)
        user_name = message.from_user.first_name or message.from_user.username or "User"
        rich_caption = (
            f"<emoji id=5463107823946717464>🎬</emoji> <b>Title:</b> <code>{title}</code>\n"
            f"<emoji id=5251203410396458957>👤</emoji> <b>Downloaded by:</b> <a href='tg://user?id={user_id}'>{user_name}</a>"
        )

        start_upload = time.time()
        await cached_upload(
            client=client,
            chat_id=message.chat.id,
            url=url,
            filename=filename,
            thumb_name=thumb_name,
            title=title,
            rich_caption=rich_caption,
            duration=duration,
            width=width,
            height=height,
            message=message,
            status_msg=status_msg,
            start_upload=start_upload,
            command_type="afs"
        )
        dl_queue.on_success(user_id)
        await status_msg.delete()

    except Exception as e:
        await status_msg.edit_text(f"<emoji id=5274099962655816924>⚠️</emoji> An error occurred.\n\nError: `{e}`", parse_mode=ParseMode.HTML)
    finally:

        if os.path.exists(filename):
            os.remove(filename)

@app.on_message(filters.command("ba"))
async def ba_link_handler(client, message: Message):
    if message.chat.id != ALLOWED_CHAT_ID and not is_admin(message.from_user.id):
        await message.reply_text(
            "<emoji id=5210952531676504517>❌</emoji> <b>Access Denied!</b>\n\nThis bot only works in the authorized group.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔗 Join Authorized Group", url="https://t.me/navigatesupport")]])
        )
        return
    user_id = message.from_user.id

    # Queue Check
    if not await dl_queue.can_start(user_id, message): return

    async with dl_queue.acquire_global(user_id, message):
        parts = message.text.split()
    player_url = None
    
    if len(parts) >= 2: player_url = parts[1]
    elif len(parts) == 1 and parts[0].startswith("http"): player_url = parts[0]
    elif message.reply_to_message and message.reply_to_message.text:
        player_url = message.reply_to_message.text.strip()
        
    if not player_url:
        await message.reply_text(
            "<emoji id=5274099962655816924>❗</emoji> <b>Please provide a Biology Adda URL.</b>\n\n"
            "Usage:\n"
            "• <code>/ba https://player.vidinfra.com/stream/.../...</code>\n"
            "• <code>/ba https://biology-adda.tenbytecdn.com/{id}/playlist.m3u8</code>\n\n"
            "<i>💡 To get the direct m3u8 link:\nOpen video in browser → F12 → Network tab → filter 'playlist.m3u8' → copy URL</i>",
            parse_mode=ParseMode.HTML
        )
        return

    import re as _re
    real_m3u8 = None

    # Case 1: User directly provided a tenbytecdn.com or m3u8 URL
    if "tenbytecdn.com" in player_url or player_url.endswith(".m3u8"):
        real_m3u8 = player_url.split("?")[0]  # Strip query params

    # Case 2: Vidinfra player URL - try to construct m3u8 URL (may not always work)
    elif "vidinfra.com" in player_url:
        await message.reply_text(
            "⚠️ <b>Vidinfra links cannot be downloaded directly.</b>\n\n"
            "Please get the direct <b>m3u8 link</b> instead:\n"
            "1️⃣ Open the video in your browser\n"
            "2️⃣ Press <b>F12</b> → Network tab\n"
            "3️⃣ Play the video, then filter by <code>playlist.m3u8</code>\n"
            "4️⃣ Copy the URL (starts with <code>https://biology-adda.tenbytecdn.com/...</code>)\n"
            "5️⃣ Send: <code>/ba [copied URL]</code>",
            parse_mode=ParseMode.HTML
        )
        return
    else:
        await message.reply_text("<emoji id=5274099962655816924>❌</emoji> Unrecognized URL. Please provide a Biology Adda m3u8 link.")
        return

    status_msg = await message.reply_text("<emoji id=5231012545799666522>🔍</emoji> <b>Stream found! Downloading...</b>", parse_mode=ParseMode.HTML)
    
    if await check_and_serve_cache(client, message, real_m3u8, status_msg):
        return
        
    filename = f"ba_video_{user_id}_{int(time.time())}.mp4"
    title = "Biology Adda Video"
    
    try:
        cmd = [
            "yt-dlp",
            "-f", "best[height<=1080]/best",
            "-o", filename,
            "--no-playlist",
            "--merge-output-format", "mp4",
            "--referer", "https://player.vidinfra.com/",
            "--user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "--add-header", "Referer: https://player.vidinfra.com/",
            "--add-header", "Origin: https://player.vidinfra.com",
            "--add-header", "Accept: */*",
            "--add-header", "Accept-Language: en-US,en;q=0.9",
            "--no-check-certificate",
            "--downloader-args", "ffmpeg:-allowed_segment_extensions ALL",
            "--concurrent-fragments", "20",
            "--buffer-size", "1M",
            real_m3u8
        ]
        
        returncode, stderr = await download_with_progress(cmd, message, status_msg)
        
        if returncode != 0 or not os.path.exists(filename):
            err = stderr.decode(errors="ignore")[:300] if stderr else "No details."
            await status_msg.edit_text(
                f"<emoji id=5274099962655816924>❌</emoji> <b>Download failed!</b>\n<code>{err}</code>",
                parse_mode=ParseMode.HTML
            )
            return

        await status_msg.edit_text("<emoji id=5449683594425410231>📤</emoji> Uploading to Telegram...", parse_mode=ParseMode.HTML)
        
        width, height, duration = await get_video_metadata(filename)
        user_name = message.from_user.first_name or "User"
        rich_caption = (
            f"<emoji id=5463107823946717464>🎬</emoji> <b>Title:</b> <code>{title}</code>\n"
            f"<emoji id=5251203410396458957>👤</emoji> <b>By:</b> <a href='tg://user?id={user_id}'>{user_name}</a>"
        )

        start_upload = time.time()
        await cached_upload(
            client=client,
            chat_id=message.chat.id,
            url=real_m3u8,
            filename=filename,
            thumb_name=None,
            title=title,
            rich_caption=rich_caption,
            duration=duration,
            width=width,
            height=height,
            message=message,
            status_msg=status_msg,
            start_upload=start_upload,
            command_type="ba"
        )
        dl_queue.on_success(user_id)
        await status_msg.delete()

    except Exception as e:
        await status_msg.edit_text(f"<emoji id=5274099962655816924>⚠️</emoji> Error: `{e}`")
    finally:
        if os.path.exists(filename): os.remove(filename)

@app.on_message(filters.command("rm"))
async def rm_link_handler(client, message: Message):
    if message.chat.id != ALLOWED_CHAT_ID and not is_admin(message.from_user.id):
        await message.reply_text(
            "<emoji id=5210952531676504517>❌</emoji> <b>Access Denied!</b>\n\nThis bot only works in the authorized group.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔗 Join Authorized Group", url="https://t.me/navigatesupport")]])
        )
        return
    user_id = message.from_user.id

    # Queue Check
    if not await dl_queue.can_start(user_id, message): return

    async with dl_queue.acquire_global(user_id, message):
        parts = message.text.split()
    url = getattr(message, "auto_url", None)
    referer = "https://iframe.mediadelivery.net"
    
    if not url:
        if len(parts) >= 2:
            url = parts[1]
        elif len(parts) == 1 and parts[0].startswith("http"):
            url = parts[0]
        elif message.reply_to_message and message.reply_to_message.text:
            url = message.reply_to_message.text.strip()
        
    if not url:
        await message.reply_text("<emoji id=5274099962655816924>❗</emoji> Please provide an RM URL.\nUsage: /rm <URL>", parse_mode=ParseMode.HTML)
        return

    # URL Validation
    allowed_domains = ["iframe.mediadelivery.net"]
    if not any(domain in url for domain in allowed_domains):
        await message.reply_text(
            "<emoji id=5274099962655816924>❌</emoji> <b>Invalid URL!</b>\n\nOnly valid URLs are allowed for this command.",
            parse_mode=ParseMode.HTML
        )
        return

    status_msg = await message.reply_text("<emoji id=5231012545799666522>🔍</emoji> Processing RM video...", parse_mode=ParseMode.HTML)

    if await check_and_serve_cache(client, message, url, status_msg):
        return

    filename = f"rm_video_{user_id}_{int(time.time())}.mp4"
    thumb_name = None
    title = "RM Video"
    
    try:
        # Fetch metadata using yt-dlp
        metadata_cmd = [
            "yt-dlp",
            "--dump-json",
            "--referer", referer,
            "--user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "--add-header", "Origin: https://iframe.mediadelivery.net",
            "--add-header", "Accept: */*",
            "--add-header", "Accept-Language: en-US,en;q=0.9",
            "--add-header", "Sec-Fetch-Site: cross-site",
            "--add-header", "Sec-Fetch-Mode: cors",
            "--no-check-certificate",
            url
        ]
        
        process_meta = await asyncio.create_subprocess_exec(
            *metadata_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout_meta, stderr_meta = await process_meta.communicate()
        
        if process_meta.returncode == 0:
            try:
                metadata = json.loads(stdout_meta.decode())
                title = metadata.get("title", "RM Video")
                thumbnail_url = metadata.get("thumbnail")
                
                if thumbnail_url:
                    thumb_name = f"rm_thumb_{user_id}_{int(time.time())}.jpg"
                    async with httpx.AsyncClient(timeout=20) as client_dl:
                        r_thumb = await client_dl.get(thumbnail_url)
                        if r_thumb.status_code == 200:
                            with open(thumb_name, "wb") as f:
                                f.write(r_thumb.content)
                        else:
                            thumb_name = None
            except Exception:
                pass

        # Construct yt-dlp command with dedicated referer and user-agent flags
        cmd = [
            "yt-dlp",
            "-f", "bestvideo[height<=1080]+bestaudio/best[height<=1080]/best",
            "-o", filename,
            "--no-playlist",
            "--merge-output-format", "mp4",
            "--referer", referer,
            "--user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "--add-header", "Origin: https://iframe.mediadelivery.net",
            "--add-header", "Accept: */*",
            "--add-header", "Accept-Language: en-US,en;q=0.9",
            "--add-header", "Sec-Fetch-Site: cross-site",
            "--add-header", "Sec-Fetch-Mode: cors",
            "--no-check-certificate",
            "--downloader-args", "ffmpeg:-allowed_segment_extensions ALL",
            "--concurrent-fragments", "10"
        ]
        cmd.append(url)
        
        await status_msg.edit_text("<emoji id=5429381339851796035>✅</emoji> Found! Downloading to server...", parse_mode=ParseMode.HTML)
        
        returncode, stderr = await download_with_progress(cmd, message, status_msg)
        
        if returncode != 0 or not os.path.exists(filename):
            await status_msg.edit_text(f"<emoji id=5274099962655816924>❌</emoji> <b>Download failed!</b>\n\nThe video might be restricted or inaccessible.", parse_mode=ParseMode.HTML)
            return

        await status_msg.edit_text("<emoji id=5449683594425410231>📤</emoji> Uploading to Telegram...", parse_mode=ParseMode.HTML)
        
        width, height, duration = await get_video_metadata(filename)
        user_name = message.from_user.first_name or message.from_user.username or "User"
        rich_caption = (
            f"<emoji id=5463107823946717464>🎬</emoji> <b>Title:</b> <code>{title}</code>\n"
            f"<emoji id=5251203410396458957>👤</emoji> <b>Downloaded by:</b> <a href='tg://user?id={user_id}'>{user_name}</a>"
        )

        start_upload = time.time()
        await cached_upload(
            client=client,
            chat_id=message.chat.id,
            url=url,
            filename=filename,
            thumb_name=thumb_name,
            title=title,
            rich_caption=rich_caption,
            duration=duration,
            width=width,
            height=height,
            message=message,
            status_msg=status_msg,
            start_upload=start_upload,
            command_type="rm"
        )
        dl_queue.on_success(user_id)
        await status_msg.delete()

    except Exception:
        await status_msg.edit_text(f"<emoji id=5274099962655816924>⚠️</emoji> <b>A processing error occurred.</b>", parse_mode=ParseMode.HTML)
        if os.path.exists(filename):
            os.remove(filename)

@app.on_message(filters.command("rmu"))
async def rmu_link_handler(client, message: Message):
    if not is_admin(message.from_user.id):
        await message.reply_text(
            "<emoji id=5210952531676504517>❌</emoji> <b>Admin Only Command!</b>\n\nOnly administrators can use this command.",
            parse_mode=ParseMode.HTML
        )
        return
    user_id = message.from_user.id
    
    # Queue Check
    if not await dl_queue.can_start(user_id, message): return

    async with dl_queue.acquire_global(user_id, message):
        parts = message.text.split()
    url = None
    referer = "https://iframe.mediadelivery.net"
    
    if len(parts) >= 2:
        url = parts[1]
    if not url and message.reply_to_message and message.reply_to_message.text:
        url = message.reply_to_message.text.strip()
        
    if not url:
        await message.reply_text("<emoji id=5274099962655816924>❗</emoji> Please provide an RM URL.\nUsage: /rmu <URL>", parse_mode=ParseMode.HTML)
        return

    # URL Validation
    allowed_domains = ["iframe.mediadelivery.net"]
    if not any(domain in url for domain in allowed_domains):
        await message.reply_text("<emoji id=5274099962655816924>❌</emoji> <b>Invalid URL!</b>", parse_mode=ParseMode.HTML)
        return

    status_msg = await message.reply_text("<emoji id=5231012545799666522>🔍</emoji> Processing RM video for YouTube...", parse_mode=ParseMode.HTML)
    filename = f"rmu_video_{user_id}_{int(time.time())}.mp4"
    title = "RM YouTube Video"
    
    try:
        # Fetch metadata
        metadata_cmd = ["yt-dlp", "--dump-json", "--referer", referer, "--no-check-certificate", url]
        process_meta = await asyncio.create_subprocess_exec(*metadata_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout_meta, _ = await process_meta.communicate()
        if process_meta.returncode == 0:
            try:
                metadata = json.loads(stdout_meta.decode())
                title = metadata.get("title", "RM Video")
            except: pass

        # Download
        cmd = [
            "yt-dlp",
            "-f", "bestvideo[height<=1080]+bestaudio/best[height<=1080]/best",
            "-o", filename,
            "--no-playlist",
            "--merge-output-format", "mp4",
            "--referer", referer,
            "--user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "--no-check-certificate",
            "--downloader-args", "ffmpeg:-allowed_segment_extensions ALL",
            "--concurrent-fragments", "10"
        ]
        cmd.append(url)
        
        await status_msg.edit_text("<emoji id=5429381339851796035>✅</emoji> Found! Downloading for YouTube upload...", parse_mode=ParseMode.HTML)
        returncode, stderr = await download_with_progress(cmd, message, status_msg)
        
        if returncode != 0 or not os.path.exists(filename):
            await status_msg.edit_text(" <emoji id=5210952531676504517>❌</emoji> <b>Download failed.</b>", parse_mode=ParseMode.HTML)
            return

        await status_msg.edit_text(" <emoji id=5217880283860194582>🚀</emoji> <b>Uploading to YouTube... (Unlisted)</b>", parse_mode=ParseMode.HTML)
        
        description = f"RM Video Uploaded via Telegram Bot by {message.from_user.first_name}"
        
        # Run upload in thread
        yt_link, channel_name = await asyncio.to_thread(upload_to_youtube, filename, title, description)
        
        await status_msg.edit_text(
            f"<emoji id=5429381339851796035>✅</emoji> <b>Successfully Uploaded to YouTube!</b>\n\n"
            f" <emoji id=5260291556899831755>🎬</emoji> <b>Channel:</b> <code>{channel_name}</code>\n"
            f" <emoji id=5463107823946717464>🎬</emoji> <b>Title:</b> <code>{title}</code>\n"
            f" <emoji id=5271604874419647061>🔗</emoji> <b>Link:</b> {yt_link}\n"
            f" <emoji id=5210956306952758910>👀</emoji> <b>Visibility:</b> Unlisted",
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True
        )
        dl_queue.on_success(user_id)

    except Exception as e:
        await status_msg.edit_text(f"⚠️ Error: `{e}`")
    finally:
        if os.path.exists(filename): os.remove(filename)

@app.on_message(filters.command("shikho"))
async def shikho_link_handler(client, message: Message):
    if message.chat.id != ALLOWED_CHAT_ID and not is_admin(message.from_user.id):
        await message.reply_text(
            "<emoji id=5210952531676504517>❌</emoji> <b>Access Denied!</b>\n\nThis bot only works in the authorized group.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔗 Join Authorized Group", url="https://t.me/navigatesupport")]])
        )
        return
    user_id = message.from_user.id

    # Queue Check
    if not await dl_queue.can_start(user_id, message): return

    async with dl_queue.acquire_global(user_id, message):
        parts = message.text.split()
    url = None
    referer = "https://app.shikho.com/"
    
    if len(parts) >= 2:
        url = parts[1]
    elif len(parts) == 1 and parts[0].startswith("http"):
        url = parts[0]
        
    if not url and message.reply_to_message and message.reply_to_message.text:
        url = message.reply_to_message.text.strip()
        
    if not url:
        await message.reply_text("<emoji id=5274099962655816924>❗</emoji> Please provide a Shikho URL.\nUsage: /shikho <URL>", parse_mode=ParseMode.HTML)
        return

    # URL Validation
    allowed_domains = ["tenbytecdn.com", "shikho.com"]
    if not any(domain in url for domain in allowed_domains):
        await message.reply_text(
            "<emoji id=5274099962655816924>❌</emoji> <b>Invalid URL!</b>\n\nOnly valid URLs are allowed for this command.",
            parse_mode=ParseMode.HTML
        )
        return

    status_msg = await message.reply_text("<emoji id=5231012545799666522>🔍</emoji> Processing Shikho video...", parse_mode=ParseMode.HTML)

    if await check_and_serve_cache(client, message, url, status_msg):
        return

    filename = f"shikho_video_{user_id}_{int(time.time())}.mp4"
    thumb_name = None
    title = "Shikho Video"
    
    try:
        # Fetch metadata using yt-dlp
        metadata_cmd = [
            "yt-dlp",
            "--dump-json",
            "--referer", referer,
            "--user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "--add-header", "Origin: https://app.shikho.com",
            "--add-header", "Accept: */*",
            "--add-header", "Accept-Language: en-US,en;q=0.9",
            "--add-header", "Sec-Fetch-Site: cross-site",
            "--add-header", "Sec-Fetch-Mode: cors",
            "--no-check-certificate",
            url
        ]
        
        process_meta = await asyncio.create_subprocess_exec(
            *metadata_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout_meta, stderr_meta = await process_meta.communicate()
        
        if process_meta.returncode == 0:
            try:
                metadata = json.loads(stdout_meta.decode())
                title = metadata.get("title", "Shikho Video")
                thumbnail_url = metadata.get("thumbnail")
                
                if thumbnail_url:
                    thumb_name = f"shikho_thumb_{user_id}_{int(time.time())}.jpg"
                    async with httpx.AsyncClient(timeout=20) as client_dl:
                        r_thumb = await client_dl.get(thumbnail_url)
                        if r_thumb.status_code == 200:
                            with open(thumb_name, "wb") as f:
                                f.write(r_thumb.content)
                        else:
                            thumb_name = None
            except Exception:
                pass

        # Construct yt-dlp command with 720p quality
        cmd = [
            "yt-dlp",
            "-f", "bestvideo[height<=1080]+bestaudio/best[height<=1080]/best",
            "-o", filename,
            "--no-playlist",
            "--merge-output-format", "mp4",
            "--referer", referer,
            "--user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "--add-header", "Origin: https://app.shikho.com",
            "--add-header", "Accept: */*",
            "--add-header", "Accept-Language: en-US,en;q=0.9",
            "--add-header", "Sec-Fetch-Site: cross-site",
            "--add-header", "Sec-Fetch-Mode: cors",
            "--no-check-certificate",
            "--downloader-args", "ffmpeg:-allowed_segment_extensions ALL",
            "--concurrent-fragments", "10"
        ]
        cmd.append(url)
        
        await status_msg.edit_text("<emoji id=5429381339851796035>✅</emoji> Found! Downloading to server...", parse_mode=ParseMode.HTML)
        
        returncode, stderr = await download_with_progress(cmd, message, status_msg)
        
        if returncode != 0 or not os.path.exists(filename):
            await status_msg.edit_text(f"<emoji id=5274099962655816924>❌</emoji> <b>Download failed!</b>\n\nThe video might be restricted or inaccessible.", parse_mode=ParseMode.HTML)
            return

        await status_msg.edit_text("<emoji id=5449683594425410231>📤</emoji> Uploading to Telegram...", parse_mode=ParseMode.HTML)
        
        width, height, duration = await get_video_metadata(filename)
        user_name = message.from_user.first_name or message.from_user.username or "User"
        rich_caption = (
            f"<emoji id=5463107823946717464>🎬</emoji> <b>Title:</b> <code>{title}</code>\n"
            f"<emoji id=5251203410396458957>👤</emoji> <b>Downloaded by:</b> <a href='tg://user?id={user_id}'>{user_name}</a>"
        )

        start_upload = time.time()
        await cached_upload(
            client=client,
            chat_id=message.chat.id,
            url=url,
            filename=filename,
            thumb_name=thumb_name,
            title=title,
            rich_caption=rich_caption,
            duration=duration,
            width=width,
            height=height,
            message=message,
            status_msg=status_msg,
            start_upload=start_upload,
            command_type="shikho"
        )
        dl_queue.on_success(user_id)
        await status_msg.delete()

    except Exception:
        await status_msg.edit_text(f"<emoji id=5274099962655816924>⚠️</emoji> <b>A processing error occurred.</b>", parse_mode=ParseMode.HTML)
    finally:
        if os.path.exists(filename):
            os.remove(filename)

@app.on_message(filters.command("hk"))
async def hk_link_handler(client, message: Message):
    if message.chat.id != ALLOWED_CHAT_ID and not is_admin(message.from_user.id):
        await message.reply_text(
            "<emoji id=5210952531676504517>❌</emoji> <b>Access Denied!</b>\n\nThis bot only works in the authorized group.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔗 Join Authorized Group", url="https://t.me/navigatesupport")]])
        )
        return
    user_id = message.from_user.id

    # Queue Check

    # Queue Check
    if not await dl_queue.can_start(user_id, message): return

    async with dl_queue.acquire_global(user_id, message):
        parts = message.text.split()
    url = None
    referer = "https://edgecoursebd.com/"
    
    if len(parts) >= 2:
        url = parts[1]
        
    if not url and message.reply_to_message and message.reply_to_message.text:
        url = message.reply_to_message.text.strip()
        
    if not url:
        await message.reply_text("<emoji id=5274099962655816924>❗</emoji> Please provide an HK (Vimeo) URL.\nUsage: /hk <URL>", parse_mode=ParseMode.HTML)
        return

    # URL Validation (Vimeo)
    if not any(domain in url for domain in ["vimeo.com", "player.vimeo.com"]):
        await message.reply_text(
            "<emoji id=5274099962655816924>❌</emoji> <b>Invalid URL!</b>\n\nOnly Vimeo URLs are allowed for this command.",
            parse_mode=ParseMode.HTML
        )
        return

    status_msg = await message.reply_text("<emoji id=5231012545799666522>🔍</emoji> Processing HK video...", parse_mode=ParseMode.HTML)

    if await check_and_serve_cache(client, message, url, status_msg):
        return

    filename = f"hk_video_{user_id}_{int(time.time())}.mp4"
    thumb_name = None
    title = "HK Video"
    
    try:
        # Fetch metadata using yt-dlp
        metadata_cmd = [
            "yt-dlp",
            "--dump-json",
            "--referer", referer,
            "--user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "--no-check-certificate",
            url
        ]
        
        process_meta = await asyncio.create_subprocess_exec(
            *metadata_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout_meta, stderr_meta = await process_meta.communicate()
        
        if process_meta.returncode == 0:
            try:
                metadata = json.loads(stdout_meta.decode())
                title = metadata.get("title", "HK Video")
                thumbnail_url = metadata.get("thumbnail")
                
                if thumbnail_url:
                    thumb_name = f"hk_thumb_{user_id}_{int(time.time())}.jpg"
                    async with httpx.AsyncClient(timeout=20) as client_dl:
                        r_thumb = await client_dl.get(thumbnail_url)
                        if r_thumb.status_code == 200:
                            with open(thumb_name, "wb") as f:
                                f.write(r_thumb.content)
                        else:
                            thumb_name = None
            except Exception:
                pass

        # Construct yt-dlp command
        cmd = [
            "yt-dlp",
            "-f", "bestvideo[height<=1080]+bestaudio/best[height<=1080]/best",
            "-o", filename,
            "--no-playlist",
            "--merge-output-format", "mp4",
            "--referer", referer,
            "--user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "--no-check-certificate",
            "--concurrent-fragments", "10"
        ]
        cmd.append(url)
        
        await status_msg.edit_text("<emoji id=5429381339851796035>✅</emoji> Found! Downloading to server...", parse_mode=ParseMode.HTML)
        
        returncode, stderr = await download_with_progress(cmd, message, status_msg)
        
        if returncode != 0 or not os.path.exists(filename):
            await status_msg.edit_text(f"<emoji id=5274099962655816924>❌</emoji> <b>Download failed!</b>\n\nThe video might be restricted or inaccessible.", parse_mode=ParseMode.HTML)
            return

        await status_msg.edit_text("<emoji id=5449683594425410231>📤</emoji> Uploading to Telegram...", parse_mode=ParseMode.HTML)
        
        width, height, duration = await get_video_metadata(filename)
        user_name = message.from_user.first_name or message.from_user.username or "User"
        rich_caption = (
            f"<emoji id=5463107823946717464>🎬</emoji> <b>Title:</b> <code>{title}</code>\n"
            f"<emoji id=5251203410396458957>👤</emoji> <b>Downloaded by:</b> <a href='tg://user?id={user_id}'>{user_name}</a>"
        )

        start_upload = time.time()
        await cached_upload(
            client=client,
            chat_id=message.chat.id,
            url=url,
            filename=filename,
            thumb_name=thumb_name,
            title=title,
            rich_caption=rich_caption,
            duration=duration,
            width=width,
            height=height,
            message=message,
            status_msg=status_msg,
            start_upload=start_upload,
            command_type="hk"
        )
        dl_queue.on_success(user_id)
        await status_msg.delete()

    except Exception as e:
        await status_msg.edit_text(f"<emoji id=5274099962655816924>⚠️</emoji> An error occurred.\n\nError: `{e}`", parse_mode=ParseMode.HTML)
    finally:
        if os.path.exists(filename):
            os.remove(filename)
        if thumb_name and os.path.exists(thumb_name):
            os.remove(thumb_name)

@app.on_message(filters.command("udvash"))
async def udvash_link_handler(client, message: Message):
    if message.chat.id != ALLOWED_CHAT_ID and not is_admin(message.from_user.id):
        await message.reply_text(
            "<emoji id=5210952531676504517>❌</emoji> <b>Access Denied!</b>\n\nThis bot only works in the authorized group.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔗 Join Authorized Group", url="https://t.me/navigatesupport")]])
        )
        return
    user_id = message.from_user.id

    # Queue Check
    if not await dl_queue.can_start(user_id, message): return

    async with dl_queue.acquire_global(user_id, message):
        parts = message.text.split()
    url = None
    
    if len(parts) >= 2:
        url = parts[1]
        
    if not url and message.reply_to_message and message.reply_to_message.text:
        url = message.reply_to_message.text.strip()
        
    if not url:
        await message.reply_text("<emoji id=5274099962655816924>❗</emoji> Please provide an Udvash URL.\nUsage: /udvash <URL>", parse_mode=ParseMode.HTML)
        return

    # URL Validation
    allowed_domains = ["udvash-unmesh.com", "udvash.com"]
    if not any(domain in url for domain in allowed_domains):
        await message.reply_text(
            "<emoji id=5274099962655816924>❌</emoji> <b>Invalid URL!</b>\n\nOnly valid URLs are allowed for this command.",
            parse_mode=ParseMode.HTML
        )
        return

    status_msg = await message.reply_text("<emoji id=5231012545799666522>🔍</emoji> Processing Udvash video...", parse_mode=ParseMode.HTML)

    if await check_and_serve_cache(client, message, url, status_msg):
        return

    filename = f"udvash_video_{user_id}_{int(time.time())}.mp4"
    thumb_name = None
    title = "Udvash Video"
    
    try:
        # Fetch metadata using yt-dlp (No special headers needed)
        metadata_cmd = [
            "yt-dlp",
            "--dump-json",
            "--no-check-certificate",
            url
        ]
        
        process_meta = await asyncio.create_subprocess_exec(
            *metadata_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout_meta, stderr_meta = await process_meta.communicate()
        
        if process_meta.returncode == 0:
            try:
                metadata = json.loads(stdout_meta.decode())
                title = metadata.get("title", "Udvash Video")
                thumbnail_url = metadata.get("thumbnail")
                
                if thumbnail_url:
                    thumb_name = f"udvash_thumb_{user_id}_{int(time.time())}.jpg"
                    async with httpx.AsyncClient(timeout=20) as client_dl:
                        r_thumb = await client_dl.get(thumbnail_url)
                        if r_thumb.status_code == 200:
                            with open(thumb_name, "wb") as f:
                                f.write(r_thumb.content)
                        else:
                            thumb_name = None
            except Exception:
                pass

        # Construct yt-dlp command for direct download
        cmd = [
            "yt-dlp",
            "-f", "bestvideo[height<=1080]+bestaudio/best[height<=1080]/best",
            "-o", filename,
            "--no-playlist",
            "--merge-output-format", "mp4",
            "--no-check-certificate",
            "--concurrent-fragments", "10"
        ]
        cmd.append(url)
        
        await status_msg.edit_text("<emoji id=5429381339851796035>✅</emoji> Found! Downloading to server...", parse_mode=ParseMode.HTML)
        
        returncode, stderr = await download_with_progress(cmd, message, status_msg)
        
        if returncode != 0 or not os.path.exists(filename):
            await status_msg.edit_text(f"<emoji id=5274099962655816924>❌</emoji> <b>Download failed!</b>\n\nPlease check the link or try again.", parse_mode=ParseMode.HTML)
            return

        await status_msg.edit_text("<emoji id=5449683594425410231>📤</emoji> Uploading to Telegram...", parse_mode=ParseMode.HTML)
        
        width, height, duration = await get_video_metadata(filename)
        user_name = message.from_user.first_name or message.from_user.username or "User"
        rich_caption = (
            f"<emoji id=5463107823946717464>🎬</emoji> <b>Title:</b> <code>{title}</code>\n"
            f"<emoji id=5251203410396458957>👤</emoji> <b>Downloaded by:</b> <a href='tg://user?id={user_id}'>{user_name}</a>"
        )

        start_upload = time.time()
        await cached_upload(
            client=client,
            chat_id=message.chat.id,
            url=url,
            filename=filename,
            thumb_name=thumb_name,
            title=title,
            rich_caption=rich_caption,
            duration=duration,
            width=width,
            height=height,
            message=message,
            status_msg=status_msg,
            start_upload=start_upload,
            command_type="udvash"
        )
        dl_queue.on_success(user_id)
        await status_msg.delete()

    except Exception:
        await status_msg.edit_text(f"<emoji id=5274099962655816924>⚠️</emoji> <b>A processing error occurred.</b>", parse_mode=ParseMode.HTML)
    finally:

        if os.path.exists(filename):
            os.remove(filename)

@app.on_message(filters.command("yt"))
async def yt_link_handler(client, message: Message):
    if message.chat.id != ALLOWED_CHAT_ID and not is_admin(message.from_user.id):
        await message.reply_text(
            "<emoji id=5210952531676504517>❌</emoji> <b>Access Denied!</b>\n\nThis bot only works in the authorized group.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔗 Join Authorized Group", url="https://t.me/navigatesupport")]])
        )
        return
    user_id = message.from_user.id

    # Queue Check
    if not await dl_queue.can_start(user_id, message): return

    async with dl_queue.acquire_global(user_id, message):
        parts = message.text.split()
    url = None
    
    if len(parts) >= 2:
        url = parts[1]
        
    if not url and message.reply_to_message and message.reply_to_message.text:
        url = message.reply_to_message.text.strip()
        
    if not url:
        await message.reply_text("<emoji id=5274099962655816924>❗</emoji> Please provide a YouTube link.\nUsage: /yt <URL>", parse_mode=ParseMode.HTML)
        return

    # URL Validation
    allowed_domains = ["youtube.com", "youtu.be", "m.youtube.com", "y2u.be"]
    if not any(domain in url for domain in allowed_domains):
        await message.reply_text(
            "<emoji id=5274099962655816924>❌</emoji> <b>Invalid Link!</b>\n\nThis is not a valid YouTube link, boss!",
            parse_mode=ParseMode.HTML
        )
        return

    status_msg = await message.reply_text("<emoji id=5231012545799666522>🔍</emoji> <b>Processing YouTube video... Please wait!</b>", parse_mode=ParseMode.HTML)

    if await check_and_serve_cache(client, message, url, status_msg):
        return

    filename = f"yt_video_{user_id}_{int(time.time())}.mp4"
    thumb_name = None
    title = "YouTube Video"
    
    try:
        # Fetch metadata using yt-dlp with a timeout and better User-Agent
        metadata_cmd = [
            "yt-dlp",
            "--dump-json",
            "--no-check-certificate",
            "--user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "--extractor-args", "youtube:player_client=android,web;player_skip=web,mweb",
            "--add-header", "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "--add-header", "Accept-Language: en-US,en;q=0.9",
            "--add-header", "Sec-Fetch-Mode: navigate",
        ]
        if os.path.exists("cookies.txt"):
            metadata_cmd.extend(["--cookies", "cookies.txt"])
        
        metadata_cmd.append(url)
        
        process_meta = await asyncio.create_subprocess_exec(
            *metadata_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        try:
            # Adding a 15-second timeout for metadata to avoid being stuck forever
            stdout_meta, stderr_meta = await asyncio.wait_for(process_meta.communicate(), timeout=30)
            
            if process_meta.returncode == 0:
                metadata = json.loads(stdout_meta.decode())
                title = metadata.get("title", "YouTube Video")
                thumbnail_url = metadata.get("thumbnail")
                
                if thumbnail_url:
                    thumb_name = f"yt_thumb_{user_id}_{int(time.time())}.jpg"
                    async with httpx.AsyncClient(timeout=20) as client_dl:
                        try:
                            r_thumb = await client_dl.get(thumbnail_url)
                            if r_thumb.status_code == 200:
                                with open(thumb_name, "wb") as f_thumb:
                                    f_thumb.write(r_thumb.content)
                            else:
                                thumb_name = None
                        except:
                            thumb_name = None
        except asyncio.TimeoutError:
            try: process_meta.kill() # Stop the stuck process
            except: pass
            print("YouTube metadata dump timeout. Proceeding with defaults.")
    except Exception as e:
        print(f"Metadata extraction error: {e}")

    cmd = [
        "yt-dlp",
        "-f", "bestvideo[height<=1080][ext=mp4]+bestaudio[ext=m4a]/best[height<=1080]/best",
        "-o", filename,
        "--no-playlist",
        "--merge-output-format", "mp4",
        "--no-check-certificate",
        "--geo-bypass",
        "--user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "--extractor-args", "youtube:player_client=android,ios",
        "--add-header", "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "--add-header", "Accept-Language: en-US,en;q=0.9",
        "--add-header", "Sec-Fetch-Mode: navigate",
        "--concurrent-fragments", "20",
        "--buffer-size", "1M"
    ]
    cmd.append(url)
    
    await status_msg.edit_text("<emoji id=5429381339851796035>✅</emoji> <b>Video found! Download starting...</b>", parse_mode=ParseMode.HTML)
    
    try:
        returncode, stderr = await download_with_progress(cmd, message, status_msg)
        
        if returncode != 0 or not os.path.exists(filename):
            error_details = stderr.decode(errors="ignore")[:200] if stderr else "No error details."
            await status_msg.edit_text(
                f"<emoji id=5274099962655816924>❌</emoji> <b>Download failed! Boss!</b>\n\n"
                f"<b>Error:</b>\n<code>{error_details}</code>", 
                parse_mode=ParseMode.HTML
            )
            return

        await status_msg.edit_text("<emoji id=5449683594425410231>📤</emoji> <b>Uploading to Telegram...</b>", parse_mode=ParseMode.HTML)
        
        width, height, duration = await get_video_metadata(filename)
        user_name = message.from_user.first_name or message.from_user.username or "User"
        rich_caption = (
            f"<emoji id=5463107823946717464>🎬</emoji> <b>Title:</b> <code>{title}</code>\n"
            f"<emoji id=5251203410396458957>👤</emoji> <b>Downloaded by:</b> <a href='tg://user?id={user_id}'>{user_name}</a>"
        )

        start_upload = time.time()
        await cached_upload(
            client=client,
            chat_id=message.chat.id,
            url=url,
            filename=filename,
            thumb_name=thumb_name,
            title=title,
            rich_caption=rich_caption,
            duration=duration,
            width=width,
            height=height,
            message=message,
            status_msg=status_msg,
            start_upload=start_upload,
            command_type="yt"
        )
        dl_queue.on_success(user_id)
        await status_msg.delete()
    except Exception as e:
        await status_msg.edit_text(f"<emoji id=5274099962655816924>⚠️</emoji> An error occurred, boss.\n\nError: `{e}`", parse_mode=ParseMode.HTML)
    finally:
        if os.path.exists(filename):
            os.remove(filename)
        if thumb_name and os.path.exists(thumb_name):
            os.remove(thumb_name)


@app.on_message(filters.command(["fb", "ig", "tik"]))
async def social_dl_handler(client: Client, message: Message):
    if message.chat.id != ALLOWED_CHAT_ID and not is_admin(message.from_user.id):
        await message.reply_text(
            "<emoji id=5210952531676504517>❌</emoji> <b>Access Denied!</b>\n\nThis bot only works in the authorized group.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔗 Join Authorized Group", url="https://t.me/navigatesupport")]])
        )
        return
    user_id = message.from_user.id

    # Queue Check
    if not await dl_queue.can_start(user_id, message): return

    async with dl_queue.acquire_global(user_id, message):
        try:
            cmd_used = message.command[0] if message.command else "social"
        except: cmd_used = "social"
    site_map = {"fb": "Facebook", "ig": "Instagram", "tik": "TikTok"}
    site_name = site_map.get(cmd_used, "Social")

    parts = message.text.split()
    url = getattr(message, "auto_url", None)
    if not url:
        if len(parts) >= 2:
            url = parts[1]
        elif len(parts) == 1 and parts[0].startswith("http"):
            url = parts[0]
        elif message.reply_to_message and message.reply_to_message.text:
            url = message.reply_to_message.text.strip()
        
    if not url:
        await message.reply_text(f"<emoji id=5274099962655816924>❗</emoji> Please provide a {site_name} link.\nUsage: /{cmd_used} <URL>")
        return

    status_msg = await message.reply_text(f"<emoji id=5231012545799666522>🔍</emoji> <b>Processing {site_name} video...</b>", parse_mode=ParseMode.HTML)
    
    if await check_and_serve_cache(client, message, url, status_msg):
        return
        
    filename = f"{cmd_used}_video_{user_id}_{int(time.time())}.mp4"
    thumb_name = None
    
    try:
        # Fetch metadata
        metadata_cmd = [
            "yt-dlp", "--dump-json", "--no-check-certificate",
            "--user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            url
        ]
        
        process_meta = await asyncio.create_subprocess_exec(
            *metadata_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout_meta, _ = await process_meta.communicate()
        
        title = f"{site_name} Video"
        if process_meta.returncode == 0:
            meta = json.loads(stdout_meta.decode())
            title = meta.get("title", f"{site_name} Video")
            thumbnail_url = meta.get("thumbnail")
            if thumbnail_url:
                thumb_name = f"{cmd_used}_thumb_{int(time.time())}.jpg"
                async with httpx.AsyncClient(timeout=20) as dl:
                    try:
                        r = await dl.get(thumbnail_url)
                        if r.status_code == 200:
                            with open(thumb_name, "wb") as f: f.write(r.content)
                        else: thumb_name = None
                    except: thumb_name = None

        # Download command
        cmd = [
            "yt-dlp",
            "-f", "bestvideo[height<=1080]+bestaudio/best[height<=1080]/best",
            "-o", filename,
            "--no-playlist",
            "--merge-output-format", "mp4",
            "--no-check-certificate",
            "--geo-bypass",
            "--user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "--concurrent-fragments", "10",
            url
        ]
        
        await status_msg.edit_text(f"<emoji id=5429381339851796035>✅</emoji> <b>{site_name} video found! Downloading...</b>", parse_mode=ParseMode.HTML)
        
        returncode, stderr = await download_with_progress(cmd, message, status_msg)
        
        if returncode != 0 or not os.path.exists(filename):
            error = stderr.decode(errors="ignore")[:200] if stderr else "Download failed."
            await status_msg.edit_text(f"<emoji id=5274099962655816924>❌</emoji> <b>Failed!</b>\n\n`{error}`")
            return

        await status_msg.edit_text("<emoji id=5449683594425410231>📤</emoji> <b>Uploading...</b>", parse_mode=ParseMode.HTML)
        
        width, height, duration = await get_video_metadata(filename)
        user_name = message.from_user.first_name or "User"
        caption = f"<emoji id=5463107823946717464>🎬</emoji> <b>Title:</b> <code>{title}</code>\n👤 <b>By:</b> <a href='tg://user?id={user_id}'>{user_name}</a>"

        start_upload = time.time()
        await cached_upload(
            client=client, chat_id=message.chat.id, url=url, filename=filename,
            thumb_name=thumb_name, title=title, rich_caption=caption, duration=duration,
            width=width, height=height, message=message, status_msg=status_msg, start_upload=start_upload, command_type=cmd_used
        )
        dl_queue.on_success(user_id)
        await status_msg.delete()

    except Exception as e:
        await status_msg.edit_text(f"⚠️ Error: `{e}`")
    finally:
        if os.path.exists(filename): os.remove(filename)
        if thumb_name and os.path.exists(thumb_name): os.remove(thumb_name)

@app.on_message(filters.command("id"))
async def get_emoji_id(client, message: Message):
    if message.chat.id != ALLOWED_CHAT_ID and not is_admin(message.from_user.id):
        await message.reply_text(
            "<emoji id=5210952531676504517>❌</emoji> <b>Access Denied!</b>\n\nThis bot only works in the authorized group.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔗 Join Authorized Group", url="https://t.me/navigatesupport")]])
        )
        return
    target = message.reply_to_message if message.reply_to_message else message
    user_id = message.from_user.id

    # Log to console for debugging

    print(f"--- DEBUG ID COMMAND ---")
    print(f"Target Msg ID: {target.id}")
    print(f"Entities: {target.entities}")
    print(f"Caption Entities: {target.caption_entities}")
    print(f"Sticker: {target.sticker}")
    
    # Header info
    user_id = target.from_user.id if target.from_user else "Unknown"
    user_name = target.from_user.first_name if target.from_user else "Unknown"
    
    response = [
        f"<b>🆔 Technical Details</b>",
        f"👤 <b>User:</b> {user_name} (<code>{user_id}</code>)",
        f"💬 <b>Chat ID:</b> <code>{target.chat.id}</code>",
        f"📄 <b>Msg Type:</b> <code>{target.media or 'Text'}</code>"
    ]
    
    found_any = False

    # Check Sticker
    if target.sticker:
        if target.sticker.custom_emoji_id:
            response.append(f"\n🎭 <b>Sticker Emoji ID:</b> <code>{target.sticker.custom_emoji_id}</code>")
            response.append(f"📝 <b>Code:</b> <code>&lt;emoji id={target.sticker.custom_emoji_id}&gt;🎭&lt;/emoji&gt;</code>")
            found_any = True
        else:
            response.append(f"\n⚠️ <i>This sticker is not a custom emoji.</i>")

    # Check Entities (Text or Caption)
    entities = target.entities or target.caption_entities
    if entities:
        for entity in entities:
            # Use Enum comparison for custom emojis
            if entity.type == MessageEntityType.CUSTOM_EMOJI:
                response.append(f"\n🎭 <b>Text Emoji ID:</b> <code>{entity.custom_emoji_id}</code>")
                response.append(f"📝 <b>Code:</b> <code>&lt;emoji id={entity.custom_emoji_id}&gt;🎭&lt;/emoji&gt;</code>")
                found_any = True
            else:
                print(f"Found other entity type: {entity.type}")

    if not found_any and not target.sticker:
        response.append(f"\n❌ <b>No Custom Emoji detected.</b>")
        response.append(f"💡 <i>Tip: Send an animated Premium emoji or an emoji sticker to find its ID.</i>")
        if entities:
            response.append(f"ℹ️ <i>Found {len(entities)} other entities, but none are custom emojis.</i>")
    
    await message.reply_text("\n".join(response), parse_mode=ParseMode.HTML)

@app.on_message(filters.document)
async def cookies_handler(client, message: Message):
    if message.chat.id != ALLOWED_CHAT_ID and not is_admin(message.from_user.id):
        return
    user_id = message.from_user.id
    
    file_name = message.document.file_name
    if file_name in ["cookies.txt", "cookies.json"]:
        status = await message.reply_text(f"📥 **Processing {file_name}...**", parse_mode=ParseMode.HTML)
        path = await client.download_media(message)
        
        if file_name == "cookies.json":
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                
                with open("cookies.txt", "w", encoding="utf-8") as f:
                    # Write Netscape header
                    f.write("# Netscape HTTP Cookie File\n# This is a generated file! Do not edit.\n\n")
                    for cookie in data:
                        domain = str(cookie.get("domain", ""))
                        flag = "TRUE" if domain.startswith(".") else "FALSE"
                        p = str(cookie.get("path", "/"))
                        s = "TRUE" if cookie.get("secure") else "FALSE"
                        e = int(cookie.get("expirationDate", 0))
                        n = str(cookie.get("name", ""))
                        v = str(cookie.get("value", ""))
                        f.write(f"{domain}\t{flag}\t{p}\t{s}\t{e}\t{n}\t{v}\n")
                
                if os.path.exists(path): os.remove(path)
                await status.edit_text("✅ **cookies.json converted to cookies.txt successfully!**\n\nYouTube downloads will now use these session cookies.", parse_mode=ParseMode.HTML)
                return
            except Exception as e:
                await status.edit_text(f"❌ **Error converting cookies.json:** `{e}`", parse_mode=ParseMode.HTML)
                if os.path.exists(path): os.remove(path)
                return

        await status.edit_text("✅ **cookies.txt has been updated successfully!**", parse_mode=ParseMode.HTML)

@app.on_message(filters.command("rmd"))
async def rmd_json_handler(client: Client, message: Message):
    if message.chat.id != ALLOWED_CHAT_ID and not is_admin(message.from_user.id):
        await message.reply_text(
            "<emoji id=5210952531676504517>❌</emoji> <b>Access Denied!</b>\n\nThis bot only works in the authorized group.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔗 Join Authorized Group", url="https://t.me/navigatesupport")]])
        )
        return
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.reply_text("<emoji id=5210952531676504517>❌</emoji> <b>Access Denied!</b>\n\nThis bot is private and only available to the authorized administrator.", parse_mode=ParseMode.HTML)
        return

    # Queue Check
    if not await dl_queue.can_start(user_id, message): return

    async with dl_queue.acquire_global(user_id, message):
        # Check if replied to a document
        if not message.reply_to_message or not message.reply_to_message.document:
            await message.reply_text("<emoji id=5274099962655816924>❗</emoji> Please reply to a JSON file with /rmd", parse_mode=ParseMode.HTML)
            return

    if not message.reply_to_message.document.file_name.endswith(".json"):
        await message.reply_text("<emoji id=5274099962655816924>❌</emoji> Only JSON files are supported.", parse_mode=ParseMode.HTML)
        return

    status_msg = await message.reply_text("<emoji id=5231012545799666522>📥</emoji> Downloading JSON file...", parse_mode=ParseMode.HTML)
    
    json_path = await client.download_media(message.reply_to_message)
    
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        videos = data.get("videos", [])
        if not videos:
            await status_msg.edit_text("<emoji id=5274099962655816924>❌</emoji> No videos found in JSON.", parse_mode=ParseMode.HTML)
            return

        total = len(videos)
        await status_msg.edit_text(f"<emoji id=5429381339851796035>✅</emoji> Found {total} videos. Starting sequential processing...", parse_mode=ParseMode.HTML)
        
        # Register task for cancellation
        task_cancel_flags[message.id] = False
        
        referer = "https://iframe.mediadelivery.net"
        
        for index, video in enumerate(videos, 1):
            # Check if task was cancelled
            if task_cancel_flags.get(message.id):
                await status_msg.edit_text("<emoji id=5274099962655816924>❌</emoji> <b>Task Cancelled!</b> Remaining videos skipped.", parse_mode=ParseMode.HTML)
                break
            url = video.get("videoURL") or video.get("videoYoutubeURL") or video.get("url")
            title = video.get("videoTitle") or video.get("title", f"Video {index}")
            thumbnail_url = video.get("bunnyThumbnailURL") or video.get("thumbnail")
            
            if not url:
                continue

            # Update progress status
            await status_msg.edit_text(
                f"<b>🔄 Processing Video {index}/{total}</b>\n\n"
                f"<emoji id=5463107823946717464>🎬</emoji> <b>Title:</b> <code>{title}</code>\n"
                f"<emoji id=5231012545799666522>🔍</emoji> Preparing download...",
                parse_mode=ParseMode.HTML
            )
            
            cached = get_cached_video(url)
            if cached:
                await status_msg.edit_text(
                    f"<b>🔄 Serving from Cache {index}/{total}...</b>\n\n"
                    f"<emoji id=5463107823946717464>🎬</emoji> <b>Title:</b> <code>{title}</code>",
                    parse_mode=ParseMode.HTML
                )
                try:
                    user_name = message.from_user.first_name or "User"
                    caption = f"<emoji id=5463107823946717464>🎬</emoji> <b>Title:</b> <code>{title}</code>\n"
                    caption += f"<emoji id=5251203410396458957>👤</emoji> <b>Fetched by:</b> <a href='tg://user?id={user_id}'>{user_name}</a>"
                    if cached.get("file_type") == "document":
                        await client.send_document(chat_id=message.chat.id, document=cached["file_id"], caption=caption, parse_mode=ParseMode.HTML, reply_to_message_id=message.id)
                    else:
                        await client.send_video(chat_id=message.chat.id, video=cached["file_id"], caption=caption, parse_mode=ParseMode.HTML, reply_to_message_id=message.id)
                except Exception as e:
                    await client.send_message(message.chat.id, f"⚠️ Cache error {index}: `{e}`")
                continue

            filename = f"rmd_video_{user_id}_{int(time.time())}_{index}.mp4"
            thumb_name = None
            
            try:
                # Download thumbnail if available
                if thumbnail_url:
                    thumb_name = f"rmd_thumb_{user_id}_{int(time.time())}_{index}.jpg"
                    async with httpx.AsyncClient(timeout=20) as client_dl:
                        try:
                            r_thumb = await client_dl.get(thumbnail_url)
                            if r_thumb.status_code == 200:
                                with open(thumb_name, "wb") as f_thumb:
                                    f_thumb.write(r_thumb.content)
                            else:
                                thumb_name = None
                        except:
                            thumb_name = None

                # Construct yt-dlp command
                is_youtube = any(domain in url for domain in ["youtube.com", "youtu.be", "m.youtube.com", "y2u.be"])
                if is_youtube:
                    # Small sleep to avoid rate limits
                    await asyncio.sleep(2)
                    cmd = [
                        "yt-dlp",
                        "-f", "bestvideo[height<=1080][ext=mp4]+bestaudio[ext=m4a]/best[height<=1080]/best",
                        "-o", filename,
                        "--no-playlist",
                        "--merge-output-format", "mp4",
                        "--no-check-certificate",
                        "--geo-bypass",
                        "--user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                        "--extractor-args", "youtube:player_client=android,ios",
                        "--add-header", "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                        "--add-header", "Accept-Language: en-US,en;q=0.9",
                        "--add-header", "Sec-Fetch-Mode: navigate",
                        "--concurrent-fragments", "20",
                        "--buffer-size", "1M"
                    ]
                else:
                    cmd = [
                        "yt-dlp",
                        "-f", "bestvideo[height<=1080]+bestaudio/best[height<=1080]/best",
                        "-o", filename,
                        "--no-playlist",
                        "--merge-output-format", "mp4",
                        "--no-check-certificate",
                        "--referer", referer,
                        "--user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                        "--add-header", "Origin: https://iframe.mediadelivery.net",
                        "--downloader-args", "ffmpeg:-allowed_segment_extensions ALL",
                        "--concurrent-fragments", "20",
                        "--buffer-size", "1M"
                    ]
                
                cmd.append(url)
                
                # Update status for downloading
                await status_msg.edit_text(
                    f"<b>📥 Downloading {index}/{total}...</b>\n\n"
                    f"🎬 <code>{title}</code>",
                    parse_mode=ParseMode.HTML
                )
                
                returncode, stderr = await download_with_progress(cmd, message, status_msg)
                
                if returncode != 0 or not os.path.exists(filename):
                    await client.send_message(
                        message.chat.id, 
                        f"❌ <b>Failed to download:</b> <code>{title}</code>",
                        reply_to_message_id=message.id,
                        parse_mode=ParseMode.HTML
                    )
                    continue

                # Uploading
                await status_msg.edit_text(
                    f"<b>📤 Uploading {index}/{total}...</b>\n\n"
                    f"🎬 <code>{title}</code>",
                    parse_mode=ParseMode.HTML
                )
                
                width, height, duration = await get_video_metadata(filename)
                user_name = message.from_user.first_name or message.from_user.username or "User"
                
                # Extract extra materials
                lecture_sheet = video.get("videoLectureSheetURL")
                note = video.get("videoNoteURL")
                practice_sheet = video.get("videoPracticeSheetURL")
                solve_sheet = video.get("videoSolveSheetURL")
                
                # Extract chapter
                video_chapter = video.get("videoChapter")
                chapter_name = video_chapter.get("chapterName") if isinstance(video_chapter, dict) else None

                # Build caption in specific order: Chapter then Title
                rich_caption = ""
                if chapter_name:
                    rich_caption += f"<emoji id=5395444784611480792>✏️</emoji> <b>Chapter:</b> <code>{chapter_name}</code>\n"
                
                rich_caption += f"<emoji id=5463107823946717464>🎬</emoji> <b>Title:</b> <code>{title}</code>\n"
                
                # Material Links
                if lecture_sheet and lecture_sheet.strip():
                    rich_caption += f"  <emoji id=5346105514575025401>➡️</emoji> <b>Lecture Sheet:</b> {lecture_sheet}\n"
                if note and note.strip():
                    rich_caption += f"  <emoji id=5346105514575025401>➡️</emoji> <b>Class Note:</b> {note}\n"
                if practice_sheet and practice_sheet.strip():
                    rich_caption += f"  <emoji id=5346105514575025401>➡️</emoji> <b>Practice Sheet:</b> {practice_sheet}\n"
                if solve_sheet and solve_sheet.strip():
                    rich_caption += f"  <emoji id=5346105514575025401>➡️</emoji> <b>Solve Sheet:</b> {solve_sheet}\n"

                # Downloaded by at the end
                rich_caption += f"\n<emoji id=5251203410396458957>👤</emoji> <b>Downloaded by:</b> <a href='tg://user?id={user_id}'>{user_name}</a>"

                start_upload = time.time()
                await cached_upload(
                    client=client,
                    chat_id=message.chat.id,
                    url=url,
                    filename=filename,
                    thumb_name=thumb_name,
                    title=title,
                    rich_caption=rich_caption,
                    duration=duration,
                    width=width,
                    height=height,
                    message=message,
                    status_msg=status_msg,
                    start_upload=start_upload,
                    command_type="rmd_json"
                )
                dl_queue.on_success(user_id)
                
            except Exception as e:
                await client.send_message(
                    message.chat.id,
                    f"⚠️ <b>Error processing video {index}:</b> `{e}`",
                    reply_to_message_id=message.id,
                    parse_mode=ParseMode.HTML
                )
            finally:
                if os.path.exists(filename):
                    os.remove(filename)
                if thumb_name and os.path.exists(thumb_name):
                    os.remove(thumb_name)
        
        await status_msg.edit_text(f"<emoji id=5429381339851796035>✨</emoji> <b>All {total} videos processed successfully!</b>", parse_mode=ParseMode.HTML)

    except Exception as e:
        await status_msg.edit_text(f"<emoji id=5274099962655816924>⚠️</emoji> Error reading JSON: `{e}`", parse_mode=ParseMode.HTML)
    finally:
        if message.id in task_cancel_flags:
            del task_cancel_flags[message.id]
        if os.path.exists(json_path):
            os.remove(json_path)

# =================== Admin Management ===================
@app.on_message(filters.command("addadmin"))
async def add_admin_handler(client, message: Message):
    if message.chat.id != ALLOWED_CHAT_ID and not is_admin(message.from_user.id):
        await message.reply_text(
            "<emoji id=5210952531676504517>❌</emoji> <b>Access Denied!</b>\n\nThis bot only works in the authorized group.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔗 Join Authorized Group", url="https://t.me/navigatesupport")]])
        )
        return
    user_id = message.from_user.id
    if not is_super_admin(user_id):
        await message.reply_text("<emoji id=5210952531676504517>❌</emoji> <b>Access Denied!</b> Only Super Admins (fixed in .env) can add other admins!", parse_mode=ParseMode.HTML)
        return

    target_id = None
    if message.reply_to_message:
        target_id = message.reply_to_message.from_user.id
    else:
        parts = message.text.split()
        if len(parts) >= 2:
            try:
                target_id = int(parts[1])
            except:
                pass

    if not target_id:
        await message.reply_text("❗ Please reply to a user or provide a User ID.\nUsage: `/addadmin [User ID]`")
        return

    if is_admin(target_id):
        await message.reply_text("ℹ️ This user is already an admin.")
        return

    DYNAMIC_ADMINS.append(target_id)
    save_admins(DYNAMIC_ADMINS)
    await message.reply_text(f"✅ User `<code>{target_id}</code>` has been added as a dynamic admin!")

@app.on_message(filters.command("rmadmin"))
async def remove_admin_handler(client, message: Message):
    if message.chat.id != ALLOWED_CHAT_ID and not is_admin(message.from_user.id):
        await message.reply_text(
            "<emoji id=5210952531676504517>❌</emoji> <b>Access Denied!</b>\n\nThis bot only works in the authorized group.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔗 Join Authorized Group", url="https://t.me/navigatesupport")]])
        )
        return
    user_id = message.from_user.id
    if not is_super_admin(user_id):
        await message.reply_text("<emoji id=5210952531676504517>❌</emoji> <b>Access Denied!</b> Only Super Admins (fixed in .env) can remove admins!", parse_mode=ParseMode.HTML)
        return

    target_id = None
    if message.reply_to_message:
        target_id = message.reply_to_message.from_user.id
    else:
        parts = message.text.split()
        if len(parts) >= 2:
            try:
                target_id = int(parts[1])
            except:
                pass

    if not target_id:
        await message.reply_text("❗ Please reply to a user or provide a User ID.\nUsage: `/rmadmin [User ID]`")
        return

    # Check if target is a super admin
    if target_id in SUPER_ADMINS:
        await message.reply_text("❌ **Access Denied!** This user is a Super Admin (from .env) and cannot be removed!")
        return

    # Don't allow removing yourself
    if target_id == user_id:
        await message.reply_text("❌ You cannot remove yourself from admins!")
        return

    if target_id not in DYNAMIC_ADMINS:
        await message.reply_text("ℹ️ This user is not a dynamic admin.")
        return

    DYNAMIC_ADMINS.remove(target_id)
    save_admins(DYNAMIC_ADMINS)
    await message.reply_text(f"✅ User `<code>{target_id}</code>` has been removed from dynamic admins!")

@app.on_message(filters.command("admins"))
async def list_admins_handler(client, message: Message):
    if message.chat.id != ALLOWED_CHAT_ID and not is_admin(message.from_user.id):
        await message.reply_text(
            "<emoji id=5210952531676504517>❌</emoji> <b>Access Denied!</b>\n\nThis bot only works in the authorized group.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔗 Join Authorized Group", url="https://t.me/navigatesupport")]])
        )
        return
    user_id = message.from_user.id
    if not is_super_admin(user_id):
        await message.reply_text("❌ Access Denied! Only Super Admins can view the full list.")
        return

    text = "<b>👮 Current Admins:</b>\n\n"
    text += f"👑 <b>Super Admins (Fixed):</b>\n"
    for i, aid in enumerate(SUPER_ADMINS, 1):
        text += f"  {i}. <code>{aid}</code>\n"
    
    if DYNAMIC_ADMINS:
        text += f"\n👤 <b>Dynamic Admins:</b>\n"
        for i, aid in enumerate(DYNAMIC_ADMINS, 1):
            text += f"  {i}. <code>{aid}</code>\n"
    
    await message.reply_text(text, parse_mode=ParseMode.HTML)

@app.on_message(filters.command("rmall"))
async def rmall_handler(client: Client, message: Message):
    if message.chat.id != ALLOWED_CHAT_ID and not is_admin(message.from_user.id):
        await message.reply_text(
            "<emoji id=5210952531676504517>❌</emoji> <b>Access Denied!</b>\n\nThis bot only works in the authorized group.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔗 Join Authorized Group", url="https://t.me/navigatesupport")]])
        )
        return
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.reply_text("<emoji id=5210952531676504517>❌</emoji> <b>Access Denied!</b>", parse_mode=ParseMode.HTML)
        return

    # Queue Check
    if not await dl_queue.can_start(user_id, message): return

    async with dl_queue.acquire_global(user_id, message):
        if not message.reply_to_message or not message.reply_to_message.document:
            await message.reply_text("❗ Please reply to the `WEBSITE_ALL_DATA_FINAL.json` file with /rmall")
            return

    status_msg = await message.reply_text("📥 Processing total website data...")
    json_path = await client.download_media(message.reply_to_message)
    
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data_root = json.load(f)
        
        # Flatten the structure: data -> subjects -> chapters -> videos
        all_videos = []
        subjects = data_root.get("data", [])
        
        for sub in subjects:
            sub_name = sub.get("subject_name", "Unknown Subject")
            chapters = sub.get("chapters", [])
            for chp in chapters:
                chp_name = chp.get("chapter_name", "Unknown Chapter")
                videos = chp.get("videos", [])
                for vid in videos:
                    # Enrich video object with metadata from parents
                    vid["subject_name_ext"] = sub_name
                    vid["chapter_name_ext"] = chp_name
                    all_videos.append(vid)
        
        if not all_videos:
            await status_msg.edit_text("❌ No videos found in the nested structure.")
            return

        total = len(all_videos)
        await status_msg.edit_text(f"✅ Found {total} videos across all subjects. Starting processing...")
        
        # Register task for cancellation
        task_cancel_flags[message.id] = False
        
        referer = "https://iframe.mediadelivery.net"
        
        for index, video in enumerate(all_videos, 1):
            # Check if task was cancelled
            if task_cancel_flags.get(message.id):
                await status_msg.edit_text("❌ <b>Task Cancelled!</b> Remaining videos skipped.")
                break
            url = video.get("original_url") or video.get("videoYoutubeURL") or video.get("stream_url") or video.get("url")
            title = video.get("title", f"Video {index}")
            subject_name = video.get("subject_name_ext")
            chapter_name = video.get("chapter_name_ext")
            
            if not url:
                continue

            await status_msg.edit_text(
                f"<b>🔄 Processing {index}/{total}</b>\n"
                f"📚 <b>Sub:</b> <code>{subject_name}</code>\n"
                f"<emoji id=5463107823946717464>🎬</emoji> <b>Title:</b> <code>{title}</code>",
                parse_mode=ParseMode.HTML
            )
            
            cached = get_cached_video(url)
            if cached:
                await status_msg.edit_text(
                    f"<b>🔄 Serving from Cache {index}/{total}...</b>\n"
                    f"📚 <b>Sub:</b> <code>{subject_name}</code>\n"
                    f"<emoji id=5463107823946717464>🎬</emoji> <b>Title:</b> <code>{title}</code>",
                    parse_mode=ParseMode.HTML
                )
                try:
                    user_name = message.from_user.first_name or "User"
                    caption = f"<emoji id=5282843764451195532>🖥</emoji> <b>Subject:</b> <code>{subject_name}</code>\n"
                    caption += f"<emoji id=5395444784611480792>✏️</emoji> <b>Chapter:</b> <code>{chapter_name}</code>\n"
                    caption += f"<emoji id=5463107823946717464>🎬</emoji> <b>Title:</b> <code>{title}</code>\n\n"
                    caption += f"\n <emoji id=5251203410396458957>👤</emoji> <b>Fetched by:</b> <a href='tg://user?id={user_id}'>{user_name}</a>"
                    
                    if cached.get("file_type") == "document":
                        await client.send_document(chat_id=message.chat.id, document=cached["file_id"], caption=caption, parse_mode=ParseMode.HTML, reply_to_message_id=message.id)
                    else:
                        await client.send_video(chat_id=message.chat.id, video=cached["file_id"], caption=caption, parse_mode=ParseMode.HTML, reply_to_message_id=message.id)
                except Exception as e:
                    pass
                continue

            filename = f"rmall_video_{user_id}_{index}.mp4"
            thumb_name = None # In this structure we don't usually have thumbs easily
            
            try:
                is_youtube = any(domain in url for domain in ["youtube.com", "youtu.be", "m.youtube.com", "y2u.be"])
                if is_youtube:
                    # Small sleep to avoid rate limits
                    await asyncio.sleep(2)
                    cmd = [
                        "yt-dlp",
                        "-f", "bestvideo[height<=1080][ext=mp4]+bestaudio[ext=m4a]/best[height<=1080]/best",
                        "-o", filename,
                        "--no-playlist",
                        "--merge-output-format", "mp4",
                        "--no-check-certificate",
                        "--geo-bypass",
                        "--user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                        "--extractor-args", "youtube:player_client=android,ios",
                        "--add-header", "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                        "--add-header", "Accept-Language: en-US,en;q=0.9",
                        "--add-header", "Sec-Fetch-Mode: navigate",
                        "--concurrent-fragments", "20",
                        "--buffer-size", "1M"
                    ]
                else:
                    cmd = [
                        "yt-dlp",
                        "-f", "best[height<=1080]/best",
                        "-o", filename,
                        "--no-playlist",
                        "--merge-output-format", "mp4",
                        "--no-check-certificate",
                        "--geo-bypass",
                        "--referer", referer,
                        "--user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                        "--add-header", "Origin: https://iframe.mediadelivery.net",
                        "--downloader-args", "ffmpeg:-allowed_segment_extensions ALL",
                        "--add-header", "Accept-Language: en-US,en;q=0.9",
                        "--concurrent-fragments", "20",
                        "--buffer-size", "1M"
                    ]
                
                cmd.append(url)
                
                returncode, stderr = await download_with_progress(cmd, message, status_msg)
                
                if returncode != 0 or not os.path.exists(filename):
                    await client.send_message(message.chat.id, f"<emoji id=5210952531676504517>❌</emoji> <b>Fail:</b> <code>{title}</code>")
                    continue

                width, height, duration = await get_video_metadata(filename)
                
                # Material Links
                l_sheet = video.get("videoLectureSheetURL")
                note = video.get("videoNoteURL")
                p_sheet = video.get("videoPracticeSheetURL")
                s_sheet = video.get("videoSolveSheetURL")
                
                # Build caption
                rich_caption = f"<emoji id=5282843764451195532>🖥</emoji> <b>Subject:</b> <code>{subject_name}</code>\n"
                rich_caption += f"<emoji id=5395444784611480792>✏️</emoji> <b>Chapter:</b> <code>{chapter_name}</code>\n"
                rich_caption += f"<emoji id=5463107823946717464>🎬</emoji> <b>Title:</b> <code>{title}</code>\n\n"
                
                if l_sheet and l_sheet.strip(): rich_caption += f"  <emoji id=5346105514575025401>➡️</emoji> <b>Lecture Sheet:</b> {l_sheet}\n"
                if note and note.strip(): rich_caption += f"  <emoji id=5346105514575025401>➡️</emoji> <b>Class Note:</b> {note}\n"
                if p_sheet and p_sheet.strip(): rich_caption += f"  <emoji id=5346105514575025401>➡️</emoji> <b>Practice Sheet:</b> {p_sheet}\n"
                if s_sheet and s_sheet.strip(): rich_caption += f"  <emoji id=5346105514575025401>➡️</emoji> <b>Solve Sheet:</b> {s_sheet}\n"
                
                user_name = message.from_user.first_name or "User"
                rich_caption += f"\n <emoji id=5251203410396458957>👤</emoji> <b>Downloaded by:</b> <a href='tg://user?id={user_id}'>{user_name}</a>"

                start_upload = time.time()
                await cached_upload(
                    client=client,
                    chat_id=message.chat.id,
                    url=url,
                    filename=filename,
                    thumb_name=None,
                    title=title,
                    rich_caption=rich_caption,
                    duration=duration,
                    width=width,
                    height=height,
                    message=message,
                    status_msg=status_msg,
                    start_upload=start_upload,
                    command_type="rmall"
                )
                dl_queue.on_success(user_id)
                
            except Exception as e:
                await client.send_message(message.chat.id, f"⚠️ Error video {index}: `{e}`")
            finally:
                if os.path.exists(filename): os.remove(filename)

        await status_msg.edit_text(" <emoji id=5458603043203327669>🔔</emoji> <b>All videos from Website Data processed!</b>")

    except Exception as e:
        await status_msg.edit_text(f"⚠️ Error reading JSON: `{e}`")
    finally:
        if message.id in task_cancel_flags:
            del task_cancel_flags[message.id]
        if os.path.exists(json_path): os.remove(json_path)

@app.on_message(filters.command("rmallu"))
async def rmallu_handler(client: Client, message: Message):
    if not is_admin(message.from_user.id):
        await message.reply_text("<emoji id=5210952531676504517>❌</emoji> <b>Admin Only Command!</b>", parse_mode=ParseMode.HTML)
        return
    user_id = message.from_user.id
    
    # Queue Check
    if not await dl_queue.can_start(user_id, message): return

    async with dl_queue.acquire_global(user_id, message):
        if not message.reply_to_message or not message.reply_to_message.document:
            await message.reply_text("❗ Please reply to the JSON file with /rmallu")
            return

    status_msg = await message.reply_text("📥 Processing total website data for YouTube...")
    json_path = await client.download_media(message.reply_to_message)
    
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data_root = json.load(f)
        
        all_videos = []
        subjects = data_root.get("data", [])
        for sub in subjects:
            sub_name = sub.get("subject_name", "Unknown Subject")
            chapters = sub.get("chapters", [])
            for chp in chapters:
                chp_name = chp.get("chapter_name", "Unknown Chapter")
                videos = chp.get("videos", [])
                for vid in videos:
                    vid["subject_name_ext"] = sub_name
                    vid["chapter_name_ext"] = chp_name
                    all_videos.append(vid)
        
        if not all_videos:
            await status_msg.edit_text("❌ No videos found in JSON.")
            return

        total = len(all_videos)
        await status_msg.edit_text(f"✅ Found {total} videos. Starting bulk YouTube upload...")
        
        task_cancel_flags[message.id] = False
        referer = "https://iframe.mediadelivery.net"
        
        for index, video in enumerate(all_videos, 1):
            if task_cancel_flags.get(message.id):
                await status_msg.edit_text("❌ <b>Task Cancelled!</b> Remaining videos skipped.")
                break
                
            url = video.get("original_url") or video.get("videoYoutubeURL") or video.get("stream_url") or video.get("url")
            title = video.get("title", f"Video {index}")
            
            if not url: continue

            await status_msg.edit_text(
                f"<b>🔄 Processing {index}/{total}</b>\n"
                f"<emoji id=5463107823946717464>🎬</emoji> <b>Title:</b> <code>{title}</code>\n"
                f"<emoji id=5231012545799666522>🔍</emoji> Downloading...",
                parse_mode=ParseMode.HTML
            )

            filename = f"rmallu_video_{user_id}_{index}.mp4"
            
            try:
                cmd = [
                    "yt-dlp",
                    "-f", "bestvideo[height<=1080]+bestaudio/best[height<=1080]/best",
                    "-o", filename,
                    "--no-playlist",
                    "--merge-output-format", "mp4",
                    "--referer", referer,
                    "--user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                    "--no-check-certificate",
                    "--concurrent-fragments", "10"
                ]
                # Add specific referers for some sites if needed
                cmd.append(url)
                
                returncode, stderr = await download_with_progress(cmd, message, status_msg)
                
                if returncode != 0 or not os.path.exists(filename):
                     await client.send_message(message.chat.id, f"❌ <b>Fail:</b> <code>{title}</code>")
                     continue

                await status_msg.edit_text(f"🚀 <b>Uploading {index}/{total} to YouTube...</b>", parse_mode=ParseMode.HTML)
                
                description = f"Bulk Upload: {title}\nSubject: {video.get('subject_name_ext')}"
                
                # Use thread-safe upload
                yt_link, channel_name = await asyncio.to_thread(upload_to_youtube, filename, title, description)
                
                await client.send_message(
                    message.chat.id,
                    f"<emoji id=5429381339851796035>✅</emoji> <b>Bulk Upload Success!</b>\n\n"
                    f"<emoji id=5463107823946717464>🎬</emoji> <b>Title:</b> <code>{title}</code>\n"
                    f"<emoji id=5271604874419647061>🔗</emoji> <b>Link:</b> {yt_link}",
                    # f"🎬 <b>Channel:</b> {channel_name}",
                    disable_web_page_preview=True,
                    reply_to_message_id=message.id
                )
                dl_queue.on_success(user_id)
                
            except Exception as e:
                await client.send_message(message.chat.id, f"⚠️ Error {index}: `{e}`")
            finally:
                if os.path.exists(filename): os.remove(filename)

        await status_msg.edit_text("<emoji id=5458603043203327669>🔔</emoji> <b>All bulk YouTube uploads processed!</b>")

    except Exception as e:
        await status_msg.edit_text(f"⚠️ Error reading JSON: `{e}`")
    finally:
        if message.id in task_cancel_flags: del task_cancel_flags[message.id]
        if os.path.exists(json_path): os.remove(json_path)

@app.on_message(filters.command("cancel") & filters.reply)
async def cancel_handler(client: Client, message: Message):
    if not is_admin(message.from_user.id):
        await message.reply_text("<emoji id=5210952531676504517>❌</emoji> <b>Access Denied!</b> Only admins can cancel downloads.", parse_mode=ParseMode.HTML)
        return

    replied_msg = message.reply_to_message
    replied_msg_id = replied_msg.id
    
    found = False
    
    # 1. Check if it's an active process (status message)
    if replied_msg_id in active_downloads:
        process = active_downloads[replied_msg_id]
        try:
            process.kill()
            found = True
        except: pass
        
    # 2. Check if it's a multi-video task (original command message)
    if replied_msg_id in task_cancel_flags:
        task_cancel_flags[replied_msg_id] = True
        found = True
        
    # 3. Handle cases where they reply to the status message of a multi-video task
    # We don't easily know the original_msg_id from the status_msg_id unless we track it
    # But often the status_msg is also in active_downloads, so the current video will stop.
    
    if found:
        await message.reply_text("✅ <b>Cancellation Signal Sent!</b>\n\nThe current process has been terminated.", parse_mode=ParseMode.HTML)
    else:
        await message.reply_text("⚠️ <b>No active process found</b> for this message.\n\nMake sure you are replying to an active progress bar or the original command message.", parse_mode=ParseMode.HTML)

@app.on_message(filters.command("up") & filters.reply)
async def up_handler(client, message: Message):
    if not is_admin(message.from_user.id):
        await message.reply_text("<emoji id=5210952531676504517>❌</emoji> <b>Access Denied!</b> Only for admins.", parse_mode=ParseMode.HTML)
        return
    
    replied_msg = message.reply_to_message
    if not (replied_msg.video or replied_msg.document or replied_msg.animation):
        await message.reply_text("❌ Please reply to a <b>video file</b> with /up", parse_mode=ParseMode.HTML)
        return
    
    # Check if document is a video
    if replied_msg.document and not (replied_msg.document.mime_type and replied_msg.document.mime_type.startswith("video/")):
        await message.reply_text("❌ This document is not a video.", parse_mode=ParseMode.HTML)
        return

    status_msg = await message.reply_text("⏳ <b>Downloading video from Telegram...</b>", parse_mode=ParseMode.HTML)
    
    try:
        file_path = await client.download_media(replied_msg)
        if not file_path:
            await status_msg.edit_text("❌ Failed to download video from Telegram.")
            return
        
        await status_msg.edit_text(" <emoji id=5217880283860194582>🚀</emoji> <b>Uploading to YouTube... (Unlisted)</b>", parse_mode=ParseMode.HTML)
        
        # Get title from caption or file name
        title = replied_msg.caption[:100] if replied_msg.caption else "Video Upload"
        if not title:
            title = os.path.basename(file_path)[:100]
            
        description = f"Uploaded via Telegram Bot by Admin {message.from_user.id}"
        
        # Run the synchronous upload in a thread so it doesn't freeze the bot
        yt_link, channel_name = await asyncio.to_thread(upload_to_youtube, file_path, title, description)
        
        await status_msg.edit_text(
            f"<emoji id=5429381339851796035>✅</emoji> <b>Successfully Uploaded to YouTube!</b>\n\n"
            f"<emoji id=5260291556899831755>🎬</emoji> <b>Channel:</b> <code>{channel_name}</code>\n"
            f"<emoji id=5271604874419647061>🔗</emoji> <b>Link:</b> {yt_link}\n"
            f"<emoji id=5210956306952758910>👀</emoji> <b>Visibility:</b> Unlisted",
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True
        )
    except Exception as e:
        await status_msg.edit_text(f"<emoji id=5210952531676504517>❌</emoji> <b>YouTube Upload Failed:</b>\n\n<code>{str(e)}</code>", parse_mode=ParseMode.HTML)
    finally:
        if 'file_path' in locals() and os.path.exists(file_path):
            os.remove(file_path)

# =================== Auto Link Detection ===================
@app.on_message(filters.text & ~filters.regex(r"^/"))
async def auto_link_handler(client, message: Message):
    # Only work in allowed chat or for admins
    if message.chat.id != ALLOWED_CHAT_ID and not is_admin(message.from_user.id):
        return

    text = message.text.strip()
    # Basic URL detection regex
    import re
    url_match = re.search(r'(https?://\S+)', text)
    if not url_match:
        return
        
    url = url_match.group(1)
    # Set a custom attribute to help handlers
    message.auto_url = url
    
    # 1. YouTube/Social (YouTube, FB, IG, TikTok)
    social_domains = [
        "youtube.com", "youtu.be", "m.youtube.com",
        "facebook.com", "fb.watch", "fb.com", "m.facebook.com",
        "instagram.com", "instagr.am",
        "tiktok.com", "vt.tiktok.com"
    ]
    if any(domain in url for domain in social_domains):
        await social_dl_handler(client, message)
        return

    # 2. RM & AFS (iframe.mediadelivery.net)
    if "iframe.mediadelivery.net" in url:
        # Both /rm and /afs use RM, we'll use rm_link_handler as default
        await rm_link_handler(client, message)
        return

    # 3. Shikho (app.shikho.com)
    if "shikho.com" in url or "tenbytecdn.com" in url:
        # Shikho and Biology Adda share tenbytecdn
        if "shikho.com" in url:
            await shikho_link_handler(client, message)
            return

    # 4. Biology Adda
    if "vidinfra.com" in url or "biology-adda.tenbytecdn.com" in url:
        await ba_link_handler(client, message)
        return

    # 5. Udvash
    if "udvash.com" in url or "udvash-unmesh.com" in url:
        await udvash_link_handler(client, message)
        return

# =================== Main ===================
if __name__ == "__main__":
    init_supabase()
    
    # Start Flask in a separate thread
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    
    print("Flask server started at http://localhost:8080")

    print("Bot is running...")
    
    app.run()
