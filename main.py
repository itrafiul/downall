import os
import time
import math
import shutil
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
from pyrogram.types import Message
from pyrogram.enums import MessageEntityType, ParseMode

# =================== Configuration ===================
# Get these from https://my.telegram.org
# Get these from https://my.telegram.org
API_ID = os.environ.get("API_ID")
API_HASH = os.environ.get("API_HASH")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
ADMIN_IDS = [int(i.strip()) for i in os.environ.get("ADMIN_IDS", "").split(",") if i.strip()]



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
                        
                        await send_video_with_fallback(
                            client=client, chat_id=chat_id, filepath=part_filename,
                            thumb=thumb, caption=part_caption, duration=int(seconds_per_part),
                            width=final_width, height=final_height, reply_to_id=reply_to_id,
                            progress=progress, progress_args=progress_args
                        )
                        if os.path.exists(part_filename): os.remove(part_filename)
                    else:
                        print(f"Part {i+1} failed: {stderr.decode()[:100]}")

                if success_count > 0:
                    return # Successfully uploaded parts
                    
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

# =================== Handlers ===================
@app.on_message(filters.command("start"))
async def start_handler(client, message: Message):
    user_id = message.from_user.id
    if ADMIN_IDS and user_id not in ADMIN_IDS:
        await message.reply_text("❌ **Access Denied!**\n\nThis bot is private and only available to the authorized administrator.", parse_mode=ParseMode.HTML)
        return
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
        f" <emoji id=5206607081334906820>✔️</emoji> <b>HK Downloader:</b> <code>/hk [link]</code>\n"
        f" <emoji id=5206607081334906820>✔️</emoji> <b>AFS Downloader:</b> <code>/afs [link]</code>\n"
        f" <emoji id=5206607081334906820>✔️</emoji> <b>YouTube:</b> <code>/yt [link]</code>\n\n"
        f"<i>Just send me a link and let the magic happen!</i> <emoji id=5220166546491459639>🔥</emoji>"
    )
    await message.reply_text(welcome_text, parse_mode=ParseMode.HTML)





@app.on_message(filters.command("afs"))
async def afs_link_handler(client, message: Message):
    user_id = message.from_user.id
    if ADMIN_IDS and user_id not in ADMIN_IDS:
        await message.reply_text("❌ **Access Denied!**\n\nThis bot is private and only available to the authorized administrator.", parse_mode=ParseMode.HTML)
        return



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
            "-f", "bestvideo[height<=720]+bestaudio/best[height<=720]/best",
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
        await send_video_with_fallback(
            client=client,
            chat_id=message.chat.id,
            filepath=filename,
            thumb=thumb_name,
            caption=rich_caption,
            duration=duration,
            width=width,
            height=height,
            reply_to_id=message.id,
            progress=upload_progress,
            progress_args=(client, status_msg, start_upload)
        )
        await status_msg.delete()

    except Exception as e:
        await status_msg.edit_text(f"<emoji id=5274099962655816924>⚠️</emoji> An error occurred.\n\nError: `{e}`", parse_mode=ParseMode.HTML)
    finally:

        if os.path.exists(filename):
            os.remove(filename)

@app.on_message(filters.command("rm"))
async def rm_link_handler(client, message: Message):
    user_id = message.from_user.id
    if ADMIN_IDS and user_id not in ADMIN_IDS:
        await message.reply_text("❌ **Access Denied!**\n\nThis bot is private and only available to the authorized administrator.", parse_mode=ParseMode.HTML)
        return



    parts = message.text.split()
    url = None
    referer = "https://iframe.mediadelivery.net"
    
    if len(parts) >= 2:
        url = parts[1]
        
    if not url and message.reply_to_message and message.reply_to_message.text:
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
            "-f", "bestvideo[height<=720]+bestaudio/best[height<=720]/best",
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
        await send_video_with_fallback(
            client=client,
            chat_id=message.chat.id,
            filepath=filename,
            thumb=thumb_name,
            caption=rich_caption,
            duration=duration,
            width=width,
            height=height,
            reply_to_id=message.id,
            progress=upload_progress,
            progress_args=(client, status_msg, start_upload)
        )
        await status_msg.delete()

    except Exception:
        await status_msg.edit_text(f"<emoji id=5274099962655816924>⚠️</emoji> <b>A processing error occurred.</b>", parse_mode=ParseMode.HTML)
    finally:

        if os.path.exists(filename):
            os.remove(filename)

@app.on_message(filters.command("shikho"))
async def shikho_link_handler(client, message: Message):
    user_id = message.from_user.id
    if ADMIN_IDS and user_id not in ADMIN_IDS:
        await message.reply_text("❌ **Access Denied!**\n\nThis bot is private and only available to the authorized administrator.", parse_mode=ParseMode.HTML)
        return



    parts = message.text.split()
    url = None
    referer = "https://app.shikho.com/"
    
    if len(parts) >= 2:
        url = parts[1]
        
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
            "-f", "bestvideo[height<=720]+bestaudio/best[height<=720]/best",
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
        await send_video_with_fallback(
            client=client,
            chat_id=message.chat.id,
            filepath=filename,
            thumb=thumb_name,
            caption=rich_caption,
            duration=duration,
            width=width,
            height=height,
            reply_to_id=message.id,
            progress=upload_progress,
            progress_args=(client, status_msg, start_upload)
        )
        await status_msg.delete()

    except Exception:
        await status_msg.edit_text(f"<emoji id=5274099962655816924>⚠️</emoji> <b>A processing error occurred.</b>", parse_mode=ParseMode.HTML)
    finally:
        if os.path.exists(filename):
            os.remove(filename)

@app.on_message(filters.command("hk"))
async def hk_link_handler(client, message: Message):
    user_id = message.from_user.id
    if ADMIN_IDS and user_id not in ADMIN_IDS:
        await message.reply_text("❌ **Access Denied!**\n\nThis bot is private and only available to the authorized administrator.", parse_mode=ParseMode.HTML)
        return

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
            "-f", "bestvideo[height<=720]+bestaudio/best[height<=720]/best",
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
        await send_video_with_fallback(
            client=client,
            chat_id=message.chat.id,
            filepath=filename,
            thumb=thumb_name,
            caption=rich_caption,
            duration=duration,
            width=width,
            height=height,
            reply_to_id=message.id,
            progress=upload_progress,
            progress_args=(client, status_msg, start_upload)
        )
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
    user_id = message.from_user.id
    if ADMIN_IDS and user_id not in ADMIN_IDS:
        await message.reply_text("❌ **Access Denied!**\n\nThis bot is private and only available to the authorized administrator.", parse_mode=ParseMode.HTML)
        return



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
            "-f", "bestvideo[height<=720]+bestaudio/best[height<=720]/best",
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
        await send_video_with_fallback(
            client=client,
            chat_id=message.chat.id,
            filepath=filename,
            thumb=thumb_name,
            caption=rich_caption,
            duration=duration,
            width=width,
            height=height,
            reply_to_id=message.id,
            progress=upload_progress,
            progress_args=(client, status_msg, start_upload)
        )
        await status_msg.delete()

    except Exception:
        await status_msg.edit_text(f"<emoji id=5274099962655816924>⚠️</emoji> <b>A processing error occurred.</b>", parse_mode=ParseMode.HTML)
    finally:

        if os.path.exists(filename):
            os.remove(filename)

@app.on_message(filters.command("yt"))
async def yt_link_handler(client, message: Message):
    user_id = message.from_user.id
    if ADMIN_IDS and user_id not in ADMIN_IDS:
        await message.reply_text("❌ **Access Denied!**\n\nThis bot is private and only available to the authorized administrator.", parse_mode=ParseMode.HTML)
        return

    parts = message.text.split()
    url = None
    
    if len(parts) >= 2:
        url = parts[1]
        
    if not url and message.reply_to_message and message.reply_to_message.text:
        url = message.reply_to_message.text.strip()
        
    if not url:
        await message.reply_text("<emoji id=5274099962655816924>❗</emoji> বস, দয়া করে একটি YouTube লিংক দিন।\nব্যবহার: /yt <URL>", parse_mode=ParseMode.HTML)
        return

    # URL Validation
    allowed_domains = ["youtube.com", "youtu.be", "m.youtube.com", "y2u.be"]
    if not any(domain in url for domain in allowed_domains):
        await message.reply_text(
            "<emoji id=5274099962655816924>❌</emoji> <b>ভুল লিংক!</b>\n\nএটি কোনো সঠিক YouTube লিংক নয়, বস!",
            parse_mode=ParseMode.HTML
        )
        return

    status_msg = await message.reply_text("<emoji id=5231012545799666522>🔍</emoji> <b>YouTube ভিডিও প্রসেস করা হচ্ছে... দয়া করে অপেক্ষা করুন!</b>", parse_mode=ParseMode.HTML)

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

    # Construct yt-dlp command with max bypasses
    cmd = [
        "yt-dlp",
        "-f", "bestvideo[height<=1080][ext=mp4]+bestaudio[ext=m4a]/best[height<=1080]/best",
        "-o", filename,
        "--no-playlist",
        "--merge-output-format", "mp4",
        "--no-check-certificate",
        "--geo-bypass",
        "--extractor-args", "youtube:player_client=android,web;player_skip=web,mweb",
        "--add-header", "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "--add-header", "Accept-Language: en-US,en;q=0.9",
        "--add-header", "Sec-Fetch-Mode: navigate",
        "--concurrent-fragments", "10"
    ]
    cmd.append(url)
    
    await status_msg.edit_text("<emoji id=5429381339851796035>✅</emoji> <b>ভিডিও পাওয়া গেছে! ডাউনলোড শুরু হচ্ছে...</b>", parse_mode=ParseMode.HTML)
    
    try:
        returncode, stderr = await download_with_progress(cmd, message, status_msg)
        
        if returncode != 0 or not os.path.exists(filename):
            error_details = stderr.decode(errors="ignore")[:200] if stderr else "No error details."
            await status_msg.edit_text(
                f"<emoji id=5274099962655816924>❌</emoji> <b>ডাউনলোড ব্যর্থ হয়েছে! বস!</b>\n\n"
                f"<b>Error:</b>\n<code>{error_details}</code>", 
                parse_mode=ParseMode.HTML
            )
            return

        await status_msg.edit_text("<emoji id=5449683594425410231>📤</emoji> <b>Telegram-এ আপলোড করা হচ্ছে...</b>", parse_mode=ParseMode.HTML)
        
        width, height, duration = await get_video_metadata(filename)
        user_name = message.from_user.first_name or message.from_user.username or "User"
        rich_caption = (
            f"<emoji id=5463107823946717464>🎬</emoji> <b>Title:</b> <code>{title}</code>\n"
            f"<emoji id=5251203410396458957>👤</emoji> <b>Downloaded by:</b> <a href='tg://user?id={user_id}'>{user_name}</a>"
        )

        start_upload = time.time()
        await send_video_with_fallback(
            client=client,
            chat_id=message.chat.id,
            filepath=filename,
            thumb=thumb_name,
            caption=rich_caption,
            duration=duration,
            width=width,
            height=height,
            reply_to_id=message.id,
            progress=upload_progress,
            progress_args=(client, status_msg, start_upload)
        )
        await status_msg.delete()
    except Exception as e:
        await status_msg.edit_text(f"<emoji id=5274099962655816924>⚠️</emoji> একটি সমস্যা হয়েছে, বস।\n\nError: `{e}`", parse_mode=ParseMode.HTML)
    finally:
        if os.path.exists(filename):
            os.remove(filename)
        if thumb_name and os.path.exists(thumb_name):
            os.remove(thumb_name)


@app.on_message(filters.command("id"))
async def get_emoji_id(client, message: Message):
    target = message.reply_to_message if message.reply_to_message else message
    user_id = message.from_user.id
    if ADMIN_IDS and user_id not in ADMIN_IDS:
        await message.reply_text("❌ **Access Denied!**\n\nThis bot is private and only available to the authorized administrator.", parse_mode=ParseMode.HTML)
        return

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
    user_id = message.from_user.id
    if ADMIN_IDS and user_id not in ADMIN_IDS:
        return
    
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
    user_id = message.from_user.id
    if ADMIN_IDS and user_id not in ADMIN_IDS:
        await message.reply_text("❌ **Access Denied!**\n\nThis bot is private and only available to the authorized administrator.", parse_mode=ParseMode.HTML)
        return

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
        
        referer = "https://iframe.mediadelivery.net"
        
        for index, video in enumerate(videos, 1):
            url = video.get("videoURL") or video.get("url")
            title = video.get("videoTitle") or video.get("title", f"Video {index}")
            thumbnail_url = video.get("bunnyThumbnailURL") or video.get("thumbnail")
            
            if not url:
                continue

            # Update progress status
            await status_msg.edit_text(
                f"<b>🔄 Processing Video {index}/{total}</b>\n\n"
                f"🎬 <b>Title:</b> <code>{title}</code>\n"
                f"<emoji id=5231012545799666522>🔍</emoji> Preparing download...",
                parse_mode=ParseMode.HTML
            )

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
                is_youtube = any(domain in url for domain in ["youtube.com", "youtu.be", "m.youtube.com"])
                
                cmd = [
                    "yt-dlp",
                    "-f", "bestvideo[height<=720]+bestaudio/best[height<=720]/best",
                    "-o", filename,
                    "--no-playlist",
                    "--merge-output-format", "mp4",
                    "--no-check-certificate",
                    "--concurrent-fragments", "10"
                ]

                # Add RM-specific headers only if NOT a YouTube link
                if not is_youtube:
                    cmd.extend([
                        "--referer", referer,
                        "--user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                        "--add-header", "Origin: https://iframe.mediadelivery.net",
                        "--downloader-args", "ffmpeg:-allowed_segment_extensions ALL",
                    ])
                
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
                await send_video_with_fallback(
                    client=client,
                    chat_id=message.chat.id,
                    filepath=filename,
                    thumb=thumb_name,
                    caption=rich_caption,
                    duration=duration,
                    width=width,
                    height=height,
                    reply_to_id=message.id,
                    progress=upload_progress,
                    progress_args=(client, status_msg, start_upload)
                )
                
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
        if os.path.exists(json_path):
            os.remove(json_path)

@app.on_message(filters.command("rmall"))
async def rmall_handler(client: Client, message: Message):
    user_id = message.from_user.id
    if ADMIN_IDS and user_id not in ADMIN_IDS:
        await message.reply_text("❌ **Access Denied!**")
        return

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
        
        referer = "https://iframe.mediadelivery.net"
        
        for index, video in enumerate(all_videos, 1):
            url = video.get("original_url") or video.get("stream_url") or video.get("url")
            title = video.get("title", f"Video {index}")
            subject_name = video.get("subject_name_ext")
            chapter_name = video.get("chapter_name_ext")
            
            if not url:
                continue

            await status_msg.edit_text(
                f"<b>🔄 Processing {index}/{total}</b>\n"
                f"📚 <b>Sub:</b> <code>{subject_name}</code>\n"
                f"🎬 <b>Title:</b> <code>{title}</code>",
                parse_mode=ParseMode.HTML
            )

            filename = f"rmall_video_{user_id}_{index}.mp4"
            thumb_name = None # In this structure we don't usually have thumbs easily
            
            try:
                # Construct yt-dlp command (using the same logic as /rmd for stealth)
                is_youtube = any(domain in url for domain in ["youtube.com", "youtu.be", "m.youtube.com"])
                
                cmd = [
                    "yt-dlp",
                    "-f", "best[height<=720]/best",
                    "-o", filename,
                    "--no-playlist",
                    "--merge-output-format", "mp4",
                    "--no-check-certificate",
                    "--geo-bypass",
                    "--extractor-args", "youtube:player_client=android,web;player_skip=web,mweb",
                    "--add-header", "Accept-Language: en-US,en;q=0.9",
                    "--concurrent-fragments", "10"
                ]
                
                if not is_youtube:
                    cmd.extend([
                        "--referer", referer,
                        "--user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                        "--add-header", "Origin: https://iframe.mediadelivery.net",
                        "--downloader-args", "ffmpeg:-allowed_segment_extensions ALL",
                    ])
                
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
                await send_video_with_fallback(
                    client=client,
                    chat_id=message.chat.id,
                    filepath=filename,
                    thumb=None,
                    caption=rich_caption,
                    duration=duration,
                    width=width,
                    height=height,
                    reply_to_id=message.id,
                    progress=upload_progress,
                    progress_args=(client, status_msg, start_upload)
                )
                
            except Exception as e:
                await client.send_message(message.chat.id, f"⚠️ Error video {index}: `{e}`")
            finally:
                if os.path.exists(filename): os.remove(filename)

        await status_msg.edit_text(" <emoji id=5458603043203327669>🔔</emoji> <b>All videos from Website Data processed!</b>")

    except Exception as e:
        await status_msg.edit_text(f"⚠️ Error reading JSON: `{e}`")
    finally:
        if os.path.exists(json_path): os.remove(json_path)

# =================== Main ===================
if __name__ == "__main__":
    # Start Flask in a separate thread
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    
    print("Flask server started at http://localhost:8080")

    print("Bot is running...")
    
    app.run()
