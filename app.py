import os
import time
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



app = Client("toydownbot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
flask_app = Flask(__name__)

API_ENDPOINT = "https://tele-social.vercel.app/down?url="



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

# =================== Progress ===================
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
            return None, None, None
        
        data = json.loads(stdout)
        width = height = duration = None
        
        for stream in data.get("streams", []):
            if stream.get("codec_type") == "video":
                w = stream.get("width")
                h = stream.get("height")
                if w and h:
                    width = int(w)
                    height = int(h)
                break
                
        duration = float(data.get("format", {}).get("duration", 0))
        # Ensure duration is at least 1 for Telegram
        duration_int = int(duration) if duration > 0 else None
        
        return width, height, duration_int
async def send_video_with_fallback(client, chat_id, filepath, thumb, caption, duration, width, height, reply_to_id=None, progress=None, progress_args=()):
    try:
        return await client.send_video(
            chat_id=chat_id,
            video=filepath,
            thumb=thumb,
            caption=caption,
            parse_mode=ParseMode.HTML,
            duration=duration,
            width=width,
            height=height,
            supports_streaming=True,
            reply_to_message_id=reply_to_id,
            progress=progress,
            progress_args=progress_args
        )
    except Exception as e:
        print(f"send_video failed: {e}. Falling back to send_document.")
        # Try finding the status message in progress_args to update it if available
        if progress_args and len(progress_args) >= 2:
            try:
                status_msg = progress_args[1]
                await status_msg.edit_text(f"⚠️ <b>Video upload failed!</b> Retrying as document...", parse_mode=ParseMode.HTML)
            except:
                pass
        
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
        f" <emoji id=5206607081334906820>✔️</emoji> <b>Shikho:</b> <code>/shikho [link]</code>\n"
        f" <emoji id=5206607081334906820>✔️</emoji> <b>Udvash:</b> <code>/udvash [link]</code>\n"
        f" <emoji id=5206607081334906820>✔️</emoji> <b>AFS Downloader:</b> <code>/afs [link]</code>\n\n"
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
        await client.send_video(
            chat_id=message.chat.id,
            video=filename,
            thumb=thumb_name,
            caption=rich_caption,
            parse_mode=ParseMode.HTML,
            duration=duration,
            width=width,
            height=height,
            supports_streaming=True,
            reply_to_message_id=message.id,
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
        await client.send_video(
            chat_id=message.chat.id,
            video=filename,
            thumb=thumb_name,
            caption=rich_caption,
            parse_mode=ParseMode.HTML,
            duration=duration,
            width=width,
            height=height,
            supports_streaming=True,
            reply_to_message_id=message.id,
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
        await client.send_video(
            chat_id=message.chat.id,
            video=filename,
            thumb=thumb_name,
            caption=rich_caption,
            parse_mode=ParseMode.HTML,
            duration=duration,
            width=width,
            height=height,
            supports_streaming=True,
            reply_to_message_id=message.id,
            progress=upload_progress,
            progress_args=(client, status_msg, start_upload)
        )
        await status_msg.delete()

    except Exception:
        await status_msg.edit_text(f"<emoji id=5274099962655816924>⚠️</emoji> <b>A processing error occurred.</b>", parse_mode=ParseMode.HTML)
    finally:

        if os.path.exists(filename):
            os.remove(filename)

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
        await client.send_video(
            chat_id=message.chat.id,
            video=filename,
            thumb=thumb_name,
            caption=rich_caption,
            parse_mode=ParseMode.HTML,
            duration=duration,
            width=width,
            height=height,
            supports_streaming=True,
            reply_to_message_id=message.id,
            progress=upload_progress,
            progress_args=(client, status_msg, start_upload)
        )
        await status_msg.delete()

    except Exception:
        await status_msg.edit_text(f"<emoji id=5274099962655816924>⚠️</emoji> <b>A processing error occurred.</b>", parse_mode=ParseMode.HTML)
    finally:

        if os.path.exists(filename):
            os.remove(filename)

# =================== Batch Processor ===================

async def process_single_video(client, chat_id, video, index, total, user_id, user_name):
    title = video.get("videoTitle", "Untitled Video")
    video_url = video.get("videoURL")
    
    if not video_url or "iframe.mediadelivery.net" not in video_url:
        return

    status_msg = await client.send_message(chat_id, f"<b>[{index}/{total}]</b> 🔍 Processing: <code>{title}</code>", parse_mode=ParseMode.HTML)
    
    filename = f"batch_video_{user_id}_{index}_{int(time.time())}.mp4"
    thumb_name = None
    
    referer = "https://iframe.mediadelivery.net"
    
    try:
        # Metadata fetch
        headers = [
            "--referer", referer,
            "--add-header", "Origin: https://iframe.mediadelivery.net",
            "--add-header", "Accept: */*",
            "--add-header", "Accept-Language: en-US,en;q=0.9",
            "--add-header", "Sec-Fetch-Site: cross-site",
            "--add-header", "Sec-Fetch-Mode: cors",
        ]

        metadata_cmd = [
            "yt-dlp", "--dump-json", "--no-check-certificate", 
            "--user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        ] + headers + [video_url]

        process_meta = await asyncio.create_subprocess_exec(
            *metadata_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout_meta, _ = await process_meta.communicate()
        
        if process_meta.returncode == 0:
            try:
                metadata = json.loads(stdout_meta.decode())
                thumbnail_url = metadata.get("thumbnail")
                if thumbnail_url:
                    thumb_name = f"batch_thumb_{user_id}_{index}_{int(time.time())}.jpg"
                    async with httpx.AsyncClient(timeout=20) as client_dl:
                        r_thumb = await client_dl.get(thumbnail_url)
                        if r_thumb.status_code == 200:
                            with open(thumb_name, "wb") as f:
                                f.write(r_thumb.content)
                        else:
                            thumb_name = None
            except:
                pass

        # Download
        cmd = [
            "yt-dlp",
            "-f", "bestvideo[height<=720]+bestaudio/best[height<=720]/best",
            "-o", filename,
            "--no-playlist",
            "--merge-output-format", "mp4",
            "--no-check-certificate",
            "--user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "--concurrent-fragments", "10",
            "--downloader-args", "ffmpeg:-allowed_segment_extensions ALL"
        ] + headers + [video_url]

        await status_msg.edit_text(f"<b>[{index}/{total}]</b> 📥 Downloading: <code>{title}</code>", parse_mode=ParseMode.HTML)
        
        returncode, stderr = await download_with_progress(cmd, status_msg, status_msg)
        
        if returncode != 0 or not os.path.exists(filename):
            await status_msg.edit_text(f"❌ <b>[{index}/{total}]</b> Download Failed: <code>{title}</code>", parse_mode=ParseMode.HTML)
            return

        # Upload
        await status_msg.edit_text(f"<b>[{index}/{total}]</b> 📤 Uploading: <code>{title}</code>", parse_mode=ParseMode.HTML)
        
        # Check file existence and size
        if not os.path.exists(filename):
             raise Exception("Video file not found after download.")
        
        file_size_mb = os.path.getsize(filename) / (1024 * 1024)
        if file_size_mb < 0.01: # 10KB
             raise Exception(f"Video file is too small ({file_size_mb:.4f} MB). Might be a corrupted download.")

        print(f"Processing: {title} | Size: {file_size_mb:.2f} MB")
        
        width, height, duration = await get_video_metadata(filename)
        
        # Safe defaults if metadata fails
        if not duration or duration <= 0:
            duration = None
        if not width or width <= 0:
            width = None
        if not height or height <= 0:
            height = None

        course = video.get("videoCourse", {}).get("courseName", "N/A")
        subject = video.get("videoSubject", {}).get("subjectName", "N/A")
        chapter = video.get("videoChapter", {}).get("chapterName", "N/A")
        lecture_sheet = video.get("videoLectureSheetURL", "")
        practice_sheet = video.get("videoPracticeSheetURL", "")
        solve_sheet = video.get("videoSolveSheetURL", "")
        note_sheet = video.get("videoNoteURL", "")
        
        # ... truncated caption logic ...
        caption = f"<emoji id=5463107823946717464>🎬</emoji> <b>Title:</b> <code>{title}</code>\n"
        if course != "N/A": caption += f"<emoji id=5251203410396458957>📚</emoji> <b>Course:</b> {course}\n"
        if subject != "N/A": caption += f"<emoji id=5251203410396458957>📖</emoji> <b>Subject:</b> {subject}\n"
        if chapter != "N/A": caption += f"<emoji id=5251203410396458957>🔖</emoji> <b>Chapter:</b> {chapter}\n"
        
        if lecture_sheet: caption += f"\n<emoji id=5213233633803893325>📄</emoji> <a href='{lecture_sheet}'>Lecture Sheet</a>"
        if practice_sheet: caption += f"\n<emoji id=5213233633803893325>📝</emoji> <a href='{practice_sheet}'>Practice Sheet</a>"
        if solve_sheet: caption += f"\n<emoji id=5213233633803893325>✅</emoji> <a href='{solve_sheet}'>Solve Sheet</a>"
        if note_sheet: caption += f"\n<emoji id=5213233633803893325>📓</emoji> <a href='{note_sheet}'>Note Sheet</a>"
        
        caption += f"\n\n<emoji id=5251203410396458957>👤</emoji> <b>Downloaded by:</b> <a href='tg://user?id={user_id}'>{user_name}</a>"

        start_upload = time.time()
        await send_video_with_fallback(
            client=client,
            chat_id=chat_id,
            filepath=filename,
            thumb=thumb_name,
            caption=caption,
            duration=duration,
            width=width,
            height=height,
            progress=upload_progress,
            progress_args=(client, status_msg, start_upload)
        )
        await status_msg.delete()

    except Exception as e:
        import traceback
        traceback.print_exc()
        await client.send_message(chat_id, f"⚠️ <b>[{index}/{total}]</b> Error: <code>{title}</code>\n`{e}`", parse_mode=ParseMode.HTML)
    finally:
        if os.path.exists(filename): os.remove(filename)
        if thumb_name and os.path.exists(thumb_name): os.remove(thumb_name)

@app.on_message(filters.command("dn"))
async def batch_dn_handler(client, message: Message):
    user_id = message.from_user.id
    if ADMIN_IDS and user_id not in ADMIN_IDS:
        await message.reply_text("❌ **Access Denied!**")
        return

    # Check if replied message OR current message has the document
    target = message.reply_to_message or message
    
    if not target.document or not target.document.file_name.lower().endswith(".json"):
        await message.reply_text("<emoji id=5274099962655816924>❗</emoji> Please reply to a JSON file OR upload a JSON file with /dn in the caption.", parse_mode=ParseMode.HTML)
        return

    status_msg = await message.reply_text("<emoji id=5231012545799666522>📥</emoji> Downloading and parsing JSON...", parse_mode=ParseMode.HTML)
    
    json_path = await target.download()
    
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        videos = data.get("videos", [])
        if not videos:
            await status_msg.edit_text("❌ **No videos found in the file!**")
            return
        
        total = len(videos)
        await status_msg.edit_text(f"<emoji id=5429381339851796035>✅</emoji> Found <b>{total}</b> videos. Starting parallel processing (max 3 at a time)...", parse_mode=ParseMode.HTML)
        
        user_name = message.from_user.first_name or "User"
        
        # Process videos one by one as requested
        for index, video in enumerate(videos, 1):
            await process_single_video(client, message.chat.id, video, index, total, user_id, user_name)
            
        await message.reply_text(f"🏁 <b>Batch production complete!</b>\nAll {total} videos processed.", parse_mode=ParseMode.HTML)
        
    except Exception as e:
        await status_msg.edit_text(f"❌ <b>Error occurred:</b>\n<code>{e}</code>", parse_mode=ParseMode.HTML)
    finally:
        if os.path.exists(json_path):
            os.remove(json_path)

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

# =================== Main ===================
if __name__ == "__main__":
    # Start Flask in a separate thread
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    
    print("Flask server started at http://localhost:8080")

    print("Bot is running...")
    
    app.run()
