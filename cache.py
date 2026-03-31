import os
from datetime import datetime, timezone
from supabase import create_client, Client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
CACHE_CHANNEL_ID = int(os.environ.get("CACHE_CHANNEL_ID", "0"))

supabase: Client = None

def init_supabase():
    """Initialize Supabase client."""
    global supabase
    if SUPABASE_URL and SUPABASE_KEY:
        try:
            supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
            print("✅ Supabase connected!")
        except Exception as e:
            print(f"⚠️ Failed to connect to Supabase: {e}")
    else:
        print("⚠️ Supabase not configured. Caching disabled.")

import re

def normalize_url(url: str) -> str:
    """Normalize URL for consistent cache lookups."""
    url = url.strip().rstrip("/")
    if "youtube.com" in url or "youtu.be" in url:
        return url # Don't strip queries for yt, as ?v= matters
        
    # Extract BunnyCDN/MediaDelivery Video ID format (LibraryID/UUID)
    # e.g. 342579/70241195-3be6-4852-a89a-f5e8dc330255
    bunny_match = re.search(r'(\d+/[a-fA-F0-9\-]{36})', url)
    if bunny_match:
        return bunny_match.group(1)
    
    # Remove common tracking parameters for educational vids
    if "?" in url:
        base = url.split("?")[0]
        return base
    return url

def get_cached_video(source_url: str) -> dict | list | None:
    """
    Check if video exists in cache. 
    Returns:
      - A single dict if it's a 1-part video
      - A list of dicts (sorted by part_number) if multi-part
      - None if not cached
    """
    if not supabase:
        return None
    try:
        normalized = normalize_url(source_url)
        print(f"DEBUG: Checking cache for URL: {normalized}")
        
        # First check the main entry (part 1 / single video)
        result = supabase.table("video_cache").select("*").eq(
            "source_url", normalized
        ).execute()
        
        if not result.data or len(result.data) == 0:
            print("❌ CACHE MISS: URL not found in DB")
            return None
        
        entry = result.data[0]
        total_parts = entry.get("total_parts", 1) or 1
        
        # Update usage stats
        try:
            supabase.table("video_cache").update({
                "last_used_at": datetime.now(timezone.utc).isoformat(),
                "use_count": entry["use_count"] + 1
            }).eq("id", entry["id"]).execute()
        except Exception as e:
            print(f"Failed to update cache usage stats: {e}")
        
        if total_parts <= 1:
            print(f"✅ CACHE HIT (single): Found {entry['title']} in DB")
            return entry
        
        # Multi-part: fetch all parts
        all_parts = [entry]  # part 1 is the main entry
        for part_num in range(2, total_parts + 1):
            part_key = f"{normalized}:part{part_num}"
            part_result = supabase.table("video_cache").select("*").eq(
                "source_url", part_key
            ).execute()
            if part_result.data and len(part_result.data) > 0:
                all_parts.append(part_result.data[0])
            else:
                print(f"⚠️ CACHE PARTIAL: Missing part {part_num} for {normalized}")
                return None  # Incomplete cache, re-download needed
        
        # Sort by part number (main = part 1, then part2, part3...)
        print(f"✅ CACHE HIT (multi-part, {total_parts} parts): Found {entry['title']} in DB")
        return all_parts
        
    except Exception as e:
        print(f"Cache lookup error: {e}")
        return None

def save_to_cache(
    source_url: str,
    file_id: str,
    file_type: str = "video",
    title: str = "",
    duration: int = 0,
    width: int = 0,
    height: int = 0,
    file_size: int = 0,
    cache_chat_id: int = 0,
    cache_message_id: int = 0,
    command_type: str = "",
    part_number: int = 1,
    total_parts: int = 1
) -> bool:
    """
    Save video info to cache. Returns True on success.
    For multi-part videos:
      - Part 1 is stored with the normalized source_url (+ total_parts field)
      - Part 2+ is stored with source_url = "{normalized}:part{N}"
    """
    if not supabase:
        return False
        
    try:
        normalized = normalize_url(source_url)
        
        # For part 2+, append :partN to the source_url key
        cache_key = normalized
        if part_number > 1:
            cache_key = f"{normalized}:part{part_number}"
        
        data = {
            "source_url": cache_key,
            "file_id": file_id,
            "file_type": file_type,
            "title": title[:200] if title else "Unknown",
            "duration": duration,
            "width": width,
            "height": height,
            "file_size": file_size,
            "cache_chat_id": cache_chat_id,
            "cache_message_id": cache_message_id,
            "command_type": command_type,
            "total_parts": total_parts,
            "part_number": part_number
        }
        
        supabase.table("video_cache").upsert(data, on_conflict="source_url").execute()
        
        print(f"✅ Successfully Cached to DB: {title} Part {part_number}/{total_parts} ({cache_key[:50]}...)")
        return True
    except Exception as e:
        print(f"Cache save error: {e}")
        return False
