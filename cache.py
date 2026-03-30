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

def normalize_url(url: str) -> str:
    """Normalize URL for consistent cache lookups."""
    url = url.strip().rstrip("/")
    if "youtube.com" in url or "youtu.be" in url:
        return url # Don't strip queries for yt, as ?v= matters
    
    # Remove common tracking parameters for educational vids
    if "?" in url:
        base = url.split("?")[0]
        return base
    return url

def get_cached_video(source_url: str) -> dict | None:
    """Check if video exists in cache. Returns cache entry or None."""
    if not supabase:
        return None
    try:
        normalized = normalize_url(source_url)
        print(f"DEBUG: Checking cache for URL: {normalized}")
        
        result = supabase.table("video_cache").select("*").eq(
            "source_url", normalized
        ).execute()
        
        if result.data and len(result.data) > 0:
            entry = result.data[0]
            # Update usage stats asynchronously or fire & forget
            try:
                supabase.table("video_cache").update({
                    "last_used_at": datetime.now(timezone.utc).isoformat(),
                    "use_count": entry["use_count"] + 1
                }).eq("id", entry["id"]).execute()
            except Exception as e:
                print(f"Failed to update cache usage stats: {e}")
            
            print(f"✅ CACHE HIT: Found {entry['title']} in DB")
            return entry
        else:
            print("❌ CACHE MISS: URL not found in DB")
            return None
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
    command_type: str = ""
) -> bool:
    """Save video info to cache. Returns True on success."""
    if not supabase:
        return False
        
    try:
        normalized = normalize_url(source_url)
        data = {
            "source_url": normalized,
            "file_id": file_id,
            "file_type": file_type,
            "title": title[:200] if title else "Unknown", # truncate too long titles
            "duration": duration,
            "width": width,
            "height": height,
            "file_size": file_size,
            "cache_chat_id": cache_chat_id,
            "cache_message_id": cache_message_id,
            "command_type": command_type
        }
        
        # We use insert instead of upsert here to avoid needing to pass primary key, 
        # or we could use upsert if source_url is unique constraint.
        # Assuming source_url text unique not null constraint in DB.
        supabase.table("video_cache").upsert(data, on_conflict="source_url").execute()
        
        print(f"✅ Successfully Cached to DB: {title} ({normalized[:50]}...)")
        return True
    except Exception as e:
        print(f"Cache save error: {e}")
        return False
