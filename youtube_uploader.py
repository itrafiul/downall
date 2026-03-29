import os
import json
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials

# If modifying these SCOPES, delete the token storage.
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
