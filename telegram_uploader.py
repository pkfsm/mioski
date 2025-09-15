import asyncio
import json
import os
import tempfile
import requests
from pyrogram import Client
from pyrogram.types import InputMediaVideo, InputMediaPhoto
from PIL import Image
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')
SESSION_STRING = os.getenv('SESSION_STRING')
GROUP_ID = int(os.getenv('GROUP_ID'))  # Your target group ID
START_FROM_ID = int(os.getenv('START_FROM_ID', '0'))  # Start uploading from this ID
GOOGLE_DRIVE_JSON_URL = os.getenv('GOOGLE_DRIVE_JSON_URL')  # Google Drive direct download link

def convert_google_drive_url(url):
    """Convert Google Drive sharing URL to direct download URL"""
    if 'drive.google.com' in url and '/file/d/' in url:
        # Extract file ID from sharing URL
        file_id = url.split('/file/d/')[1].split('/')[0]
        return f"https://drive.google.com/uc?export=download&id={file_id}"
    return url

async def download_json_data():
    """Download JSON data from Google Drive or use local file"""
    try:
        if GOOGLE_DRIVE_JSON_URL:
            logger.info(f"Downloading JSON data from Google Drive...")

            # Convert sharing URL to direct download URL if needed
            download_url = convert_google_drive_url(GOOGLE_DRIVE_JSON_URL)

            response = requests.get(download_url, timeout=60)
            response.raise_for_status()

            # Save downloaded data to local file
            with open('media_data.json', 'w') as f:
                f.write(response.text)

            # Parse and return JSON data
            media_data = json.loads(response.text)
            logger.info(f"Successfully downloaded {len(media_data)} entries from Google Drive")
            return media_data
        else:
            # Use local file
            logger.info("Using local media_data.json file")
            with open('media_data.json', 'r') as f:
                media_data = json.load(f)
            logger.info(f"Loaded {len(media_data)} entries from local file")
            return media_data

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download JSON from Google Drive: {e}")
        # Fallback to local file
        try:
            with open('media_data.json', 'r') as f:
                media_data = json.load(f)
            logger.info(f"Using local fallback file with {len(media_data)} entries")
            return media_data
        except FileNotFoundError:
            logger.error("No local media_data.json file found")
            raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON format in downloaded file: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error loading JSON data: {e}")
        raise

async def download_thumbnail(url):
    """Download and prepare thumbnail image"""
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        # Create temporary file for thumbnail
        with tempfile.NamedTemporaryFile(delete=False, suffix='.jpg') as temp_file:
            temp_file.write(response.content)
            temp_path = temp_file.name

        # Resize image if needed (Telegram requirements)
        with Image.open(temp_path) as img:
            # Convert to RGB if necessary
            if img.mode != 'RGB':
                img = img.convert('RGB')

            # Resize to reasonable dimensions
            img.thumbnail((320, 320), Image.Resampling.LANCZOS)
            img.save(temp_path, 'JPEG', quality=85)

        return temp_path
    except Exception as e:
        logger.error(f"Failed to download thumbnail from {url}: {e}")
        return None

async def upload_media(client, entry):
    """Upload a single media file to Telegram"""
    try:
        name = entry['name']
        link = entry['link']
        logo_url = entry['tvg-logo']
        entry_id = entry['id']

        logger.info(f"Processing ID {entry_id}: {name}")

        # Download thumbnail
        thumbnail_path = None
        if logo_url:
            thumbnail_path = await download_thumbnail(logo_url)

        # Send the media link with caption and thumbnail
        caption = f"🎬 {name}\n\n🔗 [Watch/Download]({link})"

        if thumbnail_path:
            try:
                # Send photo with caption
                await client.send_photo(
                    chat_id=GROUP_ID,
                    photo=thumbnail_path,
                    caption=caption,
                    parse_mode="markdown"
                )
                logger.info(f"Successfully uploaded ID {entry_id}: {name}")
            finally:
                # Clean up thumbnail file
                try:
                    os.unlink(thumbnail_path)
                except:
                    pass
        else:
            # Send text message if no thumbnail
            await client.send_message(
                chat_id=GROUP_ID,
                text=caption,
                parse_mode="markdown"
            )
            logger.info(f"Successfully sent message for ID {entry_id}: {name}")

        return True

    except Exception as e:
        logger.error(f"Failed to upload entry {entry.get('id', 'unknown')}: {e}")
        return False

async def main():
    """Main function to process and upload media"""
    try:
        # Download or load JSON data
        media_data = await download_json_data()

        # Filter entries starting from specified ID
        entries_to_upload = [entry for entry in media_data if entry['id'] >= START_FROM_ID]
        logger.info(f"Will upload {len(entries_to_upload)} entries starting from ID {START_FROM_ID}")

        if not entries_to_upload:
            logger.info("No entries to upload")
            return

        # Initialize Pyrogram client
        client = Client(
            "media_uploader",
            api_id=API_ID,
            api_hash=API_HASH,
            session_string=SESSION_STRING
        )

        async with client:
            logger.info("Connected to Telegram")

            successful_uploads = 0
            failed_uploads = 0

            for entry in entries_to_upload:
                try:
                    success = await upload_media(client, entry)
                    if success:
                        successful_uploads += 1
                    else:
                        failed_uploads += 1

                    # Add delay between uploads to avoid rate limiting
                    await asyncio.sleep(2)

                except Exception as e:
                    logger.error(f"Error processing entry {entry.get('id', 'unknown')}: {e}")
                    failed_uploads += 1

            logger.info(f"Upload completed. Successful: {successful_uploads}, Failed: {failed_uploads}")

    except Exception as e:
        logger.error(f"Main function error: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
