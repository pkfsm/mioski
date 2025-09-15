import asyncio
import json
import os
import tempfile
import requests
from pyrogram import Client
from pyrogram.types import InputMediaVideo
from PIL import Image
import logging
from urllib.parse import urlparse
import aiohttp
import aiofiles
import math

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')
SESSION_STRING = os.getenv('SESSION_STRING')
GROUP_ID = "@filexshit"  # Your target group ID
START_FROM_ID = int(os.getenv('START_FROM_ID', '0'))  # Start uploading from this ID
GOOGLE_DRIVE_JSON_URL = os.getenv('GOOGLE_DRIVE_JSON_URL')  # Google Drive direct download link
MAX_FILE_SIZE = int(os.getenv('MAX_FILE_SIZE', '1900000000'))  # 1.9GB for splitting
TELEGRAM_LIMIT = int(os.getenv('TELEGRAM_LIMIT', '2000000000'))  # 2GB Telegram absolute limit
DOWNLOAD_TIMEOUT = int(os.getenv('DOWNLOAD_TIMEOUT', '3600'))  # 1 hour timeout

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

    except Exception as e:
        logger.error(f"Error loading JSON data: {e}")
        raise

async def get_file_size(url):
    """Get file size from URL without downloading"""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.head(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status == 200:
                    size = response.headers.get('content-length')
                    return int(size) if size else 0
                return 0
    except Exception as e:
        logger.warning(f"Could not get file size for {url}: {e}")
        return 0

def get_clean_filename(name, url):
    """Generate clean filename from name and URL"""
    # Clean the name for filename use
    clean_name = "".join(c for c in name if c.isalnum() or c in (' ', '-', '_')).strip()
    clean_name = clean_name.replace(' ', '_')

    # Get extension from URL
    parsed_url = urlparse(url)
    original_filename = os.path.basename(parsed_url.path)
    extension = os.path.splitext(original_filename)[1] or '.mp4'

    return f"{clean_name}{extension}"

async def split_file(file_path, chunk_size):
    """Split large file into smaller chunks"""
    try:
        file_size = os.path.getsize(file_path)
        base_name = os.path.splitext(os.path.basename(file_path))[0]
        extension = os.path.splitext(file_path)[1]

        num_parts = math.ceil(file_size / chunk_size)
        logger.info(f"Splitting file into {num_parts} parts of ~{chunk_size/1024/1024:.0f}MB each")

        split_files = []

        async with aiofiles.open(file_path, 'rb') as source_file:
            for part_num in range(1, num_parts + 1):
                part_filename = f"{base_name}.part{part_num:03d}{extension}"
                part_path = os.path.join(os.path.dirname(file_path), part_filename)

                logger.info(f"Creating part {part_num}/{num_parts}: {part_filename}")

                async with aiofiles.open(part_path, 'wb') as part_file:
                    bytes_to_read = min(chunk_size, file_size - (part_num - 1) * chunk_size)
                    bytes_read = 0

                    while bytes_read < bytes_to_read:
                        chunk = await source_file.read(min(8192, bytes_to_read - bytes_read))
                        if not chunk:
                            break
                        await part_file.write(chunk)
                        bytes_read += len(chunk)

                actual_size = os.path.getsize(part_path)
                logger.info(f"Part {part_num} created: {actual_size/1024/1024:.1f}MB")
                split_files.append((part_path, part_filename))

        return split_files

    except Exception as e:
        logger.error(f"Error splitting file: {e}")
        return []

async def download_file(url, filename):
    """Download file from URL with progress tracking"""
    try:
        logger.info(f"Starting download: {filename}")

        # Check file size first
        file_size = await get_file_size(url)
        if file_size > TELEGRAM_LIMIT * 10:  # Don't download extremely large files (>20GB)
            logger.error(f"File too large to process: {file_size / 1024 / 1024 / 1024:.1f} GB")
            return None

        if file_size > 0:
            logger.info(f"File size: {file_size / 1024 / 1024:.1f} MB")

        async with aiohttp.ClientSession() as session:
            timeout = aiohttp.ClientTimeout(total=DOWNLOAD_TIMEOUT)
            async with session.get(url, timeout=timeout) as response:
                if response.status != 200:
                    logger.error(f"HTTP {response.status} for {url}")
                    return None

                # Create temporary file
                temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(filename)[1])
                temp_path = temp_file.name
                temp_file.close()

                # Download with progress
                downloaded = 0
                last_logged_mb = 0
                async with aiofiles.open(temp_path, 'wb') as f:
                    async for chunk in response.content.iter_chunked(8192):
                        await f.write(chunk)
                        downloaded += len(chunk)
                        current_mb = downloaded // (1024 * 1024)
                        if current_mb > last_logged_mb and current_mb % 10 == 0:  # Log every 10MB
                            if file_size > 0:
                                progress = (downloaded / file_size) * 100
                                logger.info(f"Download progress: {progress:.1f}% ({current_mb} MB)")
                            else:
                                logger.info(f"Downloaded: {current_mb} MB")
                            last_logged_mb = current_mb

                logger.info(f"Download completed: {filename} ({downloaded / 1024 / 1024:.1f} MB)")
                return temp_path

    except asyncio.TimeoutError:
        logger.error(f"Download timeout for {filename}")
        return None
    except Exception as e:
        logger.error(f"Download failed for {filename}: {e}")
        return None

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

async def upload_video_to_telegram(client, video_path, caption, thumbnail_path=None, part_info=None):
    """Upload video file to Telegram"""
    try:
        # Get file size
        file_size = os.path.getsize(video_path)
        logger.info(f"Uploading video: {file_size / 1024 / 1024:.1f} MB")

        # Add part information to caption if it's a split file
        if part_info:
            caption += f"\n\n📦 **Part {part_info['current']}/{part_info['total']}**"

        # Upload video
        message = await client.send_video(
            chat_id=GROUP_ID,
            video=video_path,
            thumb=thumbnail_path,
            caption=caption,
            parse_mode="markdown",
            supports_streaming=True,
            progress=upload_progress
        )

        logger.info(f"Successfully uploaded video to Telegram")
        return True

    except Exception as e:
        logger.error(f"Failed to upload video to Telegram: {e}")
        return False

async def upload_progress(current, total):
    """Progress callback for upload"""
    if total > 0:
        progress = (current / total) * 100
        current_mb = current // (1024 * 1024)
        total_mb = total // (1024 * 1024)
        if progress % 5 == 0:  # Log every 5%
            logger.info(f"Upload progress: {progress:.0f}% ({current_mb}/{total_mb} MB)")

async def process_media_entry(client, entry):
    """Process and upload a single media entry"""
    try:
        name = entry['name']
        link = entry['link']
        logo_url = entry['tvg-logo']
        entry_id = entry['id']

        logger.info(f"Processing ID {entry_id}: {name}")

        # Generate clean filename
        filename = get_clean_filename(name, link)

        # Download thumbnail
        thumbnail_path = None
        if logo_url:
            thumbnail_path = await download_thumbnail(logo_url)

        # Download video file
        video_path = await download_file(link, filename)
        if not video_path:
            logger.error(f"Failed to download video for ID {entry_id}")
            return False

        try:
            file_size = os.path.getsize(video_path)

            # Check if file needs splitting
            if file_size > MAX_FILE_SIZE:
                logger.info(f"File size ({file_size/1024/1024:.1f} MB) exceeds limit, splitting...")

                # Split the file
                split_files = await split_file(video_path, MAX_FILE_SIZE)

                if not split_files:
                    logger.error(f"Failed to split file for ID {entry_id}")
                    return False

                # Upload each part
                success_count = 0
                for i, (part_path, part_filename) in enumerate(split_files, 1):
                    try:
                        part_caption = f"🎬 **{name}**\n\n📁 File: `{part_filename}`"
                        part_info = {'current': i, 'total': len(split_files)}

                        success = await upload_video_to_telegram(
                            client, part_path, part_caption, thumbnail_path, part_info
                        )

                        if success:
                            success_count += 1
                            logger.info(f"✅ Uploaded part {i}/{len(split_files)}")
                        else:
                            logger.error(f"❌ Failed to upload part {i}/{len(split_files)}")

                        # Cleanup part file
                        try:
                            os.unlink(part_path)
                        except:
                            pass

                        # Delay between parts
                        if i < len(split_files):
                            await asyncio.sleep(3)

                    except Exception as part_error:
                        logger.error(f"Error uploading part {i}: {part_error}")

                return success_count == len(split_files)

            else:
                # File is small enough, upload directly
                caption = f"🎬 **{name}**\n\n📁 File: `{filename}`"
                return await upload_video_to_telegram(client, video_path, caption, thumbnail_path)

        finally:
            # Cleanup files
            try:
                if video_path and os.path.exists(video_path):
                    os.unlink(video_path)
                if thumbnail_path and os.path.exists(thumbnail_path):
                    os.unlink(thumbnail_path)
            except Exception as cleanup_error:
                logger.warning(f"Cleanup error: {cleanup_error}")

    except Exception as e:
        logger.error(f"Error processing entry {entry.get('id', 'unknown')}: {e}")
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
                    success = await process_media_entry(client, entry)
                    if success:
                        successful_uploads += 1
                        logger.info(f"✅ Successfully processed ID {entry['id']}")
                    else:
                        failed_uploads += 1
                        logger.error(f"❌ Failed to process ID {entry['id']}")

                    # Add delay between entries
                    await asyncio.sleep(5)

                except Exception as e:
                    logger.error(f"Error processing entry {entry.get('id', 'unknown')}: {e}")
                    failed_uploads += 1

            logger.info(f"Upload completed. Successful: {successful_uploads}, Failed: {failed_uploads}")

    except Exception as e:
        logger.error(f"Main function error: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
