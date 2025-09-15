import asyncio
import json
import os
import tempfile
import requests
from pyrogram import Client
from pyrogram.types import InputMediaVideo
from pyrogram.enums import ParseMode
from PIL import Image
import logging
from urllib.parse import urlparse
import aiohttp
import aiofiles
import math
from aiohttp import ClientError, ClientTimeout, ContentTypeError

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
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))  # Number of download retries

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

async def get_file_size(url, session=None):
    """Get file size from URL without downloading"""
    close_session = False
    if session is None:
        session = aiohttp.ClientSession()
        close_session = True

    try:
        timeout = ClientTimeout(total=30, connect=10)
        async with session.head(url, timeout=timeout, allow_redirects=True) as response:
            if response.status == 200:
                size = response.headers.get('content-length')
                return int(size) if size else 0
            else:
                logger.warning(f"HEAD request failed with status {response.status} for {url}")
                return 0
    except Exception as e:
        logger.warning(f"Could not get file size for {url}: {e}")
        return 0
    finally:
        if close_session:
            await session.close()

def get_clean_filename(name, url):
    """Generate clean filename from name and URL"""
    # Clean the name for filename use
    clean_name = "".join(c for c in name if c.isalnum() or c in (' ', '-', '_', '(', ')')).strip()
    clean_name = clean_name.replace(' ', '_')

    # Limit filename length
    if len(clean_name) > 50:
        clean_name = clean_name[:50]

    # Get extension from URL
    parsed_url = urlparse(url)
    original_filename = os.path.basename(parsed_url.path)
    extension = os.path.splitext(original_filename)[1] or '.mp4'

    return f"{clean_name}{extension}"

async def download_with_resume(session, url, file_path, start_byte=0):
    """Download file with resume capability"""
    headers = {}
    if start_byte > 0:
        headers['Range'] = f'bytes={start_byte}-'
        logger.info(f"Resuming download from byte {start_byte}")

    timeout = ClientTimeout(total=DOWNLOAD_TIMEOUT, sock_read=300)  # 5 min read timeout

    try:
        async with session.get(url, headers=headers, timeout=timeout) as response:
            if response.status not in [200, 206]:  # 206 for partial content
                logger.error(f"HTTP {response.status} for {url}")
                return False

            # Open file in append mode if resuming
            mode = 'ab' if start_byte > 0 else 'wb'
            async with aiofiles.open(file_path, mode) as f:
                downloaded = start_byte
                chunk_size = 8192

                async for chunk in response.content.iter_chunked(chunk_size):
                    await f.write(chunk)
                    downloaded += len(chunk)

                    # Log progress every 10MB
                    if downloaded % (10 * 1024 * 1024) == 0:
                        logger.info(f"Downloaded: {downloaded / 1024 / 1024:.1f} MB")

                return True

    except asyncio.TimeoutError:
        logger.error("Download timeout occurred")
        return False
    except ClientError as e:
        logger.error(f"Client error during download: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during download: {e}")
        return False

async def download_file(url, filename):
    """Download file from URL with retry logic and resume capability"""
    logger.info(f"Starting download: {filename}")

    # Create temporary file
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(filename)[1])
    temp_path = temp_file.name
    temp_file.close()

    connector = aiohttp.TCPConnector(
        limit=10,
        ttl_dns_cache=300,
        use_dns_cache=True,
        keepalive_timeout=30,
        enable_cleanup_closed=True
    )

    async with aiohttp.ClientSession(connector=connector) as session:
        # Check file size first
        file_size = await get_file_size(url, session)
        if file_size > TELEGRAM_LIMIT * 10:  # Don't download extremely large files (>20GB)
            logger.error(f"File too large to process: {file_size / 1024 / 1024 / 1024:.1f} GB")
            try:
                os.unlink(temp_path)
            except:
                pass
            return None

        if file_size > 0:
            logger.info(f"File size: {file_size / 1024 / 1024:.1f} MB")

        # Retry logic
        for attempt in range(MAX_RETRIES):
            try:
                logger.info(f"Download attempt {attempt + 1}/{MAX_RETRIES}")

                # Check if partial file exists
                start_byte = 0
                if os.path.exists(temp_path) and attempt > 0:
                    start_byte = os.path.getsize(temp_path)
                    if start_byte > 0:
                        logger.info(f"Found partial download, resuming from {start_byte / 1024 / 1024:.1f} MB")

                # Attempt download
                success = await download_with_resume(session, url, temp_path, start_byte)

                if success:
                    final_size = os.path.getsize(temp_path)
                    logger.info(f"Download completed: {filename} ({final_size / 1024 / 1024:.1f} MB)")

                    # Verify file size if we know the expected size
                    if file_size > 0 and abs(final_size - file_size) > 1024:  # Allow 1KB difference
                        logger.warning(f"File size mismatch: expected {file_size}, got {final_size}")
                        if attempt < MAX_RETRIES - 1:
                            logger.info("Retrying download due to size mismatch")
                            continue

                    return temp_path
                else:
                    logger.warning(f"Download attempt {attempt + 1} failed")

            except Exception as e:
                logger.error(f"Download attempt {attempt + 1} failed with error: {e}")

            # Wait before retry (exponential backoff)
            if attempt < MAX_RETRIES - 1:
                wait_time = 2 ** attempt  # 1, 2, 4 seconds
                logger.info(f"Waiting {wait_time} seconds before retry...")
                await asyncio.sleep(wait_time)

        # All attempts failed
        logger.error(f"All download attempts failed for {filename}")
        try:
            os.unlink(temp_path)
        except:
            pass
        return None

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

def escape_markdown(text):
    """Escape special characters for Telegram MarkdownV2"""
    # Characters that need to be escaped in MarkdownV2
    escape_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']

    for char in escape_chars:
        text = text.replace(char, f'\\{char}')

    return text

async def upload_video_to_telegram(client, video_path, caption, thumbnail_path=None, part_info=None):
    """Upload video file to Telegram"""
    try:
        # Get file size
        file_size = os.path.getsize(video_path)
        logger.info(f"Uploading video: {file_size / 1024 / 1024:.1f} MB")

        # Add part information to caption if it's a split file
        if part_info:
            caption += f"\n\n📦 **Part {part_info['current']}/{part_info['total']}**"

        # Try with MarkdownV2 first, then fall back to HTML, then plain text
        upload_success = False

        # Try MarkdownV2 (requires escaping)
        try:
            escaped_caption = escape_markdown(caption)
            message = await client.send_video(
                chat_id=GROUP_ID,
                video=video_path,
                thumb=thumbnail_path,
                caption=escaped_caption,
                parse_mode=ParseMode.MARKDOWN,
                supports_streaming=True,
                progress=upload_progress
            )
            upload_success = True
            logger.info("Successfully uploaded with MarkdownV2 formatting")
        except Exception as md_error:
            logger.warning(f"MarkdownV2 upload failed: {md_error}")

            # Try HTML formatting
            try:
                html_caption = caption.replace('**', '<b>').replace('**', '</b>')
                html_caption = html_caption.replace('`', '<code>').replace('`', '</code>')

                message = await client.send_video(
                    chat_id=GROUP_ID,
                    video=video_path,
                    thumb=thumbnail_path,
                    caption=html_caption,
                    parse_mode=ParseMode.HTML,
                    supports_streaming=True,
                    progress=upload_progress
                )
                upload_success = True
                logger.info("Successfully uploaded with HTML formatting")
            except Exception as html_error:
                logger.warning(f"HTML upload failed: {html_error}")

                # Fall back to plain text
                try:
                    plain_caption = caption.replace('**', '').replace('`', '')
                    message = await client.send_video(
                        chat_id=GROUP_ID,
                        video=video_path,
                        thumb=thumbnail_path,
                        caption=plain_caption,
                        supports_streaming=True,
                        progress=upload_progress
                    )
                    upload_success = True
                    logger.info("Successfully uploaded with plain text formatting")
                except Exception as plain_error:
                    logger.error(f"All formatting attempts failed: {plain_error}")

        if upload_success:
            logger.info(f"Successfully uploaded video to Telegram")
            return True
        else:
            logger.error("Failed to upload with any formatting method")
            return False

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
                        part_caption = f"🎬 {name}\n\n📁 File: {part_filename}"
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
                caption = f"🎬 {name}\n\n📁 File: {filename}"
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
