#!/usr/bin/env python3
"""
Google Drive URL Utility
Converts Google Drive sharing URLs to direct download links
"""

import sys
import re

def convert_google_drive_url(url):
    """
    Convert Google Drive sharing URL to direct download URL

    Input formats supported:
    - https://drive.google.com/file/d/FILE_ID/view?usp=sharing
    - https://drive.google.com/open?id=FILE_ID
    - https://drive.google.com/file/d/FILE_ID/view

    Output format:
    - https://drive.google.com/uc?export=download&id=FILE_ID
    """

    # Pattern 1: /file/d/FILE_ID/view format
    pattern1 = r'drive\.google\.com/file/d/([a-zA-Z0-9_-]+)'
    match1 = re.search(pattern1, url)

    if match1:
        file_id = match1.group(1)
        return f"https://drive.google.com/uc?export=download&id={file_id}"

    # Pattern 2: open?id=FILE_ID format
    pattern2 = r'drive\.google\.com/open\?id=([a-zA-Z0-9_-]+)'
    match2 = re.search(pattern2, url)

    if match2:
        file_id = match2.group(1)
        return f"https://drive.google.com/uc?export=download&id={file_id}"

    # If already a direct download URL, return as is
    if 'drive.google.com/uc?export=download' in url:
        return url

    # If no pattern matches, return original URL
    return url

def main():
    if len(sys.argv) != 2:
        print("Usage: python gdrive_converter.py <google_drive_url>")
        print("\nExample:")
        print("python gdrive_converter.py 'https://drive.google.com/file/d/1ABC123DEF456/view?usp=sharing'")
        sys.exit(1)

    input_url = sys.argv[1]
    converted_url = convert_google_drive_url(input_url)

    print("Original URL:")
    print(input_url)
    print("\nDirect Download URL:")
    print(converted_url)

    if input_url != converted_url:
        print("\n✅ URL converted successfully!")
    else:
        print("\n⚠️  URL was not converted (already direct or unsupported format)")

if __name__ == "__main__":
    main()
