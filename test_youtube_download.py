#!/usr/bin/env python3
"""
Simple test script for YouTube video download only
"""

import yt_dlp
import os
import sys
import tempfile
import shutil

# YouTube cookies file path (same as in schema.py)
YOUTUBE_COOKIES_FILE = "youtube_cookies.txt"

# Test YouTube URL
YOUTUBE_URL = sys.argv[1] if len(sys.argv) > 1 else "https://www.youtube.com/watch?v=N9Ucdn3Mr6Y"

print(f"Testing YouTube download: {YOUTUBE_URL}")

# Create temporary directory for download
temp_dir = tempfile.mkdtemp()
output_path = os.path.join(temp_dir, '%(title)s.%(ext)s')

# yt-dlp options (same as in schema.py)
ydl_opts = {
    'outtmpl': output_path,
    "format": "bestvideo+bestaudio/best",
    "merge_output_format": "mp4",
    'quiet': False,
    'noplaylist': True,
}

# Add cookies if available
if os.path.exists(YOUTUBE_COOKIES_FILE):
    print(f"Using cookies file: {YOUTUBE_COOKIES_FILE}")
    ydl_opts['cookiefile'] = YOUTUBE_COOKIES_FILE
else:
    print(f"Warning: Cookies file not found at {YOUTUBE_COOKIES_FILE}")

try:
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        print("Downloading video...")
        ydl.download([YOUTUBE_URL])
        print("Download completed!")
        
        # Get file info
        info = ydl.extract_info(YOUTUBE_URL, download=False)
        downloaded_file_path = ydl.prepare_filename(info)
        
        if os.path.exists(downloaded_file_path):
            file_size = os.path.getsize(downloaded_file_path)
            print(f"\nSUCCESS!")
            print(f"File: {downloaded_file_path}")
            print(f"Size: {file_size} bytes ({file_size / (1024*1024):.2f} MB)")
            print(f"Title: {info.get('title', 'Unknown')}")
        else:
            print(f"\nERROR: File not found at {downloaded_file_path}")
            
except Exception as e:
    print(f"\nERROR: {e}")
    import traceback
    traceback.print_exc()
finally:
    # Cleanup
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
        print(f"\nCleaned up temp directory: {temp_dir}")

