"""YouTube Video Metadata and Comments Extractor using yt-dlp"""

import yt_dlp
import os
import logging
from pathlib import Path
from typing import Dict, List
import json

logger = logging.getLogger(__name__)

class YouTubeProcessor:
    """Extract only: title, description, keywords, and comments"""
    
    def extract_video_info(self, youtube_url: str) -> Dict:
        """
        Extract only: title, description, keywords (metadata only, no download)
        
        Returns:
            dict: {
                'title': str,
                'description': str,
                'keywords': list
            }
        """
        ydl_opts = {
            "format": "best[ext=mp4]",
            'quiet': True,
            'no_warnings': True,
            'extract_flat': False,
            'noplaylist': True
        }
        
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                # NOTE: download=False means NO video file download, only metadata
                info = ydl.extract_info(youtube_url, download=False)
                
                video_data = {
                    'title': info.get('title', ''),
                    'description': info.get('description', ''),  # Full description
                    'keywords': info.get('tags', []),
                }
                
                # Store raw yt-dlp data for comparison
                video_data['_raw_ytdlp'] = {
                    'title': info.get('title', ''),
                    'description': info.get('description', ''),
                    'tags': info.get('tags', []),
                }
                
                return video_data
                
        except Exception as e:
            logger.error(f"Error extracting video info: {e}", exc_info=True)
            raise
    
    def extract_comments(self, youtube_url: str) -> tuple[List[Dict], dict]:
        """
        Extract comments from YouTube video and process one by one
        Uses cache file to avoid re-extracting (only unprocessed comments)
        """
        # Use cache file based on video ID
        video_id = youtube_url.split('watch?v=')[-1].split('&')[0]
        
        # Create cache directory if it doesn't exist
        cache_dir = Path("cache")
        cache_dir.mkdir(exist_ok=True)
        cache_file = cache_dir / f"comments_cache_{video_id}.json"
        
        # Check if cache exists
        if cache_file.exists():
            logger.info(f"📂 Loading raw comments from cache: {cache_file}")
            with open(cache_file, 'r') as f:
                cached_data = json.load(f)
                raw_comments_data = cached_data['comments']  # Raw unprocessed comments
                
                # Process cached raw comments
                comments = []
                stats = {'with_timestamp': 0, 'without_timestamp': 0, 'total': 0, 'with_replies': 0}
                self._process_comments_recursive(raw_comments_data, comments, stats, parent_id=None, raw_data=None)
                return comments, stats
        
        # Extract comments if no cache
        ydl_opts = {
            'getcomments': True,
            'no_warnings': True,
            'writecomments': False,
        }
        
        comments = []
        raw_comments_data = []  # Store unprocessed raw comments for cache
        
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                # Extract comments with getcomments=True
                info = ydl.extract_info(youtube_url, download=False)
                
                # Get comments from yt-dlp response
                comment_entries = info.get('comments', [])
                
                # Count comments and timestamps
                stats = {'with_timestamp': 0, 'without_timestamp': 0, 'total': 0, 'with_replies': 0}
                
                # Process comments recursively (parent, then replies)
                self._process_comments_recursive(comment_entries, comments, stats, parent_id=None, raw_data=raw_comments_data)
                
                # Save UNPROCESSED raw comments to cache (for next time to avoid yt-dlp call)
                logger.info(f"💾 Saving raw comments to cache: {cache_file}")
                with open(cache_file, 'w') as f:
                    json.dump({'comments': raw_comments_data, 'stats': stats}, f, indent=2)
                
                # Return comments and timestamp stats
                return comments, {
                    'with_timestamp': stats['with_timestamp'],
                    'without_timestamp': stats['without_timestamp'],
                    'with_replies': stats['with_replies'],
                    'total': stats['total']
                }
                
        except Exception as e:
            logger.warning(f"Error extracting comments: {e}", exc_info=True)
            return [], {'with_timestamp': 0, 'without_timestamp': 0, 'with_replies': 0, 'total': 0}
    
    def extract_live_chat(self, youtube_url: str) -> tuple[List[Dict], dict]:
        """
        Extract live chat messages from YouTube video
        Live chats already have timestamps, so no extraction needed
        
        Returns:
            tuple: (list of live chat messages, stats dict)
        """
        video_id = youtube_url.split('watch?v=')[-1].split('&')[0]
        
        # Create cache directory if it doesn't exist
        cache_dir = Path("cache")
        cache_dir.mkdir(exist_ok=True)
        cache_file = cache_dir / f"livechat_cache_{video_id}.json"
        
        # Check if cache exists
        if cache_file.exists():
            logger.info(f"📂 Loading live chat from cache: {cache_file}")
            with open(cache_file, 'r') as f:
                cached_data = json.load(f)
                live_chats = cached_data.get('live_chats', [])
                stats = cached_data.get('stats', {'total': 0})
                return live_chats, stats
        
        # Extract live chat using yt-dlp - download as VTT format
        ydl_opts = {
            'writesubtitles': True,
            'writeautomaticsub': False,
            'subtitleslangs': ['live_chat'],
            'subtitlesformat': 'vtt',  # Download as VTT format
            'skip_download': True,
            'no_warnings': True,
            'quiet': False,
        }
        
        live_chats = []
        stats = {'total': 0}
        
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(youtube_url, download=False)
                
                # Check if live chat is available in subtitles
                subtitles = info.get('subtitles', {})
                live_chat_subtitle = subtitles.get('live_chat', [])
                
                if not live_chat_subtitle:
                    logger.info("No live chat available for this video")
                    return [], stats
                
                # Download live chat VTT file directly to cache folder
                ydl_opts_download = {
                    'writesubtitles': True,
                    'writeautomaticsub': False,
                    'subtitleslangs': ['en'],
                    'subtitlesformat': 'vtt',  # Download as VTT format
                    'skip_download': True,
                    'outtmpl': str(cache_dir / '%(id)s.%(ext)s'),
                    'no_warnings': True,
                    'quiet': False,
                }
                
                # Check for cookies.txt
                if os.path.exists('cookies.txt'):
                    ydl_opts_download['cookiefile'] = 'cookies.txt'
                
                with yt_dlp.YoutubeDL(ydl_opts_download) as ydl_download:
                    ydl_download.download([youtube_url])
                
                # Look for live chat VTT file in cache folder
                live_chat_files = list(cache_dir.glob(f"{video_id}.en.vtt"))
                
                if live_chat_files:
                    live_chat_file = live_chat_files[0]
                    logger.info(f"Found live chat VTT file: {live_chat_file}")
                    
                    # Parse VTT file
                    live_chats = self._parse_vtt_live_chat(live_chat_file)
                    stats['total'] = len(live_chats)
                else:
                    logger.debug("Live chat VTT file not found after download attempt")
                
                # Save to cache
                if live_chats:
                    logger.info(f"💾 Saving {len(live_chats)} live chat messages to cache: {cache_file}")
                    with open(cache_file, 'w') as f:
                        json.dump({'live_chats': live_chats, 'stats': stats}, f, indent=2)
                else:
                    logger.info("No live chat messages extracted")
                
                return live_chats, stats
                
        except Exception as e:
            logger.warning(f"Error extracting live chat: {e}", exc_info=True)
            # Live chat might not be available, which is fine
            return [], stats
    
    def _parse_vtt_live_chat(self, vtt_file: Path) -> List[Dict]:
        """
        Parse VTT format live chat file
        
        VTT format:
        00:08:39.050 --> 00:08:44.010
        Author Name:
        Message text here
        
        Returns:
            List of live chat message dicts
        """
        import re
        
        live_chats = []
        
        with open(vtt_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Split by double newlines to get cue blocks
        # Skip WEBVTT header
        blocks = re.split(r'\n\n+', content)
        
        for block in blocks:
            block = block.strip()
            if not block or block.startswith('WEBVTT') or block.startswith('Kind:') or block.startswith('Language:'):
                continue
            
            lines = block.split('\n')
            if len(lines) < 2:
                continue
            
            # First line should be timestamp: "00:08:39.050 --> 00:08:44.010"
            timestamp_line = lines[0].strip()
            timestamp_match = re.match(r'(\d{2}):(\d{2}):(\d{2})\.(\d{3})\s*-->\s*', timestamp_line)
            
            if not timestamp_match:
                continue
            
            # Convert timestamp to seconds
            hours, minutes, seconds, milliseconds = map(int, timestamp_match.groups())
            timestamp_seconds = hours * 3600 + minutes * 60 + seconds
            
            # Next line(s) contain author and message
            # Format: "Author Name:\nMessage text" or "Author Name:\nMessage line 1\nMessage line 2"
            if len(lines) < 2:
                continue
            
            # First line after timestamp is usually the author
            first_content_line = lines[1].strip()
            author = 'Unknown'
            message_lines = []
            
            # Check if first line is author (ends with colon)
            if first_content_line.endswith(':'):
                author = first_content_line[:-1].strip()
                message_lines = lines[2:]  # Rest are message lines
            else:
                # No author line, all content is message
                message_lines = lines[1:]
            
            message = '\n'.join(message_lines).strip()
            
            if not message:
                continue
            
            live_chat_msg = {
                'comment': message,
                'user_name': author,
                'created_by_id': '',
                'profile_picture': '',  # VTT doesn't have profile pictures
                'commented_at': str(timestamp_seconds),
                'yt_id': f"livechat_{len(live_chats)}",
                'parent_id': None,
            }
            live_chats.append(live_chat_msg)
        
        return live_chats
    
    def _process_comments_recursive(
        self, 
        comment_entries: List[Dict], 
        comments: List[Dict], 
        stats: Dict,
        parent_id: str = None,
        raw_data: List[Dict] = None
    ):
        """
        Process comments: parent first, then replies (2 levels only)
        """
        if raw_data is None:
            raw_data = []
        
        for raw_comment in comment_entries:
            
            # Store raw comment data (UNPROCESSED) for cache
            raw_data.append(raw_comment)
            
            # Log raw comment (debug level)
            logger.debug(f"Raw comment: {json.dumps(raw_comment, indent=2, default=str, ensure_ascii=False)}")
            
            # Extract video timeline position from comment text
            comment_text = raw_comment.get('text', '')
            commented_at = self._extract_timestamp(comment_text)
            
            # Remove timestamp strings from comment text (e.g., "22:03", "at 1:30")
            if commented_at > 0:
                comment_text = self._remove_timestamp_from_text(comment_text)
                stats['with_timestamp'] += 1
            else:
                stats['without_timestamp'] += 1
            
            stats['total'] += 1
            
            mapped_comment = {
                'comment': comment_text.strip(),  # Clean comment text
                'user_name': raw_comment.get('author', 'Unknown User'),
                'created_by_id': '',
                'profile_picture': raw_comment.get('author_thumbnail', ''),
                'commented_at': str(commented_at),  # Seconds as string (e.g., "1323")
                'yt_id': raw_comment.get('id', ''),
                'parent_id': parent_id,  # Set parent_id for replies
            }
            
            comments.append(mapped_comment)
            
            # Process replies if exists (replies don't have replies)
            if 'replies' in raw_comment and raw_comment.get('replies'):
                stats['with_replies'] += 1
                replies = raw_comment['replies']
                reply_list = []
                
                if isinstance(replies, dict) and 'comments' in replies:
                    reply_list = replies['comments']
                elif isinstance(replies, list):
                    reply_list = replies
                
                if reply_list:
                    # Process replies
                    self._process_comments_recursive(
                        reply_list, 
                        comments, 
                        stats, 
                        parent_id=raw_comment.get('id', ''),
                        raw_data=raw_data
                    )
    
    @staticmethod
    def _extract_timestamp(text: str) -> int:
        """
        Try to extract video timestamp from comment text
        Looks for patterns like: "at 1:30", "2:45", "10:15:30", etc.
        Returns 0 if no timestamp found
        """
        import re
        
        patterns = [
            r'(?:at|@)\s*(\d{1,2}):(\d{2})(?::(\d{2}))?',  # "at 1:30" or "at 10:15:30"
            r'(\d{1,2}):(\d{2})(?::(\d{2}))?\s+(?:is|was|at)',  # "1:30 is"
            r'^(\d{1,2}):(\d{2})(?::(\d{2}))?$',  # Just "1:30" at start
            r'(\d{1,2}):(\d{2})(?::(\d{2}))?',  # Any "1:30" pattern
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                groups = match.groups()
                if len(groups) == 3 and groups[2]:
                    hours, minutes, seconds = map(int, groups)
                    return hours * 3600 + minutes * 60 + seconds
                elif len(groups) >= 2:
                    minutes, seconds = map(int, groups[:2])
                    return minutes * 60 + seconds
        
        return 0  # No timestamp found
    
    @staticmethod
    def _remove_timestamp_from_text(text: str) -> str:
        """
        Remove timestamp patterns from comment text
        Removes patterns like: "at 1:30", "22:03", "10:15:30", etc.
        """
        import re
        
        # Patterns to remove (same as extraction patterns)
        patterns = [
            r'(?:at|@)\s*\d{1,2}:\d{2}(?::\d{2})?',  # "at 1:30" or "at 10:15:30"
            r'\d{1,2}:\d{2}(?::\d{2})?\s+(?:is|was|at)',  # "1:30 is"
            r'^\d{1,2}:\d{2}(?::\d{2})?\s*',  # Just "1:30" at start
            r'\d{1,2}:\d{2}(?::\d{2})?',  # Any "1:30" pattern
        ]
        
        cleaned_text = text
        for pattern in patterns:
            cleaned_text = re.sub(pattern, '', cleaned_text, flags=re.IGNORECASE)
        
        # Clean up extra whitespace
        cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()
        
        return cleaned_text
    
    @staticmethod
    def _parse_timestamp_value(timestamp_value) -> int:
        """Parse timestamp value (could be int, float, string, etc.) to seconds"""
        if isinstance(timestamp_value, (int, float)):
            return int(timestamp_value)
        elif isinstance(timestamp_value, str):
            # Try to parse string like "1:30" or "90"
            import re
            match = re.match(r'(\d{1,2}):(\d{2})(?::(\d{2}))?', timestamp_value)
            if match:
                groups = match.groups()
                if len(groups) == 3 and groups[2]:
                    hours, minutes, seconds = map(int, groups)
                    return hours * 3600 + minutes * 60 + seconds
                elif len(groups) >= 2:
                    minutes, seconds = map(int, groups[:2])
                    return minutes * 60 + seconds
            # Try to parse as number
            try:
                return int(float(timestamp_value))
            except:
                pass
        return 0
    
    def download_video(self, youtube_url: str, output_dir: str = "cache") -> str:
        """
        Download video file from YouTube to local directory
        Uses cache to avoid re-downloading if video already exists
        
        Args:
            youtube_url: YouTube video URL
            output_dir: Directory to save video file (default: "cache")
            
        Returns:
            str: Path to downloaded video file
        """
        # Create output directory if it doesn't exist
        cache_dir = Path(output_dir)
        cache_dir.mkdir(exist_ok=True)
        
        # Get video ID for filename
        video_id = youtube_url.split('watch?v=')[-1].split('&')[0]
        
        # Check if video already exists in cache (any extension)
        cached_files = list(cache_dir.glob(f"{video_id}.*"))
        # Filter out non-video files (like .json cache files)
        video_extensions = {'.mp4', '.webm', '.mkv', '.flv', '.3gp', '.avi', '.mov', '.m4v'}
        cached_video = None
        for cached_file in cached_files:
            if cached_file.suffix.lower() in video_extensions:
                cached_video = cached_file
                break
        
        if cached_video and cached_video.exists():
            file_size = os.path.getsize(cached_video)
            logger.info(f"📂 Using cached video: {cached_video} ({file_size / (1024*1024):.2f} MB)")
            return str(cached_video)
        
        # If not cached, proceed with download
        output_path = cache_dir / f"{video_id}.%(ext)s"
        
        ydl_opts = {
            'outtmpl': str(output_path),
            # "format": "bestvideo[ext=webm]+bestaudio/bestvideo[ext=mp4]+bestaudio/bestvideo+bestaudio/best",
            'format':"18",
            'quiet': False,
            'noplaylist': True,
            'http_headers': {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                "Referer": "https://www.youtube.com/"
            }
        }
        
        # Check for cookies.txt
        if os.path.exists('cookies.txt'):
            ydl_opts['cookiefile'] = 'cookies.txt'
            logger.info("Using cookies.txt for download authentication")
        
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                logger.info(f"Downloading video from: {youtube_url}")
                # Download the video
                ydl.download([youtube_url])
                
                # Get the actual downloaded file path
                info = ydl.extract_info(youtube_url, download=False)
                downloaded_file = ydl.prepare_filename(info)
                
                if not os.path.exists(downloaded_file):
                    raise RuntimeError(f"Downloaded file not found at: {downloaded_file}")
                
                file_size = os.path.getsize(downloaded_file)
                logger.info(f"✅ Video downloaded: {downloaded_file} ({file_size / (1024*1024):.2f} MB)")
                
                return downloaded_file
                
        except Exception as e:
            logger.error(f"Error downloading video: {e}", exc_info=True)
            raise
