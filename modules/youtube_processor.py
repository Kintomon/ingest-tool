"""YouTube Video Metadata and Comments Extractor"""

import yt_dlp
import os
import logging
from pathlib import Path
from typing import Dict, List
import json
import re

logger = logging.getLogger(__name__)

class YouTubeProcessor:
    def extract_video_info(self, youtube_url: str) -> Dict:
        ydl_opts = {
            "format": "best[ext=mp4]",
            'quiet': True,
            'no_warnings': True,
            'extract_flat': False,
            'noplaylist': True
        }
        
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(youtube_url, download=False)
                
                video_data = {
                    'title': info.get('title', ''),
                    'description': info.get('description', ''),
                    'keywords': info.get('tags', []),
                }
                
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
        video_id = youtube_url.split('watch?v=')[-1].split('&')[0]
        
        cache_dir = Path("cache")
        cache_dir.mkdir(exist_ok=True)
        cache_file = cache_dir / f"comments_cache_{video_id}.json"
        
        if cache_file.exists():
            logger.info(f"📂 Loading raw comments from cache: {cache_file}")
            with open(cache_file, 'r') as f:
                cached_data = json.load(f)
                raw_comments_data = cached_data['comments']
                
                comments = []
                stats = {'with_timestamp': 0, 'without_timestamp': 0, 'total': 0, 'with_replies': 0}
                self._process_flat_comments(raw_comments_data, comments, stats)
                
                return comments, stats
        
        ydl_opts = {
            'getcomments': True,
            'no_warnings': True,
            'writecomments': False,
        }
        
        comments = []
        raw_comments_data = []
        
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(youtube_url, download=False)
                
                comment_entries = info.get('comments', [])
                
                stats = {'with_timestamp': 0, 'without_timestamp': 0, 'total': 0, 'with_replies': 0}
                
                self._process_flat_comments(comment_entries, comments, stats)
                
                logger.info(f"💾 Saving raw comments to cache: {cache_file}")
                with open(cache_file, 'w') as f:
                    json.dump({'comments': comment_entries, 'stats': stats}, f, indent=2)
                
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
        video_id = youtube_url.split('watch?v=')[-1].split('&')[0]
        
        cache_dir = Path("cache")
        cache_dir.mkdir(exist_ok=True)
        cache_file = cache_dir / f"livechat_cache_{video_id}.json"
        
        if cache_file.exists():
            logger.info(f"📂 Loading live chat from cache: {cache_file}")
            with open(cache_file, 'r') as f:
                cached_data = json.load(f)
                live_chats = cached_data.get('live_chats', [])
                stats = cached_data.get('stats', {'total': 0})
                return live_chats, stats
        
        ydl_opts = {
            'writesubtitles': True,
            'writeautomaticsub': False,
            'subtitleslangs': ['live_chat'],
            'subtitlesformat': 'json',
            'skip_download': True,
            'no_warnings': True,
            'quiet': False,
        }
        
        live_chats = []
        stats = {'total': 0}
        
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(youtube_url, download=False)
                
                subtitles = info.get('subtitles', {})
                live_chat_subtitle = subtitles.get('live_chat', [])
                
                if not live_chat_subtitle:
                    logger.info("No live chat available for this video")
                    return [], stats
                
                ydl_opts_download = {
                    'writesubtitles': True,
                    'writeautomaticsub': False,
                    'subtitleslangs': ['live_chat'],
                    'subtitlesformat': 'json',
                    'skip_download': True,
                    'outtmpl': str(cache_dir / '%(id)s.%(ext)s'),
                    'no_warnings': True,
                    'quiet': False,
                }
                
                if os.path.exists('cookies.txt'):
                    ydl_opts_download['cookiefile'] = 'cookies.txt'
                
                with yt_dlp.YoutubeDL(ydl_opts_download) as ydl_download:
                    ydl_download.download([youtube_url])
                
                live_chat_files = list(cache_dir.glob(f"{video_id}.live_chat.json"))
                
                if live_chat_files:
                    logger.info(f"📂 Parsing live chat file: {live_chat_files[0]}")
                    live_chats = self._parse_json_live_chat(live_chat_files[0])
                    stats['total'] = len(live_chats)
                
                if live_chats:
                    logger.info(f"💾 Saving {len(live_chats)} live chat messages to cache: {cache_file}")
                    with open(cache_file, 'w') as f:
                        json.dump({'live_chats': live_chats, 'stats': stats}, f, indent=2)
                else:
                    logger.info("No live chat messages extracted")
                
                return live_chats, stats
                
        except Exception as e:
            logger.warning(f"Error extracting live chat: {e}", exc_info=True)
            return [], stats
    
    def _parse_json_live_chat(self, json_file: Path) -> List[Dict]:
        live_chats = []
        
        with open(json_file, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                if line_num == 1:
                    continue
                
                line = line.strip()
                if not line:
                    continue
                
                try:
                    data = json.loads(line)
                    
                    replay_chat_item_action = data.get('replayChatItemAction', {})
                    video_offset_msec_str = replay_chat_item_action.get('videoOffsetTimeMsec', '0')
                    
                    try:
                        video_offset_msec = int(video_offset_msec_str)
                        timestamp_seconds = video_offset_msec // 1000
                    except (ValueError, TypeError):
                        timestamp_seconds = 0
                    
                    actions = replay_chat_item_action.get('actions', [])
                    if not actions:
                        continue
                    
                    add_chat_item_action = actions[0].get('addChatItemAction', {})
                    item = add_chat_item_action.get('item', {})
                    
                    text_message_renderer = item.get('liveChatTextMessageRenderer')
                    if not text_message_renderer:
                        continue
                    
                    message_runs = text_message_renderer.get('message', {}).get('runs', [])
                    message_text_parts = []
                    for run in message_runs:
                        if 'text' in run:
                            message_text_parts.append(run['text'])
                    
                    message_text = ''.join(message_text_parts).strip()
                    if not message_text:
                        continue
                    
                    author_name = text_message_renderer.get('authorName', {}).get('simpleText', 'Unknown')
                    
                    message_id = text_message_renderer.get('id') or add_chat_item_action.get('clientId', f"livechat_{len(live_chats)}")
                    
                    live_chat_msg = {
                        'comment': message_text,
                        'user_name': author_name,
                        'created_by_id': '',
                        'profile_picture': '',
                        'commented_at': str(timestamp_seconds),
                        'yt_id': message_id,
                        'parent_id': None,
                    }
                    live_chats.append(live_chat_msg)
                    
                except json.JSONDecodeError as e:
                    logger.warning(f"Line {line_num}: Error parsing live chat JSON: {e} - Line content: {line[:200]}...")
                except Exception as e:
                    logger.warning(f"Line {line_num}: Unexpected error processing live chat JSON: {e} - Line content: {line[:200]}...")
        
        return live_chats
    
    def _process_flat_comments(self, comment_entries: List[Dict], comments: List[Dict], stats: Dict):
        parent_comments = []
        reply_comments = []
        parents_with_replies = set()
        
        for raw_comment in comment_entries:
            parent_field = raw_comment.get('parent', 'root')
            
            if parent_field == 'root':
                parent_comments.append(raw_comment)
            else:
                reply_comments.append(raw_comment)
                parents_with_replies.add(parent_field)
        
        stats['with_replies'] = len(parents_with_replies)
        
        for raw_comment in parent_comments:
            comment_text = raw_comment.get('text', '')
            commented_at = self._extract_timestamp(comment_text)
            
            if commented_at > 0:
                comment_text = self._remove_timestamp_from_text(comment_text)
                stats['with_timestamp'] += 1
            else:
                stats['without_timestamp'] += 1
            
            stats['total'] += 1
            
            yt_id = raw_comment.get('id', '')
            
            mapped_comment = {
                'comment': comment_text.strip(),
                'user_name': raw_comment.get('author', 'Unknown User'),
                'created_by_id': '',
                'profile_picture': raw_comment.get('author_thumbnail', ''),
                'commented_at': str(commented_at),
                'yt_id': yt_id,
                'parent_id': None,
            }
            
            comments.append(mapped_comment)
        
        for raw_comment in reply_comments:
            comment_text = raw_comment.get('text', '')
            commented_at = self._extract_timestamp(comment_text)
            
            if commented_at > 0:
                comment_text = self._remove_timestamp_from_text(comment_text)
                stats['with_timestamp'] += 1
            else:
                stats['without_timestamp'] += 1
            
            stats['total'] += 1
            
            parent_yt_id = raw_comment.get('parent', '')
            
            mapped_comment = {
                'comment': comment_text.strip(),
                'user_name': raw_comment.get('author', 'Unknown User'),
                'created_by_id': '',
                'profile_picture': raw_comment.get('author_thumbnail', ''),
                'commented_at': str(commented_at),
                'yt_id': raw_comment.get('id', ''),
                'parent_id': parent_yt_id,
            }
            
            comments.append(mapped_comment)
    
    
    @staticmethod
    def _extract_timestamp(text: str) -> int:
        
        patterns = [
            r'(?:at|@)\s*(\d{1,2}):(\d{2})(?::(\d{2}))?',
            r'(\d{1,2}):(\d{2})(?::(\d{2}))?\s+(?:is|was|at)',
            r'^(\d{1,2}):(\d{2})(?::(\d{2}))?$',
            r'(\d{1,2}):(\d{2})(?::(\d{2}))?',
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
        
        return 0
    
    @staticmethod
    def _remove_timestamp_from_text(text: str) -> str:
        
        patterns = [
            r'(?:at|@)\s*\d{1,2}:\d{2}(?::\d{2})?',
            r'\d{1,2}:\d{2}(?::\d{2})?\s+(?:is|was|at)',
            r'^\d{1,2}:\d{2}(?::\d{2})?\s*',
            r'\d{1,2}:\d{2}(?::\d{2})?',
        ]
        
        cleaned_text = text
        for pattern in patterns:
            cleaned_text = re.sub(pattern, '', cleaned_text, flags=re.IGNORECASE)
        
        cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()
        
        return cleaned_text
    
    @staticmethod
    def _parse_timestamp_value(timestamp_value) -> int:
        if isinstance(timestamp_value, (int, float)):
            return int(timestamp_value)
        elif isinstance(timestamp_value, str):
            match = re.match(r'(\d{1,2}):(\d{2})(?::(\d{2}))?', timestamp_value)
            if match:
                groups = match.groups()
                if len(groups) == 3 and groups[2]:
                    hours, minutes, seconds = map(int, groups)
                    return hours * 3600 + minutes * 60 + seconds
                elif len(groups) >= 2:
                    minutes, seconds = map(int, groups[:2])
                    return minutes * 60 + seconds
            try:
                return int(float(timestamp_value))
            except:
                pass
        return 0
    
    def download_video(self, youtube_url: str, output_dir: str = "cache") -> str:
        cache_dir = Path(output_dir)
        cache_dir.mkdir(exist_ok=True)
        
        video_id = youtube_url.split('watch?v=')[-1].split('&')[0]
        
        cached_files = list(cache_dir.glob(f"{video_id}.*"))
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
        
        output_path = cache_dir / f"{video_id}.%(ext)s"
        
        ydl_opts = {
            'outtmpl': str(output_path),
            'format':"22/18/bestvideo+bestaudio/best",
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
        
        if os.path.exists('cookies.txt'):
            ydl_opts['cookiefile'] = 'cookies.txt'
            logger.info("Using cookies.txt for download authentication")
        
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                logger.info(f"Downloading video from: {youtube_url}")
                ydl.download([youtube_url])
                
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
