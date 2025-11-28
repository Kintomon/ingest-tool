"""Batch Processor"""

import json
import logging
import os
from typing import List, Dict

logger = logging.getLogger(__name__)


class BatchProcessor:
    def __init__(self, youtube_processor, asset_creator, comment_importer, user_randomizer):
        self.youtube_processor = youtube_processor
        self.asset_creator = asset_creator
        self.comment_importer = comment_importer
        self.user_randomizer = user_randomizer
    
    def load_list_file(self, file_path: str, comments_only: bool = False) -> List[Dict]:
        videos = []
        
        with open(file_path, 'r') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                
                try:
                    parts = [p.strip() for p in line.split(',')]
                    if len(parts) >= 2:
                        videos.append({
                            'url': parts[0],
                            'category': parts[1],
                        })
                    else:
                        logger.warning(f"Line {line_num}: Invalid format, expected: url,category")
                except Exception as e:
                    logger.warning(f"Line {line_num}: Error parsing: {e}")
        
        logger.info(f"📋 Loaded {len(videos)} videos from {file_path}")
        return videos
    
    def process_video(self, video_url: str, category: str, dry_run: bool = False, video_only: bool = False, comments_only: bool = False, asset_id: str = None, max_items_limit: int = None, skip_live_chat: bool = False) -> Dict:
        logger.info("=" * 80)
        logger.info(f"🎬 Processing: {video_url}")
        logger.info(f"📁 Category: {category}")
        logger.info("=" * 80)
        
        result = {
            'url': video_url,
            'category': category,
            'success': False,
            'asset_id': None,
            'error': None,
            'comments_imported': 0,
        }
        
        downloaded_file = None
        
        try:
            logger.info("[Step 1/5] Extracting video metadata...")
            video_info = self.youtube_processor.extract_video_info(video_url)
            logger.info(f"📹 Title: {video_info['title']}")
            
            if comments_only:
                logger.info("[Mode] COMMENTS ONLY - Skipping video download/upload")
                if not asset_id:
                    if dry_run:
                        asset_id = "dry-run-asset-id"
                        logger.info(f"Using dummy asset_id for dry_run: {asset_id}")
                    else:
                        error_msg = "COMMENTS_ONLY mode requires asset_id in config.yaml"
                        logger.error(f"❌ {error_msg}")
                        result['error'] = error_msg
                        return result
                
                result['asset_id'] = asset_id
                logger.info(f"📌 Using configured asset_id: {asset_id}")
            
            if not comments_only:
                logger.info("[Step 2/5] Downloading video from YouTube...")
                downloaded_file = self.youtube_processor.download_video(video_url, output_dir="cache")
                
                logger.info("[Step 3/5] Getting signed URL from backend...")
                file_name = os.path.basename(downloaded_file)
                
                metadata_payload = {}
                if category:
                    metadata_payload['categories'] = [category]
                if video_info.get('keywords'):
                    metadata_payload['preferredKeywords'] = video_info.get('keywords', [])
                
                signed_url_result = self.asset_creator.get_signed_url(
                    file_name=file_name,
                    asset_name=video_info['title'],
                    asset_description=video_info['description'],
                    metadata=metadata_payload if metadata_payload else None
                )
                
                if signed_url_result.get('error'):
                    result['error'] = f"Failed to get signed URL: {signed_url_result['error']}"
                    return result
                
                upload_url = signed_url_result['upload_url']
                asset_id = signed_url_result['asset_id']
                result['asset_id'] = asset_id
                logger.info(f"✅ Got signed URL for asset: {asset_id}")
                
                logger.info("[Step 4/5] Uploading video file to signed URL...")
                upload_result = self.asset_creator.upload_file_to_signed_url(
                    file_path=downloaded_file,
                    upload_url=upload_url
                )
                
                if not upload_result.get('success'):
                    result['error'] = f"Upload failed: {upload_result.get('error', 'Unknown error')}"
                    return result
                
                logger.info("✅ Video uploaded successfully!")
                
                try:
                    if os.path.exists(downloaded_file):
                        os.remove(downloaded_file)
                        logger.info(f"🗑️  Cleaned up downloaded file: {downloaded_file}")
                except Exception as e:
                    logger.warning(f"Failed to clean up downloaded file: {e}")
                
                if video_only:
                    logger.info("[Mode] VIDEO ONLY - Skipping comments import")
                    result['success'] = True
                    logger.info("✅ Video upload complete!")
                    return result
            
            livechat_imported = 0
            
            if skip_live_chat:
                logger.info("[Step 5/5] Skipping live chat extraction (skip_live_chat=true)")
            else:
                logger.info("[Step 5/5] Extracting and importing live chats...")
            live_chats, livechat_stats = self.youtube_processor.extract_live_chat(video_url)
            
            if live_chats:
                original_count = len(live_chats)
                if max_items_limit and max_items_limit > 0 and original_count > max_items_limit:
                    live_chats = live_chats[:max_items_limit]
                    logger.info(f"📊 Limiting live chats to first {max_items_limit} (out of {original_count} total)")
                
                live_chats = self.user_randomizer.anonymize_comments(live_chats)
                
                if dry_run:
                    logger.info(f"📊 DRY RUN: Found {len(live_chats)} live chat messages (showing import-ready data)...")
                else:
                    logger.info(f"📊 Processing {len(live_chats)} live chat messages...")
                
                    if comments_only and not dry_run and result.get('asset_id'):
                        target_asset_id = result['asset_id']
                    else:
                        target_asset_id = result.get('asset_id') or asset_id
                    
                livechat_stats_result = self.comment_importer.import_live_chats(
                    live_chats, 
                        target_asset_id
                )
                livechat_imported = livechat_stats_result['imported']
                logger.info(f"✅ Live chats processed: {livechat_stats_result['imported']}/{livechat_stats_result['total']}")
            else:
                logger.info("No live chat available for this video")
            
            logger.info("[Step 5/5] Extracting and importing comments (with timestamps only)...")
            comments, timestamp_stats = self.youtube_processor.extract_comments(video_url)
            
            comments_with_timestamp = []
            parent_ids_needed = {}
            
            for comment in comments:
                timestamp = int(comment.get('commented_at', '0') or '0')
                if timestamp > 0:
                    comments_with_timestamp.append(comment)
                    parent_id = comment.get('parent_id')
                    if parent_id:
                        if parent_id not in parent_ids_needed or timestamp < parent_ids_needed[parent_id]:
                            parent_ids_needed[parent_id] = timestamp
            
            yt_ids_in_list = {c.get('yt_id') for c in comments_with_timestamp}
            for comment in comments:
                yt_id = comment.get('yt_id')
                if yt_id in parent_ids_needed and yt_id not in yt_ids_in_list:
                    comment['commented_at'] = str(parent_ids_needed[yt_id])
                    comments_with_timestamp.append(comment)
                    yt_ids_in_list.add(yt_id)
            
            comment_order = {c.get('yt_id'): idx for idx, c in enumerate(comments)}
            comments_with_timestamp.sort(key=lambda c: comment_order.get(c.get('yt_id'), 0))
            
            result['timestamp_stats'] = timestamp_stats
            result['timestamp_stats']['filtered'] = len(comments_with_timestamp)
            result['timestamp_stats']['livechat_imported'] = livechat_imported
            
            if comments_with_timestamp:
                original_count = len(comments_with_timestamp)
                if max_items_limit and max_items_limit > 0 and original_count > max_items_limit:
                    comments_with_timestamp = comments_with_timestamp[:max_items_limit]
                    logger.info(f"📊 Limiting comments to first {max_items_limit} (out of {original_count} total)")
                
                comments_with_timestamp = self.user_randomizer.anonymize_comments(comments_with_timestamp)
                
                if dry_run:
                    logger.info(f"📊 DRY RUN: Found {len(comments_with_timestamp)} comments with timestamps (filtered from {len(comments)} total, showing import-ready data)...")
                else:
                    logger.info(f"📊 Processing {len(comments_with_timestamp)} comments with timestamps (filtered from {len(comments)} total)...")
                
                if comments_only and not dry_run and result.get('asset_id'):
                    target_asset_id = result['asset_id']
                else:
                    target_asset_id = result.get('asset_id') or asset_id
                
                comment_stats = self.comment_importer.import_comments(
                    comments_with_timestamp, 
                    target_asset_id
                )
                result['comments_imported'] = comment_stats['imported']
                logger.info(f"✅ Comments processed: {comment_stats['imported']}/{comment_stats['total']}")
            else:
                logger.warning(f"⚠️  No comments with timestamps to import (found {len(comments)} total comments)")
                result['comments_imported'] = 0
            
            result['success'] = True
            logger.info("✅ Video processing complete!")
            
        except Exception as e:
            result['error'] = str(e)
            result['success'] = False
            logger.error(f"Processing failed for {video_url}: {e}")
        finally:
            if downloaded_file and os.path.exists(downloaded_file):
                try:
                    os.remove(downloaded_file)
                    logger.info(f"🗑️  Cleaned up downloaded file: {downloaded_file}")
                except Exception as e:
                    logger.warning(f"Failed to clean up downloaded file: {e}")
        
        return result
    
    def process_list(self, list_file: str, dry_run: bool = False, video_only: bool = False, comments_only: bool = False, max_items_limit: int = None, asset_id: str = None, skip_live_chat: bool = False) -> List[Dict]:
        videos = self.load_list_file(list_file, comments_only=comments_only)
        results = []
        
        logger.info(f"\n🚀 Starting batch processing: {len(videos)} videos\n")
        
        for idx, video in enumerate(videos, 1):
            logger.info(f"\n[{idx}/{len(videos)}] Processing {video['url']}")
            
            result = self.process_video(
                video_url=video['url'],
                category=video['category'],
                dry_run=dry_run,
                video_only=video_only,
                comments_only=comments_only,
                asset_id=asset_id,
                max_items_limit=max_items_limit,
                skip_live_chat=skip_live_chat
            )
            
            results.append(result)
            
            if result['success']:
                logger.info(f"✓ Success: {result['asset_id']}, {result['comments_imported']} comments")
            else:
                logger.error(f"✗ Failed: {result['error']}")
        
        self._print_summary(results)
        
        return results
    
    @staticmethod
    def _print_summary(results: List[Dict]):
        total = len(results)
        successful = sum(1 for r in results if r['success'])
        failed = total - successful
        total_comments = sum(r.get('comments_imported', 0) for r in results)
        
        # Calculate timestamp stats across all videos
        total_with_timestamp = sum(r.get('timestamp_stats', {}).get('with_timestamp', 0) for r in results)
        total_without_timestamp = sum(r.get('timestamp_stats', {}).get('without_timestamp', 0) for r in results)
        total_with_replies = sum(r.get('timestamp_stats', {}).get('with_replies', 0) for r in results)
        total_comments_processed = sum(r.get('timestamp_stats', {}).get('total', 0) for r in results)
        total_filtered = sum(r.get('timestamp_stats', {}).get('filtered', 0) for r in results)
        total_livechat = sum(r.get('timestamp_stats', {}).get('livechat_imported', 0) for r in results)
        
        logger.info("\n" + "=" * 80)
        logger.info("📊 TIMESTAMP DETECTION SUMMARY")
        logger.info("=" * 80)
        logger.info(f"   ✓ Comments with timestamp: {total_with_timestamp}")
        logger.info(f"   ✗ Comments without timestamp: {total_without_timestamp}")
        logger.info(f"   📤 Comments published (filtered): {total_filtered}")
        logger.info(f"   💬 Live chat messages published: {total_livechat}")
        logger.info(f"   💬 With replies: {total_with_replies}")
        logger.info(f"   Total comments processed: {total_comments_processed}")
        logger.info("=" * 80)
        
        logger.info("\n" + "=" * 80)
        logger.info("📊 BATCH PROCESSING SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total videos: {total}")
        logger.info(f"✅ Successful: {successful}")
        logger.info(f"❌ Failed: {failed}")
        logger.info(f"💬 Total comments imported: {total_comments}")
        logger.info("=" * 80)
