"""Batch Processor - Process list.txt file"""

import json
import logging
import os
from typing import List, Dict

logger = logging.getLogger(__name__)


class BatchProcessor:
    """Process multiple YouTube URLs from list.txt file"""
    
    def __init__(self, youtube_processor, asset_creator, comment_importer, user_randomizer):
        """
        Initialize batch processor
        
        Args:
            youtube_processor: YouTubeProcessor instance
            asset_creator: AssetCreator instance
            comment_importer: CommentImporter instance
            user_randomizer: UserRandomizer instance
        """
        self.youtube_processor = youtube_processor
        self.asset_creator = asset_creator
        self.comment_importer = comment_importer
        self.user_randomizer = user_randomizer
    
    def load_list_file(self, file_path: str, comments_only: bool = False) -> List[Dict]:
        """
        Load from list.txt: youtube_url,category (always just 2 fields)
        
        Returns:
            List of dicts: [{'url': str, 'category': str}, ...]
        """
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
    
    def process_video(self, video_url: str, category: str, dry_run: bool = False, video_only: bool = False, comments_only: bool = False, asset_id: str = None, max_items_limit: int = None) -> Dict:
        """
        Process single video: extract info, download, upload to signed URL, import comments
        
        Works like frontend: download video → get signed URL → upload file → import comments
        
        Args:
            video_url: YouTube video URL
            category: Video category
            dry_run: If True, don't actually upload
            video_only: If True, only upload video (skip comments)
            comments_only: If True, only import comments (skip video upload)
            asset_id: Required if comments_only=True, existing asset ID to import comments to
            max_items_limit: Maximum number of items to process (None = all, number = limit)
        """
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
            # Step 1: Extract video metadata (title, description, keywords)
            logger.info("[Step 1/5] Extracting video metadata...")
            video_info = self.youtube_processor.extract_video_info(video_url)
            logger.info(f"📹 Title: {video_info['title']}")
            
            # COMMENTS ONLY MODE: Skip video download/upload, just import comments to existing asset
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
                
                # Use configured asset_id (required when not in dry_run)
                result['asset_id'] = asset_id
                logger.info(f"📌 Using configured asset_id: {asset_id}")
            
            # NORMAL MODE or VIDEO ONLY: Download and upload video
            if not comments_only:
                # Step 2: Download video file
                logger.info("[Step 2/5] Downloading video from YouTube...")
                downloaded_file = self.youtube_processor.download_video(video_url, output_dir="cache")
                
                # Step 3: Get signed URL from backend (like frontend)
                logger.info("[Step 3/5] Getting signed URL from backend...")
                file_name = os.path.basename(downloaded_file)
                
                # Prepare metadata exactly like Frontend (categories array, preferredKeywords array)
                metadata_payload = {}
                if category:
                    metadata_payload['categories'] = [category]  # Frontend uses array
                if video_info.get('keywords'):
                    metadata_payload['preferredKeywords'] = video_info.get('keywords', [])  # Frontend uses preferredKeywords
                
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
                
                # Step 4: Upload file to signed URL (like frontend)
                logger.info("[Step 4/5] Uploading video file to signed URL...")
                upload_result = self.asset_creator.upload_file_to_signed_url(
                    file_path=downloaded_file,
                    upload_url=upload_url
                )
                
                if not upload_result.get('success'):
                    result['error'] = f"Upload failed: {upload_result.get('error', 'Unknown error')}"
                    return result
                
                logger.info("✅ Video uploaded successfully!")
                
                # Clean up downloaded file
                try:
                    if os.path.exists(downloaded_file):
                        os.remove(downloaded_file)
                        logger.info(f"🗑️  Cleaned up downloaded file: {downloaded_file}")
                except Exception as e:
                    logger.warning(f"Failed to clean up downloaded file: {e}")
                
                # VIDEO ONLY MODE: Skip comments import
                if video_only:
                    logger.info("[Mode] VIDEO ONLY - Skipping comments import")
                    result['success'] = True
                    logger.info("✅ Video upload complete!")
                    return result
            
            # Step 5: Extract and import live chats first (they have timestamps)
            logger.info("[Step 5/5] Extracting and importing live chats...")
            live_chats, livechat_stats = self.youtube_processor.extract_live_chat(video_url)
            
            livechat_imported = 0
            if live_chats:
                # Apply max_items_limit if set
                original_count = len(live_chats)
                if max_items_limit and max_items_limit > 0 and original_count > max_items_limit:
                    live_chats = live_chats[:max_items_limit]
                    logger.info(f"📊 Limiting live chats to first {max_items_limit} (out of {original_count} total)")
                
                # Anonymize live chat users
                live_chats = self.user_randomizer.anonymize_comments(live_chats)
                
                if dry_run:
                    logger.info(f"📊 DRY RUN: Found {len(live_chats)} live chat messages (showing import-ready data)...")
                else:
                    logger.info(f"📊 Processing {len(live_chats)} live chat messages...")
                
                # In comments_only mode (not dry_run), use configured asset_id from yaml
                # Otherwise use result asset_id (from upload) or fallback to passed asset_id
                if comments_only and not dry_run and result.get('asset_id'):
                    target_asset_id = result['asset_id']  # Use configured asset_id from yaml
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
            
            # Step 5 (continued): Extract and import comments (only those with timestamps)
            logger.info("[Step 5/5] Extracting and importing comments (with timestamps only)...")
            comments, timestamp_stats = self.youtube_processor.extract_comments(video_url)
            
            # Filter comments to only include those with detected timestamps
            comments_with_timestamp = [
                comment for comment in comments 
                if int(comment.get('commented_at', '0') or '0') > 0
            ]
            
            # Store timestamp stats in result
            result['timestamp_stats'] = timestamp_stats
            result['timestamp_stats']['filtered'] = len(comments_with_timestamp)
            result['timestamp_stats']['livechat_imported'] = livechat_imported
            
            if comments_with_timestamp:
                # Apply max_items_limit if set
                original_count = len(comments_with_timestamp)
                if max_items_limit and max_items_limit > 0 and original_count > max_items_limit:
                    comments_with_timestamp = comments_with_timestamp[:max_items_limit]
                    logger.info(f"📊 Limiting comments to first {max_items_limit} (out of {original_count} total)")
                
                # Anonymize comment users
                comments_with_timestamp = self.user_randomizer.anonymize_comments(comments_with_timestamp)
                
                if dry_run:
                    logger.info(f"📊 DRY RUN: Found {len(comments_with_timestamp)} comments with timestamps (filtered from {len(comments)} total, showing import-ready data)...")
                else:
                    logger.info(f"📊 Processing {len(comments_with_timestamp)} comments with timestamps (filtered from {len(comments)} total)...")
                
                # In comments_only mode (not dry_run), use configured asset_id from yaml
                # Otherwise use result asset_id (from upload) or fallback to passed asset_id
                if comments_only and not dry_run and result.get('asset_id'):
                    target_asset_id = result['asset_id']  # Use configured asset_id from yaml
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
            result['success'] = False  # Explicitly mark as failed
            logger.error(f"Processing failed for {video_url}: {e}")
            # Don't raise, just return the failed result so the loop continues
        finally:
            # Clean up downloaded file if it exists and wasn't already cleaned up
            if downloaded_file and os.path.exists(downloaded_file):
                try:
                    os.remove(downloaded_file)
                    logger.info(f"🗑️  Cleaned up downloaded file: {downloaded_file}")
                except Exception as e:
                    logger.warning(f"Failed to clean up downloaded file: {e}")
        
        return result
    
    def process_list(self, list_file: str, dry_run: bool = False, video_only: bool = False, comments_only: bool = False, max_items_limit: int = None, asset_id: str = None) -> List[Dict]:
        """
        Process entire list.txt file
        
        Args:
            list_file: Path to input file
            dry_run: If True, don't actually upload/comments
            video_only: If True, only upload videos (skip comments)
            comments_only: If True, only import comments (skip video upload)
            max_items_limit: Maximum number of items to process (None = all, number = limit)
            asset_id: Asset ID to use in comments_only mode (from env var or previous upload)
            
        Returns:
            List of result dicts
        """
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
                asset_id=asset_id,  # Use asset_id from env var (comments_only) or from upload (normal mode)
                max_items_limit=max_items_limit
            )
            
            results.append(result)
            
            if result['success']:
                logger.info(f"✓ Success: {result['asset_id']}, {result['comments_imported']} comments")
            else:
                logger.error(f"✗ Failed: {result['error']}")
        
        # Summary
        self._print_summary(results)
        
        return results
    
    @staticmethod
    def _print_summary(results: List[Dict]):
        """Print batch processing summary"""
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
