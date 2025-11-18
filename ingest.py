#!/usr/bin/env python3
"""
YouTube Video Ingestion Tool

Reads list.txt and processes each YouTube URL.
Supports configuration via config.yaml and environment variables.
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from modules.config import Config
from modules.logger_config import setup_logging
from modules.retry import validate_jwt_token
from modules.youtube_processor import YouTubeProcessor
from modules.asset_creator import AssetCreator
from modules.comment_importer import CommentImporter
from modules.user_randomizer import UserRandomizer
from modules.batch_processor import BatchProcessor
from modules.cache_cleanup import cleanup_cache_files


def main():
    # Load configuration
    config = Config()
    
    # Setup logging
    log_level = config.get('logging.level', 'INFO')
    log_file = config.get('logging.log_file', '')
    verbose = config.get_bool('logging.verbose', False)
    setup_logging(log_level=log_level, log_file=log_file if log_file else None, verbose=verbose)
    
    import logging
    logger = logging.getLogger(__name__)
    
    # Get configuration values
    jwt_token = config.get('api.jwt_token', env_var='JWT_TOKEN') or config.get('JWT_TOKEN', '')
    backend_url = config.get('api.backend_url', 'https://api-dev.incast.ai')
    publish_url = config.get('api.publish_url', 'https://api-dev.incast.ai/publish-comment')
    list_file = config.get('processing.list_file', 'list.txt')
    
    dry_run = config.get_bool('modes.dry_run', False)
    video_only = config.get_bool('modes.video_only', False)
    comments_only = config.get_bool('modes.comments_only', False)
    
    # In comments_only mode, get asset_id from config
    asset_id = None
    if comments_only:
        asset_id = config.get('modes.asset_id', '')
        if not asset_id and not dry_run:
            logger.error("❌ asset_id required in comments_only mode!")
            logger.error("   Set asset_id in config.yaml under modes.asset_id")
            logger.error("   Example: modes:\n     asset_id: 'your-asset-id-here'")
            sys.exit(1)
        elif asset_id:
            logger.info(f"📌 Using asset_id from config: {asset_id}")
        elif dry_run:
            asset_id = "dry-run-asset-id"  # Dummy for dry_run
            logger.info("🧪 DRY RUN: Using dummy asset_id")
    
    max_retries = config.get_int('processing.max_retries', 3)
    rate_limit = config.get_float('processing.rate_limit', 0.5)
    cache_cleanup_days = config.get_int('cache.cleanup_after_days', 30)
    
    # Get max items limit (default to 10 in dry_run, "all" otherwise)
    max_items_limit = config.get('processing.max_items_limit', '10' if dry_run else 'all')
    if max_items_limit and max_items_limit.lower() == 'all':
        max_items_limit = None  # None means no limit
    else:
        try:
            max_items_limit = int(max_items_limit) if max_items_limit else None
        except (ValueError, TypeError):
            max_items_limit = 10 if dry_run else None
    
    # Validate JWT token (not required in dry_run mode)
    if not dry_run:
        if not jwt_token or jwt_token == 'YOUR_JWT_TOKEN_HERE':
            logger.error("❌ JWT token not configured!")
            logger.error("   Set JWT_TOKEN environment variable or configure in config.yaml")
            logger.error("   Example: export JWT_TOKEN='your_token_here'")
            sys.exit(1)
        
        if not validate_jwt_token(jwt_token):
            logger.warning("⚠️  JWT token format appears invalid, but continuing anyway...")
    else:
        # In dry_run mode, JWT is optional (we won't make API calls)
        if not jwt_token or jwt_token == 'YOUR_JWT_TOKEN_HERE':
            logger.info("ℹ️  JWT token not set (optional in dry_run mode)")
            jwt_token = "test-token"  # Dummy token for dry_run mode
    
    # Check input file exists
    if not os.path.exists(list_file):
        logger.error(f"❌ Error: Input file '{list_file}' not found")
        logger.error(f"   Create {list_file} with format: youtube_url,category")
        if comments_only:
            logger.error(f"   For COMMENTS_ONLY mode: youtube_url,category,asset_id")
        sys.exit(1)
    
    # Print banner
    logger.info("=" * 80)
    logger.info("🎬 YouTube Video Ingestion Tool")
    logger.info("=" * 80)
    if dry_run:
        logger.info("🧪 DRY RUN MODE")
    if video_only:
        logger.info("📹 VIDEO ONLY MODE (skip comments)")
    if comments_only:
        logger.info("💬 COMMENTS ONLY MODE (skip video upload)")
    logger.info("=" * 80)
    logger.info(f"Input file: {list_file}")
    logger.info(f"Backend: {backend_url}")
    logger.info(f"Max retries: {max_retries}")
    logger.info(f"Rate limit: {rate_limit}s")
    logger.info(f"Max items limit: {max_items_limit if max_items_limit else 'all (no limit)'}")
    logger.info("=" * 80)
    
    # Cleanup old cache files if enabled
    if config.get_bool('cache.enabled', True) and cache_cleanup_days > 0:
        cleanup_cache_files(days_old=cache_cleanup_days)
    
    # Initialize modules
    youtube_processor = YouTubeProcessor()
    youtube_processor.extract_video_info("https://www.youtube.com/watch?v=x-0JvQUUj6U")
    user_randomizer = UserRandomizer()
    
    asset_creator = AssetCreator(
        backend_url=backend_url,
        jwt_token=jwt_token,
        dry_run=dry_run,
        max_retries=max_retries
    )
    
    comment_importer = CommentImporter(
        publish_url=publish_url,
        jwt_token=jwt_token,
        dry_run=dry_run,
        max_retries=max_retries
    )
    
    # Process
    try:
        batch_processor = BatchProcessor(
            youtube_processor=youtube_processor,
            asset_creator=asset_creator,
            comment_importer=comment_importer,
            user_randomizer=user_randomizer
        )
        
        # Process list
        results = batch_processor.process_list(
            list_file, 
            dry_run=dry_run, 
            video_only=video_only, 
            comments_only=comments_only,
            max_items_limit=max_items_limit,
            asset_id=asset_id
        )
        
        if dry_run:
            logger.info("\n🧪 DRY RUN Complete - Review output above")
        else:
            successful = sum(1 for r in results if r['success'])
            if successful > 0:
                logger.info(f"\n✅ Batch complete: {successful}/{len(results)} successful")
                if successful < len(results):
                    sys.exit(1)
            else:
                logger.error("\n❌ All videos failed!")
                sys.exit(1)
    
    except KeyboardInterrupt:
        logger.warning("\n\n⚠️  Interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"\n❌ Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
