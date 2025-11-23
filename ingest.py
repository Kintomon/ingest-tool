#!/usr/bin/env python3
"""
YouTube Video Ingestion Tool

Reads list.txt and processes each YouTube URL.
Supports configuration via config.yaml and environment variables.
"""

import os
import sys
from getpass import getpass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from modules.config import Config
from modules.logger_config import setup_logging
from modules.youtube_processor import YouTubeProcessor
from modules.asset_creator import AssetCreator
from modules.comment_importer import CommentImporter
from modules.user_randomizer import UserRandomizer
from modules.batch_processor import BatchProcessor
from modules.cache_cleanup import cleanup_cache_files
from modules.auth_wrapper import AuthWrapper, AuthError


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
    backend_url = config.get('api.backend_url', 'https://api-dev.incast.ai')
    publish_url = config.get('api.publish_url', 'https://api-dev.incast.ai/publish-comment')
    list_file = config.get('processing.list_file', 'list.txt')
    firebase_api_key = config.get('firebase.api_key', env_var='FIREBASE_API_KEY') or config.get('FIREBASE_API_KEY', '')
    
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
    skip_live_chat = config.get_bool('processing.skip_live_chat', False)
    
    # Get max items limit (default to 10 in dry_run, "all" otherwise)
    max_items_limit = config.get('processing.max_items_limit', '10' if dry_run else 'all')
    if max_items_limit and max_items_limit.lower() == 'all':
        max_items_limit = None  # None means no limit
    else:
        try:
            max_items_limit = int(max_items_limit) if max_items_limit else None
        except (ValueError, TypeError):
            max_items_limit = 10 if dry_run else None
    
    # Ensure Firebase API key is provided
    if not firebase_api_key or firebase_api_key == 'DUMMY_API_KEY':
        logger.error("❌ Firebase API key not configured!")
        logger.error("   Set FIREBASE_API_KEY environment variable or configure firebase.api_key in config.yaml")
        sys.exit(1)
    
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

    # Prompt user for credentials
    logger.info("\n🔐 Authentication Required")
    logger.info("=" * 80)
    
    email = input("Email: ").strip()
    if not email:
        logger.error("❌ Email is required!")
        sys.exit(1)
    
    password = getpass("Password: ").strip()
    if not password:
        logger.error("❌ Password is required!")
        sys.exit(1)
    
    logger.info("=" * 80 + "\n")

    auth_wrapper = AuthWrapper(firebase_api_key=firebase_api_key, backend_url=backend_url)

    try:
        auth_result = auth_wrapper.authenticate(email=email, password=password)
    except AuthError as exc:
        logger.error(f"❌ Authentication failed: {exc}")
        sys.exit(1)

    jwt_token = auth_result['jwt_token']
    refresh_token = auth_result['refresh_token']
    
    # Cleanup old cache files if enabled
    if config.get_bool('cache.enabled', True) and cache_cleanup_days > 0:
        cleanup_cache_files(days_old=cache_cleanup_days)
    
    # Initialize modules
    youtube_processor = YouTubeProcessor()
    user_randomizer = UserRandomizer()
    
    asset_creator = AssetCreator(
        backend_url=backend_url,
        jwt_token=jwt_token,
        refresh_token=refresh_token if refresh_token else None,
        dry_run=dry_run,
        max_retries=max_retries
    )
    
    comment_importer = CommentImporter(
        publish_url=publish_url,
        jwt_token=jwt_token,
        refresh_token=refresh_token if refresh_token else None,
        dry_run=dry_run,
        max_retries=max_retries
    )
    # Set backend URL for token refresh capability
    comment_importer.set_backend_url(backend_url)
    
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
            asset_id=asset_id,
            skip_live_chat=skip_live_chat
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
