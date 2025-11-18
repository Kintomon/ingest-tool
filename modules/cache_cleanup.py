"""Cache cleanup utility"""

import os
import logging
from pathlib import Path
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


def cleanup_cache_files(days_old: int = 30, pattern: str = None):
    """
    Clean up cache files older than specified days
    
    Args:
        days_old: Delete cache files older than this many days
        pattern: Glob pattern for cache files (if None, cleans both comments and livechat)
        
    Returns:
        Number of files deleted
    """
    if days_old <= 0:
        logger.warning("Cache cleanup disabled (days_old <= 0)")
        return 0
    
    # Use cache directory
    cache_dir = Path("cache")
    if not cache_dir.exists():
        logger.debug("Cache directory does not exist, nothing to clean")
        return 0
    
    cutoff_time = datetime.now() - timedelta(days=days_old)
    deleted_count = 0
    
    # Default patterns: both comments and livechat cache files
    patterns = [pattern] if pattern else ["comments_cache_*.json", "livechat_cache_*.json"]
    
    for pattern_item in patterns:
        for cache_file in cache_dir.glob(pattern_item):
            try:
                file_time = datetime.fromtimestamp(cache_file.stat().st_mtime)
                if file_time < cutoff_time:
                    cache_file.unlink()
                    deleted_count += 1
                    logger.debug(f"Deleted old cache file: {cache_file}")
            except Exception as e:
                logger.warning(f"Error deleting cache file {cache_file}: {e}")
    
    if deleted_count > 0:
        logger.info(f"Cleaned up {deleted_count} cache file(s) older than {days_old} days")
    
    return deleted_count

