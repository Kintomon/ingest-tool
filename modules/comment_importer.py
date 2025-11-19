"""Comment Importer via Cloud Function"""

import json
import logging
import time
import requests
from typing import Dict, List
from .retry import retry_with_backoff

logger = logging.getLogger(__name__)


class CommentImporter:
    """Import comments via Cloud Function (triggers NLP processing)"""
    
    def __init__(self, publish_url: str, jwt_token: str, dry_run: bool = False, max_retries: int = 3):
        """
        Initialize comment importer
        
        Args:
            publish_url: Cloud Function URL (e.g., 'https://api-dev.incast.ai/publish-comment')
            jwt_token: JWT authentication token
            dry_run: If True, don't actually make API calls
            max_retries: Maximum number of retry attempts for API calls
        """
        self.publish_url = publish_url
        self.jwt_token = jwt_token
        self.dry_run = dry_run
        self.max_retries = max_retries
        self.imported_count = 0
        self.failed_count = 0
    
    def import_comments(
        self,
        comments: List[Dict],
        asset_id: str,
        rate_limit: float = 0.5
    ) -> Dict[str, int]:
        """
        Import comments via Cloud Function
        Maps YouTube comment IDs to InCast comment IDs for parent_id tracking
        
        Args:
            comments: List of comment dicts (from YouTubeProcessor)
            asset_id: Asset UUID
            rate_limit: Seconds between API calls
            
        Returns:
            dict: {'imported': int, 'failed': int, 'total': int}
        """
        pubnub_channel = f"comments_{asset_id}"
        
        # Map YouTube comment IDs to InCast comment IDs
        parent_map = {}
        
        # Note: max_items_limit is applied in batch_processor before passing to this method
        
        # Import comments (parents first, then replies)
        total = len(comments)
        
        # Only define retry wrapper if not in dry_run mode
        if not self.dry_run:
            @retry_with_backoff(max_retries=self.max_retries, exceptions=(requests.RequestException,))
            def _import_single_comment(payload_data):
                response = requests.post(
                    self.publish_url,
                    json=payload_data,
                    cookies={'JWT': self.jwt_token},
                    headers={'Content-Type': 'application/json'},
                    timeout=30
                )
                # Log response details for debugging (especially 500 errors)
                if response.status_code >= 400:
                    error_text = response.text[:500]
                    if response.status_code >= 500:
                        logger.error(f"API Server Error ({response.status_code}): {error_text}")
                    else:
                        logger.debug(f"API Error Response ({response.status_code}): {error_text}")
                response.raise_for_status()
                return response.json()
        
        for idx, comment in enumerate(comments, 1):
            yt_id = comment.get('yt_id', f"yt_{idx}")
            youtube_parent_id = comment.get('parent_id')
            
            # Look up parent ID from YouTube ID mapping
            incast_parent_id = parent_map.get(youtube_parent_id, None) if youtube_parent_id else None
            
            # Convert commented_at to number (FE sends as number, BE converts to float then string)
            commented_at = comment.get('commented_at', '0')
            try:
                commented_at = float(commented_at) if commented_at else 0
            except (ValueError, TypeError):
                commented_at = 0
            
            payload = {
                'comment': comment['comment'],
                'created_by_id': comment.get('created_by_id', ''),
                'user_name': comment.get('user_name', 'Unknown'),
                'profile_picture': comment.get('profile_picture', ''),
                'pubnub_channel': pubnub_channel,
                'commented_at': commented_at,  # Number (matches FE format)
                'asset_id': asset_id,
            }
            # Only include parent_id if it exists (for replies)
            if incast_parent_id:
                payload['parent_id'] = incast_parent_id
            
            if self.dry_run:
                self.imported_count += 1
                # Generate a fake UUID for dry-run mode
                fake_uuid = f"dry-run-{idx}"
                parent_map[yt_id] = fake_uuid
                
                # Log import-ready data for review
                logger.info(f"\n{'='*80}")
                logger.info(f"🧪 DRY RUN - Import Ready Data [{idx}/{total}]")
                logger.info(f"{'='*80}")
                logger.info(f"Asset ID: {asset_id}")
                logger.info(f"YouTube ID: {yt_id}")
                logger.info(f"Parent ID: {incast_parent_id or 'None (top-level)'}")
                logger.info(f"User: {payload['user_name']}")
                logger.info(f"Timestamp: {payload['commented_at']} seconds")
                logger.info(f"Comment: {payload['comment'][:200]}{'...' if len(payload['comment']) > 200 else ''}")
                logger.info(f"Full Payload:")
                logger.info(json.dumps(payload, indent=2, ensure_ascii=False))
                logger.info(f"{'='*80}\n")
            else:
                try:
                    response_data = _import_single_comment(payload)
                    
                    # Handle string response
                    if isinstance(response_data, str):
                        response_data = json.loads(response_data)
                    
                    comment_response = response_data.get('comment', {})
                    incast_comment_id = comment_response.get('id')
                    
                    if incast_comment_id:
                        parent_map[yt_id] = incast_comment_id
                        self.imported_count += 1
                        if idx % 10 == 0 or idx == total:
                            logger.debug(f"  [{idx}/{total}] Imported comment")
                    else:
                        logger.warning(f"  [{idx}/{total}] No comment_id in response")
                        self.failed_count += 1
                        
                except requests.HTTPError as e:
                    self.failed_count += 1
                    status_code = e.response.status_code if hasattr(e, 'response') else 'Unknown'
                    error_detail = ''
                    if hasattr(e, 'response') and e.response is not None:
                        try:
                            error_detail = e.response.text[:500]  # First 500 chars of error response
                            logger.error(f"  [{idx}/{total}] HTTP {status_code} error response: {error_detail}")
                        except:
                            pass
                    logger.error(f"  [{idx}/{total}] HTTP error: {status_code}")
                except requests.RequestException as e:
                    self.failed_count += 1
                    logger.error(f"  [{idx}/{total}] Request error: {str(e)[:200]}")
                except Exception as e:
                    self.failed_count += 1
                    logger.error(f"  [{idx}/{total}] Error: {str(e)[:200]}", exc_info=True)
            
            # Rate limiting
            if idx < total:  # Don't sleep after last comment
                time.sleep(rate_limit)
        
        logger.info(f"Comments import complete: {self.imported_count} imported, {self.failed_count} failed, {total} total")
        return {
            'imported': self.imported_count,
            'failed': self.failed_count,
            'total': total
        }
    
    def import_live_chats(
        self,
        live_chats: List[Dict],
        asset_id: str,
        rate_limit: float = 0.5
    ) -> Dict[str, int]:
        """
        Import live chat messages via Cloud Function
        Same as import_comments but for live chat messages
        
        Args:
            live_chats: List of live chat message dicts
            asset_id: Asset UUID
            rate_limit: Seconds between API calls
            
        Returns:
            dict: {'imported': int, 'failed': int, 'total': int}
        """
        # Reset counters for live chat import
        self.imported_count = 0
        self.failed_count = 0
        
        # Use same import logic as comments
        return self.import_comments(live_chats, asset_id, rate_limit)

