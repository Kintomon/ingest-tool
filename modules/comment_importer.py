"""Comment Importer via Cloud Function"""

import json
import logging
import time
import requests
import base64
from typing import Dict, List, Optional
from .retry import retry_with_backoff

logger = logging.getLogger(__name__)


class CommentImporter:
    """Import comments via Cloud Function (triggers NLP processing)"""
    
    def __init__(self, publish_url: str, jwt_token: str, refresh_token: Optional[str] = None, dry_run: bool = False, max_retries: int = 3):
        """
        Initialize comment importer
        
        Args:
            publish_url: Cloud Function URL (e.g., 'https://api-dev.incast.ai/publish-comment')
            jwt_token: JWT authentication token (can be updated via refresh)
            refresh_token: Refresh token for token refresh (optional)
            dry_run: If True, don't actually make API calls
            max_retries: Maximum number of retry attempts for API calls
        """
        self.publish_url = publish_url
        self.jwt_token = jwt_token
        self.refresh_token = refresh_token
        self.dry_run = dry_run
        self.max_retries = max_retries
        self.imported_count = 0
        self.failed_count = 0
        self.backend_url = None  # Will be set if needed for token refresh
    
    def set_backend_url(self, backend_url: str):
        """Set backend URL for token refresh"""
        self.backend_url = backend_url.rstrip('/') + '/graphql/'
    
    def _decode_jwt_payload(self, token: str) -> Optional[Dict]:
        """Decode JWT payload to get expiry"""
        try:
            parts = token.split('.')
            if len(parts) < 2:
                return None
            
            payload_b64 = parts[1]
            padding = '=' * (4 - len(payload_b64) % 4)
            payload_b64 += padding
            
            payload_bytes = base64.urlsafe_b64decode(payload_b64)
            return json.loads(payload_bytes)
        except Exception:
            return None
    
    def _token_expires_soon(self, token: str, threshold_seconds: int = 900) -> bool:
        """Check if token expires soon (15 min threshold)"""
        payload = self._decode_jwt_payload(token)
        if not payload or 'exp' not in payload:
            return True
        
        exp_timestamp = payload['exp']
        current_timestamp = int(time.time())
        expires_in = exp_timestamp - current_timestamp
        
        return expires_in < threshold_seconds
    
    def _refresh_token(self) -> bool:
        """Refresh JWT token using refreshToken mutation (requires refreshToken parameter)"""
        if not self.backend_url:
            logger.warning("Cannot refresh token: backend_url not set")
            return False
        
        if not self.refresh_token:
            logger.warning("Cannot refresh token: refresh_token not provided (authenticate first)")
            return False
        
        if self.dry_run:
            logger.info("🧪 DRY RUN: Would refresh token")
            return True
        
        mutation = """
        mutation RefreshToken($refreshToken: String!) {
            refreshToken(refreshToken: $refreshToken) {
                token
                payload
                refreshToken
                refreshExpiresIn
            }
        }
        """
        
        variables = {
            "refreshToken": self.refresh_token
        }
        
        payload = {
            "query": mutation,
            "variables": variables
        }
        
        try:
            response = requests.post(
                self.backend_url,
                json=payload,
                cookies={'JWT': self.jwt_token},
                headers={'Content-Type': 'application/json'},
                timeout=60
            )
            
            if response.status_code != 200:
                logger.error(f"Token refresh failed: HTTP {response.status_code}")
                return False
            
            data = response.json()
            
            if 'errors' in data:
                error_msg = data['errors'][0].get('message', 'Unknown error')
                logger.error(f"Token refresh GraphQL errors: {error_msg}")
                return False
            
            result = data.get('data', {}).get('refreshToken', {})
            if not result:
                logger.error("Token refresh returned no data")
                return False
            
            # Update JWT token from response
            new_token = result.get('token')
            new_refresh_token = result.get('refreshToken')
            
            if new_token:
                self.jwt_token = new_token
                logger.info("✅ JWT token refreshed successfully")
            else:
                logger.warning("Token refresh: No new token in response")
                return False
            
            # Update refresh token if provided
            if new_refresh_token:
                self.refresh_token = new_refresh_token
                logger.info("✅ Refresh token updated")
            
            return True
                
        except Exception as e:
            logger.error(f"Error refreshing token: {e}", exc_info=True)
            return False
    
    def _ensure_valid_token(self):
        """Check and refresh token if needed"""
        if self._token_expires_soon(self.jwt_token):
            logger.info("🔄 Token expires soon, refreshing before comment import...")
            if not self._refresh_token():
                logger.warning("⚠️  Token refresh failed, continuing with current token")
    
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
        
        # Ensure token is valid before starting long operation
        self._ensure_valid_token()
        
        # Map YouTube comment IDs to InCast comment IDs
        parent_map = {}
        
        # Note: max_items_limit is applied in batch_processor before passing to this method
        
        # Import comments (parents first, then replies)
        total = len(comments)
        
        # Check token periodically during long operations (every 50 comments)
        last_token_check = 0
        TOKEN_CHECK_INTERVAL = 50
        
        # Only define retry wrapper if not in dry_run mode
        if not self.dry_run:
            def _make_request(payload_data):
                """Make a single request without retry"""
                response = requests.post(
                    self.publish_url,
                    json=payload_data,
                    cookies={'JWT': self.jwt_token},
                    headers={'Content-Type': 'application/json'},
                    timeout=30
                )
                # Log response details for debugging
                if response.status_code >= 400:
                    error_text = response.text[:500]
                    if response.status_code >= 500:
                        logger.error(f"API Server Error ({response.status_code}): {error_text}")
                    else:
                        logger.debug(f"API Error Response ({response.status_code}): {error_text}")
                response.raise_for_status()
                return response.json()
            
            @retry_with_backoff(max_retries=self.max_retries, exceptions=(requests.RequestException,))
            def _import_single_comment_non_500(payload_data):
                """Import comment with retry (for non-500 errors)"""
                return _make_request(payload_data)
            
            def _import_single_comment_with_500_handling(payload_data, comment_idx, total):
                """Import single comment with special handling for 500 errors (retry only once)"""
                # First attempt
                try:
                    response = requests.post(
                        self.publish_url,
                        json=payload_data,
                        cookies={'JWT': self.jwt_token},
                        headers={'Content-Type': 'application/json'},
                        timeout=30
                    )
                    
                    # If 500 error, handle it specially (retry only once with detailed logging)
                    if response.status_code == 500:
                        # Log detailed information about the failed request
                        logger.error(f"\n{'='*80}")
                        logger.error(f"❌ 500 Server Error at comment [{comment_idx}/{total}]")
                        logger.error(f"{'='*80}")
                        logger.error(f"URL: {self.publish_url}")
                        logger.error(f"Payload:")
                        logger.error(json.dumps(payload_data, indent=2, ensure_ascii=False))
                        logger.error(f"Response Status: {response.status_code}")
                        logger.error(f"Response Headers: {dict(response.headers)}")
                        try:
                            error_text = response.text
                            logger.error(f"Response Body (first 2000 chars): {error_text[:2000]}")
                        except:
                            logger.error("Response Body: (unable to read)")
                        logger.error(f"{'='*80}\n")
                        
                        # Retry once
                        logger.warning(f"🔄 Retrying comment [{comment_idx}/{total}] after 500 error (1 retry only)...")
                        time.sleep(2.0)  # Wait 2 seconds before retry
                        try:
                            retry_response = requests.post(
                                self.publish_url,
                                json=payload_data,
                                cookies={'JWT': self.jwt_token},
                                headers={'Content-Type': 'application/json'},
                                timeout=30
                            )
                            if retry_response.status_code >= 400:
                                error_text = retry_response.text[:2000]
                                logger.error(f"❌ Retry also failed with {retry_response.status_code}: {error_text}")
                            retry_response.raise_for_status()
                            logger.info(f"✅ Retry successful for comment [{comment_idx}/{total}]")
                            return retry_response.json()
                        except Exception as retry_e:
                            logger.error(f"❌ Retry failed for comment [{comment_idx}/{total}]: {retry_e}")
                            raise
                    
                    # For non-500 errors, raise to let retry decorator handle it
                    response.raise_for_status()
                    return response.json()
                    
                except requests.HTTPError as e:
                    # If it's a 500 error that wasn't caught above, handle it
                    if hasattr(e, 'response') and e.response is not None and e.response.status_code == 500:
                        raise  # Should have been handled above
                    # For other HTTP errors, use the retry decorator
                    return _import_single_comment_non_500(payload_data)
                except requests.RequestException:
                    # For other request exceptions, use the retry decorator
                    return _import_single_comment_non_500(payload_data)
        
        for idx, comment in enumerate(comments, 1):
            # Periodic token refresh check during long operations
            if idx - last_token_check >= TOKEN_CHECK_INTERVAL:
                if self._token_expires_soon(self.jwt_token):
                    logger.info(f"🔄 Token expires soon at comment {idx}/{total}, refreshing...")
                    self._refresh_token()
                last_token_check = idx
            
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
                    response_data = _import_single_comment_with_500_handling(payload, idx, total)
                    
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

