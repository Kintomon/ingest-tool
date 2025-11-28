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
    def __init__(self, publish_url: str, jwt_token: str, refresh_token: Optional[str] = None, dry_run: bool = False):
        self.publish_url = publish_url
        self.jwt_token = jwt_token
        self.refresh_token = refresh_token
        self.dry_run = dry_run
        self.imported_count = 0
        self.failed_count = 0
        self.backend_url = None
    
    def set_backend_url(self, backend_url: str):
        self.backend_url = backend_url.rstrip('/') + '/graphql/'
    
    def _decode_jwt_payload(self, token: str) -> Optional[Dict]:
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
        payload = self._decode_jwt_payload(token)
        if not payload or 'exp' not in payload:
            return True
        
        exp_timestamp = payload['exp']
        current_timestamp = int(time.time())
        expires_in = exp_timestamp - current_timestamp
        
        return expires_in < threshold_seconds
    
    def _refresh_token(self) -> bool:
        if not self.backend_url:
            logger.warning("Cannot refresh token: backend_url not set")
            return False
        
        if not self.refresh_token:
            logger.warning("Cannot refresh token: refresh_token not provided (authenticate first)")
            return False
        
        if self.dry_run:
            logger.info("🧪 DRY RUN: Would refresh token")
            return True
        
        # Backend uses django-graphql-jwt with JWT_LONG_RUNNING_REFRESH_TOKEN
        # The refresh token must be sent as a cookie, NOT in GraphQL variables
        # The new JWT token is returned in cookies, not in the response body
        mutation = """
        mutation RefreshToken {
            refreshToken {
                payload
                refreshExpiresIn
            }
        }
        """
        
        payload = {
            "query": mutation
        }
        
        try:
            response = requests.post(
                self.backend_url,
                json=payload,
                cookies={
                    'JWT': self.jwt_token,
                    'JWT-refresh-token': self.refresh_token
                },
                headers={'Content-Type': 'application/json'},
                timeout=60
            )
            
            if response.status_code != 200:
                try:
                    error_body = response.text
                    logger.error(f"Token refresh failed: HTTP {response.status_code}")
                    logger.error(f"Response body: {error_body}")
                except:
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
            
            # The new JWT token is returned in cookies, NOT in the GraphQL response
            new_cookies = response.cookies
            if 'JWT' in new_cookies:
                self.jwt_token = new_cookies['JWT']
                logger.info("✅ JWT token refreshed successfully")
            else:
                logger.error("Token refresh: No JWT cookie in response")
                return False
            
            # Note: With JWT_LONG_RUNNING_REFRESH_TOKEN, the refresh token cookie persists
            # and doesn't need to be updated on each refresh
            
            return True
                
        except Exception as e:
            logger.error(f"Error refreshing token: {e}", exc_info=True)
            return False
    
    def _ensure_valid_token(self):
        if self._token_expires_soon(self.jwt_token):
            logger.info("🔄 Token expires soon, refreshing before comment import...")
            if not self._refresh_token():
                logger.warning("⚠️  Token refresh failed, continuing with current token")
    
    def import_comments(self, comments: List[Dict], asset_id: str, rate_limit: float = 0.5) -> Dict[str, int]:
        pubnub_channel = f"comments_{asset_id}"
        
        self._ensure_valid_token()
        
        parent_map = {}
        
        total = len(comments)
        
        last_token_check = 0
        TOKEN_CHECK_INTERVAL = 50
        
        for idx, comment in enumerate(comments, 1):
            if idx - last_token_check >= TOKEN_CHECK_INTERVAL:
                if self._token_expires_soon(self.jwt_token):
                    logger.info(f"🔄 Token expires soon at comment {idx}/{total}, refreshing...")
                    self._refresh_token()
                last_token_check = idx
            
            yt_id = comment.get('yt_id', f"yt_{idx}")
            youtube_parent_id = comment.get('parent_id')
            
            incast_parent_id = parent_map.get(youtube_parent_id, None) if youtube_parent_id else None
            
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
                'commented_at': commented_at,
                'asset_id': asset_id,
            }
            if incast_parent_id:
                payload['parent_id'] = incast_parent_id
            
            if self.dry_run:
                self.imported_count += 1
                fake_uuid = f"dry-run-{idx}"
                parent_map[yt_id] = fake_uuid
                
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
                    response = requests.post(
                        self.publish_url,
                        json=payload,
                        cookies={'JWT': self.jwt_token},
                        headers={'Content-Type': 'application/json'},
                        timeout=30
                    )
                    response.raise_for_status()
                    response_data = response.json()
                    
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
                    
                    comment_preview = payload['comment'][:30] if len(payload['comment']) > 30 else payload['comment']
                    
                    error_detail = ""
                    is_nlp_error = False
                    if hasattr(e, 'response') and e.response is not None:
                        try:
                            error_body = e.response.text
                            error_detail = f" - {error_body[:200]}"
                            if "NLP" in error_body or "nlp" in error_body:
                                is_nlp_error = True
                        except:
                            pass
                    
                    if status_code == 500:
                        if is_nlp_error:
                            logger.info(f"⚠️  [{idx}/{total}] NLP processing failed (HTTP 500){error_detail}: {comment_preview}...")
                        else:
                            logger.info(f"⚠️  [{idx}/{total}] Server error (HTTP 500){error_detail}: {comment_preview}...")
                    else:
                        logger.info(f"⏭️  [{idx}/{total}] Skipped comment (HTTP {status_code}){error_detail}: {comment_preview}...")
                    
                except requests.RequestException as e:
                    self.failed_count += 1
                    logger.info(f"⏭️  [{idx}/{total}] Skipped comment (network error)")
                    
                except Exception as e:
                    self.failed_count += 1
                    logger.info(f"⏭️  [{idx}/{total}] Skipped comment (error)")
            
            if idx < total:
                time.sleep(rate_limit)
        
        logger.info(f"Comments import complete: {self.imported_count} imported, {self.failed_count} failed, {total} total")
        return {
            'imported': self.imported_count,
            'failed': self.failed_count,
            'total': total
        }
    
    def import_live_chats(self, live_chats: List[Dict], asset_id: str, rate_limit: float = 0.5) -> Dict[str, int]:
        self.imported_count = 0
        self.failed_count = 0
        
        return self.import_comments(live_chats, asset_id, rate_limit)

