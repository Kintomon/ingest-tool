"""Asset Creator via GraphQL Mutation"""

import json
import logging
import requests
from typing import Dict, Optional
from .retry import retry_with_backoff

logger = logging.getLogger(__name__)


class AssetCreator:
    """Create asset in InCast via GraphQL CreateAssetFromUrl mutation"""
    
    def __init__(self, backend_url: str, jwt_token: str, dry_run: bool = False, max_retries: int = 3):
        """
        Initialize asset creator
        
        Args:
            backend_url: Backend GraphQL URL (e.g., 'https://api-dev.incast.ai/graphql/')
            jwt_token: JWT authentication token
            dry_run: If True, don't actually make API calls
            max_retries: Maximum number of retry attempts for API calls
        """
        self.backend_url = backend_url.rstrip('/') + '/graphql/'
        self.jwt_token = jwt_token
        self.dry_run = dry_run
        self.max_retries = max_retries
    
    def create_asset_from_url(
        self,
        video_url: str,
        asset_name: str,
        asset_description: str,
        category: Optional[str] = None,
        metadata: Optional[Dict] = None
    ) -> Dict:
        """
        Create asset using CreateAssetFromUrl GraphQL mutation
        
        NOTE: This sends the video URL to backend, which streams directly from URL to GCS.
        NO local video download happens!
        
        Args:
            video_url: YouTube or other video URL (sent as-is, backend handles download)
            asset_name: Video title/name
            asset_description: Video description
            category: Category string (e.g., 'Finance')
            metadata: Additional metadata dict
            
        Returns:
            dict: {
                'asset_id': str,
                'asset_name': str,
                'asset_description': str,
                'message': str,
                'error': Optional[str]
            }
        """
        # Prepare metadata
        if not metadata:
            metadata = {}
        
        # Add category if provided (backend expects 'category' only)
        if category:
            metadata['category'] = category
        
        # GraphQL mutation
        mutation = """
        mutation CreateAssetFromUrl(
            $videoUrl: String!
            $assetName: String!
            $assetDescription: String!
            $metadata: JSONString
        ) {
            createAssetFromUrl(
                videoUrl: $videoUrl
                assetName: $assetName
                assetDescription: $assetDescription
                metadata: $metadata
            ) {
                assetId
                assetName
                assetDescription
                message
                error
            }
        }
        """
        
        variables = {
            "videoUrl": video_url,
            "assetName": asset_name,
            "assetDescription": asset_description,
            "metadata": json.dumps(metadata) if metadata else None,
        }
        
        payload = {
            "query": mutation,
            "variables": variables
        }
        
        # Log comparison: yt-dlp data → API payload
        logger.debug("=" * 80)
        logger.debug("DATA COMPARISON: yt-dlp → GraphQL API")
        logger.debug("=" * 80)
        logger.debug(f"[yt-dlp EXTRACTED] Title: {asset_name}, Description: {len(asset_description)} chars, Keywords: {metadata.get('keywords', [])}")
        logger.debug(f"[GRAPHQL PAYLOAD] Endpoint: {self.backend_url}")
        logger.debug(f"Full mutation: {json.dumps(payload, indent=2)}")
        
        # Dry run mode - don't make actual API calls
        if self.dry_run:
            logger.info("🧪 DRY RUN: Would send GraphQL mutation (NO actual API call)")
            return {
                'asset_id': 'dry-run-asset-id',
                'asset_name': asset_name,
                'asset_description': asset_description,
                'message': 'Dry run mode - no asset created',
            }
        
        # Make API call with retry logic (only if not dry_run)
        try:
            @retry_with_backoff(max_retries=self.max_retries, exceptions=(requests.RequestException,))
            def _make_request():
                return requests.post(
                    self.backend_url,
                    json=payload,
                    cookies={'JWT': self.jwt_token},
                    headers={'Content-Type': 'application/json'},
                    timeout=600000
                )
            
            response = _make_request()
            
            if response.status_code != 200:
                error_msg = f"HTTP {response.status_code}"
                logger.error(f"GraphQL request failed: {error_msg}")
                return {'error': error_msg}
            
            data = response.json()
            
            if 'errors' in data:
                error_msg = data['errors'][0].get('message', 'Unknown error')
                logger.error(f"GraphQL errors: {error_msg}")
                return {'error': error_msg}
            
            result = data.get('data', {}).get('createAssetFromUrl', {})
            
            if result.get('error'):
                logger.error(f"Asset creation failed: {result['error']}")
                return result
            
            asset_id = result.get('assetId')
            logger.info(f"✅ Asset created: {asset_id}")
            
            return {
                'asset_id': asset_id,
                'asset_name': result.get('assetName'),
                'asset_description': result.get('assetDescription'),
                'message': result.get('message'),
            }
            
        except Exception as e:
            logger.error(f"Error creating asset: {e}", exc_info=True)
            return {'error': str(e)}

