"""Asset Creator via GraphQL Mutation - Works like Frontend"""

import json
import logging
import requests
import mimetypes
from typing import Dict, Optional
from .retry import retry_with_backoff

logger = logging.getLogger(__name__)


class AssetCreator:
    """Create asset in InCast via GraphQL getSignedUrl mutation (like frontend)"""
    
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
    
    def get_signed_url(
        self,
        file_name: str,
        asset_name: str,
        asset_description: str,
        metadata: Optional[Dict] = None
    ) -> Dict:
        """
        Get signed URL from backend (like frontend getSignedUrl mutation)
        
        Args:
            file_name: Video file name (e.g., 'video.mp4')
            asset_name: Video title/name
            asset_description: Video description
            metadata: Additional metadata dict
            
        Returns:
            dict: {
                'upload_url': str,
                'asset_id': str,
                'asset_name': str,
                'asset_description': str,
                'error': Optional[str]
            }
        """
        # GraphQL mutation (same as frontend)
        mutation = """
        mutation GetSignedUrl(
            $fileName: String!
            $assetName: String!
            $assetDescription: String!
            $metadata: JSONString
        ) {
            getSignedUrl(
                fileName: $fileName
                assetName: $assetName
                assetDescription: $assetDescription
                metadata: $metadata
            ) {
                uploadUrl
                assetId
                assetName
                assetDescription
                error
            }
        }
        """
        
        variables = {
            "fileName": file_name,
            "assetName": asset_name,
            "assetDescription": asset_description,
            "metadata": json.dumps(metadata) if metadata else None,
        }
        
        payload = {
            "query": mutation,
            "variables": variables
        }
        
        # Dry run mode
        if self.dry_run:
            logger.info("🧪 DRY RUN: Would get signed URL (NO actual API call)")
            return {
                'upload_url': 'https://dry-run-upload-url.example.com',
                'asset_id': 'dry-run-asset-id',
                'asset_name': asset_name,
                'asset_description': asset_description,
            }
        
        # Make API call with retry logic
        try:
            @retry_with_backoff(max_retries=self.max_retries, exceptions=(requests.RequestException,))
            def _make_request():
                return requests.post(
                    self.backend_url,
                    json=payload,
                    cookies={'JWT': self.jwt_token},
                    headers={'Content-Type': 'application/json'},
                    timeout=60
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
            
            result = data.get('data', {}).get('getSignedUrl', {})
            
            if result.get('error'):
                logger.error(f"Failed to get signed URL: {result['error']}")
                return result
            
            upload_url = result.get('uploadUrl')
            asset_id = result.get('assetId')
            logger.info(f"✅ Got signed URL for asset: {asset_id}")
            
            return {
                'upload_url': upload_url,
                'asset_id': asset_id,
                'asset_name': result.get('assetName'),
                'asset_description': result.get('assetDescription'),
            }
            
        except Exception as e:
            logger.error(f"Error getting signed URL: {e}", exc_info=True)
            return {'error': str(e)}
    
    def upload_file_to_signed_url(self, file_path: str, upload_url: str, content_type: Optional[str] = None) -> Dict:
        """
        Upload file to signed URL using PUT request (like frontend)
        
        Args:
            file_path: Local path to video file
            upload_url: Signed URL from getSignedUrl
            content_type: MIME type (auto-detected if not provided)
            
        Returns:
            dict: {
                'success': bool,
                'error': Optional[str]
            }
        """
        import os
        
        if not os.path.exists(file_path):
            return {
                'success': False,
                'error': f"File not found: {file_path}"
            }
        
        # Auto-detect content type if not provided
        if not content_type:
            content_type = mimetypes.guess_type(file_path)[0] or 'video/mp4'
        
        if self.dry_run:
            file_size = os.path.getsize(file_path)
            logger.info(f"🧪 DRY RUN: Would upload file to signed URL")
            logger.info(f"   File: {file_path} ({file_size / (1024*1024):.2f} MB)")
            logger.info(f"   Content-Type: {content_type}")
            return {'success': True}
        
        try:
            # Upload file to signed URL (like frontend does)
            with open(file_path, 'rb') as f:
                response = requests.put(
                    upload_url,
                    data=f,
                    headers={
                        'Content-Type': content_type
                    },
                    timeout=600  # 10 minutes for large files
                )
            
            if response.status_code not in [200, 204]:
                error_msg = f"Upload failed: HTTP {response.status_code}"
                logger.error(error_msg)
                return {'success': False, 'error': error_msg}
            
            logger.info("✅ File uploaded to signed URL successfully")
            return {'success': True}
            
        except Exception as e:
            error_msg = f"Error uploading file: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return {'success': False, 'error': error_msg}

