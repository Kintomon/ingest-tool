"""Auth wrapper for Firebase + Backend login."""

import logging
from typing import Dict, Optional

import requests

logger = logging.getLogger(__name__)


class AuthError(Exception):
    pass


class AuthWrapper:
    FIREBASE_SIGNIN_URL = "https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword"

    def __init__(self, firebase_api_key: str, backend_url: str, timeout: int = 60):
        if not firebase_api_key:
            raise ValueError("Firebase API key is required for authentication")

        self.firebase_api_key = firebase_api_key
        self.backend_url = backend_url.rstrip('/') + '/graphql/'
        self.timeout = timeout

    def authenticate(self, email: str, password: str) -> Dict[str, str]:
        firebase_data = self._firebase_sign_in(email, password)
        backend_tokens = self._backend_login(firebase_data['id_token'])

        return {
            'jwt_token': backend_tokens['token'],
            'refresh_token': backend_tokens['refreshToken'],
            'firebase_refresh_token': firebase_data['refresh_token'],
            'firebase_user_id': firebase_data.get('local_id'),
        }

    def _firebase_sign_in(self, email: str, password: str) -> Dict[str, str]:
        url = f"{self.FIREBASE_SIGNIN_URL}?key={self.firebase_api_key}"
        payload = {
            "email": email,
            "password": password,
            "returnSecureToken": True,
        }

        try:
            response = requests.post(url, json=payload, timeout=self.timeout)
            if response.status_code != 200:
                error_detail = self._extract_error(response)
                raise AuthError(f"Firebase sign-in failed: {error_detail}")
            data = response.json()
        except requests.RequestException as exc:
            raise AuthError(f"Firebase sign-in error: {exc}") from exc

        required_fields = ['idToken', 'refreshToken', 'localId']
        for field in required_fields:
            if field not in data:
                raise AuthError(f"Firebase response missing '{field}'")

        logger.info("✅ Firebase authentication successful")

        return {
            'id_token': data['idToken'],
            'refresh_token': data['refreshToken'],
            'local_id': data['localId'],
        }

    def _backend_login(self, id_token: str) -> Dict[str, str]:
        mutation = """
        mutation LoginMutation($idToken: String!) {
            loginMutation(idToken: $idToken) {
                payload
            }
        }
        """
        variables = {"idToken": id_token}
        payload = {"query": mutation, "variables": variables}

        try:
            response = requests.post(
                self.backend_url,
                json=payload,
                headers={'Content-Type': 'application/json'},
                timeout=self.timeout,
            )
            if response.status_code != 200:
                logger.error(f"Backend login failed. Response: {response.text[:500]}")
                raise AuthError(f"Backend login failed: HTTP {response.status_code}")

            data = response.json()
        except requests.RequestException as exc:
            raise AuthError(f"Backend login error: {exc}") from exc

        if 'errors' in data:
            message = data['errors'][0].get('message', 'Unknown error')
            raise AuthError(f"Backend login error: {message}")

        login_data: Optional[Dict] = data.get('data', {}).get('loginMutation')
        if not login_data:
            raise AuthError("Backend login returned no data")

        jwt_token = response.cookies.get('JWT')
        refresh_token = response.cookies.get('JWT-refresh-token')

        if not jwt_token:
             jwt_token = login_data.get('token')
        
        if not refresh_token:
             refresh_token = login_data.get('refreshToken')

        if not jwt_token or not refresh_token:
            logger.error(f"Cookies: {response.cookies}")
            logger.error(f"Login Data: {login_data}")
            raise AuthError("Backend login successful but missing JWT or Refresh Token in cookies/response")

        logger.info("✅ Backend authentication successful")
        
        return {
            'token': jwt_token,
            'refreshToken': refresh_token
        }

    @staticmethod
    def _extract_error(response: requests.Response) -> str:
        try:
            data = response.json()
            message = data.get('error', {}).get('message')
            return message or response.text
        except (ValueError, AttributeError):
            return response.text[:200]

