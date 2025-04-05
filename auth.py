import requests
import base64
from datetime import datetime, timedelta
from typing import Optional


class DomoAuthError(Exception):
    """Custom exception for Domo authentication errors"""
    pass


class Authentication:
    """Handles authentication with the Domo API.
    
    This class manages OAuth2 authentication tokens for the Domo API,
    automatically refreshing them when they expire.
    
    Args:
        client_id (str): The Domo client ID
        secret (str): The Domo client secret
        scope (str): The OAuth scope for the token
        token_expiry_buffer (int, optional): Minutes before token expiry to refresh. Defaults to 5.    
    """
    
    def __init__(self, client_id: str, secret: str, scope: str, token_expiry_buffer: int = 5):
        self.client_id = client_id
        self.secret = secret
        self.scope = scope
        self.token_expiry_buffer = token_expiry_buffer
        self._token: Optional[str] = None
        self._token_expiry: Optional[datetime] = None
        self._domain: Optional[str] = None

    @property
    def token(self) -> str:
        """Get the access token, refreshing if expired.
        
        Returns:
            str: The current valid access token
        """
        if not self._token or self._is_token_expired():
            self._refresh_token()
        return self._token

    def _is_token_expired(self) -> bool:
        """Check if the current token is expired or about to expire.
        
        Returns:
            bool: True if token is expired or will expire soon, False otherwise
        """
        if not self._token_expiry:
            return True
        return datetime.now() >= (self._token_expiry - timedelta(minutes=self.token_expiry_buffer))

    def _refresh_token(self) -> None:
        """Get a new access token from Domo.
        
        Raises:
            DomoAuthError: If token refresh fails
        """
        auth_url = 'https://api.domo.com/oauth/token'               
        params = {
    'grant_type': 'client_credentials',
    'scope': self.scope
}
        auth = f'{self.client_id}:{self.secret}'
        auth_base64 = base64.b64encode(auth.encode()).decode("utf-8")
        headers = {
            'Accept': "application/json",
            'Authorization': f'Basic {auth_base64}'
        }
        
        try:
            response = requests.get(auth_url, headers=headers, params=params)
            response.raise_for_status()
            token_data = response.json()
        except requests.exceptions.RequestException as e:
            raise DomoAuthError(f'Failed to get access token: {str(e)}') from e
        except ValueError as e:
            raise DomoAuthError('Failed to parse token response as JSON') from e
            
        self._token = token_data.get('access_token')
        if not self._token:
            raise DomoAuthError('No access token in response')
            
        expires_in = token_data.get('expires_in', 3600)
        self._token_expiry = datetime.now() + timedelta(seconds=expires_in)
        self._domain = token_data.get('domain')

    def get_credential_domain(self) -> Optional[str]:
        """Get the domain associated with these credentials.
        
        Returns:
            Optional[str]: The domain associated with the credentials, or None if not available
        """
        if not self._domain:
            self._refresh_token()
        return self._domain
