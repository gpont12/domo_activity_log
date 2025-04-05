import os
from typing import List, Dict, Any, Optional
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv

from auth import Authentication
from utils import date_to_unix_ms, make_request


class DomoActivityLogError(Exception):
    """Custom exception for Domo activity log errors"""
    pass


class DomoActivityLog:
    """
    Handles fetching activity logs from the Domo API.
    
    Args:
        client_id (str): Domo client ID
        client_secret (str): Domo client secret
        environment (str, optional): Environment name for configuration
    """
    
    def __init__(self, client_id: Optional[str] = None, client_secret: Optional[str] = None, environment: Optional[str] = None):
        load_dotenv()
        self.environment = environment
        self.client_id = client_id
        self.client_secret = client_secret

        if not self.client_id or not self.client_secret:
            raise DomoActivityLogError("Client ID and Client Secret must be provided or set in environment variables.")

        self.auth = Authentication(self.client_id, self.client_secret, 'audit')
        self._base_url = "https://api.domo.com/v1/audit"

    def _make_request(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Make a request to the audit API with proper headers.
        
        Args:
            params (Dict[str, Any]): Query parameters for the request
            
        Returns:
            List[Dict[str, Any]]: JSON response data
            
        Raises:
            DomoActivityLogError: If the request fails
        """
        headers = {
            'Accept': "application/json",
            "Authorization": f"Bearer {self.auth.token}"
        }
        print(params)
        try:
            return make_request(self._base_url, headers, params=params)
        except Exception as e:
            raise DomoActivityLogError(f"Failed to fetch activity logs: {str(e)}")

    def get_logs(self, start_date: str, end_date: str, batch_size: int = 1000) -> Optional[pd.DataFrame]:
        """
        Get activity logs for a date range with pagination.
        
        Args:
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            batch_size (int, optional): Number of logs to fetch per request. Defaults to 1000.
            
        Returns:
            Optional[pd.DataFrame]: DataFrame containing all activity logs, or None if no data
            
        Raises:
            DomoActivityLogError: If dates are invalid or API request fails
        """
        try:
            start = date_to_unix_ms(start_date)
            end = date_to_unix_ms(end_date)

            if start > end:
                raise ValueError("Start date must be before end date")

        except ValueError as e:
            raise DomoActivityLogError(f"Invalid date format or range: {str(e)}")

        offset = 0
        all_logs = []

        while True:
            try:
                print(f"Fetching logs with offset: {offset}")
                json_data = self._make_request({
                    "start": start,
                    "end": end,
                    "limit": batch_size,
                    "offset": offset
                })

                if not json_data:
                    break

                all_logs.extend(json_data)
                offset += batch_size
                
            except DomoActivityLogError as e:
                print(f"Error fetching batch at offset {offset}: {str(e)}")
                break

        if not all_logs:
            return None

        df = pd.DataFrame(all_logs)
        df['domain'] = self.auth.get_credential_domain()
        return df
