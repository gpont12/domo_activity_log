import os
from typing import Dict, Any, Optional
from io import StringIO
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from auth import Authentication, DomoAuthError
from utils import make_request


class DomoDatasetError(Exception):
    """Custom exception for Domo dataset operations"""
    pass


class DomoDataset:
    """
    Handles operations on Domo datasets including creation, data upload, and retrieval.
    
    Args:
        dsid (str, optional): Dataset ID. If not provided, a new dataset will be created.
    """

    def __init__(self, dsid: Optional[str] = None):
        load_dotenv()
        self.dsid = dsid
        
        client_id = os.getenv("CLIENT_ID")
        client_secret = os.getenv("CLIENT_SECRET")
        
        if not client_id or not client_secret:
            raise DomoDatasetError("CLIENT_ID and CLIENT_SECRET must be set in environment variables")
            
        self.auth = Authentication(client_id, client_secret, 'data')
        self._base_url = 'https://api.domo.com/v1/datasets'

    def _make_request(self, method: str, endpoint: str, headers: Optional[Dict[str, str]] = None,
                     data: Optional[Any] = None, params: Optional[Dict[str, Any]] = None) -> Any:
        """
        Make a request to the Domo API with proper headers and content type handling.
        
        Args:
            method (str): HTTP method (GET, POST, PUT)
            endpoint (str): API endpoint
            headers (Dict[str, str], optional): Additional headers
            data (Any, optional): Request body
            params (Dict[str, Any], optional): Query parameters
            
        Returns:
            Any: Response data (JSON or text)
            
        Raises:
            DomoDatasetError: If the request fails
        """
        url = f"{self._base_url}/{endpoint.lstrip('/')}"
        headers = headers or {}
        headers['Authorization'] = f"Bearer {self.auth.token}"
        
        if method == 'POST' and not endpoint:
            headers['Content-Type'] = 'application/json'
        elif method == 'PUT' and endpoint == '/data':
            headers['Content-Type'] = 'text/csv'
            
        try:
            return make_request(url, headers, method, data, params)
        except Exception as e:
            raise DomoDatasetError(f"API request failed: {str(e)}")

    def create_dataset(self, name: str, schema: Dict[str, Any]) -> str:
        """
        Create a new Domo dataset.
        
        Args:
            name (str): Name of the dataset
            schema (Dict[str, Any]): Schema definition for the dataset
            
        Returns:
            str: ID of the created dataset
            
        Raises:
            DomoDatasetError: If dataset creation fails
        """
        try:
            response = self._make_request('POST', '', headers={'Content-Type': 'application/json'},
                                       data={"name": name, "schema": schema})
            self.dsid = response['id']
            return self.dsid
        except Exception as e:
            raise DomoDatasetError(f"Failed to create dataset: {str(e)}")

    def upload_data(self, data: pd.DataFrame) -> bool:
        """
        Upload data to the Domo dataset.
        
        Args:
            data (pd.DataFrame): Data to upload
            
        Returns:
            bool: True if upload was successful
            
        Raises:
            DomoDatasetError: If upload fails or dataset ID is missing
        """
        if not self.dsid:
            # Create new dataset if ID not provided
            dtype_mapping = {
                'object': 'STRING',
                'int64': 'LONG',
                'float64': 'DOUBLE',
                'datetime64[ns]': 'DATETIME',
                'datetime64[ns, tz]': 'DATETIME',
                'datetime64[ns, date]': 'DATE'
            }
            
            schema = {
                "columns": [
                    {
                        "type": dtype_mapping.get(str(data[col].dtype), "STRING"),
                        "name": col
                    }
                    for col in data.columns
                ]
            }
            
            self.create_dataset('Activity Log', schema)
            
        try:
            csv_data = StringIO()
            data.to_csv(csv_data, index=False)
            
            self._make_request(
                'PUT',
                f'{self.dsid}/data',
                headers={'Content-Type': 'text/csv'},
                data=csv_data.getvalue()
            )
            return True
        except Exception as e:
            raise DomoDatasetError(f"Failed to upload data: {str(e)}")

    def get_data(self, output_path: str = 'data/dataset.csv') -> str:
        """
        Download dataset data to a CSV file.
        
        Args:
            output_path (str): Path to save the CSV file
            
        Returns:
            str: Path to the saved file
            
        Raises:
            DomoDatasetError: If download fails or dataset ID is missing
        """
        if not self.dsid:
            raise DomoDatasetError("Dataset ID is required to fetch data")

        try:
            response = self._make_request(
                'GET',
                f'{self.dsid}/data',
                params={'includeHeader': 'true'},
                headers={'Accept': 'text/csv', 'Content-Type': 'text/csv'}
            )

            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(response)
            return str(output_path)
            
        except Exception as e:
            raise DomoDatasetError(f"Failed to download data: {str(e)}")
