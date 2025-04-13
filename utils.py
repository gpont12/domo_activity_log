from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, Union

import pandas as pd
import requests
from requests.exceptions import RequestException


def make_request(url: str,
                 headers: Dict[str, str],
                 method: str = 'GET',
                 data: Optional[Union[Dict[str, Any], str]] = None,
                 params: Optional[Dict[str, Any]] = None) -> Union[Dict[str, Any], str]:
    """
    Centralized request handler for making HTTP requests to Domo API.
    
    Args:
        url (str): The URL to make the request to
        headers (Dict[str, str]): Headers for the request
        method (str): HTTP method (default: 'GET')
        data (Union[Dict[str, Any], str], optional): Request body data
        params (Dict[str, Any], optional): URL parameters
        
    Returns:
        Union[Dict[str, Any], str]: Response data (JSON or text)
        
    Raises:
        RequestException: If the API request fails
        ValueError: If the response is not valid JSON when expected
    """
    try:
        content_type = headers.get('Content-Type', 'application/json')
        accept_type = headers.get('Accept', 'application/json')

        response = requests.request(
            method=method,
            url=url,
            headers=headers,
            json=data if content_type == 'application/json' else None,
            data=data if content_type != 'application/json' else None,
            params=params
        )
        response.raise_for_status()

        if 'application/json' in response.headers.get('Content-Type', ''):
            return response.json()
        return response.text

    except RequestException as e:
        raise RequestException(f"Failed to make {method} request to {url}: {str(e)}")
    except ValueError as e:
        raise ValueError(f"Invalid JSON response from {url}: {str(e)}")


def date_to_unix_ms(date_str: str) -> int:
    """
    Convert a date string to Unix timestamp in milliseconds.
    
    Args:
        date_str (str): Date string in YYYY-MM-DD format
        
    Returns:
        int: Unix timestamp in milliseconds
        
    Raises:
        ValueError: If date string is invalid
    """
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return int(dt.timestamp() * 1000)
    except ValueError as e:
        raise ValueError(f"Invalid date format. Expected YYYY-MM-DD, got: {date_str}") from e


def save_dataframe_to_csv(df: pd.DataFrame, file_path: str, index: bool = False) -> None:
    """
    Save a pandas DataFrame to a CSV file, creating the directory if needed.
    
    Args:
        df (pd.DataFrame): DataFrame to save
        file_path (str): Path to save the CSV file
        index (bool, optional): Whether to include index in CSV. Defaults to False.
    """
    path = Path(file_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=index)
    print(f"DataFrame saved to {file_path}")


def validate_and_convert_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Validate and convert data types for specific columns"""

    # Convert userId and objectId to integers
    integer_columns = ['userId', 'objectId']
    for col in integer_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')  # Using Int64 to handle NaN values

    # Convert time to datetime with explicit format
    if 'time' in df.columns:
        # Format matches '2025-04-01 15:36:41'
        df['time'] = pd.to_datetime(df['time'], format='%Y-%m-%d %H:%M:%S', errors='coerce')

    return df
