from typing import Optional
import pandas as pd

from domo_dataset import DomoDataset
from utils import save_dataframe_to_csv
from activty_log_pull import DomoActivityLog


def get_activity_data_from_csv(start_date: str, end_date: str, creds_file: str = 'data/instance_creds.csv') -> Optional[pd.DataFrame]:
    """
    Fetch activity logs from multiple Domo instances using credentials from a CSV file.
    
    Args:
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
        creds_file (str): Path to the credentials CSV file
        
    Returns:
        Optional[pd.DataFrame]: Combined activity log data from all instances, or None if no data
    """
    try:
        credentials = pd.read_csv(creds_file)
    except FileNotFoundError:
        print(f"Error: Credentials file not found at {creds_file}")
        return None
    except pd.errors.EmptyDataError:
        print(f"Error: Credentials file is empty: {creds_file}")
        return None

    all_activity_data = pd.DataFrame()

    for _, row in credentials.iterrows(): 
        try:
            client_id = row['client_id']
            client_secret = row['client_secret']
            print(f"Processing instance with Client ID: {client_id}")

            activity_log = DomoActivityLog(client_id=client_id, client_secret=client_secret)
            activity_log_data = activity_log.get_logs(start_date, end_date)

            if activity_log_data is not None and not activity_log_data.empty:
                all_activity_data = pd.concat([all_activity_data, activity_log_data], ignore_index=True)
            
        except KeyError as e:
            print(f"Error: Missing required column in credentials file: {e}")
            continue
        except Exception as e:
            print(f"Error processing instance {client_id}: {str(e)}")
            continue

    return all_activity_data if not all_activity_data.empty else None


def send_data_to_domo(dsid: str, data: pd.DataFrame) -> None:
    """
    Send data to Domo.
    
    Args:
        dsid (str): Domo dataset ID
        data (pd.DataFrame): Data to send
    """
    domo_dataset = DomoDataset(dsid)
    domo_dataset.upload_data(data)


if __name__ == '__main__':
    start_date = '2025-04-01'
    end_date = '2025-04-03'
    dsid = None

    activity_data = get_activity_data_from_csv(start_date, end_date)
    if activity_data is not None:
        save_dataframe_to_csv(activity_data, 'data/activity_log_data.csv')
        send_data_to_domo(dsid, activity_data)
