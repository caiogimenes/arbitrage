import yfinance as yf
import datetime
import pandas as pd
import logging
from typing import List

def download_historical_data(tickers: List[str],
                            start_date: datetime.datetime,
                            end_date: datetime.datetime) -> pd.DataFrame:
    
    """A component resposable solely for downloading historical data."""
    
    logging.info(f"Downloading historical data for {len(tickers)} tickers...")
    
    if not tickers:
        logging.error(f"Tickers List empty!")
        return None
    
    try:
        df = yf.download(tickers=tickers, start=start_date, end=end_date, group_by='ticker')
        logging.info("Download complete.")
        return df
      
    except Exception as e:
        logging.error(f"Failed during yfinance download: {e}")
        raise