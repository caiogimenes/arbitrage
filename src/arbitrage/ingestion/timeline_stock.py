import yfinance as yf
import datetime
import pandas as pd

class TimelineStocks:
    def __init__(self, stocks: list) -> None:
        self.stock = stocks
        
    def get_timeline(self, start_date: datetime, 
                     end_date:datetime) -> pd.DataFrame:
        
        data = yf.download(self.stock, start=start_date, 
                             end=end_date)
        
        return data
        
        