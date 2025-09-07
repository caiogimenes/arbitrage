import investpy
import logging
import pandas as pd
from typing import List

class TickerProvider:
    
    def _format_brazilian_tickers(self, tickers:pd.DataFrame) -> pd.DataFrame:
        
        """Append the '.SA' suffix to a list of Brazilian tickers."""
        
        formated_tickers = [ticker + '.SA' for ticker in tickers]
        return formated_tickers

    def fetch(self, country:str, limit: int = None) -> List[str]:
        
        """Search tickers list of the country using investpy."""         
        
        try:
            logging.info(f"Fetching tickers for country: {country} ")
            tickers_country = investpy.get_stocks(country=country)
            
            if limit is not None:
                tickers_country = tickers_country.head(limit)
                
            if country == 'brazil':
                tickers_country = self._format_brazilian_tickers(tickers_country['symbol'])
                return tickers_country
            
            return tickers_country['symbol']
            
        except Exception as e:
            logging.error(f'Failed ro fetch tickers for {country}: {e}')
            raise