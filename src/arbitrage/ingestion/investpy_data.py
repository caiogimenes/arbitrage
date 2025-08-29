import pandas as pd
from typing import List
import investpy


pd.set_option('display.max_rows', 10)


class GetStocksCountry:
    def __init__(self, country: str)->None:
        self.country = country
    
    def get_stock_country(self) -> List:
        try:
            stocks_data = investpy.get_stocks(country=self.country)
            return stocks_data['symbol'].to_list()
        except Exception as e:
            raise ValueError('Função stock com problemas') from e
            
