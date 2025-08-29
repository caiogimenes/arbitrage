import datetime, json
from src.arbitrage.ingestion.events_timeline import save_event_kafka
from src.arbitrage.ingestion.investpy_data import GetStocksCountry
from src.arbitrage.ingestion.timeline_stock import TimelineStocks


@staticmethod
def _add_sa_stock(stocks:list):
    return [stock + '.SA' for stock in stocks]

class RunIngestion:
    def __init__(self, country:str,
                 start_date=datetime,
                 end_date=datetime):
        self.country = country
        self.start_date = start_date
        self.end_date = end_date

    def run_app(self):
        try:
            stocks = GetStocksCountry(country=self.country)
            ticks = stocks.get_stock_country()
            ticks_sa = _add_sa_stock(stocks=ticks)
            
            
            timeline = TimelineStocks(stocks=ticks_sa)
            data_timeline = timeline.get_timeline(start_date=self.start_date, 
                                    end_date=self.end_date)
            
            timeline_dict = data_timeline.to_dict(orient='split')
            
            save_event_kafka(name_event='acoes_timeline',
                                data=timeline_dict,
                                len=len(ticks_sa),
                                source='historico-acoes-yfinance')
        
        except Exception as e:
            raise ValueError('Falha na camada de Ingestion.') from e