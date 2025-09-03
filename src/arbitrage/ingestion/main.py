import datetime, json
import yfinance as yf
import investpy
from typing import List

from src.arbitrage.ingestion.events_timeline import save_event_kafka


class RunIngestion:
    def __init__(self, country:str,
                 start_date=datetime,
                 end_date=datetime):
        self.country = country
        self.start_date = start_date
        self.end_date = end_date
        self.ticks_df : List[str] = None
        
    @staticmethod
    def _add_sa_stock(stocks:list):
        if not stocks:
            print("Nenhum ticker encontrado!")
            return
        
        return [stock + '.SA' for stock in stocks]
    
    def _fecth_country_tickers(self) -> List:
        self.ticks_df = investpy.get_stocks(country=self.country)
        ticks_list = self.ticks_df['symbol'].to_list()
        return ticks_list
    
    
    def run_app(self):
        try:
            
            ticks_list = self._fecth_country_tickers()
            ticks_list_sa = self._add_sa_stock(stocks=ticks_list)
            
            all_ticks_df =  yf.download(ticks_list_sa[0:50], 
                                        start=self.start_date,
                                        end=self.end_date,
                                        group_by='ticker')
            
            
            for tick in ticks_list_sa[0:50]:
                
                tick_timeline = all_ticks_df[tick].dropna()
                timeline_dict = tick_timeline.to_dict(orient='split')
                
                save_event_kafka(
                    name_event='acoes_timeline',
                    data={
                        'ticker':tick,
                        'timeline':timeline_dict
                    },
                    asset_count=1,
                    source='historico-acoes-yfinance'
                )
        
        except Exception as e:
            raise ValueError('Falha na camada de Ingestion.') from e
        

if __name__ == "__main__":

    data_inicio = datetime.datetime(2020, 1, 1)
    data_fim = datetime.datetime(2024, 12, 31)
    
    run_i = RunIngestion(
        country='brazil',
        start_date=data_inicio,
        end_date=data_fim
    )
    
    run_i.run_app()
