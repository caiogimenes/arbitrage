import datetime
import logging


from src.arbitrage.ingestion import config
from src.arbitrage.ingestion.pipeline import PipelineIngestion
from src.arbitrage.ingestion.publishers import KafkaPublisher
from src.arbitrage.ingestion.histdownloader import download_historical_data
from src.arbitrage.ingestion.tickerscountry import TickerProvider

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

    
if __name__ == "__main__":

    data_inicio = datetime.date(
        config.START_YEAR, config.START_MONTH, config.START_DAY
    )
    data_fim = datetime.datetime(
        config.END_YEAR, config.END_MONTH, config.END_DAY
    )
    
    ticker_fecher = TickerProvider()
    kafka_publisher = KafkaPublisher(
                        kafka_server=config.KAFKA_SERVER,
                        topic=f'{config.KAFKA_TOPIC}_{config.COUNTRY}'
                    )
    
    pipeline = PipelineIngestion(
                    fetcher=ticker_fecher,
                    publisher=kafka_publisher,
                    downloader= download_historical_data,        
                    country=config.COUNTRY,
                    start_date=data_inicio,
                    end_date=data_fim,
                    limit=config.TICKS_LIMIT
                )           
    
    result = pipeline.run()
    logging.info(result)
