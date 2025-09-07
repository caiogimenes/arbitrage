import pytest
import pandas as pd
import datetime
from unittest.mock import ANY

from src.arbitrage.ingestion.tickerscountry import TickerProvider
from src.arbitrage.ingestion.histdownloader import download_historical_data
from src.arbitrage.ingestion.publishers import KafkaPublisher
from src.arbitrage.ingestion import config


def test_fetch_country_tickers():
    country = config.COUNTRY
    tickers_country = TickerProvider()
    
    list_stock = tickers_country.fetch(country=country, limit=2)
    
    assert isinstance(list_stock, list)
    assert list_stock is not None
    
    
    
def test_download_historical_data():
    
    stocks = ['B3SA3.SA', 'PETR4.SA', 'COGN3.SA']

    start_date = datetime.datetime(
        config.START_YEAR, config.START_MONTH, config.START_DAY
    )
    
    end_date = datetime.datetime(
        config.END_YEAR, config.END_MONTH, config.END_DAY
    )
    
    data = download_historical_data(stocks, start_date, end_date)
    
    assert isinstance(data, pd.DataFrame)
    assert 'PETR4.SA' in data.columns.levels[0]
    
    

def test_publisher_kafka(mocker):
    
    mocker_produce_class =mocker.patch(
        'src.arbitrage.ingestion.publishers.KafkaProducer'
    )
    
    mock_producer_instance = mocker_produce_class.return_value
    
    test_kafka = KafkaPublisher(kafka_server=config.KAFKA_SERVER,
                                topic='test-2')  
    
    test_data = {'col1': [1, 2]}
  
    
    test_kafka.publisher(
        data=test_data,
        asset_count=2,
        source='test-source'
    )
    
    mocker_produce_class.assert_called_once_with(
        bootstrap_servers=config.KAFKA_SERVER,
        value_serializer= ANY
    )
    
    mock_producer_instance.send.assert_called_once()
    mock_producer_instance.flush.assert_called_once()


    _ , call_kwargs =  mock_producer_instance.send.call_args
    assert call_kwargs['value']['asset_count'] == 2
    assert call_kwargs['value']['data'] == test_data
    
    
    
    
    
    
    