import pytest
import pandas as pd
import datetime
from unittest.mock import MagicMock

from src.arbitrage.ingestion.investpy_data import GetStocksCountry
from src.arbitrage.ingestion.timeline_stock import TimelineStocks
from src.arbitrage.ingestion.events_timeline import save_event_kafka

def test_get_stock_b3():
       
    data = GetStocksCountry(country='brazil')
    stocks_b3 = data.get_stock_country()
    
    assert stocks_b3 is not None  
    assert isinstance(stocks_b3, list)    
    assert len(stocks_b3) != 0
    assert 'PETR4' in stocks_b3

def test_get_timeline_stock():
    
    stocks = ['B3SA3.SA', 'PETR4.SA', 'COGN3.SA']
    date_in = datetime.datetime(year=2020, month=1, day=1)
    date_out = datetime.datetime(year=2020, month=12, day=31)
    
    timeline = TimelineStocks(stocks=stocks)
    df_timeline = timeline.get_timeline(start_date=date_in, 
                                        end_date=date_out)
    
    assert df_timeline is not None
    assert isinstance(df_timeline, pd.DataFrame)

def test_save_event_kafka_sends_correct_payload(mocker):
    
    mock_producer_instance = MagicMock()
    
    mocker.patch(
        'src.arbitrage.ingestion.events_timeline.KafkaProducer',
        return_value=mock_producer_instance 
    )
    
    test_data = {'col1': [1, 2]}
    
    save_event_kafka(
        name_event='test-topic',
        data=test_data,
        asset_count=2,
        source='test-source'
    )
    
    mock_producer_instance.send.assert_called_once()
    mock_producer_instance.flush.assert_called_once()
    
    call_args = mock_producer_instance.send.call_args
    sent_topic = call_args[0][0]
    sent_payload = call_args[1]['value']
    
    assert sent_topic == 'test-topic'
    assert sent_payload['data'] == test_data
    
    
    
    
    
    
    