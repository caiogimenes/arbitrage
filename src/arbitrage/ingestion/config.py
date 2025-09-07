import os

COUNTRY = os.getenv('COUNTRY', 'brazil')
TICKS_LIMIT = int(os.getenv('TICKERS_LIMIT', 50))

START_DAY = int(os.getenv('START_DAY', 1))
START_MONTH = int(os.getenv('START_MONTH', 1))
START_YEAR = int(os.getenv('START_YEAR', 2023))


END_DAY = int(os.getenv('END_DAY', 31))
END_MONTH = int(os.getenv('END_MONTH', 1))
END_YEAR = int(os.getenv('END_YEAR', 2025))

KAFKA_TOPIC = os.getenv('TOPIC', 'stock_limite')

KAFKA_SERVER = os.getenv('SERVER', 'kafka:9092')