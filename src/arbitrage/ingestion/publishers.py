from kafka import KafkaProducer
import json
import logging

class KafkaPublisher:
    def __init__(self, kafka_server, topic: str):
        
        """
        Initializes the ingestion pipeline.

        Args:
            kafka_server (str): The address (host:port) of the Kafka broker.
            topic (str): The name of the topic where messages will be publshes.

        """
        
        self._producer = None
        self.server = kafka_server
        self.topic = topic
        
                
    def _get_producer(self):
        """Creates and reuses the KafkaProducer instance"""
        if self._producer is None:
            logging.info("Creating a new KafkaProducer instance...")
            self._producer = KafkaProducer(
                bootstrap_servers= self.server,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
            )
        return self._producer

    def publisher(self, 
                data: dict, 
                asset_count: int, 
                source: str) -> None:
        
        """
        A componet responsable to publish events to Kafka.
        
        Args:
            data (dict): The data dictionary to be send as message.
            asser_count (int): The number of tickers included in the data payload.
            source (str):  The origin of the data (e.g., 'b3', 'investing_api'.

        """
        logging.info("Starting layer Kafka.....")
        try:

            producer = self._get_producer()
            
            payload = {
                'source': source,
                'asset_count': asset_count,
                'data': data
            }    
            
            producer.send(self.topic, value=payload) 
            producer.flush()
        
        except Exception as e:
            logging.error(f"Failed to publish event to topic {self.topic}: {e}")
            raise
