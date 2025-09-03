from kafka import KafkaProducer
import json

KAFKA_SERVER = 'kafka:9092'

_producer = None

def get_kafka_producer():
    """
    Função que cria e reutiliza a instância do KafkaProducer.
    """
    global _producer
    if _producer is None:
        print("Criando uma nova instância do KafkaProducer...")
        _producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
        )
    return _producer

def save_event_kafka(name_event: str, data: dict, asset_count: int, source: str):
    KAFKA_NAME = name_event
    try:

        producer = get_kafka_producer()
        
        payload = {
            'source': source,
            'asset_count': asset_count,
            'data': data
        }    
        
        producer.send(KAFKA_NAME, value=payload) 
        producer.flush()
    except Exception as e:
        raise ValueError(f'Falha ao converter ou publicar evento {name_event} no Kafka.') from e