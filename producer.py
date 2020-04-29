import argparse

from kafka import KafkaProducer
from json import dumps
from file_utils import get_file_content

kafka_url = 'localhost:9092'
default_topic_name = 'topic1'
default_json_path = 'data/MOCK_DATA.json'

parser = argparse.ArgumentParser()
parser.add_argument('--topic', nargs='?', type=str, help='name of the topic', default=default_topic_name)
parser.add_argument('--json-file', nargs='?', type=str, help='json path', default=default_json_path)


def publish_message(producer_instance, topic_name, value):
    try:
        producer_instance.send(topic_name, value=value)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


def connect_kafka_producer():
    try:
        return KafkaProducer(bootstrap_servers=[kafka_url],
                             value_serializer=lambda m: dumps(m).encode('utf-8'))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))


def start_producing():
    args = parser.parse_args()

    all_persons = get_file_content(args.json_file)
    if all_persons is None:
        return

    kafka_producer = connect_kafka_producer()
    if kafka_producer:
        for person in all_persons:
            publish_message(kafka_producer, args.topic, person)
        if kafka_producer is not None:
            kafka_producer.close()


if __name__ == '__main__':
    start_producing()
