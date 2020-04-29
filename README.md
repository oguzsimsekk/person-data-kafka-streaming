# person-data
Kafka runs on `localhost:9092` by default


### Prerequisites

- python3.7
- java8
- `brew install apache-spark`
- `brew install scala`
- `brew install kafka`



### Installing
Please run following commands at first;
```
pip install -r requirements.txt
```


Start Zookeeper by running command below:
```
zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties
```

Start Kafka by running command below:
```
kafka-server-start /usr/local/etc/kafka/server.properties
```

To produce new Kafka messages: 
```
python producer.py --topic topic1 --json-file data/MOCK_DATA.json
```

To consume new Kafka messages: 
```
python consumer.py --topic topic1
```