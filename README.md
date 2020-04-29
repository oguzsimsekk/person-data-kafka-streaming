# person-data-kafka-streaming
Kafka broker runs on `localhost:9092` by default


### Prerequisites

- `python3.7`
- `java8`
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

For Usage;

```
Run the producer with or without the input parameters.
Run the consumer with specifying the topic name.

Normalization could be applied as the invalid/incorrect data has been produced.

To see the effect of streaming and window calculations;
    While the consumer console is running, try to run producer again.
    The batch/window calculations will appear as producer runs. 
```