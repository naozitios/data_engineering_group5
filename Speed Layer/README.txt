Kafka Setup

Site references:
https://kafka.apache.org/quickstart#quickstart_createtopic
https://medium.com/the-research-nest/creating-a-real-time-data-stream-using-apache-kafka-in-python-132e0e5630d3

Setting up streaming data 
1. Download Kafka as per first site reference, pip install necessary dependencies
2. Go to location where kafka is installed
3. Run zookeeper in terminal
bin/zookeeper-server-start.sh config/zookeeper.properties
4. Run kafka in a new terminal
bin/kafka-server-start.sh config/server.properties
5. If a you want to create a new topic run this in a new terminal
bin/kafka-topics.sh --create --topic TOPIC_NAME --bootstrap-server localhost:9092
6. Activate producer.py to start collecting data and sending it to the kafka server
7. Activate consumer.py to start retrieving data from kafka server