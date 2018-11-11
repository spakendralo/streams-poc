Start something like the Confluent open source platform 5 with ./confluent start
Create 3 topics
/kafka-topics --create --zookeeper localhost:2181 --partitions 3 --replication-factor 1 --topic consumers8
/kafka-topics --create --zookeeper localhost:2181 --partitions 3 --replication-factor 1 --topic accounts8
/kafka-topics --create --zookeeper localhost:2181 --partitions 3 --replication-factor 1 --topic output8

