kafka-topics --bootstrap-server localhost:9092 --delete --topic topic.device.status
kafka-topics --bootstrap-server localhost:9092 --delete --topic topic.device.trip.event
kafka-topics --bootstrap-server localhost:9092 --delete --topic trip-processor-table

kafka-topics --bootstrap-server localhost:9092 --create --topic topic.device.status --partitions 2
kafka-topics --bootstrap-server localhost:9092 --create --topic topic.device.trip.event --partitions 2
kafka-topics --bootstrap-server localhost:9092 --create --topic trip-processor-table --partitions 2 --config cleanup.policy=compact
