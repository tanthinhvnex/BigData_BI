# Chạy Docker Compose để khởi tạo các service của Kafka:
docker-compose -f zk-single-kafka-single.yml up -d

# Chạy các câu lệnh bên trong Container Kafka1 vừa tạo:
docker exec -it kafka1 /bin/bash

# Các lệnh sau cũng cần được chạy trong Container Kafka1
## Tạo topic
Tạo các topic lần lượt là: gpt, copilot, gemini
kafka-topics --create --topic gpt --bootstrap-server kafka1:19092 --partitions 1 --replication-factor 1
kafka-topics --create --topic gemini --bootstrap-server kafka1:19092 --partitions 1 --replication-factor 1
kafka-topics --create --topic copilot --bootstrap-server kafka1:19092 --partitions 1 --replication-factor 1

kafka-topics --create --topic gpt-2 --bootstrap-server kafka1:19092 --partitions 1 --replication-factor 1

kafka-topics --create --topic gemini-2 --bootstrap-server kafka1:19092 --partitions 1 --replication-factor 1

kafka-topics --create --topic copilot-2 --bootstrap-server kafka1:19092 --partitions 1 --replication-factor 1

Chỉ cần tạo các topic này 1 lần duy nhất

## Liệt kê các topic hiện có
kafka-topics --bootstrap-server localhost:9092 --list

## Lấy offset mới nhất
kafka-get-offsets --broker-list kafka1:19092 --topic gpt --time -1

## Lấy offset cũ nhất
kafka-get-offsets --broker-list kafka1:19092 --topic gpt --time -2

## Xóa đi các topic
kafka-topics --delete --topic gpt --bootstrap-server kafka1:19092

kafka-topics --delete --topic gemini --bootstrap-server kafka1:19092

kafka-topics --delete --topic copilot --bootstrap-server kafka1:19092

kafka-topics --delete --topic gpt-2 --bootstrap-server kafka1:19092

kafka-topics --delete --topic gemini-2 --bootstrap-server kafka1:19092

kafka-topics --delete --topic copilot-2 --bootstrap-server kafka1:19092
