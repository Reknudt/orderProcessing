#!/bin/bash
set -e

echo "=== Запуск Kafka и зависимостей ==="

# 1. Запускаем контейнеры
#cd src/main/docker
docker-compose up -d

echo "Ожидание запуска Kafka..."
sleep 10

# 2. Проверяем здоровье Kafka
#echo "Проверка здоровья Kafka..."
#until docker exec kafka-local kafka-topics.sh --bootstrap-server localhost:19092 --list 1>/dev/null; do
#    echo "Kafka ещё не готова, ждём..."
#    sleep 5
#done

# 3. Создаём топики для тестов
echo "Создание тестовых топиков..."
TOPICS=(
    "orders:3:1"
    "order-status-updates:3:1"
    "jsons:3:1"
    "validated-jsons:3:1"
    "dlq-jsons:3:1"
)

for topic_config in "${TOPICS[@]}"; do
    IFS=':' read -r topic partitions replication <<< "$topic_config"

    if ! docker exec kafka-local bash -c "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --list | grep -q "$topic" "; then
        echo "Создаём топик: $topic (partitions: $partitions, replication: $replication)"
        docker exec kafka-local bash -c "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --create --topic "$topic" --partitions "$partitions" --replication-factor "$replication" --config retention.ms=604800000"
    else
        echo "Топик $topic уже существует"
    fi
done

# 4. Информация о запущенных сервисах
echo ""
echo "=== СЕРВИСЫ ЗАПУЩЕНЫ ==="
echo "Kafka UI:      http://localhost:8081"
echo "Kafka:         localhost:19092"
echo ""
echo "Ваше приложение Quarkus должно использовать:"
echo "  kafka.bootstrap.servers=localhost:19092"
echo ""
echo "Для остановки: docker-compose down"