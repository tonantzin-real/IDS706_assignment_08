import json
import psycopg2
from kafka import KafkaConsumer


def run_consumer():
    """Consumes messages from Kafka and inserts them into PostgreSQL."""
    try:
        print("[Consumer] Connecting to Kafka at localhost:9092...")
        consumer = KafkaConsumer(
            "orders",
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id="transactions-consumer-group",
        )
        print("[Consumer] ✓ Connected to Kafka successfully!")

        print("[Consumer] Connecting to PostgreSQL...")
        conn = psycopg2.connect(
            dbname="kafka_db",
            user="kafka_user",
            password="kafka_password",
            host="localhost",
            port="5432",
        )
        conn.autocommit = True
        cur = conn.cursor()
        print("[Consumer] ✓ Connected to PostgreSQL successfully!")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS transactions (
                transaction_id VARCHAR(50) PRIMARY KEY,
                status VARCHAR(50),
                category VARCHAR(50),
                value NUMERIC(10, 2),
                timestamp TIMESTAMP,
                city VARCHAR(100),
                payment_method VARCHAR(50),
                discount NUMERIC(4, 2)
            );
            """
        )
        print("[Consumer] ✓ Table 'transactions' ready.")
        print("[Consumer] 🎧 Listening for messages...\n")

        message_count = 0
        for message in consumer:
            try:
                data = message.value

                insert_query = """
                    INSERT INTO transactions (transaction_id, status, category, value, timestamp, city, payment_method, discount)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (transaction_id) DO NOTHING;
                """
                cur.execute(
                    insert_query,
                    (
                        data["order_id"],
                        data["status"],
                        data["category"],
                        data["value"],
                        data["timestamp"],
                        data.get("city", "N/A"),
                        data["payment_method"],
                        data["discount"],
                    ),
                )
                message_count += 1
                print(
                    f"[Consumer] ✓ #{message_count} Inserted transaction {data['order_id']} | {data['category']} | ${data['value']} | {data['city']}"
                )

            except Exception as e:
                print(f"[Consumer ERROR] Failed to process message: {e}")
                continue

    except Exception as e:
        print(f"[Consumer ERROR] {e}")
        import traceback

        traceback.print_exc()
        raise


if __name__ == "__main__":
    run_consumer()
