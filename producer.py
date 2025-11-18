import time
import json
import uuid
import random
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker

fake = Faker()


def generate_synthetic_order():
    """Generates synthetic bank transaction data."""
    categories = [
        "PIX Transfer",
        "TED Transfer",
        "DOC Transfer",
        "Credit Card Payment",
        "Loan Payment",
        "Salary Deposit",
        "ATM Withdrawal",
    ]
    statuses = ["Pending", "Confirmed", "Rejected", "Reversed"]
    cities = [
        "São Paulo",
        "Rio de Janeiro",
        "Belo Horizonte",
        "Curitiba",
        "Salvador",
        "Recife",
        "Porto Alegre",
    ]
    payment_methods = [
        "PIX",
        "Debit Card",
        "Credit Card",
        "Bank Transfer",
        "Cash Withdrawal",
    ]
    discounts = [0, 0.01, 0.02]  # represents transaction fee discount %

    category = random.choice(categories)
    status = random.choice(statuses)
    city = random.choice(cities)
    payment_method = random.choice(payment_methods)
    discount = random.choice(discounts)

    gross_value = fake.pyfloat(min_value=50, max_value=500, right_digits=2)
    net_value = gross_value * (1 - discount)

    return {
        "order_id": str(uuid.uuid4())[:8],
        "status": status,
        "category": category,
        "value": round(net_value, 2),
        "timestamp": datetime.now().isoformat(),
        "city": city,
        "payment_method": payment_method,
        "discount": round(discount, 2),
    }


def run_producer():
    """Kafka producer that sends synthetic orders to the 'orders' topic."""
    try:
        print("[Producer] Connecting to Kafka at localhost:9092...")
        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=30000,
            max_block_ms=60000,
            retries=5,
        )
        print("[Producer] ✓ Connected to Kafka successfully!")

        count = 0
        while True:
            order = generate_synthetic_order()
            print(f"[Producer] Sending order #{count}: {order}")

            future = producer.send("orders", value=order)
            record_metadata = future.get(timeout=10)
            print(
                f"[Producer] ✓ Sent to partition {record_metadata.partition} at offset {record_metadata.offset}"
            )

            producer.flush()
            count += 1

            sleep_time = random.uniform(0.5, 2.0)
            time.sleep(sleep_time)

    except Exception as e:
        print(f"[Producer ERROR] {e}")
        import traceback

        traceback.print_exc()
        raise


if __name__ == "__main__":
    run_producer()
