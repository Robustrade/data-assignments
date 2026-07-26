"""
End-to-end demo: writes a small, realistic set of changes through
SourceWriter, which does two things per call - runs the real SQL against
source.duckdb (so source's own PK/FK/CHECK/NOT NULL constraints apply) and
publishes the matching event to Kafka. This is the "real" way to generate
data for this pipeline: source, Kafka, lake, and warehouse all end up
consistent with each other, unlike calling KafkaChangeCapture directly
(which only touches Kafka, and was fine for testing pipeline mechanics but
left source.duckdb empty).

Run pipeline/spark_consumer.py afterward (see README/DESIGN.md for the
docker command) to process these into the lake and warehouse.
"""
import sys
sys.path.insert(0, "/u01/kulu-assignment/data-assignments/submission/Shabbirsheikh")

import duckdb
from decimal import Decimal
from datetime import datetime, timezone

from source.models import init_source_schema
from pipeline.kafka_producer import make_producer, KafkaChangeCapture
from pipeline.source_writer import SourceWriter

SOURCE_DB = "/u01/kulu-assignment/data-assignments/submission/Shabbirsheikh/data/source.duckdb"

source_conn = duckdb.connect(SOURCE_DB)
init_source_schema(source_conn)

producer = make_producer()
capture = KafkaChangeCapture(producer)
writer = SourceWriter(source_conn, capture)

now = datetime.now(timezone.utc)

# customer 1: Asha, places an order with 2 line items, pays, ships
writer.insert("customers", "c100", {
    "customer_id": "c100", "name": "Asha Verma", "email": "asha@example.com",
    "status": "active", "created_at": now, "updated_at": now,
})
writer.insert("orders", "o100", {
    "order_id": "o100", "customer_id": "c100", "status": "pending",
    "total_amount": Decimal("64.97"), "discount_code": None,
    "created_at": now, "updated_at": now,
})
writer.insert("order_items", "i100", {
    "order_item_id": "i100", "order_id": "o100", "sku": "TSHIRT-BLUE-M",
    "quantity": 2, "unit_price": Decimal("19.99"),
})
writer.insert("order_items", "i101", {
    "order_item_id": "i101", "order_id": "o100", "sku": "CAP-BLACK",
    "quantity": 1, "unit_price": Decimal("24.99"),
})
writer.insert("payments", "p100", {
    "payment_id": "p100", "order_id": "o100", "amount": Decimal("64.97"),
    "status": "pending", "created_at": now, "paid_at": None,
})
writer.update("orders", "o100", {
    "order_id": "o100", "customer_id": "c100", "status": "paid",
    "total_amount": Decimal("64.97"), "discount_code": None,
    "created_at": now, "updated_at": now,
})
writer.update("payments", "p100", {
    "payment_id": "p100", "order_id": "o100", "amount": Decimal("64.97"),
    "status": "settled", "created_at": now, "paid_at": now,
})
writer.update("orders", "o100", {
    "order_id": "o100", "customer_id": "c100", "status": "shipped",
    "total_amount": Decimal("64.97"), "discount_code": None,
    "created_at": now, "updated_at": now,
})

# customer 2: Ravi, cancels his order before payment
writer.insert("customers", "c101", {
    "customer_id": "c101", "name": "Ravi Kumar", "email": "ravi@example.com",
    "status": "active", "created_at": now, "updated_at": now,
})
writer.insert("orders", "o101", {
    "order_id": "o101", "customer_id": "c101", "status": "pending",
    "total_amount": Decimal("19.99"), "discount_code": "WELCOME10",
    "created_at": now, "updated_at": now,
})
writer.insert("order_items", "i102", {
    "order_item_id": "i102", "order_id": "o101", "sku": "CAP-BLACK",
    "quantity": 1, "unit_price": Decimal("19.99"),
})
writer.update("orders", "o101", {
    "order_id": "o101", "customer_id": "c101", "status": "cancelled",
    "total_amount": Decimal("19.99"), "discount_code": "WELCOME10",
    "created_at": now, "updated_at": now,
})

# customer 3: gets soft-deleted (closed account) after being created -
# source row is REALLY deleted, warehouse will show it as _deleted=true
writer.insert("customers", "c102", {
    "customer_id": "c102", "name": "Meera Iyer", "email": "meera@example.com",
    "status": "active", "created_at": now, "updated_at": now,
})
writer.delete("customers", "c102", {
    "customer_id": "c102", "name": "Meera Iyer", "email": "meera@example.com",
    "status": "active", "created_at": now, "updated_at": now,
})

producer.flush()
producer.close()
source_conn.close()
print("Seeded demo events through SourceWriter - source.duckdb and Kafka topic 'cdc-events-v4' are now consistent.")
print("Now run pipeline/spark_consumer.py (see README/DESIGN.md for the docker command) to process them into lake + warehouse.")
