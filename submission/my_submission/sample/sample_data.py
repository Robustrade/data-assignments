"""
    Sample data generators for e-commerce CDC pipeline.

    Provides factories and sample records for testing the full pipeline.
"""

from datetime import datetime, timezone
from decimal import Decimal


def sample_customers() -> list[dict]:
    """Generate sample customer records."""
    return [
        {
            "customer_id": "CUST001",
            "name": "Alice Johnson",
            "email": "alice@example.com",
            "country": "USA",
            "status": "active",
            "created_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
            "updated_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        },
        {
            "customer_id": "CUST002",
            "name": "Bob Smith",
            "email": "bob@example.com",
            "country": "UK",
            "status": "active",
            "created_at": datetime(2026, 1, 5, tzinfo=timezone.utc),
            "updated_at": datetime(2026, 1, 5, tzinfo=timezone.utc),
        },
        {
            "customer_id": "CUST003",
            "name": "Carol White",
            "email": "carol@example.com",
            "country": "Canada",
            "status": "suspended",
            "created_at": datetime(2026, 2, 1, tzinfo=timezone.utc),
            "updated_at": datetime(2026, 5, 10, tzinfo=timezone.utc),
        },
    ]


def sample_products() -> list[dict]:
    """Generate sample product records."""
    return [
        {
            "product_id": "PROD001",
            "sku": "SKU-MOUSE-001",
            "name": "Wireless Mouse",
            "category": "Electronics",
            "price": Decimal("29.99"),
            "stock_qty": 150,
            "status": "active",
            "created_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
            "updated_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        },
        {
            "product_id": "PROD002",
            "sku": "SKU-KEYBOARD-001",
            "name": "Mechanical Keyboard",
            "category": "Electronics",
            "price": Decimal("89.99"),
            "stock_qty": 80,
            "status": "active",
            "created_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
            "updated_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        },
        {
            "product_id": "PROD003",
            "sku": "SKU-MONITOR-001",
            "name": "4K Monitor",
            "category": "Electronics",
            "price": Decimal("299.99"),
            "stock_qty": 25,
            "status": "active",
            "created_at": datetime(2026, 2, 1, tzinfo=timezone.utc),
            "updated_at": datetime(2026, 2, 1, tzinfo=timezone.utc),
        },
        {
            "product_id": "PROD004",
            "sku": "SKU-CABLE-001",
            "name": "USB-C Cable",
            "category": "Accessories",
            "price": Decimal("9.99"),
            "stock_qty": 500,
            "status": "active",
            "created_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
            "updated_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        },
    ]


def sample_orders() -> list[dict]:
    """Generate sample order records."""
    return [
        {
            "order_id": "ORD001",
            "customer_id": "CUST001",
            "order_date": datetime(2026, 5, 1).date(),
            "status": "delivered",
            "total_amount": Decimal("119.98"),
            "currency": "USD",
            "created_at": datetime(2026, 5, 1, 10, 0, tzinfo=timezone.utc),
            "updated_at": datetime(2026, 5, 15, 14, 30, tzinfo=timezone.utc),
        },
        {
            "order_id": "ORD002",
            "customer_id": "CUST002",
            "order_date": datetime(2026, 5, 5).date(),
            "status": "shipped",
            "total_amount": Decimal("389.98"),
            "currency": "USD",
            "created_at": datetime(2026, 5, 5, 11, 15, tzinfo=timezone.utc),
            "updated_at": datetime(2026, 5, 20, 9, 0, tzinfo=timezone.utc),
        },
        {
            "order_id": "ORD003",
            "customer_id": "CUST001",
            "order_date": datetime(2026, 5, 10).date(),
            "status": "pending",
            "total_amount": Decimal("39.96"),
            "currency": "USD",
            "created_at": datetime(2026, 5, 10, 14, 20, tzinfo=timezone.utc),
            "updated_at": datetime(2026, 5, 10, 14, 20, tzinfo=timezone.utc),
        },
    ]


def sample_order_items() -> list[dict]:
    """Generate sample order item records."""
    return [
        {
            "order_item_id": "ITEM001",
            "order_id": "ORD001",
            "product_id": "PROD001",
            "quantity": 1,
            "unit_price": Decimal("29.99"),
            "line_total": Decimal("29.99"),
            "created_at": datetime(2026, 5, 1, 10, 0, tzinfo=timezone.utc),
        },
        {
            "order_item_id": "ITEM002",
            "order_id": "ORD001",
            "product_id": "PROD002",
            "quantity": 1,
            "unit_price": Decimal("89.99"),
            "line_total": Decimal("89.99"),
            "created_at": datetime(2026, 5, 1, 10, 0, tzinfo=timezone.utc),
        },
        {
            "order_item_id": "ITEM003",
            "order_id": "ORD002",
            "product_id": "PROD002",
            "quantity": 1,
            "unit_price": Decimal("89.99"),
            "line_total": Decimal("89.99"),
            "created_at": datetime(2026, 5, 5, 11, 15, tzinfo=timezone.utc),
        },
        {
            "order_item_id": "ITEM004",
            "order_id": "ORD002",
            "product_id": "PROD003",
            "quantity": 1,
            "unit_price": Decimal("299.99"),
            "line_total": Decimal("299.99"),
            "created_at": datetime(2026, 5, 5, 11, 15, tzinfo=timezone.utc),
        },
        {
            "order_item_id": "ITEM005",
            "order_id": "ORD003",
            "product_id": "PROD004",
            "quantity": 4,
            "unit_price": Decimal("9.99"),
            "line_total": Decimal("39.96"),
            "created_at": datetime(2026, 5, 10, 14, 20, tzinfo=timezone.utc),
        },
    ]


def sample_shipments() -> list[dict]:
    """Generate sample shipment records."""
    return [
        {
            "shipment_id": "SHIP001",
            "order_id": "ORD001",
            "status": "delivered",
            "carrier": "FedEx",
            "tracking_num": "FX123456789",
            "shipped_at": datetime(2026, 5, 2, 15, 0, tzinfo=timezone.utc),
            "delivered_at": datetime(2026, 5, 15, 14, 30, tzinfo=timezone.utc),
            "created_at": datetime(2026, 5, 2, tzinfo=timezone.utc),
        },
        {
            "shipment_id": "SHIP002",
            "order_id": "ORD002",
            "status": "in_transit",
            "carrier": "UPS",
            "tracking_num": "UPS987654321",
            "shipped_at": datetime(2026, 5, 6, 10, 0, tzinfo=timezone.utc),
            "delivered_at": None,  # Not delivered yet
            "created_at": datetime(2026, 5, 6, tzinfo=timezone.utc),
        },
        {
            "shipment_id": "SHIP003",
            "order_id": "ORD003",
            "status": "pending",
            "carrier": None,
            "tracking_num": None,
            "shipped_at": None,
            "delivered_at": None,
            "created_at": datetime(2026, 5, 10, tzinfo=timezone.utc),
        },
    ]


def sample_inventory_logs() -> list[dict]:
    """Generate sample inventory transaction logs."""
    return [
        {
            "log_id": "LOG001",
            "product_id": "PROD001",
            "transaction_type": "purchase",
            "qty_change": -1,
            "reason": "Sold in order ORD001",
            "created_at": datetime(2026, 5, 1, 10, 5, tzinfo=timezone.utc),
        },
        {
            "log_id": "LOG002",
            "product_id": "PROD002",
            "transaction_type": "purchase",
            "qty_change": -2,
            "reason": "Sold in orders ORD001, ORD002",
            "created_at": datetime(2026, 5, 5, 11, 20, tzinfo=timezone.utc),
        },
        {
            "log_id": "LOG003",
            "product_id": "PROD003",
            "transaction_type": "purchase",
            "qty_change": -1,
            "reason": "Sold in order ORD002",
            "created_at": datetime(2026, 5, 5, 11, 20, tzinfo=timezone.utc),
        },
        {
            "log_id": "LOG004",
            "product_id": "PROD004",
            "transaction_type": "purchase",
            "qty_change": -4,
            "reason": "Sold in order ORD003",
            "created_at": datetime(2026, 5, 10, 14, 25, tzinfo=timezone.utc),
        },
        {
            "log_id": "LOG005",
            "product_id": "PROD001",
            "transaction_type": "adjustment",
            "qty_change": 10,
            "reason": "Stock count correction",
            "created_at": datetime(2026, 5, 18, 9, 0, tzinfo=timezone.utc),
        },
    ]


def sample_returns() -> list[dict]:
    """Generate sample return records."""
    return [
        {
            "return_id": "RET001",
            "order_item_id": "ITEM002",
            "reason": "Product defective",
            "status": "approved",
            "refund_amount": Decimal("89.99"),
            "created_at": datetime(2026, 5, 12, tzinfo=timezone.utc),
            "updated_at": datetime(2026, 5, 15, tzinfo=timezone.utc),
        },
    ]
