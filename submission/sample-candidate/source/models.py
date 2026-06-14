from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional, Dict, Any


# =====================================================
# ENUMS
# =====================================================

class CustomerStatus(str, Enum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    SUSPENDED = "SUSPENDED"


class ProductStatus(str, Enum):
    ACTIVE = "ACTIVE"
    DISCONTINUED = "DISCONTINUED"


class OrderStatus(str, Enum):
    CREATED = "CREATED"
    PAID = "PAID"
    SHIPPED = "SHIPPED"
    DELIVERED = "DELIVERED"
    CANCELLED = "CANCELLED"


class PaymentStatus(str, Enum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    PENDING = "PENDING"


class CDCOperation(str, Enum):
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"


# =====================================================
# STRONG ENTITIES
# =====================================================

@dataclass
class Customer:

    customer_id: int

    email: str

    first_name: str

    last_name: Optional[str]

    phone: Optional[str]

    customer_status: CustomerStatus

    created_at: datetime

    updated_at: datetime


@dataclass
class Product:

    product_id: int

    sku: str

    product_name: str

    category: Optional[str]

    unit_price: Decimal

    product_status: ProductStatus

    created_at: datetime

    updated_at: datetime


@dataclass
class Order:

    order_id: int

    customer_id: int

    order_status: OrderStatus

    total_amount: Decimal

    created_at: datetime

    updated_at: datetime


# =====================================================
# WEAK ENTITIES
# =====================================================

@dataclass
class OrderItem:

    order_item_id: int

    order_id: int

    product_id: int

    quantity: int

    unit_price: Decimal

    line_amount: Decimal

    created_at: datetime


@dataclass
class PaymentAttempt:

    payment_attempt_id: int

    order_id: int

    payment_status: PaymentStatus

    gateway_transaction_id: Optional[str]

    amount: Decimal

    attempt_timestamp: datetime


# =====================================================
# CDC EVENT
# =====================================================

@dataclass
class CDCEvent:

    sequence_number: int

    table_name: str

    operation: CDCOperation

    primary_key: Any

    before_image: Optional[Dict[str, Any]]

    after_image: Optional[Dict[str, Any]]

    event_timestamp: datetime


# =====================================================
# SCHEMA CONTRACT
# =====================================================

@dataclass
class SchemaContract:

    table_name: str

    columns: Dict[str, str]


# =====================================================
# DATA QUALITY FAILURE
# =====================================================

@dataclass
class ValidationFailure:

    rule_name: str

    table_name: str

    record_id: str

    failure_reason: str

    event_timestamp: datetime