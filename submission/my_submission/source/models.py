"""
    Source schema for an e-commerce order management system.

    Domain: customers place orders; orders contain line items; products have inventory.

    Strong entities  : customers, products, orders
    Weak entities    : order_items, inventory_logs, shipments, returns

    Key invariants:
    - order.total_amount = SUM(order_items.line_total)
    - product.stock_qty >= 0 (never negative)
    - order_item.line_total = quantity * unit_price
    - shipment.delivered_at >= shipment.shipped_at
    - return.refund_amount <= order_item.line_total
    - order.status follows: pending → confirmed → shipped → delivered (or cancelled at any point)
"""

import duckdb

# Expected columns per table — used by schema-contract checks.
SCHEMA_CONTRACT: dict[str, list[str]] = {
    "customers": ["customer_id", "name", "email", "country", "status", "created_at", "updated_at"],
    "products": ["product_id", "sku", "name", "category", "price", "stock_qty", "status", "created_at", "updated_at"],
    "orders": ["order_id", "customer_id", "order_date", "status", "total_amount", "currency", "created_at", "updated_at"],
    "order_items": ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "line_total", "created_at"],
    "inventory_logs": ["log_id", "product_id", "transaction_type", "qty_change", "reason", "created_at"],
    "shipments": ["shipment_id", "order_id", "status", "carrier", "tracking_num", "shipped_at", "delivered_at", "created_at"],
    "returns": ["return_id", "order_item_id", "reason", "status", "refund_amount", "created_at", "updated_at"],
}


def create_source_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create all source tables with constraints in the given DuckDB connection."""
    
    # ═══════════════════════════════════════════════════════════════════════════
    # STRONG ENTITY: customers
    # ═══════════════════════════════════════════════════════════════════════════
    conn.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            customer_id  VARCHAR PRIMARY KEY,
            name         VARCHAR NOT NULL,
            email        VARCHAR NOT NULL,
            country      VARCHAR,
            status       VARCHAR NOT NULL
                     CHECK (status IN ('active', 'suspended', 'closed')),
            created_at   TIMESTAMP NOT NULL,
            updated_at   TIMESTAMP NOT NULL
        )
    """)

    # ═══════════════════════════════════════════════════════════════════════════
    # STRONG ENTITY: products
    # ═══════════════════════════════════════════════════════════════════════════
    conn.execute("""
        CREATE TABLE IF NOT EXISTS products (
            product_id   VARCHAR PRIMARY KEY,
            sku          VARCHAR NOT NULL UNIQUE,
            name         VARCHAR NOT NULL,
            category     VARCHAR,
            price        DECIMAL(18, 2) NOT NULL CHECK (price > 0),
            stock_qty    INT NOT NULL DEFAULT 0 CHECK (stock_qty >= 0),
            status       VARCHAR NOT NULL
                     CHECK (status IN ('active', 'discontinued')),
            created_at   TIMESTAMP NOT NULL,
            updated_at   TIMESTAMP NOT NULL
        )
    """)

    # ═══════════════════════════════════════════════════════════════════════════
    # STRONG ENTITY: orders
    # ═══════════════════════════════════════════════════════════════════════════
    conn.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id     VARCHAR PRIMARY KEY,
            customer_id  VARCHAR NOT NULL REFERENCES customers(customer_id),
            order_date   DATE NOT NULL,
            status       VARCHAR NOT NULL
                     CHECK (status IN ('pending', 'confirmed', 'shipped', 'delivered', 'cancelled')),
            total_amount DECIMAL(18, 2) NOT NULL CHECK (total_amount > 0),
            currency     VARCHAR NOT NULL,
            created_at   TIMESTAMP NOT NULL,
            updated_at   TIMESTAMP NOT NULL
        )
    """)

    # ═══════════════════════════════════════════════════════════════════════════
    # WEAK ENTITY: order_items (lifecycle tied to orders)
    # ═══════════════════════════════════════════════════════════════════════════
    conn.execute("""
        CREATE TABLE IF NOT EXISTS order_items (
            order_item_id  VARCHAR PRIMARY KEY,
            order_id       VARCHAR NOT NULL REFERENCES orders(order_id),
            product_id     VARCHAR NOT NULL REFERENCES products(product_id),
            quantity       INT NOT NULL CHECK (quantity > 0),
            unit_price     DECIMAL(18, 2) NOT NULL CHECK (unit_price > 0),
            line_total     DECIMAL(18, 2) NOT NULL CHECK (line_total > 0),
            created_at     TIMESTAMP NOT NULL
        )
    """)

    # ═══════════════════════════════════════════════════════════════════════════
    # WEAK ENTITY: inventory_logs (append-only transaction log)
    # ═══════════════════════════════════════════════════════════════════════════
    conn.execute("""
        CREATE TABLE IF NOT EXISTS inventory_logs (
            log_id           VARCHAR PRIMARY KEY,
            product_id       VARCHAR NOT NULL REFERENCES products(product_id),
            transaction_type VARCHAR NOT NULL
                         CHECK (transaction_type IN ('purchase', 'return', 'adjustment')),
            qty_change       INT NOT NULL,
            reason           VARCHAR,
            created_at       TIMESTAMP NOT NULL
        )
    """)

    # ═══════════════════════════════════════════════════════════════════════════
    # WEAK ENTITY: shipments (lifecycle tied to orders)
    # ═══════════════════════════════════════════════════════════════════════════
    conn.execute("""
        CREATE TABLE IF NOT EXISTS shipments (
            shipment_id  VARCHAR PRIMARY KEY,
            order_id     VARCHAR NOT NULL REFERENCES orders(order_id),
            status       VARCHAR NOT NULL
                     CHECK (status IN ('pending', 'shipped', 'in_transit', 'delivered')),
            carrier      VARCHAR,
            tracking_num VARCHAR,
            shipped_at   TIMESTAMP,
            delivered_at TIMESTAMP,
            created_at   TIMESTAMP NOT NULL
        )
    """)

    # ═══════════════════════════════════════════════════════════════════════════
    # WEAK ENTITY: returns (lifecycle tied to order_items)
    # ═══════════════════════════════════════════════════════════════════════════
    conn.execute("""
        CREATE TABLE IF NOT EXISTS returns (
            return_id     VARCHAR PRIMARY KEY,
            order_item_id VARCHAR NOT NULL REFERENCES order_items(order_item_id),
            reason        VARCHAR NOT NULL,
            status        VARCHAR NOT NULL
                      CHECK (status IN ('requested', 'approved', 'rejected', 'refunded')),
            refund_amount DECIMAL(18, 2) NOT NULL CHECK (refund_amount > 0),
            created_at    TIMESTAMP NOT NULL,
            updated_at    TIMESTAMP NOT NULL
        )
    """)


def create_indexes(conn: duckdb.DuckDBPyConnection) -> None:
    """
        Create indexes on frequently accessed columns for performance.
        
        Indexes on:
        - Foreign key columns (for joins in CDC processing)
        - Search columns (email, sku, order_date for lookups)
        - CDC-relevant columns (table_name, primary_key in lake)
    """
    # Foreign key indexes (for joins during CDC processing)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON order_items(product_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_inventory_logs_product_id ON inventory_logs(product_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shipments_order_id ON shipments(order_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_returns_order_item_id ON returns(order_item_id)")
    
    # Search/lookup indexes
    conn.execute("CREATE INDEX IF NOT EXISTS idx_products_sku ON products(sku)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_customers_email ON customers(email)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_order_date ON orders(order_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status)")
    
    # CDC lake indexes (for replaying changes efficiently)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_lake_table_name ON lake_cdc_events(table_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_lake_primary_key ON lake_cdc_events(primary_key)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_lake_sequence ON lake_cdc_events(sequence)")
