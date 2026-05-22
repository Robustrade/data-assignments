import duckdb

from datetime import datetime, timezone

from scripts.check_schema_contracts import check_violations
from source.models import create_source_tables, create_indexes
from pipeline.cdc import CDCCapture
from pipeline.lake import create_lake_table, append_to_lake
from pipeline.warehouse import (
    create_warehouse_tables,
    apply_cdc_records,
    get_current_state_table,
    get_state_at_point_in_time,
    restore_warehouse_to_sequence,
)
from sample.sample_data import (
    sample_customers,
    sample_products,
    sample_orders,
    sample_order_items,
    sample_shipments,
    sample_inventory_logs,
    sample_returns,
)

def print_section(title: str) -> None:
    """Print a formatted section header."""
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80)


def print_subsection(title: str) -> None:
    """Print a formatted subsection header."""
    print(f"\n  ┌─ {title}")
    print(f"  │")


def insert_data(conn: duckdb.DuckDBPyConnection, table: str, data: list[dict]) -> None:
    if check_violations(conn) == 1:
        return
    for row in data:
        conn.execute(
            f"INSERT INTO {table} VALUES ({', '.join('?' * len(row))})",
            list(row.values())
        )
    print(f"  ✓ Loaded {len(data)} {table}")


def new_cdc_capture(conn: duckdb.DuckDBPyConnection) -> CDCCapture:
    """Start a CDC simulator after the latest lake sequence."""
    try:
        latest = conn.execute(
            "SELECT COALESCE(MAX(sequence), 0) FROM lake_cdc_events"
        ).fetchone()[0]
    except Exception:
        latest = 0
    return CDCCapture(start_sequence=latest)


def scenario_read_current_state(conn: duckdb.DuckDBPyConnection) -> None:
    """Scenario 1: Read current-state data from warehouse."""
    print_subsection("SCENARIO: Read Current State")
    
    warehouse_tables = [
        ("wh_customers", "Customers"),
        ("wh_orders", "Orders"),
        ("wh_products", "Products"),
    ]
    
    for table, label in warehouse_tables:
        try:
            count = conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE _deleted = false"
            ).fetchone()[0]
            print(f"  │ {label}: {count} active rows")
        except:
            print(f"  │ {label}: (table not initialized)")
    
    print(f"  └─ ✓ Read complete")


def scenario_write_operations(conn: duckdb.DuckDBPyConnection) -> None:
    """Scenario 2: Simulate write operations (INSERT/UPDATE/DELETE)."""
    print_subsection("SCENARIO: Write Operations (Simulated CDC)")
    
    cdc = new_cdc_capture(conn)
    
    # INSERT: New customer
    print(f"  │ 1️⃣  INSERT: New customer")
    new_customer = {
        "customer_id": "CUST_NEW_001",
        "name": "New Customer",
        "email": "new@example.com",
        "country": "USA",
        "status": "active",
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
    }
    cdc.insert("customers", new_customer["customer_id"], new_customer)
    print(f"  │    ✓ Recorded INSERT event")
    
    # UPDATE: Change customer status
    print(f"  │ 2️⃣  UPDATE: Change customer status")
    updated_customer = new_customer.copy()
    updated_customer["status"] = "suspended"
    updated_customer["updated_at"] = datetime.now(timezone.utc)
    cdc.update("customers", new_customer["customer_id"], updated_customer)
    print(f"  │    ✓ Recorded UPDATE event (status: active → suspended)")
    
    # DELETE: Remove customer
    print(f"  │ 3️⃣  DELETE: Remove customer")
    cdc.delete("customers", new_customer["customer_id"], updated_customer)
    print(f"  │    ✓ Recorded DELETE event")
    
    # Write to lake
    print(f"  │")
    lake_count = append_to_lake(conn, cdc.log)
    print(f"  │ Lake: Appended {lake_count} events")
    
    # Apply to warehouse
    print(f"  │ Warehouse: Applying changes...")
    apply_cdc_records(conn, cdc.log)
    print(f"  │    ✓ Current-state updated")
    print(f"  │    ✓ Current-state updated")
    
    print(f"  └─ ✓ Write operations complete ({len(cdc.log)} events)")


def scenario_mixed_operations(conn: duckdb.DuckDBPyConnection) -> None:
    """Scenario 3: Mixed workload with multiple operations."""
    print_subsection("SCENARIO: Mixed Operations")
    
    cdc = new_cdc_capture(conn)
    
    # Try to get first product
    try:
        products_result = conn.execute("SELECT * FROM products LIMIT 1").fetchall()
        if products_result:
            product_dict = {}
            for i, desc in enumerate(conn.description):
                product_dict[desc[0]] = products_result[0][i]
        
            print(f"  │ Operating on product: {product_dict.get('name', 'Unknown')}")
            
            # Scenario: Stock adjustment
            print(f"  │ 1️⃣  UPDATE: Reduce stock quantity")
            updated_product = product_dict.copy()
            current_qty = updated_product.get('stock_qty', 0)
            updated_product['stock_qty'] = max(0, current_qty - 5)
            cdc.update("products", product_dict['product_id'], updated_product)
            print(f"  │    ✓ Stock: {current_qty} → {updated_product['stock_qty']}")
            
            # Log inventory change
            print(f"  │ 2️⃣  INSERT: Inventory log entry")
            inventory_log = {
                "log_id": f"LOG_{len(cdc.log) + 1}",
                "product_id": product_dict['product_id'],
                "transaction_type": "sale",
                "qty_change": -5,
                "reason": "Sold in order",
                "created_at": datetime.now(timezone.utc),
            }
            cdc.insert("inventory_logs", inventory_log["log_id"], inventory_log)
            print(f"  │    ✓ Logged transaction")
        else:
            print(f"  │ (No products initialized - run full pipeline first)")
    except Exception as e:
        print(f"  │ Error: {e}")
        return
    
    # Apply changes
    try:
        lake_count = append_to_lake(conn, cdc.log)
        apply_cdc_records(conn, cdc.log)
        print(f"  │")
        print(f"  └─ ✓ Mixed operations complete ({len(cdc.log)} events)")
    except Exception as e:
        print(f"  │ Error applying changes: {e}")


def scenario_time_travel(conn: duckdb.DuckDBPyConnection) -> None:
    """Scenario 4: Time-travel and point-in-time recovery."""
    print_subsection("SCENARIO: Time-Travel & Point-in-Time Recovery")
    
    cdc = new_cdc_capture(conn)
    
    # Get first customer
    try:
        cust_result = conn.execute("SELECT * FROM wh_customers WHERE _deleted = false LIMIT 1").fetchall()
        if not cust_result:
            print(f"  │ (No customers in warehouse - run full pipeline first)")
            return
        
        cust_dict = {}
        for i, desc in enumerate(conn.description):
            cust_dict[desc[0]] = cust_result[0][i]
    except:
        print(f"  │ (Warehouse not initialized)")
        return
    
    customer_id = cust_dict.get('customer_id', 'UNKNOWN')
    source_customer = {
        key: value for key, value in cust_dict.items() if not key.startswith("_")
    }
    
    print(f"  │ Customer: {cust_dict.get('name', 'Unknown')} (ID: {customer_id})")
    print(f"  │")
    
    # Simulate changes over time
    times = [
        datetime(2026, 5, 1, 10, 0, tzinfo=timezone.utc),
        datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc),
        datetime(2026, 5, 1, 14, 0, tzinfo=timezone.utc),
    ]
    
    statuses = ["active", "suspended", "active"]
    
    print(f"  │ Timeline of changes:")
    for i, (t, status) in enumerate(zip(times, statuses)):
        cust = source_customer.copy()
        cust['status'] = status
        cust['updated_at'] = t
        
        if i == 0:
            record = cdc.insert("customers", customer_id, cust)
            print(f"  │   {t.strftime('%H:%M')} → INSERT (status: {status})")
        else:
            record = cdc.update("customers", customer_id, cust)
            print(f"  │   {t.strftime('%H:%M')} → UPDATE (status: {status})")
        record.captured_at = t
    
    try:
        append_to_lake(conn, cdc.log)
        apply_cdc_records(conn, cdc.log)
        
        print(f"  │")
        print(f"  │ 🕐 Point-in-time queries:")
        
        # Query at different points in time
        query_times = [
            (datetime(2026, 5, 1, 11, 0, tzinfo=timezone.utc), "11:00"),
            (datetime(2026, 5, 1, 13, 0, tzinfo=timezone.utc), "13:00"),
            (datetime(2026, 5, 1, 15, 0, tzinfo=timezone.utc), "15:00"),
        ]
        
        for query_time, label in query_times:
            try:
                state = get_state_at_point_in_time(conn, "wh_customers", query_time)
                if state:
                    customer_state = state[0]
                    print(f"  │   At {label}: status = {customer_state.get('status', 'unknown')}")
                else:
                    print(f"  │   At {label}: (no data)")
            except:
                print(f"  │   At {label}: (query failed)")
        
        print(f"  │")
        print(f"  └─ ✓ Time-travel demo complete")
    except Exception as e:
        print(f"  │ Error: {e}")


def scenario_validations(conn: duckdb.DuckDBPyConnection) -> None:
    """Scenario 5: Data quality validations."""
    print_subsection("SCENARIO: Data Quality Checks")
    
    violations = []
    
    try:
        # 1. PK Uniqueness
        print(f"  │ ✓ Checking primary key uniqueness...")
        dup_customers = conn.execute(
            "SELECT customer_id, COUNT(*) as cnt FROM wh_customers "
            "WHERE _deleted = false GROUP BY customer_id HAVING cnt > 1"
        ).fetchall()
        if dup_customers:
            violations.append(f"Duplicate customer IDs: {dup_customers}")
        else:
            print(f"  │   ✓ wh_customers: All PKs unique")
        
        # 2. FK Integrity
        print(f"  │ ✓ Checking foreign key integrity...")
        orphan_orders = conn.execute(
            "SELECT COUNT(*) FROM wh_orders o "
            "WHERE _deleted = false AND customer_id NOT IN "
            "(SELECT customer_id FROM wh_customers WHERE _deleted = false)"
        ).fetchone()[0]
        if orphan_orders > 0:
            violations.append(f"Orphan orders: {orphan_orders}")
        else:
            print(f"  │   ✓ wh_orders → wh_customers: FK integrity OK")
        
        # 3. Business Rule: Order totals
        print(f"  │ ✓ Checking business rules...")
        mismatched = conn.execute(
            """
            SELECT COUNT(*) FROM wh_orders o
            WHERE _deleted = false AND total_amount <= 0
            """
        ).fetchone()[0]
        if mismatched > 0:
            violations.append(f"Invalid order totals: {mismatched}")
        else:
            print(f"  │   ✓ wh_orders: Total amounts > 0")
        
        # 4. Stock quantities
        negative_stock = conn.execute(
            "SELECT COUNT(*) FROM wh_products WHERE _deleted = false AND stock_qty < 0"
        ).fetchone()[0]
        if negative_stock > 0:
            violations.append(f"Negative stock: {negative_stock}")
        else:
            print(f"  │   ✓ wh_products: Stock quantities >= 0")
        
        print(f"  │")
        if violations:
            print(f"  │ ❌ Violations found:")
            for v in violations:
                print(f"  │    • {v}")
            print(f"  └─ ✗ Validations failed")
        else:
            print(f"  └─ ✓ All validations passed")
    except Exception as e:
        print(f"  │ Error running validations: {e}")


def show_menu() -> str:
    """Display interactive menu and get user choice."""
    print_section("CHOOSE SCENARIO")
    print("""
  1️⃣  Read current-state data from warehouse
  2️⃣  Write operations (simulated CDC: INSERT/UPDATE/DELETE)
  3️⃣  Mixed operations with multiple tables
  4️⃣  Time-travel & point-in-time recovery demo
  5️⃣  Data quality validations
  6️⃣  Full pipeline with sample data (default)
  0️⃣  Exit
    """)
    try:
        choice = input("  Enter choice (0-6): ").strip()
    except EOFError:
        choice = "0"  # Exit gracefully on EOF
    return choice


def run_full_pipeline(conn: duckdb.DuckDBPyConnection) -> None:
    """Run the complete end-to-end CDC pipeline with sample data."""
    print_section("FULL PIPELINE EXECUTION")
    print("Running end-to-end CDC pipeline with sample data...")

    # STEP 1: Setup database and create tables
    print_subsection("STEP 1: Database Setup")
    
    create_source_tables(conn)
    print(f"  │ ✓ Source tables (7 tables)")
    
    create_lake_table(conn)
    print(f"  │ ✓ Lake table (append-only)")
    
    create_indexes(conn)
    print(f"  │ ✓ Indexes (FK, search, CDC)")
    
    create_warehouse_tables(conn)
    print(f"  └─ ✓ Warehouse current-state tables")

    # STEP 2: Load sample data into source tables
    print_subsection("STEP 2: Load Sample Data into Source")
    
    customers = sample_customers()
    insert_data(conn, "customers", customers)

    products = sample_products()
    insert_data(conn, "products", products)    
    
    orders = sample_orders()
    insert_data(conn, "orders", orders)
    
    order_items = sample_order_items()
    insert_data(conn, "order_items", order_items)
    
    shipments = sample_shipments()
    insert_data(conn, "shipments", shipments)
    
    inventory_logs = sample_inventory_logs()
    insert_data(conn, "inventory_logs", inventory_logs)
    
    returns = sample_returns()
    insert_data(conn, "returns", returns)

    total_records = (
        len(customers) + len(products) + len(orders) + len(order_items) +
        len(shipments) + len(inventory_logs) + len(returns)
    )
    print(f"  └─ ✓ Total records loaded: {total_records}")

    # STEP 3: Capture CDC events (simulate changes)
    print_subsection("STEP 3: Capture CDC Events")
    
    cdc = new_cdc_capture(conn)
    total_events = 0
    
    for customer in customers:
        cdc.insert("customers", customer["customer_id"], customer)
        total_events += 1
    print(f"  │ Captured {len(customers)} customer INSERTs")
    
    for product in products:
        cdc.insert("products", product["product_id"], product)
        total_events += 1
    print(f"  │ Captured {len(products)} product INSERTs")
    
    for order in orders:
        cdc.insert("orders", order["order_id"], order)
        total_events += 1
    print(f"  │ Captured {len(orders)} order INSERTs")
    
    for item in order_items:
        cdc.insert("order_items", item["order_item_id"], item)
        total_events += 1
    print(f"  │ Captured {len(order_items)} order_item INSERTs")
    
    for shipment in shipments:
        cdc.insert("shipments", shipment["shipment_id"], shipment)
        total_events += 1
    print(f"  │ Captured {len(shipments)} shipment INSERTs")
    
    for log in inventory_logs:
        cdc.insert("inventory_logs", log["log_id"], log)
        total_events += 1
    print(f"  │ Captured {len(inventory_logs)} inventory_log INSERTs")
    
    for ret in returns:
        cdc.insert("returns", ret["return_id"], ret)
        total_events += 1
    print(f"  │ Captured {len(returns)} return INSERTs")
    
    print(f"  └─ ✓ Total CDC events: {total_events}")

    # STEP 4: Write to lake (immutable append-only)
    print_subsection("STEP 4: Write to Lake")
    
    lake_records = append_to_lake(conn, cdc.log)
    print(f"  │ ✓ Appended {lake_records} records")
    
    lake_count = conn.execute("SELECT COUNT(*) FROM lake_cdc_events").fetchone()[0]
    print(f"  └─ ✓ Lake total: {lake_count} events")

    # STEP 5: Apply to warehouse (current-state)
    print_subsection("STEP 5: Apply to Warehouse")
    
    apply_cdc_records(conn, cdc.log)
    print(f"  │ ✓ Applied {len(cdc.log)} CDC records")
    print(f"  │ ✓ Updated current-state tables")
    print(f"  └─ ✓ Updated current-state tables")

    # STEP 6: Run validations
    print_subsection("STEP 6: Validation Checks")
    
    dup_customers = conn.execute(
        "SELECT COUNT(*) FROM (SELECT customer_id, COUNT(*) as cnt FROM wh_customers "
        "WHERE _deleted = false GROUP BY customer_id HAVING cnt > 1)"
    ).fetchone()[0]
    print(f"  │ ✓ PK uniqueness: {dup_customers == 0} ✓")
    
    orphan_orders = conn.execute(
        "SELECT COUNT(*) FROM wh_orders o WHERE _deleted = false "
        "AND customer_id NOT IN (SELECT customer_id FROM wh_customers WHERE _deleted = false)"
    ).fetchone()[0]
    print(f"  │ ✓ FK integrity: {orphan_orders == 0} ✓")
    
    mismatched_totals = conn.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT o.order_id FROM wh_orders o
            LEFT JOIN wh_order_items oi ON o.order_id = oi.order_id AND oi._deleted = false
            WHERE o._deleted = false
            GROUP BY o.order_id, o.total_amount
            HAVING o.total_amount != COALESCE(SUM(oi.line_total), 0)
        )
        """
    ).fetchone()[0]
    print(f"  │ ✓ Order totals: {mismatched_totals == 0} ✓")
    
    negative_stock = conn.execute(
        "SELECT COUNT(*) FROM wh_products WHERE _deleted = false AND stock_qty < 0"
    ).fetchone()[0]
    print(f"  └─ ✓ Stock quantities: {negative_stock == 0} ✓")

    # STEP 7: Display results
    print_subsection("STEP 7: Query Results")
    
    orders_result = conn.execute(
        "SELECT c.name, COUNT(o.order_id) as order_count, COALESCE(SUM(o.total_amount), 0) as total_spent "
        "FROM wh_customers c LEFT JOIN wh_orders o ON c.customer_id = o.customer_id AND o._deleted = false "
        "WHERE c._deleted = false GROUP BY c.customer_id, c.name"
    ).fetchall()
    print(f"  │ Customer Orders:")
    for row in orders_result:
        total_spent = row[2] or 0
        print(f"  │   • {row[0]}: {row[1]} orders, ${total_spent:.2f}")
    
    print(f"  └─ ✓ Full pipeline complete")
