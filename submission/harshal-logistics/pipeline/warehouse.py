"""
Warehouse layer — current-state snapshot built from CDC events.

Each warehouse table mirrors the source table with two extra columns:
  _cdc_seq  : sequence of the last CDC event that touched this row
  _deleted  : soft-delete flag set when a DELETE event is received

Also implements Point-in-Time recovery by replaying append-only logs.
"""

from __future__ import annotations

import json
import duckdb
from datetime import datetime

from pipeline.cdc import CDCRecord

# Source table → warehouse table
_TABLE_MAP: dict[str, str] = {
    "trade_partners": "wh_trade_partners",
    "trade_contracts": "wh_trade_contracts",
    "shipment_cargo": "wh_shipment_cargo",
    "customs_clearances": "wh_customs_clearances",
    "settlement_transactions": "wh_settlement_transactions",
}

# Source table → primary key column name
_PK_MAP: dict[str, str] = {
    "trade_partners": "partner_id",
    "trade_contracts": "contract_id",
    "shipment_cargo": "cargo_id",
    "customs_clearances": "clearance_id",
    "settlement_transactions": "transaction_id",
}


def create_warehouse_tables(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS wh_trade_partners (
            partner_id         VARCHAR PRIMARY KEY,
            company_name       VARCHAR,
            country            VARCHAR,
            partner_type       VARCHAR,
            compliance_status  VARCHAR,
            created_at         TIMESTAMP,
            updated_at         TIMESTAMP,
            _cdc_seq           INTEGER NOT NULL,
            _deleted           BOOLEAN NOT NULL DEFAULT false
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS wh_trade_contracts (
            contract_id         VARCHAR PRIMARY KEY,
            seller_id           VARCHAR,
            buyer_id            VARCHAR,
            commodity_type      VARCHAR,
            weight_metric_tons  DECIMAL(18, 4),
            price_per_ton_usd   DECIMAL(18, 2),
            total_value_usd     DECIMAL(18, 2),
            status              VARCHAR,
            created_at          TIMESTAMP,
            updated_at          TIMESTAMP,
            _cdc_seq           INTEGER NOT NULL,
            _deleted           BOOLEAN NOT NULL DEFAULT false
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS wh_shipment_cargo (
            cargo_id            VARCHAR PRIMARY KEY,
            contract_id         VARCHAR,
            carrier_name        VARCHAR,
            container_number    VARCHAR,
            port_of_loading     VARCHAR,
            port_of_discharge    VARCHAR,
            estimated_delivery  TIMESTAMP,
            actual_delivery     TIMESTAMP,
            _cdc_seq           INTEGER NOT NULL,
            _deleted           BOOLEAN NOT NULL DEFAULT false
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS wh_customs_clearances (
            clearance_id      VARCHAR PRIMARY KEY,
            contract_id       VARCHAR,
            clearing_country  VARCHAR,
            duty_fee_usd      DECIMAL(18, 2),
            status            VARCHAR,
            cleared_at        TIMESTAMP,
            _cdc_seq           INTEGER NOT NULL,
            _deleted           BOOLEAN NOT NULL DEFAULT false
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS wh_settlement_transactions (
            transaction_id      VARCHAR PRIMARY KEY,
            contract_id         VARCHAR,
            payment_amount_usd  DECIMAL(18, 2),
            settlement_type     VARCHAR,
            status              VARCHAR,
            settled_at          TIMESTAMP,
            _cdc_seq           INTEGER NOT NULL,
            _deleted           BOOLEAN NOT NULL DEFAULT false
        )
    """)


def apply_cdc_records(
    conn: duckdb.DuckDBPyConnection, records: list[CDCRecord]
) -> None:
    """
    Apply CDC records to the warehouse current-state tables in sequence order.

    - Out-of-order and duplicate safety: ignore changes if sequence <= _cdc_seq.
    - insert / update  → upsert (insert new row or overwrite existing)
    - delete           → set _deleted = true
    """
    for record in sorted(records, key=lambda r: r.sequence):
        wh_table = _TABLE_MAP.get(record.table)
        pk_col = _PK_MAP.get(record.table)
        if not wh_table or not pk_col:
            continue

        pk_val = record.primary_key

        # Check existing row sequence for deduplication / out-of-order checks
        existing_row = conn.execute(
            f"SELECT _cdc_seq, _deleted FROM {wh_table} WHERE {pk_col} = ?",
            [pk_val],
        ).fetchone()

        if existing_row:
            existing_seq = existing_row[0]
            # Safety Check: If we receive an older or duplicate event, discard it.
            if record.sequence <= existing_seq:
                continue

        if record.operation == "delete":
            if existing_row:
                conn.execute(
                    f"UPDATE {wh_table} SET _deleted = true, _cdc_seq = ?"
                    f" WHERE {pk_col} = ?",
                    [record.sequence, pk_val],
                )
            else:
                # If deleted before inserted, insert a soft-deleted placeholder row
                conn.execute(
                    f"INSERT INTO {wh_table} ({pk_col}, _cdc_seq, _deleted) VALUES (?, ?, true)",
                    [pk_val, record.sequence],
                )
            continue

        # For insert or update, prepare final columns
        data = {**record.data, "_cdc_seq": record.sequence, "_deleted": False}
        cols = list(data.keys())
        vals = list(data.values())
        placeholders = ", ".join(["?"] * len(vals))

        if existing_row:
            set_clause = ", ".join([f"{c} = ?" for c in cols])
            conn.execute(
                f"UPDATE {wh_table} SET {set_clause} WHERE {pk_col} = ?",
                vals + [pk_val],
            )
        else:
            col_list = ", ".join(cols)
            conn.execute(
                f"INSERT INTO {wh_table} ({col_list}) VALUES ({placeholders})",
                vals,
            )


def reconstruct_warehouse_at(conn: duckdb.DuckDBPyConnection, lsn: int) -> None:
    """
    Reconstructs the warehouse state exactly at a past sequence number (LSN).
    Trashes current warehouse tables and replays lake history up to sequence LSN.
    """
    # 1. Drop existing tables
    for wh_table in _TABLE_MAP.values():
        conn.execute(f"DROP TABLE IF EXISTS {wh_table}")

    # 2. Re-create empty schemas
    create_warehouse_tables(conn)

    # 3. Pull all matching records from the append-only Lake
    rows = conn.execute(
        """
        SELECT sequence, operation, table_name, primary_key, data, captured_at
        FROM lake_cdc_events
        WHERE sequence <= ?
        ORDER BY sequence
        """,
        [lsn],
    ).fetchall()

    reconstructed: list[CDCRecord] = []
    for r in rows:
        seq, op, table_name, pk, data_str, captured_at = r
        data = json.loads(data_str)
        # Re-create datetime object from serialized string
        rec = CDCRecord(
            operation=op,
            table=table_name,
            primary_key=pk,
            data=data,
            captured_at=captured_at,
            sequence=seq,
        )
        reconstructed.append(rec)

    # 4. Apply history up to LSN
    apply_cdc_records(conn, reconstructed)
