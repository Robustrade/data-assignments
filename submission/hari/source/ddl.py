"""Generate the source database DDL from the contract.

Everything here is derived from ``contracts/source_contract.yml``: tables, column
CHECKs, table CHECKs, indexes, lifecycle / immutability triggers, the ``change_log`` and
``_txn_context`` support tables, and the CDC triggers that write ``change_log``.
"""

from __future__ import annotations

from pipeline.contract import Contract, Table, cdc_trigger_names

CHANGE_LOG_COLUMNS = (
    'txn_id, schema_version, table_name, op, pk, "before", "after", changed_at'
)


def _quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def column_check_sql(table: Table) -> list[tuple[str, str]]:
    """Per-column CHECK clauses as ``(column, sql)``; the same rules feed the warehouse."""
    out = []
    for col in table.columns:
        if col.enum:
            values = ", ".join(_quote(v) for v in col.enum)
            out.append((col.name, f"CHECK ({col.name} IN ({values}))"))
        if col.min is not None:
            out.append((col.name, f"CHECK ({col.name} >= {col.min})"))
        if col.max is not None:
            out.append((col.name, f"CHECK ({col.name} <= {col.max})"))
    return out


def table_sql(table: Table) -> str:
    checks: dict[str, list[str]] = {}
    for cname, sql in column_check_sql(table):
        checks.setdefault(cname, []).append(sql)
    lines = []
    for col in table.columns:
        parts = [col.name, col.type]
        if not col.nullable:
            parts.append("NOT NULL")
        parts.extend(checks.get(col.name, []))
        lines.append(" ".join(parts))
    lines.append(f"PRIMARY KEY ({', '.join(table.primary_key)})")
    for fk in table.foreign_keys:
        lines.append(
            f"FOREIGN KEY ({', '.join(fk.columns)}) "
            f"REFERENCES {fk.references}({', '.join(fk.ref_columns)})"
        )
    for check in table.table_checks:
        lines.append(f"CHECK ({check.expr})")
    body = ",\n    ".join(lines)
    return f"CREATE TABLE IF NOT EXISTS {table.name} (\n    {body}\n)"


def index_sql(table: Table) -> list[str]:
    out = []
    for idx in table.indexes:
        unique = "UNIQUE " if idx.unique else ""
        where = f" WHERE {idx.where}" if idx.where else ""
        out.append(
            f"CREATE {unique}INDEX IF NOT EXISTS {idx.name} "
            f"ON {table.name} ({', '.join(idx.columns)}){where}"
        )
    return out


def guard_trigger_sql(table: Table) -> list[str]:
    """Immutability, append-only, no-delete and lifecycle triggers (S5, S8, S14)."""
    out = []
    changed = " OR ".join(f"OLD.{c} IS NOT NEW.{c}" for c in table.primary_key)
    out.append(
        f"CREATE TRIGGER IF NOT EXISTS imm_{table.name}_pk BEFORE UPDATE ON {table.name} "
        f"WHEN {changed} BEGIN "
        f"SELECT RAISE(ABORT, 'primary key of {table.name} is immutable'); END"
    )
    if table.no_delete:
        out.append(
            f"CREATE TRIGGER IF NOT EXISTS nodel_{table.name} BEFORE DELETE ON {table.name} "
            f"BEGIN SELECT RAISE(ABORT, '{table.name} rows are never deleted'); END"
        )
    if table.append_only:
        out.append(
            f"CREATE TRIGGER IF NOT EXISTS ao_{table.name}_upd BEFORE UPDATE ON {table.name} "
            f"BEGIN SELECT RAISE(ABORT, '{table.name} is append-only'); END"
        )
    if table.transition_column:
        col = table.transition_column
        # SQLite < 3.47 only accepts a string literal as the RAISE message, so each
        # illegal (from, to) pair gets its own guarded RAISE with a literal message.
        domain = next(c.enum for c in table.columns if c.name == col)
        raises = [
            f"SELECT RAISE(ABORT, {_quote(f'illegal transition {table.name}: {src} -> {dst}')}) "
            f"WHERE OLD.{col} = {_quote(src)} AND NEW.{col} = {_quote(dst)};"
            for src in domain
            for dst in domain
            if src != dst and dst not in table.transitions.get(src, ())
        ]
        out.append(
            f"CREATE TRIGGER IF NOT EXISTS lc_{table.name}_{col} "
            f"BEFORE UPDATE OF {col} ON {table.name} "
            f"WHEN OLD.{col} <> NEW.{col} BEGIN {' '.join(raises)} END"
        )
    return out


def _json_object(table: Table, ref: str, columns: list[str]) -> str:
    pairs = ", ".join(f"'{c}', {ref}.{c}" for c in columns)
    return f"json_object({pairs})"


def cdc_trigger_sql(table: Table, version: int) -> list[tuple[str, str]]:
    """The three CDC triggers of a table, stamped with the contract ``version``."""
    cols = table.column_names
    pk = list(table.primary_key)
    context = "(SELECT txn_id FROM _txn_context)"
    now = (
        'COALESCE((SELECT "now" FROM _txn_context), '
        "strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))"
    )
    ins, upd, dele = cdc_trigger_names(table.name)

    def body(op: str, pk_ref: str, before: str, after: str) -> str:
        return (
            f"INSERT INTO change_log ({CHANGE_LOG_COLUMNS}) VALUES ("
            f"{context}, {version}, '{table.name}', '{op}', "
            f"{_json_object(table, pk_ref, pk)}, {before}, {after}, {now});"
        )

    return [
        (
            ins,
            f"CREATE TRIGGER {ins} AFTER INSERT ON {table.name} BEGIN "
            + body("insert", "NEW", "NULL", _json_object(table, "NEW", cols))
            + " END",
        ),
        (
            upd,
            f"CREATE TRIGGER {upd} AFTER UPDATE ON {table.name} BEGIN "
            + body(
                "update",
                "NEW",
                _json_object(table, "OLD", cols),
                _json_object(table, "NEW", cols),
            )
            + " END",
        ),
        (
            dele,
            f"CREATE TRIGGER {dele} AFTER DELETE ON {table.name} BEGIN "
            + body("delete", "OLD", _json_object(table, "OLD", cols), "NULL")
            + " END",
        ),
    ]


SUPPORT_SQL = [
    """CREATE TABLE IF NOT EXISTS change_log (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    txn_id TEXT,
    schema_version INTEGER NOT NULL,
    table_name TEXT NOT NULL,
    op TEXT NOT NULL CHECK (op IN ('insert', 'update', 'delete')),
    pk TEXT NOT NULL,
    "before" TEXT,
    "after" TEXT,
    changed_at TEXT NOT NULL
)""",
    "CREATE INDEX IF NOT EXISTS ix_change_log_txn ON change_log (txn_id)",
    'CREATE TABLE IF NOT EXISTS _txn_context (txn_id TEXT, "now" TEXT)',
    (
        'INSERT INTO _txn_context (txn_id, "now") SELECT NULL, NULL '
        "WHERE NOT EXISTS (SELECT 1 FROM _txn_context)"
    ),
    (
        "CREATE TRIGGER IF NOT EXISTS cdc_change_log_txn AFTER INSERT ON change_log "
        "WHEN NEW.txn_id IS NULL BEGIN "
        "UPDATE change_log SET txn_id = 'auto-' || NEW.seq WHERE seq = NEW.seq; END"
    ),
]


def schema_statements(contract: Contract) -> list[str]:
    """Every statement needed to create a source database from scratch."""
    stmts: list[str] = []
    for table in contract.tables.values():
        stmts.append(table_sql(table))
        stmts.extend(index_sql(table))
    stmts.extend(SUPPORT_SQL)
    for table in contract.tables.values():
        stmts.extend(guard_trigger_sql(table))
    for table in contract.tables.values():
        stmts.extend(sql for _, sql in cdc_trigger_sql(table, contract.version))
    return stmts


def regenerate_cdc_triggers(conn, contract: Contract) -> None:
    """Drop and re-create every CDC trigger so they are stamped with ``contract.version``."""
    old = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'trigger' AND name LIKE 'cdc\\_%' "
        "ESCAPE '\\' AND name <> 'cdc_change_log_txn'"
    ).fetchall()
    for (name,) in old:
        conn.execute(f'DROP TRIGGER IF EXISTS "{name}"')
    for table in contract.tables.values():
        for _, sql in cdc_trigger_sql(table, contract.version):
            conn.execute(sql)
