"""Schema compatibility checks used before CDC events are written downstream."""

from __future__ import annotations

from dataclasses import dataclass

from source.models import SCHEMA_CONTRACT


class IncompatibleSchemaChange(Exception):
    """Raised when a source schema breaks the downstream data contract."""


@dataclass(frozen=True)
class ContractViolation:
    table: str
    message: str


def validate_event_schema(table: str, columns: dict[str, str]) -> list[ContractViolation]:
    if table not in SCHEMA_CONTRACT:
        return [ContractViolation(table, "table is not registered in the schema contract")]

    expected = SCHEMA_CONTRACT[table]["columns"]  # type: ignore[index]
    violations: list[ContractViolation] = []

    for name, expected_type in expected.items():  # type: ignore[union-attr]
        if name not in columns:
            violations.append(ContractViolation(table, f"{name}: missing column"))
            continue
        actual_type = _normalize(columns[name])
        if actual_type != _normalize(expected_type):
            violations.append(
                ContractViolation(
                    table,
                    f"{name}: expected {expected_type}, received {columns[name]}",
                )
            )

    return violations


def assert_compatible(table: str, columns: dict[str, str]) -> None:
    violations = validate_event_schema(table, columns)
    if violations:
        details = "; ".join(f"{v.table}: {v.message}" for v in violations)
        raise IncompatibleSchemaChange(details)


def _normalize(type_name: object) -> str:
    return str(type_name).upper().replace(" ", "")

