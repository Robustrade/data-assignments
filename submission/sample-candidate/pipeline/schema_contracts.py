from dataclasses import dataclass
from typing import Dict, List


@dataclass
class SchemaViolation:

    table_name: str

    violation_type: str

    message: str


class SchemaContractException(Exception):
    pass


class SchemaValidator:

    """
    Compares actual source schema
    against expected schema contract.
    """

    def __init__(
            self,
            expected_contracts: Dict
    ):

        self.expected_contracts = (
            expected_contracts
        )

    def validate(
            self,
            actual_contracts: Dict
    ):

        violations = []

        for table_name in (
                self.expected_contracts.keys()
        ):

            if table_name not in actual_contracts:

                violations.append(
                    SchemaViolation(
                        table_name,
                        "TABLE_MISSING",
                        f"{table_name} missing"
                    )
                )

                continue

            expected_columns = (
                self.expected_contracts[
                    table_name
                ]
            )

            actual_columns = (
                actual_contracts[
                    table_name
                ]
            )

            violations.extend(
                self._compare_columns(
                    table_name,
                    expected_columns,
                    actual_columns
                )
            )

        if violations:

            raise SchemaContractException(
                self._build_error_message(
                    violations
                )
            )

        return True

    def _compare_columns(
            self,
            table_name,
            expected_columns,
            actual_columns
    ):

        violations = []

        for column_name in (
                expected_columns.keys()
        ):

            if column_name not in actual_columns:

                violations.append(
                    SchemaViolation(
                        table_name,
                        "COLUMN_MISSING",
                        f"{column_name} removed"
                    )
                )

                continue

            expected_type = (
                expected_columns[
                    column_name
                ]
            )

            actual_type = (
                actual_columns[
                    column_name
                ]
            )

            if expected_type != actual_type:

                violations.append(
                    SchemaViolation(
                        table_name,
                        "TYPE_CHANGE",
                        (
                            f"{column_name} "
                            f"changed from "
                            f"{expected_type}"
                            f" to "
                            f"{actual_type}"
                        )
                    )
                )

        return violations

    @staticmethod
    def _build_error_message(
            violations: List[SchemaViolation]
    ):

        messages = []

        for violation in violations:

            messages.append(
                (
                    f"[{violation.table_name}] "
                    f"{violation.violation_type}: "
                    f"{violation.message}"
                )
            )

        return "\n".join(messages)