from __future__ import annotations

from typing import Any, Dict, List

from src.cdc.events import CDCEvent, create_event


PRIMARY_KEYS = {
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "payments": "payment_id",
}


class CDCGenerator:
    """
    Generates CDC events by comparing two snapshots
    of the source database.

    The generator supports:

    INSERT -> record exists only in the current snapshot
    UPDATE -> record exists in both snapshots but changed
    DELETE -> record exists only in the previous snapshot
    """

    def __init__(self, primary_keys: Dict[str, str] | None = None):
        self.primary_keys = primary_keys or PRIMARY_KEYS

    def generate(
        self,
        previous_snapshot: Dict[str, List[Dict[str, Any]]],
        current_snapshot: Dict[str, List[Dict[str, Any]]],
    ) -> List[CDCEvent]:
        """
        Compare two source snapshots and return CDC events.
        """

        events: List[CDCEvent] = []

        for table_name, primary_key in self.primary_keys.items():
            previous_records = self._index_records(
                previous_snapshot.get(table_name, []),
                primary_key,
            )

            current_records = self._index_records(
                current_snapshot.get(table_name, []),
                primary_key,
            )

            # INSERT
            for key, current_record in current_records.items():
                if key not in previous_records:
                    events.append(
                        create_event(
                            table_name=table_name,
                            operation="INSERT",
                            primary_key={primary_key: key},
                            before=None,
                            after=current_record,
                        )
                    )

            # UPDATE
            for key, current_record in current_records.items():
                if key not in previous_records:
                    continue

                previous_record = previous_records[key]

                if previous_record != current_record:
                    events.append(
                        create_event(
                            table_name=table_name,
                            operation="UPDATE",
                            primary_key={primary_key: key},
                            before=previous_record,
                            after=current_record,
                        )
                    )

            # DELETE
            for key, previous_record in previous_records.items():
                if key not in current_records:
                    events.append(
                        create_event(
                            table_name=table_name,
                            operation="DELETE",
                            primary_key={primary_key: key},
                            before=previous_record,
                            after=None,
                        )
                    )

        return events

    @staticmethod
    def _index_records(
        records: List[Dict[str, Any]],
        primary_key: str,
    ) -> Dict[Any, Dict[str, Any]]:
        """
        Index records by their primary-key value.
        """

        indexed: Dict[Any, Dict[str, Any]] = {}

        for record in records:
            if primary_key not in record:
                raise ValueError(
                    "Record is missing primary key '{}'".format(
                        primary_key
                    )
                )

            key = record[primary_key]

            if key in indexed:
                raise ValueError(
                    "Duplicate primary key '{}' value '{}'".format(
                        primary_key,
                        key,
                    )
                )

            indexed[key] = record

        return indexed