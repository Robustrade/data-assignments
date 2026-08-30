{#-
    GENERATED FILE - DO NOT EDIT.

    Source of truth : contracts/*.json
    Regenerate with : python scripts/generate_dbt_entities.py
    Verified by     : python scripts/check_schema_contracts.py (--check mode)

    Holding the entity spec in one place is what lets the warehouse models,
    the not-null assertions and the enum domain assertions all be derived from
    the same published contract instead of a hand-maintained copy of it.
-#}
{% macro cdc_entities() %}
{% do return(
{
    "customer": {
        "primary_key": [
            "customer_id"
        ],
        "contract_version": 1,
        "columns": [
            {
                "name": "customer_id",
                "type": "BIGINT",
                "nullable": false
            },
            {
                "name": "email",
                "type": "VARCHAR",
                "nullable": false
            },
            {
                "name": "full_name",
                "type": "VARCHAR",
                "nullable": false
            },
            {
                "name": "status",
                "type": "VARCHAR",
                "nullable": false
            },
            {
                "name": "phone",
                "type": "VARCHAR",
                "nullable": true
            },
            {
                "name": "created_at",
                "type": "VARCHAR",
                "nullable": false
            },
            {
                "name": "updated_at",
                "type": "VARCHAR",
                "nullable": false
            }
        ],
        "enums": {
            "status": [
                "ACTIVE",
                "SUSPENDED",
                "CLOSED"
            ]
        }
    },
    "product": {
        "primary_key": [
            "product_id"
        ],
        "contract_version": 1,
        "columns": [
            {
                "name": "product_id",
                "type": "BIGINT",
                "nullable": false
            },
            {
                "name": "sku",
                "type": "VARCHAR",
                "nullable": false
            },
            {
                "name": "name",
                "type": "VARCHAR",
                "nullable": false
            },
            {
                "name": "unit_price",
                "type": "DECIMAL(18,2)",
                "nullable": false
            },
            {
                "name": "currency",
                "type": "VARCHAR",
                "nullable": false
            },
            {
                "name": "discontinued_at",
                "type": "VARCHAR",
                "nullable": true
            },
            {
                "name": "created_at",
                "type": "VARCHAR",
                "nullable": false
            },
            {
                "name": "updated_at",
                "type": "VARCHAR",
                "nullable": false
            }
        ],
        "enums": {}
    },
    "customer_order": {
        "primary_key": [
            "order_id"
        ],
        "contract_version": 1,
        "columns": [
            {
                "name": "order_id",
                "type": "BIGINT",
                "nullable": false
            },
            {
                "name": "customer_id",
                "type": "BIGINT",
                "nullable": false
            },
            {
                "name": "status",
                "type": "VARCHAR",
                "nullable": false
            },
            {
                "name": "currency",
                "type": "VARCHAR",
                "nullable": false
            },
            {
                "name": "total_amount",
                "type": "DECIMAL(18,2)",
                "nullable": false
            },
            {
                "name": "placed_at",
                "type": "VARCHAR",
                "nullable": false
            },
            {
                "name": "cancelled_at",
                "type": "VARCHAR",
                "nullable": true
            },
            {
                "name": "created_at",
                "type": "VARCHAR",
                "nullable": false
            },
            {
                "name": "updated_at",
                "type": "VARCHAR",
                "nullable": false
            }
        ],
        "enums": {
            "status": [
                "PENDING",
                "PAID",
                "SHIPPED",
                "DELIVERED",
                "CANCELLED"
            ]
        }
    },
    "order_item": {
        "primary_key": [
            "order_id",
            "line_no"
        ],
        "contract_version": 1,
        "columns": [
            {
                "name": "order_id",
                "type": "BIGINT",
                "nullable": false
            },
            {
                "name": "line_no",
                "type": "BIGINT",
                "nullable": false
            },
            {
                "name": "product_id",
                "type": "BIGINT",
                "nullable": false
            },
            {
                "name": "quantity",
                "type": "BIGINT",
                "nullable": false
            },
            {
                "name": "unit_price",
                "type": "DECIMAL(18,2)",
                "nullable": false
            },
            {
                "name": "extended_amount",
                "type": "DECIMAL(18,2)",
                "nullable": false
            },
            {
                "name": "created_at",
                "type": "VARCHAR",
                "nullable": false
            },
            {
                "name": "updated_at",
                "type": "VARCHAR",
                "nullable": false
            }
        ],
        "enums": {}
    },
    "payment": {
        "primary_key": [
            "payment_id"
        ],
        "contract_version": 1,
        "columns": [
            {
                "name": "payment_id",
                "type": "BIGINT",
                "nullable": false
            },
            {
                "name": "order_id",
                "type": "BIGINT",
                "nullable": false
            },
            {
                "name": "method",
                "type": "VARCHAR",
                "nullable": false
            },
            {
                "name": "amount",
                "type": "DECIMAL(18,2)",
                "nullable": false
            },
            {
                "name": "currency",
                "type": "VARCHAR",
                "nullable": false
            },
            {
                "name": "status",
                "type": "VARCHAR",
                "nullable": false
            },
            {
                "name": "created_at",
                "type": "VARCHAR",
                "nullable": false
            },
            {
                "name": "settled_at",
                "type": "VARCHAR",
                "nullable": true
            },
            {
                "name": "updated_at",
                "type": "VARCHAR",
                "nullable": false
            }
        ],
        "enums": {
            "method": [
                "CARD",
                "BANK_TRANSFER",
                "WALLET"
            ],
            "status": [
                "INITIATED",
                "SETTLED",
                "FAILED",
                "REFUNDED"
            ]
        }
    },
    "payment_attempt": {
        "primary_key": [
            "payment_id",
            "attempt_no"
        ],
        "contract_version": 1,
        "columns": [
            {
                "name": "payment_id",
                "type": "BIGINT",
                "nullable": false
            },
            {
                "name": "attempt_no",
                "type": "BIGINT",
                "nullable": false
            },
            {
                "name": "result",
                "type": "VARCHAR",
                "nullable": false
            },
            {
                "name": "failure_code",
                "type": "VARCHAR",
                "nullable": true
            },
            {
                "name": "attempted_at",
                "type": "VARCHAR",
                "nullable": false
            },
            {
                "name": "created_at",
                "type": "VARCHAR",
                "nullable": false
            },
            {
                "name": "updated_at",
                "type": "VARCHAR",
                "nullable": false
            }
        ],
        "enums": {
            "result": [
                "SUCCESS",
                "DECLINED",
                "ERROR",
                "TIMEOUT"
            ]
        }
    }
}
) %}
{% endmacro %}
