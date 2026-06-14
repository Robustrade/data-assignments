import json
import logging

from pipeline.schema_contracts import (
    SchemaValidator,
    SchemaContractException
)

from source.postgres import (
    PostgresMetadataReader
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

logger = logging.getLogger(__name__)


SCHEMA_CONTRACT_FILE = (
    "config/schema_contracts.json"
)

ENUM_CONTRACT_FILE = (
    "config/enum_contracts.json"
)


def load_json(file_path):

    with open(file_path, "r") as f:

        return json.load(f)


def validate_table_schemas():

    logger.info(
        "Loading schema contracts..."
    )

    expected_contracts = load_json(
        SCHEMA_CONTRACT_FILE
    )

    metadata_reader = (
        PostgresMetadataReader()
    )

    actual_contracts = (
        metadata_reader.get_table_schemas()
    )

    validator = SchemaValidator(
        expected_contracts
    )

    validator.validate(
        actual_contracts
    )

    logger.info(
        "Schema validation successful."
    )


def validate_enum_domains():

    logger.info(
        "Loading enum contracts..."
    )

    expected_enums = load_json(
        ENUM_CONTRACT_FILE
    )

    metadata_reader = (
        PostgresMetadataReader()
    )

    actual_enums = (
        metadata_reader.get_enum_domains()
    )

    for enum_name, expected_values in (
            expected_enums.items()
    ):

        actual_values = actual_enums.get(
            enum_name,
            []
        )

        if set(actual_values) != set(
                expected_values
        ):

            raise SchemaContractException(
                f"Enum contract violation "
                f"for {enum_name}. "
                f"Expected={expected_values}, "
                f"Actual={actual_values}"
            )

    logger.info(
        "Enum validation successful."
    )


def main():

    try:

        logger.info(
            "=" * 50
        )

        logger.info(
            "Starting schema validation"
        )

        validate_table_schemas()

        validate_enum_domains()

        logger.info(
            "Schema validation passed"
        )

        logger.info(
            "=" * 50
        )

    except Exception as e:

        logger.error(
            "Schema validation failed"
        )

        logger.exception(e)

        raise


if __name__ == "__main__":

    main()