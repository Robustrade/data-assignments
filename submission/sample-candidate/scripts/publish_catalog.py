import json
import logging
from pathlib import Path


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

logger = logging.getLogger(__name__)


CATALOG_FILE = "catalog/catalog.json"


class CatalogPublisher:

    def __init__(
            self,
            catalog_file
    ):

        self.catalog_file = catalog_file

    def load_catalog(self):

        with open(
                self.catalog_file,
                "r"
        ) as file:

            return json.load(file)

    def validate_catalog(self):

        catalog = self.load_catalog()

        datasets = catalog.get(
            "datasets",
            []
        )

        if len(datasets) == 0:

            raise ValueError(
                "Catalog contains no datasets"
            )

        for dataset in datasets:

            required_fields = [

                "name",
                "layer",
                "path",
                "owner",
                "description",
                "refresh_frequency"
            ]

            for field in required_fields:

                if field not in dataset:

                    raise ValueError(
                        f"Missing field "
                        f"{field} "
                        f"in dataset "
                        f"{dataset}"
                    )

        return True

    def publish(self):

        catalog = self.load_catalog()

        logger.info(
            "=" * 50
        )

        logger.info(
            "Publishing catalog metadata"
        )

        for dataset in catalog["datasets"]:

            logger.info(
                f"Dataset: "
                f"{dataset['name']}"
            )

            logger.info(
                f"Layer: "
                f"{dataset['layer']}"
            )

            logger.info(
                f"Path: "
                f"{dataset['path']}"
            )

            logger.info(
                f"Owner: "
                f"{dataset['owner']}"
            )

            logger.info(
                f"Description: "
                f"{dataset['description']}"
            )

            logger.info(
                "-" * 50
            )

        logger.info(
            "Catalog successfully published"
        )

        logger.info(
            "=" * 50
        )


def main():

    try:

        publisher = CatalogPublisher(
            CATALOG_FILE
        )

        publisher.validate_catalog()

        publisher.publish()

    except Exception as e:

        logger.exception(
            "Catalog publication failed"
        )

        raise


if __name__ == "__main__":

    main()