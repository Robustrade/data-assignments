import json


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
        ) as f:

            return json.load(f)

    def list_datasets(self):

        catalog = self.load_catalog()

        return catalog["datasets"]