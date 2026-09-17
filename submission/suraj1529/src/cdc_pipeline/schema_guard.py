class SchemaChangeError(RuntimeError):
    pass

class SchemaGuard:
    def __init__(self, contract):
        self.contract = contract

    def validate(self, observed):
        errors = []
        for table, expected_cols in self.contract.items():
            actual = {c["name"]: c for c in observed.get(table, [])}
            expected = {c["name"]: c for c in expected_cols}
            for name, col in expected.items():
                if name not in actual:
                    errors.append(f"{table}: missing column {name}")
                    continue
                a = actual[name]
                if a["type"].upper() != col["type"].upper():
                    errors.append(f"{table}.{name}: type changed {col['type']} -> {a['type']}")
                if bool(a["notnull"]) != bool(col["notnull"]):
                    errors.append(f"{table}.{name}: nullability changed")
                if int(a["pk"]) != int(col["pk"]):
                    errors.append(f"{table}.{name}: primary-key definition changed")
            for name in actual:
                if name not in expected:
                    # Additive columns are accepted; the curated model can be updated deliberately.
                    continue
        if errors:
            raise SchemaChangeError("INCOMPATIBLE SOURCE SCHEMA: " + "; ".join(errors))
