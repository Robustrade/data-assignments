"""Exceptions raised by the pipeline."""


class PipelineError(Exception):
    """Base class for pipeline errors."""


class SchemaContractViolation(PipelineError):
    """The source schema (or an event) no longer matches the registered contract."""

    def __init__(self, violations):
        self.violations = list(violations)
        text = "; ".join(v["message"] for v in self.violations[:5])
        super().__init__(f"schema contract violated: {text}")


class QualityBlocked(PipelineError):
    """A blocking validation-parity check failed; the batch was rolled back."""

    def __init__(self, offending):
        self.offending = list(offending)
        text = "; ".join(
            f"{o['rule_id']} {o['table_name']} seq={o['seq']}: {o['reason']}"
            for o in self.offending[:5]
        )
        super().__init__(f"blocking quality check failed: {text}")


class PipelineLocked(PipelineError):
    """Another runner holds a live lease on the pipeline."""


class PipelineStateError(PipelineError):
    """The operation is not allowed in the pipeline's current status."""


class SimulatedCrash(BaseException):
    """Raised by the test-only fault hook; behaves like a killed process.

    Deliberately a ``BaseException`` so it is not handled like an ordinary failure:
    the batch rolls back but the pipeline status is left untouched.
    """
