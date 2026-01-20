class ValidationResult:
    def __init__(self, name, passed, metrics=None, details=None):
        self.name = name
        self.passed = passed
        self.metrics = metrics or {}
        self.details = details or {}

    def __repr__(self):
        return f"{self.name}(passed={self.passed}, metrics={self.metrics})"

