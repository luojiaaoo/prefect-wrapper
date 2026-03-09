class PrefectServiceError(Exception):
    """Base exception for Prefect service operations."""


class DeploymentNotFoundError(PrefectServiceError):
    """Raised when a deployment cannot be found."""


class FlowRunNotFoundError(PrefectServiceError):
    """Raised when a flow run cannot be found."""
