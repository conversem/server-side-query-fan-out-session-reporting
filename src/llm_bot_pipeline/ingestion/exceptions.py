"""
Custom exceptions for the ingestion module.

Provides specialized exception classes for handling various error
conditions during log ingestion and parsing.
"""


class IngestionError(Exception):
    """
    Base exception for all ingestion-related errors.

    All other ingestion exceptions inherit from this class,
    allowing for broad exception catching when needed.
    """

    pass


class ValidationError(IngestionError):
    """
    Raised when data validation fails.

    Used for schema validation errors, type mismatches, or
    missing required fields in ingestion records.

    Attributes:
        field: The field name that failed validation (optional)
        value: The invalid value (optional)
        message: Detailed error message
    """

    def __init__(
        self,
        message: str,
        field: str | None = None,
        value: object | None = None,
    ):
        self.field = field
        self.value = value
        self.message = message
        super().__init__(self._format_message())

    def _format_message(self) -> str:
        """Format the error message with field and value context."""
        if self.field and self.value is not None:
            return f"{self.message} (field='{self.field}', value={self.value!r})"
        elif self.field:
            return f"{self.message} (field='{self.field}')"
        return self.message


class ParseError(IngestionError):
    """
    Raised when log file parsing fails.

    Used for format errors, malformed records, or unsupported
    file formats during parsing.

    Attributes:
        line_number: The line number where parsing failed (optional)
        line_content: The content of the problematic line (optional)
        message: Detailed error message
    """

    def __init__(
        self,
        message: str,
        line_number: int | None = None,
        line_content: str | None = None,
    ):
        self.line_number = line_number
        self.line_content = line_content
        self.message = message
        super().__init__(self._format_message())

    def _format_message(self) -> str:
        """Format the error message with line context."""
        if self.line_number is not None and self.line_content:
            # Truncate long lines for readability
            content = (
                self.line_content[:100] + "..."
                if len(self.line_content) > 100
                else self.line_content
            )
            return f"{self.message} (line {self.line_number}: {content!r})"
        elif self.line_number is not None:
            return f"{self.message} (line {self.line_number})"
        return self.message


class ProviderNotFoundError(IngestionError):
    """
    Raised when an ingestion provider is not registered.

    Used when attempting to retrieve an adapter for an unknown
    or unregistered provider name.

    Attributes:
        provider_name: The name of the missing provider
        available_providers: List of registered provider names
    """

    def __init__(
        self,
        provider_name: str,
        available_providers: list[str] | None = None,
    ):
        self.provider_name = provider_name
        self.available_providers = available_providers or []
        super().__init__(self._format_message())

    def _format_message(self) -> str:
        """Format the error message with available providers."""
        if self.available_providers:
            available = ", ".join(sorted(self.available_providers))
            return (
                f"Unknown provider: '{self.provider_name}'. "
                f"Available providers: {available}"
            )
        return f"Unknown provider: '{self.provider_name}'. No providers registered."


class SourceValidationError(IngestionError):
    """
    Raised when source configuration validation fails.

    Used when an IngestionSource has invalid configuration,
    inaccessible paths, or incompatible settings.

    Attributes:
        source_type: The type of source that failed validation
        reason: Detailed explanation of why validation failed
    """

    def __init__(
        self,
        message: str,
        source_type: str | None = None,
        reason: str | None = None,
    ):
        self.source_type = source_type
        self.reason = reason
        self.message = message
        super().__init__(self._format_message())

    def _format_message(self) -> str:
        """Format the error message with source context."""
        parts = [self.message]
        if self.source_type:
            parts.append(f"source_type='{self.source_type}'")
        if self.reason:
            parts.append(f"reason: {self.reason}")
        return " - ".join(parts)

