"""
Provider registry for ingestion adapters.

Provides registration and discovery of ingestion adapter implementations.
"""

import logging
from typing import Type

from .base import IngestionAdapter
from .exceptions import ProviderNotFoundError

logger = logging.getLogger(__name__)


class IngestionRegistry:
    """
    Registry for ingestion adapters.

    Provides centralized management of provider adapters with
    support for registration, lookup, and auto-discovery.

    Usage:
        # Register using decorator
        @IngestionRegistry.register('cloudflare')
        class CloudflareAdapter(IngestionAdapter):
            ...

        # Or register manually
        IngestionRegistry.register_provider('cloudflare', CloudflareAdapter)

        # Get adapter instance
        adapter = IngestionRegistry.get_adapter('cloudflare')

        # List all providers
        providers = IngestionRegistry.list_providers()
    """

    _adapters: dict[str, Type[IngestionAdapter]] = {}

    @classmethod
    def register(cls, provider_name: str):
        """
        Decorator to register an adapter class.

        Args:
            provider_name: Provider identifier for registry lookup

        Returns:
            Decorator function

        Example:
            @IngestionRegistry.register('cloudflare')
            class CloudflareAdapter(IngestionAdapter):
                ...
        """

        def decorator(adapter_class: Type[IngestionAdapter]) -> Type[IngestionAdapter]:
            cls.register_provider(provider_name, adapter_class)
            return adapter_class

        return decorator

    @classmethod
    def register_provider(
        cls, provider_name: str, adapter_class: Type[IngestionAdapter]
    ) -> None:
        """
        Register an adapter class for a provider.

        Args:
            provider_name: Provider identifier (e.g., 'cloudflare')
            adapter_class: Class implementing IngestionAdapter

        Raises:
            TypeError: If adapter_class doesn't inherit from IngestionAdapter
        """
        if not issubclass(adapter_class, IngestionAdapter):
            raise TypeError(
                f"Adapter class must inherit from IngestionAdapter, "
                f"got {adapter_class.__name__}"
            )

        provider_name = provider_name.lower()

        if provider_name in cls._adapters:
            logger.warning(
                f"Overwriting existing adapter for provider '{provider_name}'"
            )

        cls._adapters[provider_name] = adapter_class
        logger.debug(f"Registered ingestion adapter: {provider_name}")

    @classmethod
    def get_adapter(cls, provider_name: str) -> IngestionAdapter:
        """
        Get an adapter instance by provider name.

        Args:
            provider_name: Provider identifier

        Returns:
            Instantiated adapter for the provider

        Raises:
            ProviderNotFoundError: If provider is not registered
        """
        provider_name = provider_name.lower()

        if provider_name not in cls._adapters:
            raise ProviderNotFoundError(
                provider_name=provider_name,
                available_providers=list(cls._adapters.keys()),
            )

        return cls._adapters[provider_name]()

    @classmethod
    def get_adapter_class(cls, provider_name: str) -> Type[IngestionAdapter]:
        """
        Get an adapter class by provider name (without instantiation).

        Args:
            provider_name: Provider identifier

        Returns:
            Adapter class for the provider

        Raises:
            ProviderNotFoundError: If provider is not registered
        """
        provider_name = provider_name.lower()

        if provider_name not in cls._adapters:
            raise ProviderNotFoundError(
                provider_name=provider_name,
                available_providers=list(cls._adapters.keys()),
            )

        return cls._adapters[provider_name]

    @classmethod
    def list_providers(cls) -> list[str]:
        """
        List all registered provider names.

        Returns:
            Sorted list of provider identifiers
        """
        return sorted(cls._adapters.keys())

    @classmethod
    def is_provider_registered(cls, provider_name: str) -> bool:
        """
        Check if a provider is registered.

        Args:
            provider_name: Provider identifier

        Returns:
            True if provider is registered
        """
        return provider_name.lower() in cls._adapters

    @classmethod
    def clear(cls) -> None:
        """
        Clear all registered adapters.

        Primarily used for testing to reset registry state.
        """
        cls._adapters.clear()
        logger.debug("Cleared ingestion adapter registry")


# =============================================================================
# Convenience Functions
# =============================================================================


def get_adapter(provider_name: str) -> IngestionAdapter:
    """
    Get an adapter instance by provider name.

    Convenience function wrapping IngestionRegistry.get_adapter().

    Args:
        provider_name: Provider identifier

    Returns:
        Instantiated adapter for the provider

    Raises:
        ProviderNotFoundError: If provider is not registered
    """
    return IngestionRegistry.get_adapter(provider_name)


def register_adapter(provider_name: str, adapter_class: Type[IngestionAdapter]) -> None:
    """
    Register an adapter class for a provider.

    Convenience function wrapping IngestionRegistry.register_provider().

    Args:
        provider_name: Provider identifier
        adapter_class: Class implementing IngestionAdapter
    """
    IngestionRegistry.register_provider(provider_name, adapter_class)


def list_providers() -> list[str]:
    """
    List all registered provider names.

    Convenience function wrapping IngestionRegistry.list_providers().

    Returns:
        Sorted list of provider identifiers
    """
    return IngestionRegistry.list_providers()

