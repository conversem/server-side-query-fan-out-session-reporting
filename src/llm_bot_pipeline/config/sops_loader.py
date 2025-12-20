"""
SOPS-encrypted configuration loader.

Supports loading secrets from SOPS-encrypted YAML files.
"""

import os
import subprocess
from pathlib import Path
from typing import Any, Optional

import yaml


def decrypt_sops_file(file_path: Path) -> dict[str, Any]:
    """
    Decrypt a SOPS-encrypted file and return parsed YAML.

    Args:
        file_path: Path to the encrypted file

    Returns:
        Decrypted configuration as dictionary

    Raises:
        FileNotFoundError: If file doesn't exist
        RuntimeError: If SOPS decryption fails
    """
    if not file_path.exists():
        raise FileNotFoundError(f"Encrypted config file not found: {file_path}")

    try:
        result = subprocess.run(
            ["sops", "-d", str(file_path)],
            capture_output=True,
            text=True,
            check=True,
        )
        return yaml.safe_load(result.stdout)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"SOPS decryption failed: {e.stderr}") from e
    except FileNotFoundError:
        raise RuntimeError(
            "SOPS not installed. Install with: brew install sops (macOS) "
            "or download from https://github.com/getsops/sops/releases"
        )


def load_config(
    encrypted_path: Optional[Path] = None,
    fallback_to_env: bool = True,
) -> dict[str, Any]:
    """
    Load configuration from SOPS-encrypted file or environment variables.

    Priority:
    1. SOPS-encrypted file (if provided and exists)
    2. Environment variables (if fallback_to_env=True)

    Args:
        encrypted_path: Path to SOPS-encrypted config file
        fallback_to_env: Whether to fall back to environment variables

    Returns:
        Configuration dictionary
    """
    config = {}

    # Try loading from SOPS file
    if encrypted_path and encrypted_path.exists():
        try:
            config = decrypt_sops_file(encrypted_path)
            return config
        except RuntimeError as e:
            if not fallback_to_env:
                raise
            print(f"⚠ SOPS decryption failed, falling back to env vars: {e}")

    # Fall back to environment variables
    if fallback_to_env:
        config = {
            "storage": {
                "backend": "sqlite",
                "sqlite_db_path": os.environ.get(
                    "SQLITE_DB_PATH", "data/llm-bot-logs.db"
                ),
            },
            "cloudflare": {
                "api_token": os.environ.get("CLOUDFLARE_API_TOKEN", ""),
                "zone_id": os.environ.get("CLOUDFLARE_ZONE_ID", ""),
            },
        }

    return config


def check_sops_installed() -> bool:
    """Check if SOPS is installed and accessible."""
    try:
        subprocess.run(
            ["sops", "--version"],
            capture_output=True,
            check=True,
        )
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False
