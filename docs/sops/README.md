# SOPS Secrets Management

This project uses [SOPS](https://github.com/getsops/sops) (Secrets OPerationS) by Mozilla for encrypting sensitive configuration.

## Quick Reference

| Item | Location |
|------|----------|
| **SOPS Config** | `.sops.yaml` (project root) |
| **Encrypted Config** | `config.enc.yaml` (project root) |
| **Age Private Key** | `~/.sops/age/keys.txt` (your home directory) |
| **Age Public Key** | Your key from `age-keygen` output |

## Documentation

| Document | Description |
|----------|-------------|
| [Quick Start](quickstart.md) | Get started in 5 minutes |
| [Key Management](key-management.md) | Where keys are stored, how to rotate |
| [Daily Usage](daily-usage.md) | Common commands and workflows |
| [Troubleshooting](troubleshooting.md) | Common issues and solutions |

## TL;DR - Setup and Edit

```bash
# 1. Generate your Age key (one-time)
mkdir -p ~/.sops/age
age-keygen -o ~/.sops/age/keys.txt

# 2. Set the key file (add to ~/.bashrc or ~/.zshrc)
export SOPS_AGE_KEY_FILE=~/.sops/age/keys.txt

# 3. Configure SOPS with your public key
cp .sops.yaml.example .sops.yaml
# Edit .sops.yaml and add your public key from step 1

# 4. Create encrypted config
sops -e config.example.yaml > config.enc.yaml

# 5. Edit the encrypted config (opens in your $EDITOR)
sops config.enc.yaml
```

## What's Encrypted?

The `config.enc.yaml` file contains:

```yaml
storage:
  backend: "sqlite"
  sqlite_db_path: "***NOT ENCRYPTED***"

cloudflare:
  api_token: "***ENCRYPTED***"   # Cloudflare API token
  zone_id: "***ENCRYPTED***"     # Cloudflare zone ID
```

## Security Model

```
┌─────────────────────────────────────────────────────────────┐
│                    What Gets Committed                       │
├─────────────────────────────────────────────────────────────┤
│  ✅ .sops.yaml           Public key only, safe to commit    │
│  ✅ config.enc.yaml      Encrypted, safe to commit          │
│  ✅ config.example.yaml  Template with placeholders         │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                  What NEVER Gets Committed                   │
├─────────────────────────────────────────────────────────────┤
│  ❌ ~/.sops/age/keys.txt    Private key - keep secret!      │
│  ❌ Decrypted YAML files    Any unencrypted secrets         │
└─────────────────────────────────────────────────────────────┘
```
