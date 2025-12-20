# SOPS Quick Start

Get SOPS working in 5 minutes.

## Prerequisites

### Install SOPS and Age

**macOS:**
```bash
brew install sops age
```

**Linux (Debian/Ubuntu):**
```bash
# SOPS
curl -LO https://github.com/getsops/sops/releases/download/v3.9.2/sops-v3.9.2.linux.amd64
chmod +x sops-v3.9.2.linux.amd64
sudo mv sops-v3.9.2.linux.amd64 /usr/local/bin/sops

# Age
sudo apt install age
```

**Windows:**
```powershell
scoop install sops age
```

## First-Time Setup

### 1. Generate Your Age Key

```bash
mkdir -p ~/.sops/age
age-keygen -o ~/.sops/age/keys.txt
```

This outputs your public key:
```
Public key: age1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

**Save this public key!** You'll need it for the next step.

### 2. Set Environment Variable

Add to your shell profile (`~/.bashrc`, `~/.zshrc`, or `~/.profile`):

```bash
export SOPS_AGE_KEY_FILE=~/.sops/age/keys.txt
```

Then reload:
```bash
source ~/.bashrc  # or ~/.zshrc
```

### 3. Configure SOPS

```bash
# Copy the example SOPS config
cp .sops.yaml.example .sops.yaml

# Edit .sops.yaml and replace the placeholder with your public key
# age1YOUR_PUBLIC_KEY_HERE  →  age1xxxxxxxxx... (your actual key)
```

Your `.sops.yaml` should look like:
```yaml
creation_rules:
  - path_regex: \.enc\.yaml$
    age: age1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

### 4. Create Encrypted Config

```bash
# Edit the example config with your actual credentials
cp config.example.yaml config.yaml
# Edit config.yaml with your Cloudflare API token and zone ID

# Encrypt it
sops -e config.yaml > config.enc.yaml

# Remove the unencrypted version
rm config.yaml
```

### 5. Verify Setup

```bash
# Test decryption
sops -d config.enc.yaml
```

If you see decrypted YAML output, you're ready!

## Daily Usage

### Edit Encrypted Config

```bash
sops config.enc.yaml
```

This opens your `$EDITOR` with decrypted content. Save and close to re-encrypt.

### View Decrypted Config (without editing)

```bash
sops -d config.enc.yaml
```

### Add a New Secret

```bash
sops config.enc.yaml
# Add your new key/value
# Save and close
```

## Files in This Project

| File | Purpose | Commit? |
|------|---------|---------|
| `.sops.yaml.example` | Template for SOPS rules | ✅ Yes |
| `.sops.yaml` | Your SOPS rules with your key | ❌ No (gitignored) |
| `config.enc.yaml` | Your encrypted secrets | ❌ No (gitignored) |
| `config.example.yaml` | Template for new setups | ✅ Yes |

## Next Steps

- [Key Management](key-management.md) - Understand where keys live
- [Daily Usage](daily-usage.md) - Common workflows
- [Troubleshooting](troubleshooting.md) - When things go wrong
