# SOPS Daily Usage

Common commands and workflows for working with encrypted configs.

## Essential Commands

### Edit Encrypted Config

```bash
sops config.enc.yaml
```

Opens in your `$EDITOR` (vim, nano, code, etc.) with decrypted content.
Save and close to automatically re-encrypt.

### View Decrypted Content

```bash
sops -d config.enc.yaml
```

Prints decrypted YAML to stdout. Useful for:
- Debugging
- Piping to other commands
- Verifying contents

### View Specific Value

```bash
sops -d config.enc.yaml | grep api_token
```

Or with `yq`:
```bash
sops -d config.enc.yaml | yq '.cloudflare.api_token'
```

## Common Workflows

### Update Cloudflare Credentials

```bash
# Open editor
sops config.enc.yaml

# Edit these lines:
cloudflare:
  api_token: "NEW_TOKEN_HERE"
  zone_id: "NEW_ZONE_ID"

# Save and close - automatically re-encrypted
```

### Add New Configuration Section

```bash
sops config.enc.yaml

# Add new section:
new_service:
  api_key: "secret_key"
  endpoint: "https://api.example.com"

# Save and close
```

### Export to Environment Variables

```bash
# Export all config as env vars (example script)
eval $(sops -d config.enc.yaml | python3 -c "
import sys, yaml
config = yaml.safe_load(sys.stdin)
for section, values in config.items():
    if isinstance(values, dict):
        for key, value in values.items():
            print(f'export {section.upper()}_{key.upper()}=\"{value}\"')
")
```

### Create New Encrypted File

```bash
# From template
sops -e config.example.yaml > config.enc.yaml

# From scratch (opens editor)
sops config.new.enc.yaml
```

## Integration with Python

### Loading in Code

```python
from llm_bot_pipeline.config import get_settings

# Automatically decrypts config.enc.yaml
settings = get_settings()

# Access values
print(settings.cloudflare_api_token)
print(settings.cloudflare_zone_id)
```

### Manual Loading

```python
from llm_bot_pipeline.config.sops_loader import decrypt_sops_file
from pathlib import Path

config = decrypt_sops_file(Path("config.enc.yaml"))
print(config["cloudflare"]["api_token"])
```

## CI/CD Usage

### GitHub Actions

```yaml
# .github/workflows/deploy.yml
jobs:
  deploy:
    steps:
      - name: Install SOPS
        run: |
          curl -LO https://github.com/getsops/sops/releases/download/v3.9.2/sops-v3.9.2.linux.amd64
          chmod +x sops-v3.9.2.linux.amd64
          sudo mv sops-v3.9.2.linux.amd64 /usr/local/bin/sops

      - name: Decrypt config
        env:
          SOPS_AGE_KEY: ${{ secrets.SOPS_AGE_KEY }}
        run: |
          echo "$SOPS_AGE_KEY" > /tmp/age-key.txt
          export SOPS_AGE_KEY_FILE=/tmp/age-key.txt
          sops -d config.enc.yaml > config.yaml
          # Use config.yaml in subsequent steps
```

### Setting Up CI Secret

1. Get your private key:
   ```bash
   cat ~/.sops/age/keys.txt
   ```

2. Add to GitHub repository secrets as `SOPS_AGE_KEY`

## Editor Integration

### VS Code

Install the "SOPS" extension for syntax highlighting of encrypted files.

To edit, still use terminal:
```bash
sops config.enc.yaml
```

### Set Default Editor

```bash
# Use VS Code
export EDITOR="code --wait"

# Use Vim
export EDITOR="vim"

# Use Nano
export EDITOR="nano"
```

Add to your shell profile for persistence.

## Verification Commands

### Check File is Encrypted

```bash
head -5 config.enc.yaml
```

Should show `ENC[AES256_GCM,data:...` values.

### Verify You Can Decrypt

```bash
sops -d config.enc.yaml > /dev/null && echo "✓ Decryption works"
```

### Check SOPS Version

```bash
sops --version
```

### Check Key File

```bash
ls -la ~/.sops/age/keys.txt
```

## Batch Operations

### Re-encrypt All Files

```bash
for f in *.enc.yaml; do
  sops updatekeys "$f"
done
```

### Decrypt Multiple Files

```bash
for f in *.enc.yaml; do
  sops -d "$f" > "${f%.enc.yaml}.yaml"
done
# Remember to delete decrypted files after use!
```

## Best Practices

1. **Never commit decrypted files**
   ```bash
   # Add to .gitignore if you create temp decrypted files
   *.decrypted.yaml
   config.yaml  # Only config.enc.yaml should exist
   ```

2. **Always verify after editing**
   ```bash
   sops -d config.enc.yaml | head -10
   ```

3. **Use `--in-place` carefully**
   ```bash
   # This modifies the file directly
   sops --rotate --in-place config.enc.yaml
   ```

4. **Set SOPS_AGE_KEY_FILE in shell profile**
   Don't rely on remembering to export it each session.
