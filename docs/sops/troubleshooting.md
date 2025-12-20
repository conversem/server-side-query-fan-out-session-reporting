# SOPS Troubleshooting

Common issues and how to fix them.

## Error: "failed to get the data key"

### Symptom
```
Failed to get the data key required to decrypt the SOPS file.
```

### Causes & Solutions

**1. SOPS_AGE_KEY_FILE not set**
```bash
# Check if set
echo $SOPS_AGE_KEY_FILE

# Set it
export SOPS_AGE_KEY_FILE=~/.sops/age/keys.txt
```

**2. Key file doesn't exist**
```bash
# Check if file exists
ls -la ~/.sops/age/keys.txt

# If missing, generate new key
mkdir -p ~/.sops/age
age-keygen -o ~/.sops/age/keys.txt
```

**3. Wrong key file**
```bash
# Verify your public key matches .sops.yaml
grep "public key" ~/.sops/age/keys.txt
cat .sops.yaml
```

---

## Error: "could not decrypt with any master key"

### Symptom
```
could not decrypt data key with any of the master keys
```

### Cause
The file was encrypted with a different key than you have.

### Solutions

**1. Get the correct key**
- Ask the person who encrypted it for the private key
- Or ask them to re-encrypt with your public key

**2. Re-encrypt with your key (if you have access to plaintext)**
```bash
# If someone can decrypt it
sops -d config.enc.yaml > /tmp/config.yaml

# Re-encrypt with your key
sops -e /tmp/config.yaml > config.enc.yaml

# Clean up
rm /tmp/config.yaml
```

**3. Add your key to recipients (team scenario)**
```bash
# Someone with access updates .sops.yaml to include your key
# Then runs:
sops updatekeys config.enc.yaml
```

---

## Error: "no matching creation rules found"

### Symptom
```
error loading config: no matching creation rules found
```

### Cause
The file path doesn't match any regex in `.sops.yaml`.

### Solutions

**1. Check filename matches pattern**
```bash
# .sops.yaml expects *.enc.yaml
# Wrong: config.yaml, secrets.yaml
# Right: config.enc.yaml, secrets.enc.yaml
```

**2. Use explicit key when encrypting**
```bash
# Bypass .sops.yaml rules
sops -e --age age1YOUR_PUBLIC_KEY_HERE input.yaml > output.enc.yaml
```

---

## Error: "sops: command not found"

### Symptom
```bash
sops: command not found
```

### Solutions

**macOS:**
```bash
brew install sops
```

**Linux:**
```bash
curl -LO https://github.com/getsops/sops/releases/download/v3.9.2/sops-v3.9.2.linux.amd64
chmod +x sops-v3.9.2.linux.amd64
sudo mv sops-v3.9.2.linux.amd64 /usr/local/bin/sops
```

**Verify installation:**
```bash
which sops
sops --version
```

---

## Error: "age: command not found"

### Symptom
```bash
age-keygen: command not found
```

### Solutions

**macOS:**
```bash
brew install age
```

**Linux (Debian/Ubuntu):**
```bash
sudo apt install age
```

**Linux (from source):**
```bash
curl -LO https://github.com/FiloSottile/age/releases/download/v1.2.0/age-v1.2.0-linux-amd64.tar.gz
tar xzf age-v1.2.0-linux-amd64.tar.gz
sudo mv age/age /usr/local/bin/
sudo mv age/age-keygen /usr/local/bin/
```

---

## Error: File appears corrupted or garbled

### Symptom
The encrypted file has strange content or can't be parsed.

### Solutions

**1. Check file encoding**
```bash
file config.enc.yaml
# Should say: ASCII text or UTF-8 text
```

**2. Restore from backup**
If you have a backup of the plaintext:
```bash
sops -e backup_config.yaml > config.enc.yaml
```

---

## Environment Variable Not Persisting

### Symptom
Have to re-export `SOPS_AGE_KEY_FILE` every terminal session.

### Solution
Add to your shell profile:

**Bash (~/.bashrc):**
```bash
echo 'export SOPS_AGE_KEY_FILE=~/.sops/age/keys.txt' >> ~/.bashrc
source ~/.bashrc
```

**Zsh (~/.zshrc):**
```bash
echo 'export SOPS_AGE_KEY_FILE=~/.sops/age/keys.txt' >> ~/.zshrc
source ~/.zshrc
```

**Fish (~/.config/fish/config.fish):**
```fish
echo 'set -gx SOPS_AGE_KEY_FILE ~/.sops/age/keys.txt' >> ~/.config/fish/config.fish
```

---

## Python: "SOPS not installed" Error

### Symptom
```
RuntimeError: SOPS not installed. Install with: brew install sops
```

### Cause
The `sops_loader.py` module can't find the `sops` binary.

### Solutions

**1. Verify SOPS is in PATH**
```bash
which sops
```

**2. Check Python can find it**
```python
import subprocess
result = subprocess.run(["sops", "--version"], capture_output=True)
print(result.stdout.decode())
```

**3. If using virtual environment**
SOPS should be installed system-wide, not in the venv.

---

## Python: Decryption Works in Shell but Not in Code

### Symptom
`sops -d config.enc.yaml` works, but Python code fails.

### Cause
Environment variable not available to Python process.

### Solutions

**1. Check Python sees the env var**
```python
import os
print(os.environ.get("SOPS_AGE_KEY_FILE"))
```

**2. Set in Python before importing**
```python
import os
os.environ["SOPS_AGE_KEY_FILE"] = os.path.expanduser("~/.sops/age/keys.txt")

from llm_bot_pipeline.config import get_settings
```

**3. Source profile before running**
```bash
source ~/.bashrc && python scripts/run_pipeline.py
```

---

## Diagnostic Commands

Run these to gather information for debugging:

```bash
# SOPS version
sops --version

# Age version  
age --version

# Key file location
echo $SOPS_AGE_KEY_FILE

# Key file exists
ls -la ~/.sops/age/keys.txt

# Key file content (shows public key comment)
head -2 ~/.sops/age/keys.txt

# .sops.yaml content
cat .sops.yaml

# Test decryption
sops -d config.enc.yaml 2>&1 | head -5
```

---

## Getting Help

If none of the above helps:

1. **Check SOPS GitHub Issues**: https://github.com/getsops/sops/issues
2. **Age GitHub**: https://github.com/FiloSottile/age
3. **Include diagnostic output** when asking for help
