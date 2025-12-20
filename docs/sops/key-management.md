# SOPS Key Management

Understanding where keys are stored and how to manage them.

## Key Locations

### Age Private Key (SECRET!)

```
Location: ~/.sops/age/keys.txt
```

**Contents:**
```
# created: 2024-XX-XX
# public key: age1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
AGE-SECRET-KEY-1XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
```

⚠️ **NEVER share or commit this file!**

### Age Public Key

```
Location: .sops.yaml (in project root)
Value: Your public key from age-keygen output
```

The public key is safe to share. It can only **encrypt**, not decrypt.

## Key Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Your Machine                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   ~/.sops/age/keys.txt                                          │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │  Private Key (SECRET)                                    │   │
│   │  AGE-SECRET-KEY-1XXXXX...                               │   │
│   │                                                          │   │
│   │  Used for: DECRYPTION                                    │   │
│   └─────────────────────────────────────────────────────────┘   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ decrypts
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Your Local Files                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   .sops.yaml                    config.enc.yaml                 │
│   ┌───────────────────────┐    ┌───────────────────────────┐   │
│   │  Public Key           │    │  Encrypted Data           │   │
│   │  age1xxxxxxxx...      │    │  cloudflare:              │   │
│   │                       │    │    api_token: ENC[AES...] │   │
│   │  Used for: ENCRYPTION │    │    zone_id: ENC[AES...]   │   │
│   └───────────────────────┘    └───────────────────────────┘   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Viewing Your Keys

### View Your Public Key

```bash
grep "public key" ~/.sops/age/keys.txt
```

Output:
```
# public key: age1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

### View Project's Expected Public Key

```bash
cat .sops.yaml
```

## Key Rotation

### When to Rotate

- Key may have been compromised
- Regular security policy (e.g., annually)
- Team member with access leaves

### How to Rotate

1. **Generate new key:**
   ```bash
   age-keygen -o ~/.sops/age/keys_new.txt
   ```

2. **Decrypt with old key, re-encrypt with new:**
   ```bash
   # Keep old key available temporarily
   export SOPS_AGE_KEY_FILE=~/.sops/age/keys.txt
   
   # Decrypt
   sops -d config.enc.yaml > /tmp/config_plain.yaml
   
   # Update .sops.yaml with new public key
   # Then re-encrypt
   sops -e /tmp/config_plain.yaml > config.enc.yaml
   
   # Securely delete plaintext
   rm /tmp/config_plain.yaml
   ```

3. **Update `.sops.yaml` with new public key**

4. **Replace old key file:**
   ```bash
   mv ~/.sops/age/keys_new.txt ~/.sops/age/keys.txt
   ```

## Multi-User Setup

For teams, you can encrypt to multiple keys:

### .sops.yaml for Multiple Recipients

```yaml
creation_rules:
  - path_regex: \.enc\.yaml$
    age: >-
      age1abc123...,
      age1def456...,
      age1ghi789...
```

Any recipient with their private key can decrypt.

### Adding a Team Member

1. They generate their key: `age-keygen -o ~/.sops/age/keys.txt`
2. They share their **public key** with you
3. Add their public key to `.sops.yaml`
4. Re-encrypt the config:
   ```bash
   sops updatekeys config.enc.yaml
   ```

### Removing a Team Member

1. Remove their public key from `.sops.yaml`
2. Rotate all secrets (they may have copies)
3. Re-encrypt with remaining keys:
   ```bash
   sops -d config.enc.yaml | sops -e /dev/stdin > config.enc.yaml.new
   mv config.enc.yaml.new config.enc.yaml
   ```

## Backup Recommendations

### What to Backup

| File | Backup? | Notes |
|------|---------|-------|
| `~/.sops/age/keys.txt` | ✅ **Critical** | Store securely (password manager, secure vault) |
| `.sops.yaml` | ✅ Locally | Your SOPS configuration |
| `config.enc.yaml` | ✅ Locally | Your encrypted secrets |

### Backup Methods

1. **Password Manager** (1Password, Bitwarden)
   - Store the contents of `keys.txt` as a secure note

2. **Encrypted USB Drive**
   - Keep in a safe location

3. **Print and store securely**
   - As last resort backup
