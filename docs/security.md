# Security

## Dependency auditing

Run `pip-audit` to check for known vulnerabilities in installed packages:

```bash
pip install pip-audit
pip-audit
```

This is also run in CI (see `.github/workflows/ci.yml` security-audit job).

## Static analysis (bandit)

Bandit scans Python code for common security issues. It runs in pre-commit and CI:

```bash
bandit -r src/ scripts/ --exclude tests/
```
