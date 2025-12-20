# Contributing to Server-Side Query Fan-Out Session Reporting

Thank you for your interest in contributing! This document provides guidelines for contributing to this project.

## Getting Started

1. Fork the repository
2. Clone your fork:
   ```bash
   git clone https://github.com/YOUR_USERNAME/server-side-query-fan-out-session-reporting.git
   cd server-side-query-fan-out-session-reporting
   ```
3. Set up the development environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   pip install -r requirements.txt
   pip install -e .  # Install in editable mode
   ```

## Code Style

This project uses:

- **[Black](https://black.readthedocs.io/)** for code formatting
- **[isort](https://pycqa.github.io/isort/)** for import sorting

Before submitting a PR, format your code:

```bash
black src/ tests/ scripts/
isort src/ tests/ scripts/
```

## Testing

All changes must pass the test suite:

```bash
# Run all tests
python -m pytest tests/

# Run with coverage
python -m pytest tests/ --cov=src/llm_bot_pipeline

# Run specific test file
python -m pytest tests/unit/test_temporal_analysis.py -v
```

### Test Requirements

- New features should include unit tests
- Bug fixes should include a regression test
- All tests must pass before merging

## Pull Request Process

1. **Create a feature branch:**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes** and commit with clear messages:
   ```bash
   git commit -m "Add feature: description of what you added"
   ```

3. **Ensure all tests pass:**
   ```bash
   python -m pytest tests/
   ```

4. **Format your code:**
   ```bash
   black src/ tests/ scripts/
   isort src/ tests/ scripts/
   ```

5. **Push and create a PR:**
   ```bash
   git push origin feature/your-feature-name
   ```
   Then open a Pull Request on GitHub.

## PR Guidelines

- Provide a clear description of the changes
- Reference any related issues
- Keep PRs focused—one feature or fix per PR
- Update documentation if needed

## Reporting Issues

When reporting bugs, please include:

- Python version
- Operating system
- Steps to reproduce
- Expected vs actual behavior
- Relevant error messages or logs

## Questions?

For questions about the research methodology, see the [research article](https://conversem.com/the-query-fan-out-session/).

For other questions, open an issue on GitHub.

## License

By contributing, you agree that your contributions will be licensed under the [AGPL-3.0 License](LICENSE).
