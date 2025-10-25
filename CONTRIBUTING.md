# Contributing to Google Drive Integration Worker

Thank you for your interest in contributing to the Clustera Google Drive Integration Worker! This document provides guidelines and instructions for contributing.

## Code of Conduct

By participating in this project, you agree to abide by our Code of Conduct, which promotes a welcoming and inclusive environment for all contributors.

## Getting Started

### Prerequisites

- Python 3.11+
- Poetry for dependency management
- Docker and Docker Compose
- Git with submodule support
- Access to Google Cloud Console (for OAuth setup)

### Development Setup

1. **Clone the repository with submodules:**
```bash
git clone --recursive https://github.com/clusterahq/clustera-integration-google-drive.git
cd clustera-integration-google-drive
```

2. **Initialize submodules if needed:**
```bash
git submodule update --init --recursive
```

3. **Install dependencies:**
```bash
poetry install --with dev
```

4. **Set up pre-commit hooks:**
```bash
poetry run pre-commit install
```

5. **Copy environment template:**
```bash
cp .env.example .env
# Edit .env with your configuration
```

6. **Start local development environment:**
```bash
docker-compose up -d
```

## Development Workflow

### 1. Create a Feature Branch

```bash
git checkout -b feature/your-feature-name
```

Use conventional branch naming:
- `feature/` - New features
- `fix/` - Bug fixes
- `docs/` - Documentation changes
- `refactor/` - Code refactoring
- `test/` - Test additions/changes
- `chore/` - Maintenance tasks

### 2. Make Your Changes

Follow these guidelines:

#### Code Style

- Follow PEP 8 and use `ruff` for linting
- Use type hints for all functions
- Maximum line length: 100 characters
- Use descriptive variable and function names

#### Testing

- Write tests for all new functionality
- Maintain >90% test coverage
- Run tests before committing:

```bash
# Run all tests
poetry run pytest

# Run with coverage
poetry run pytest --cov=google_drive_worker --cov-report=term-missing

# Run specific test file
poetry run pytest tests/unit/test_worker.py

# Run integration tests (requires Docker)
poetry run pytest tests/integration/
```

#### Documentation

- Update docstrings for new/modified functions
- Update README.md if adding features
- Update API documentation if changing interfaces
- Add entries to CHANGELOG.md (unreleased section)

### 3. Commit Your Changes

Use conventional commits:

```bash
git add .
git commit -m "feat: add support for team drives"
```

Commit types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation only
- `style`: Code style changes
- `refactor`: Code refactoring
- `test`: Test changes
- `chore`: Maintenance

### 4. Push and Create Pull Request

```bash
git push origin feature/your-feature-name
```

Create a pull request on GitHub with:
- Clear title following conventional commits
- Description of changes
- Link to related issues
- Test results/coverage report

## Testing Guidelines

### Unit Tests

Located in `tests/unit/`:
- Test individual functions/methods
- Mock external dependencies
- Focus on edge cases
- Use parametrized tests for multiple scenarios

Example:
```python
@pytest.mark.parametrize("input,expected", [
    ("test", "TEST"),
    ("", ""),
    (None, None),
])
def test_uppercase(input, expected):
    assert uppercase(input) == expected
```

### Integration Tests

Located in `tests/integration/`:
- Test end-to-end workflows
- Use Testcontainers for dependencies
- Test Kafka message flows
- Verify S3 uploads

### Performance Tests

```python
@pytest.mark.performance
def test_large_file_processing():
    # Test with large datasets
    pass
```

## Code Review Process

1. **Automated Checks**: CI/CD runs on all PRs
2. **Review Requirements**: At least one approval required
3. **Review Focus**:
   - Code quality and style
   - Test coverage
   - Documentation completeness
   - Security considerations
   - Performance implications

## Common Development Tasks

### Adding a New API Endpoint Integration

1. Add method to `GoogleDriveClient` class
2. Add corresponding processor in `Worker`
3. Write unit tests for both
4. Update API documentation
5. Add integration test

### Modifying Kafka Message Schema

1. **Never break backward compatibility**
2. Update schema documentation
3. Add migration logic if needed
4. Test with old and new message formats
5. Update all consumers/producers

### Updating Dependencies

```bash
# Update specific dependency
poetry add package@latest

# Update all dependencies
poetry update

# Check for security vulnerabilities
poetry run safety check
```

## Debugging Tips

### Enable Debug Logging

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Inspect Kafka Messages

```bash
# View messages in topic
docker-compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic integration.trigger \
  --from-beginning
```

### Check Worker Health

```bash
# View logs
docker-compose logs -f google-drive-worker

# Check metrics
curl http://localhost:8080/metrics
```

## Release Process

1. **Version Bump**: Update version in `pyproject.toml`
2. **Changelog**: Move unreleased items to new version
3. **Tag Release**: `git tag -a v1.0.1 -m "Release v1.0.1"`
4. **Push Tags**: `git push origin --tags`
5. **GitHub Release**: Create release from tag
6. **Docker Image**: CI/CD builds and pushes automatically

## Troubleshooting

### Common Issues

#### Import Errors
```bash
# Reinstall with submodules
git submodule update --init --recursive
poetry install
```

#### Kafka Connection Issues
```bash
# Restart Kafka
docker-compose restart kafka
```

#### Test Failures
```bash
# Run with verbose output
poetry run pytest -vvs

# Run with debugging
poetry run pytest --pdb
```

## Getting Help

- **Documentation**: Check `/docs` directory
- **Issues**: Search/create GitHub issues
- **Slack**: #integrations-platform channel
- **Email**: platform@clustera.ai

## Recognition

Contributors are recognized in:
- CHANGELOG.md (for significant contributions)
- GitHub contributors page
- Release notes

Thank you for contributing to the Google Drive Integration Worker!