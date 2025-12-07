# Changelog

All notable changes to the Google Drive Integration Worker will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Comprehensive integration testing suite with Testcontainers
- Complete Kubernetes deployment manifests with HPA
- Docker Compose configuration for local development
- Enhanced documentation (API reference, OAuth setup, troubleshooting)
- GitHub Actions CI/CD pipeline
- Prometheus metrics and health checks

## [1.0.0] - 2025-01-25

### Added
- Initial release of Google Drive integration worker
- Full OAuth 2.0 authentication support
- Comprehensive file syncing capabilities
- Change tracking with incremental sync
- Webhook support for real-time updates
- Permission tracking for all files
- Revision history support
- S3 payload offloading for large files
- Idempotency with deterministic key generation
- Rate limiting with exponential backoff
- Comprehensive error handling and retry logic
- Dead letter queue support
- Support for shared drives
- Extensive unit test coverage (97+ tests)
- Docker containerization
- Kubernetes-ready deployment

### Features
- **Authentication**: OAuth 2.0 with refresh token support
- **File Operations**: List, get metadata, track changes
- **Permissions**: Full permission enumeration
- **Revisions**: Version history tracking
- **Webhooks**: Real-time change notifications
- **Performance**: Concurrent connection support
- **Reliability**: At-least-once delivery guarantee
- **Scalability**: Horizontal scaling support
- **Monitoring**: Prometheus metrics
- **Security**: M2M token authentication for Control Plane

### Technical Details
- Python 3.11+ support
- Async/await architecture
- Kafka consumer with manual offset management
- S3 integration for large payloads
- Comprehensive logging in JSON format
- Environment-based configuration
- Submodule-based shared toolkit integration

## [0.9.0] - 2025-01-24

### Added
- Beta release for testing
- Core worker functionality
- Basic test coverage
- Initial documentation

## [0.1.0] - 2025-01-20

### Added
- Project initialization
- Basic repository structure
- Initial planning documents

[Unreleased]: https://github.com/clusterahq/clustera-integration-google-drive/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/clusterahq/clustera-integration-google-drive/compare/v0.9.0...v1.0.0
[0.9.0]: https://github.com/clusterahq/clustera-integration-google-drive/compare/v0.1.0...v0.9.0
[0.1.0]: https://github.com/clusterahq/clustera-integration-google-drive/releases/tag/v0.1.0