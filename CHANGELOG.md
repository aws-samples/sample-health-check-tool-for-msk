# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0] - 2026-09-19

### Added
- Explicit "not assessed" state (with reason) for checks that cannot run; data quality section with coverage, unpublished metrics and collection errors
- Checks: heap after GC, disk growth projection, under-replicated partitions, IAMTooManyConnections, encryption in transit / in cluster / at rest, public access, storage auto scaling via Application Auto Scaling, intelligent rebalancing (Express, via `Provisioned.Rebalancing`), Kafka version status from ListKafkaVersions, right-sizing signal
- Metric catalog with monitoring-level requirements, ListMetrics discovery and per-listener connection metrics
- JSON manifest next to the PDF; `--compare-with` (new / resolved / persisting findings), `--redact`, `--workload`, `--days`, `--no-network`, `--no-manifest`
- PDF: real table of contents and bookmarks, action plan with priority, severity and confidence, observed value vs threshold and source per finding, threshold lines on charts, per-broker statistics tables, methodology section

### Changed
- Status label bounded by the worst finding; new HIGH severity for posture and resilience gaps (unauthenticated listener, plaintext traffic, 2 AZs in production) that lead to Needs Attention rather than Critical; informational findings no longer reduce the score
- CloudWatch queries return all five statistics; Minimum for ActiveControllerCount, Maximum for partition-state metrics; connection metrics read as Sum per minute (broker total)
- CPU User + System aligned by timestamp before percentiles; throughput, partitions and connections evaluated per broker
- Broker size limits with provenance and confidence in `reference.py`; unknown sizes report not assessed instead of a default limit; disk threshold aligned with the documented 85% action level
- Recommendations consolidated per check with confirm / verify steps and documentation links; priority separated from severity
- reportlab pin relaxed to `<5.0.0`; package version 1.1.0

### Fixed
- Analyzers for disk, heap, under-min-ISR and memory that existed but were never executed
- Throughput check that only looked at the first broker
- Storage auto scaling read from the provisioned-throughput flag
- `ConnectionCreationRate` treated as a DEFAULT-level metric
- Charts inserted with a different aspect ratio than rendered

## [1.0.2] - 2025-11-28

### Added
- Storage growth projection for Standard clusters with capacity forecasting
- Network throughput validation against instance-specific limits
- Per-broker partition capacity checks with Express-specific limits
- Intelligent partition rebalancing recommendations based on available capacity
- Context-aware recommendation prioritization
- CPU imbalance filtering for low usage scenarios (<30%)

### Changed
- Express broker partition limits updated to AWS maximum values (1500-32000 per broker)
- Network throughput thresholds lowered to 70% for earlier warning
- CPU analysis now focuses on sustained high usage (P95 >60%) instead of average
- Low-impact connection/memory imbalance recommendations are now filtered out

### Fixed
- Partition imbalance recommendations now suggest rebalancing when capacity is available
- Express network limits corrected (23.4-750 MB/s ingress, 58.5-1875 MB/s egress)

## [1.0.1] - 2025-11-27

### Removed
- Intelligent rebalancing check due to AWS API limitation (field not returned by describe-cluster-v2)

### Changed
- Updated boto3 to 1.37.38

## [1.0.0] - 2025-11-27

### Added
- Initial release
- Support for MSK Standard (Provisioned) and MSK Serverless (Express) clusters
- 18 metrics collection for both cluster types
- Category-based health scoring system (Reliability 35%, Performance 30%, Security 20%, Cost 15%)
- PDF report generation with visualizations and recommendations
- Smart period detection for clusters younger than 30 days
- Real-time Kafka version validation
- Message distribution imbalance detection (10% threshold)
- Connection monitoring (ClientConnectionCount and ConnectionCount)
- Executive Summary with health score breakdown
- Comprehensive findings documentation
- 40/49 tests passing (82% coverage)

### Features
- Automated CloudWatch metrics collection
- AWS best practices validation
- Professional PDF reports with charts
- Prioritized recommendations with impact analysis
- Configuration review and security assessment
