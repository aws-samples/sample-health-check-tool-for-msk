# Health Check for Amazon MSK

> **Note:** This is a sample tool that demonstrates how to automate the collection and validation of Amazon MSK metrics against AWS best practices. It serves as an example implementation that you can use as-is or customize for your specific operational requirements.

An automated health analysis and reporting tool for Amazon MSK clusters that generates a PDF report with charts, evidence tables and prioritized recommendations, plus a JSON manifest for auditing and run-to-run comparison.

## Overview

This sample Python CLI tool demonstrates how to automate operational reviews of Amazon MSK clusters. It shows how to:
- Collect up to 30 days of CloudWatch metrics programmatically, with the statistic each question needs
- Analyze metrics and configuration against published AWS MSK best practices and quotas
- Generate a PDF report with charts, evidence tables and a JSON manifest
- Provide prioritized recommendations with what to confirm first and how to verify afterwards

The tool is designed to be a starting point for building your own MSK monitoring and reporting solutions. You can use it as-is for basic health checks or extend it with additional metrics, custom thresholds, and organization-specific best practices.

**Smart period detection:** the tool reads the cluster creation time and shortens the metrics window for clusters younger than 30 days, so charts and statistics cover the data that exists.

## Features

### Cluster support
- MSK Provisioned with Standard brokers (kafka.t3.small, kafka.m5.*, kafka.m7g.*)
- MSK Provisioned with Express brokers (express.m7g.*)
- MSK Serverless is not supported

### What the report contains
1. **Executive summary** - status bounded by the worst finding (a critical finding is never reported as Healthy), health score, score per category, the findings that need attention with observed value and threshold, first actions, and the change since a previous run when `--compare-with` is used.
2. **Data quality and scope** - window actually observed, monitoring level, metrics not published by the cluster (with the reason), collection errors, and every check that could not be assessed.
3. **Action plan** - one row per recommendation with priority (urgency), severity (technical impact), confidence, action and affected brokers.
4. **Findings that need attention** - per category: observed vs threshold, confidence, source link, explanation, chart with the threshold line, per-broker statistics (avg, P95, hourly max, 1-minute peak, last, coverage) and a recommendation split into action, what to confirm first, how to verify afterwards and impact if not addressed.
5. **Healthy checks** and their charts, **cluster inventory**, **methodology and scoring** (statistics used, thresholds and their sources) and **references**.

A JSON manifest is written next to the PDF with the same content in machine-readable form (window, coverage, every finding with evidence, scores, recommendations, tool and rules version). Pass it to a later run with `--compare-with` to get new, resolved and persisting findings.

### Checks
See [FINDINGS.md](FINDINGS.md) for the full catalog with statistics, thresholds, severities, confidence and sources. In short:

- **Reliability** - active controller (fraction of minutes with a controller), offline partitions (Maximum), partitions below min ISR, under-replicated partitions, data-log disk usage (critical only while the latest value is at or above 85%; an earlier peak that came back is a warning) with growth projection (Standard), availability zones (2 vs 3 on Standard; Express always 3), storage auto scaling read from Application Auto Scaling (Standard), Kafka version against the MSK version catalog (DEPRECATED status) and the documented recommended version, intelligent rebalancing status (Express).
- **Performance** - CPU User + System per broker (series aligned by timestamp before summing), heap after GC, inbound/outbound throughput per broker against the published Express limits (sustained and throttle quota) or Standard guidelines, with short bursts above the limit reported as informational, partitions per broker against recommended/maximum, client connections and connection creation rate against the IAM quotas (3000 per broker, 100/s; 4/s on kafka.t3.small), IAMTooManyConnections, per-broker balance of CPU, partitions, leaders, traffic, messages and connections above an activity floor, enhanced monitoring level (always recommends at least PER_BROKER and lists the checks left out at DEFAULT).
- **Security** - unauthenticated listener, client-broker and in-cluster encryption, encryption at rest, public access, broker log delivery.
- **Cost** - Graviton counterpart available, right-sizing signal when every utilisation dimension is far below the size limits.

Checks that cannot run (metric not published at the cluster's monitoring level, permission missing, broker size not in the catalog) are reported as **not assessed** with the reason and do not count as healthy.

### Metrics collected
Every metric is queried per broker (or per cluster) with all five CloudWatch statistics in one call, over hourly buckets aligned to the hour:

`ActiveControllerCount`, `OfflinePartitionsCount`, `GlobalPartitionCount`, `GlobalTopicCount`, `CpuUser`, `CpuSystem`, `CpuIdle`, `MemoryUsed`, `MemoryFree`, `HeapMemoryAfterGC`, `KafkaDataLogsDiskUsed` (Standard only), `LeaderCount`, `PartitionCount`, `UnderMinIsrPartitionCount`, `UnderReplicatedPartitions`, `BytesInPerSec`, `BytesOutPerSec`, `MessagesInPerSec`, `ClientConnectionCount` (per `Client Authentication` listener when published that way), `ConnectionCount`, `ConnectionCreationRate` and `IAMTooManyConnections` (both require `PER_BROKER` enhanced monitoring).

Connection metrics are published by MSK as one datapoint per network processor per minute, so the report reads them as `Sum` per minute (broker total) instead of `Average`.

### Health score
- Each category starts at 100; every CRITICAL finding multiplies it by 0.60, every HIGH by 0.70 and every WARNING by 0.85. Informational findings and checks not assessed do not change the score.
- Overall score = Reliability 35% + Performance 30% + Security 20% + Cost 15%.
- Status label: **Critical** if any critical finding (operational impact now or imminent), **Needs Attention** if any high or warning finding, **Healthy** otherwise. HIGH is used for posture and resilience gaps (unauthenticated listener, plaintext client traffic, 2 AZs on a production Standard cluster) that may be deliberate choices and do not label the cluster as unhealthy. The score is shown next to the label but never overrides it.

## Installation

### Prerequisites
- Python 3.8+
- AWS credentials configured
- IAM permissions (see below)

### Install from Source

```bash
# Clone the repository
git clone https://github.com/aws-samples/sample-health-check-tool-for-msk.git
cd sample-health-check-tool-for-msk

# Install dependencies
pip install -r requirements.txt

# Install the package
pip install -e .
```

## Usage

### Basic usage

```bash
msk-health-check \
  --region us-east-1 \
  --cluster-arn arn:aws:kafka:us-east-1:123456789012:cluster/my-cluster/uuid
```

The command writes `msk_health_check_<cluster>_<timestamp>.pdf` and the matching `.json` manifest to the output directory.

### Options

```bash
# Output directory and workload profile (affects severity of 2-AZ, storage auto scaling and broker log checks)
msk-health-check --region us-west-2 --cluster-arn arn:... --output-dir ./reports --workload production

# Compare with a previous run (new, resolved, changed and persisting findings in the summary)
msk-health-check --region us-east-1 --cluster-arn arn:... --compare-with ./reports/msk_health_check_prod_20260801_120000.json

# Mask account id, ARN and cluster name for sharing
msk-health-check --region us-east-1 --cluster-arn arn:... --redact

# Shorter window, no documentation lookup, debug log
msk-health-check --region us-east-1 --cluster-arn arn:... --days 7 --no-network --debug --log-file run.log
```

| Option | Description | Required |
|--------|-------------|----------|
| `--region` | AWS region of the cluster | Yes |
| `--cluster-arn` | Cluster ARN | Yes |
| `--output-dir` | Directory for the PDF and JSON manifest (default: current directory) | No |
| `--days` | Metrics window in days, 1-30 (default 30; shortened automatically for younger clusters) | No |
| `--workload` | `production` (default) or `non-production` | No |
| `--redact` | Mask account id, ARN and cluster name in the outputs | No |
| `--compare-with` | Manifest JSON of a previous run | No |
| `--no-manifest` | Do not write the JSON manifest | No |
| `--no-network` | Skip the lookup of the recommended Kafka version on the AWS documentation site | No |
| `--debug` / `--log-file` | Logging | No |

The intelligent rebalancing check (Express) needs an SDK whose Kafka API model includes the `Rebalancing` field (boto3 releases from 2025 on); older SDKs report that check as not assessed.

## IAM Permissions

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "MSKHealthCheck",
      "Effect": "Allow",
      "Action": [
        "kafka:DescribeClusterV2",
        "kafka:ListKafkaVersions",
        "cloudwatch:GetMetricStatistics",
        "cloudwatch:GetMetricWidgetImage",
        "cloudwatch:ListMetrics",
        "application-autoscaling:DescribeScalableTargets",
        "application-autoscaling:DescribeScalingPolicies"
      ],
      "Resource": "*"
    }
  ]
}
```

`cloudwatch:ListMetrics` lets the tool tell "not published" apart from "no data" and discover per-listener connection metrics; `application-autoscaling:Describe*` is needed for the storage auto scaling check. Without them the affected checks are reported as not assessed.

## Architecture

### Project structure

```
msk-health-check/
├── msk_health_check/
│   ├── cli.py                  # CLI entry point, exit codes, manifest and comparison
│   ├── validators.py           # Input validation
│   ├── aws_clients.py          # AWS client management
│   ├── cluster_info.py         # Cluster configuration (DescribeClusterV2, ListKafkaVersions, Application Auto Scaling)
│   ├── metrics_collector.py    # Metric catalog, CloudWatch collection with all statistics, coverage and failures
│   ├── reference.py            # Broker size limits, thresholds and documentation sources with provenance
│   ├── analyzer.py             # Checks, findings with explicit state, scoring and status
│   ├── recommendations.py      # One recommendation per finding: action, confirm, verify, impact, links
│   ├── visualizations.py       # CloudWatch widget images with threshold annotations and metric math
│   ├── manifest.py             # JSON manifest, redaction and run comparison
│   ├── pdf_builder.py          # PDF report with table of contents, bookmarks and evidence tables
│   └── logging_config.py       # Logging configuration
├── tests/                      # Unit and property-based tests
├── FINDINGS.md                 # Catalog of checks, thresholds and sources
├── requirements.txt
├── setup.py
└── README.md
```

### Data flow

1. **Input validation** - ARN and region
2. **Cluster configuration** - DescribeClusterV2, ListKafkaVersions, Application Auto Scaling
3. **Metric discovery and collection** - ListMetrics, then GetMetricStatistics with Average, Maximum, Minimum, Sum and SampleCount per metric and broker
4. **Analysis** - every check produces a finding with a state (critical, warning, informational, healthy, not assessed), observed value, threshold, confidence and source
5. **Scoring** - category scores, weighted overall score, status bounded by the worst finding
6. **Recommendations** - one per actionable finding
7. **Charts** - CloudWatch widget images with threshold lines and metric math for derived series
8. **Outputs** - PDF report and JSON manifest; optional comparison with a previous manifest

## Development

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=msk_health_check

# Run specific test file
pytest tests/test_analyzer.py

# Run property-based tests
pytest -k property
```

### Code Quality

```bash
# Format code
black msk_health_check/

# Lint code
pylint msk_health_check/

# Type checking
mypy msk_health_check/
```

## Exit Codes

| Code | Description |
|------|-------------|
| 0 | Success |
| 1 | Invalid input (region, ARN, previous manifest) or cluster not found |
| 2 | Authentication error (missing or expired AWS session) |
| 3 | Insufficient permissions |
| 4 | AWS API or file system error |

## Troubleshooting

### Common Issues

**Issue: "Cluster not found"**
- Verify the cluster ARN is correct
- Ensure you're using the correct region
- Check IAM permissions for `kafka:DescribeClusterV2`

**Issue: "Insufficient permissions"**
- Verify IAM policy includes all required actions
- Check if you're using the correct AWS profile
- Ensure credentials are properly configured

**Issue: checks reported as "not assessed"**
- Section 2 of the report lists the reason for each one
- `ConnectionCreationRate` and `IAMTooManyConnections` need `PER_BROKER` enhanced monitoring
- Storage auto scaling needs `application-autoscaling:Describe*`; metric discovery needs `cloudwatch:ListMetrics`
- A cluster younger than one hour has no hourly datapoints yet

**Issue: "PDF generation failed"**
- Ensure output directory exists and is writable
- Check available disk space
- Verify reportlab is properly installed

## Best Practices

### When to Run

- **Weekly**: For production clusters, with `--compare-with` pointing to the previous manifest
- **After changes**: Post-deployment validation
- **Before scaling**: Capacity planning
- **Incident response**: Root cause analysis

### Interpreting Results

- **Status** is what to act on: Critical means at least one finding with data unavailability, a quota reached or a sustained breach of a published limit; Needs Attention means high or warning findings only (posture gaps and approaching limits).
- **Score** summarises how many findings exist and how heavy they are; two clusters with the same status can have different scores.
- **Confidence** (high / medium / low) is printed with each finding; low-confidence findings rest on tool guidelines rather than published quotas.
- **Not assessed** checks are listed in section 2 with the reason; enable the missing metric or permission to include them.
- **Priority** in the action plan (P1 immediate, P2 this week, P3 this month, P4 next maintenance window, P5 optional) reflects urgency and can differ from severity.

## References

- [Findings Catalog](FINDINGS.md) - Every check with statistics, thresholds, severities and sources
- [AWS MSK Best Practices](https://docs.aws.amazon.com/msk/latest/developerguide/bestpractices.html)
- [AWS MSK Best Practices - Express](https://docs.aws.amazon.com/msk/latest/developerguide/bestpractices-express.html)
- [Amazon MSK quotas](https://docs.aws.amazon.com/msk/latest/developerguide/limits.html)
- [MSK Monitoring](https://docs.aws.amazon.com/msk/latest/developerguide/monitoring.html)
- [MSK Broker Instance Sizes](https://docs.aws.amazon.com/msk/latest/developerguide/broker-instance-sizes.html)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on how to contribute to this project.

## License

This sample code is made available under the MIT-0 license. See the LICENSE file for details.

## Disclaimer

This tool is provided as a sample for educational and demonstration purposes. While it follows AWS best practices, it should be reviewed and tested in your environment before use in production. AWS does not provide official support for this sample code.

## Support

For issues, questions, or contributions:
- GitHub Issues: [Report a bug](https://github.com/aws-samples/sample-health-check-tool-for-msk/issues)
- Documentation: [Wiki](https://github.com/aws-samples/sample-health-check-tool-for-msk/wiki)

## Changelog

### v1.1.0 (2026-09-19)
- Status bounded by the worst finding; HIGH severity for posture gaps that do not make the cluster unhealthy; informational findings no longer reduce the score
- Explicit "not assessed" state with reason for every check that cannot run; coverage shown in the report
- All CloudWatch statistics collected per query; Minimum/Maximum used for controller and partition-state metrics; connection metrics read as Sum per minute (broker total); CPU User + System aligned by timestamp
- Per-broker evaluation of throughput, partitions, connections and connection creation rate against published limits; Express sustained vs throttle quota; unknown broker sizes report not assessed instead of a default limit
- Checks added or wired: heap after GC, disk usage with growth projection, under-replicated partitions, IAMTooManyConnections, encryption in transit / in cluster / at rest, public access, storage auto scaling via Application Auto Scaling, intelligent rebalancing (Express), Kafka version status from the MSK catalog, right-sizing signal
- Metric catalog with monitoring-level requirements; ListMetrics discovery; per-listener connection metrics
- PDF restructured: executive summary with named findings, data quality section, action plan with priority/severity/confidence, findings with observed vs threshold and source, threshold lines on charts, statistics tables, healthy section, inventory, methodology, real table of contents and bookmarks, no emojis
- JSON manifest next to the PDF; `--compare-with`, `--redact`, `--workload`, `--days`, `--no-network`, `--no-manifest`
- reportlab pin relaxed to `<5.0.0` (3.x has no wheels for Python 3.13); version 1.1.0

### v1.0.2 (2025-11-28)
- Added storage growth projection for Standard clusters
- Fixed Express broker partition limits (1500-32000 per broker)
- Fixed network throughput limits for Express (23.4-750 MB/s ingress)
- Improved CPU analysis to focus on sustained high usage (P95 >60%)
- Added intelligent partition rebalancing recommendations
- Enhanced recommendation prioritization based on context
- Ignore low-impact imbalances (CPU <30%, connections <1)
- Network threshold lowered to 70% for earlier warning

### v1.0.1 (2025-11-27)
- Removed intelligent rebalancing check (AWS API limitation - field not returned)
- Updated boto3 to 1.37.38

### v1.0.0 (2025-11-27)
- Initial release
- Support for MSK Standard and Express clusters
- 18 metrics for both Standard and Express
- Category-based health scoring (prevents negative scores)
- PDF reports with visualizations
- Real-time Kafka version validation
- Message distribution imbalance detection (10% threshold)
- Connection monitoring (ClientConnectionCount and ConnectionCount)
- Executive Summary with health score breakdown
- Comprehensive findings documentation
- 40/49 tests passing (82% coverage)
