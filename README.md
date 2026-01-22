# Health Check for Amazon MSK

> **Note:** This is a sample tool that demonstrates how to automate the collection and validation of Amazon MSK metrics against AWS best practices. It serves as an example implementation that you can use as-is or customize for your specific operational requirements.

An automated health analysis and reporting tool for Amazon MSK clusters that generates comprehensive PDF reports with metrics, visualizations, and actionable recommendations.

## Overview

This sample Python CLI tool demonstrates how to automate operational reviews of Amazon MSK clusters. It shows how to:
- Collect up to 30 days of CloudWatch metrics programmatically
- Analyze metrics against AWS MSK best practices
- Generate  PDF reports with visualizations
- Provide prioritized, actionable recommendations

The tool is designed to be a starting point for building your own MSK monitoring and reporting solutions. You can use it as-is for basic health checks or extend it with additional metrics, custom thresholds, and organization-specific best practices.

**Smart Period Detection:** The tool automatically detects cluster age and adjusts the metrics collection period. For clusters younger than 30 days, it collects all available metrics since creation, ensuring charts are not empty.

## Features

### Cluster Support
- ✅ **MSK Provisioned (Standard)** - Full support with 18 metrics
- ✅ **MSK Serverless (Express)** - Full support with 18 metrics

### Analysis Categories

**Reliability & Availability (35% weight)**
- Active Controller monitoring
- Offline Partitions detection
- Under-Replicated Partitions tracking
- Partition distribution balance
- Leader distribution balance
- Under Min ISR detection

**Performance & Capacity (30% weight)**
- CPU usage monitoring (P95 < 60%)
- Memory usage tracking (Heap after GC < 60%)
- Disk usage analysis with growth projection (Standard only)
- Network throughput validation against instance limits
- Partition capacity checks (per-broker and cluster-wide)
- Client connection count monitoring
- Total connection count monitoring
- Message distribution balance (10% threshold)
- Connection churn detection

**Security (20% weight)**
- Encryption in-transit validation
- Encryption at-rest validation
- Authentication configuration (IAM, SASL/SCRAM, mTLS)
- Enhanced monitoring status

**Cost Optimization (15% weight)**
- Instance type recommendations (Graviton)
- Storage auto-scaling configuration
- Right-sizing opportunities

### Metrics Analyzed

**Standard (Provisioned) - 18 Metrics:**
- `ActiveControllerCount`, `OfflinePartitionsCount`, `GlobalPartitionCount`, `GlobalTopicCount`
- `CpuUser`, `CpuSystem`, `CpuIdle`
- `MemoryUsed`, `MemoryFree`, `HeapMemoryAfterGC`
- `KafkaDataLogsDiskUsed`
- `LeaderCount`, `PartitionCount`, `UnderMinIsrPartitionCount`
- `BytesInPerSec`, `BytesOutPerSec`, `MessagesInPerSec`
- `ClientConnectionCount`, `ConnectionCount`

**Express (Serverless) - 18 Metrics:**
- `ActiveControllerCount`, `OfflinePartitionsCount`, `GlobalPartitionCount`, `GlobalTopicCount`
- `CpuUser`, `CpuSystem`, `CpuIdle`
- `MemoryUsed`, `MemoryFree`, `HeapMemoryAfterGC`
- `LeaderCount`, `PartitionCount`, `UnderMinIsrPartitionCount`
- `BytesInPerSec`, `BytesOutPerSec`, `MessagesInPerSec`
- `ClientConnectionCount`, `ConnectionCount`

### Health Score System

**Category-Based Scoring:**
- Each category starts at 100 points
- Weighted average based on category importance
- Multiplicative deductions prevent negative scores

**Severity Impact:**
- **CRITICAL**: -40% of category score per issue
- **WARNING**: -15% of category score per issue
- **INFORMATIONAL**: -5% of category score per issue

**Score Interpretation:**
- 80-100: Healthy ✅
- 60-79: Needs Attention ⚠️
- 0-59: Critical 🔴

### Report Features

**PDF Report:**
- Title page with cluster identification
- Table of contents
- Executive summary with health score
- Detailed cluster information (ARN, Account ID, Region)
- Findings summary by severity
- Detailed metrics analysis with CloudWatch charts
- Configuration analysis
- Consolidated recommendations table
- References & resources

## Installation

### Prerequisites
- Python 3.8+
- AWS credentials configured
- IAM permissions (see below)

### Install from Source

```bash
# Clone the repository
git clone https://github.com/yourusername/msk-health-check.git
cd msk-health-check

# Install dependencies
pip install -r requirements.txt

# Install the package
pip install -e .
```

## Usage

### Basic Usage

```bash
msk-health-check \
  --region us-east-1 \
  --cluster-arn arn:aws:kafka:us-east-1:123456789012:cluster/my-cluster/uuid
```

### Advanced Options

```bash
# Custom output directory
msk-health-check \
  --region us-west-2 \
  --cluster-arn arn:aws:kafka:us-west-2:123456789012:cluster/prod-cluster/uuid \
  --output-dir ./reports

# Enable debug logging
msk-health-check \
  --region us-east-1 \
  --cluster-arn arn:aws:kafka:us-east-1:123456789012:cluster/my-cluster/uuid \
  --debug

# Save logs to file
msk-health-check \
  --region us-east-1 \
  --cluster-arn arn:aws:kafka:us-east-1:123456789012:cluster/my-cluster/uuid \
  --log-file msk-health-check.log
```

### Command-Line Options

| Option | Description | Required |
|--------|-------------|----------|
| `--region` | AWS region where cluster is located | Yes |
| `--cluster-arn` | Full ARN of the MSK cluster | Yes |
| `--output-dir` | Directory to save PDF report (default: current directory) | No |
| `--debug` | Enable debug logging | No |
| `--log-file` | Path to save log file | No |

## IAM Permissions

The tool requires the following IAM permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "kafka:DescribeClusterV2",
        "kafka:ListClusters",
        "cloudwatch:GetMetricStatistics",
        "cloudwatch:GetMetricWidgetImage",
        "cloudwatch:ListMetrics"
      ],
      "Resource": "*"
    }
  ]
}
```

### Minimal IAM Policy

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "MSKHealthCheckPermissions",
      "Effect": "Allow",
      "Action": [
        "kafka:DescribeClusterV2",
        "cloudwatch:GetMetricStatistics",
        "cloudwatch:GetMetricWidgetImage"
      ],
      "Resource": "*"
    }
  ]
}
```

## Architecture

### Project Structure

```
msk-health-check/
├── msk_health_check/
│   ├── __init__.py
│   ├── cli.py                  # CLI entry point
│   ├── validators.py           # Input validation
│   ├── aws_clients.py          # AWS client management
│   ├── cluster_info.py         # Cluster information retrieval
│   ├── metrics_collector.py    # CloudWatch metrics collection
│   ├── analyzer.py             # Metrics analysis and scoring
│   ├── recommendations.py      # Recommendation generation
│   ├── visualizations.py       # Chart generation
│   ├── pdf_builder.py          # PDF report building
│   └── logging_config.py       # Logging configuration
├── tests/                      # Unit and integration tests
├── requirements.txt            # Python dependencies
├── setup.py                    # Package configuration
└── README.md                   # This file
```

### Data Flow

1. **Input Validation** → Validates cluster ARN and region
2. **Cluster Info** → Retrieves cluster configuration via MSK API
3. **Metrics Collection** → Collects 30 days of CloudWatch metrics
4. **Analysis** → Analyzes metrics against best practices
5. **Scoring** → Calculates category-based health score
6. **Recommendations** → Generates prioritized recommendations
7. **Visualization** → Creates CloudWatch charts
8. **PDF Generation** → Builds PDF report

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
| 1 | Cluster not found |
| 2 | Authentication error |
| 3 | Insufficient permissions |
| 4 | File system error |

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

**Issue: "No metrics data"**
- Cluster must be running for at least 1 hour
- Verify CloudWatch metrics are enabled
- Check if cluster has DEFAULT monitoring level

**Issue: "PDF generation failed"**
- Ensure output directory exists and is writable
- Check available disk space
- Verify reportlab is properly installed

## Best Practices

### When to Run

- **Weekly**: For production clusters
- **After changes**: Post-deployment validation
- **Before scaling**: Capacity planning
- **Incident response**: Root cause analysis

### Interpreting Results

**Health Score:**
- 90-100: Excellent, maintain current configuration
- 80-89: Good, minor optimizations recommended
- 70-79: Fair, address warnings soon
- 60-69: Poor, immediate attention needed
- <60: Critical, urgent action required

**Recommendations Priority:**
- CRITICAL (Priority 1): Immediate action required
- HIGH (Priority 2): Address within 1 week
- MEDIUM (Priority 3): Address within 1 month
- LOW (Priority 4): Consider for next maintenance window
- INFO (Priority 5): Optional improvements

## References

- [Findings Documentation](FINDINGS.md) - Detailed explanation of all findings and recommendations
- [AWS MSK Best Practices](https://docs.aws.amazon.com/msk/latest/developerguide/bestpractices.html)
- [MSK Monitoring](https://docs.aws.amazon.com/msk/latest/developerguide/monitoring.html)
- [MSK Broker Instance Sizes](https://docs.aws.amazon.com/msk/latest/developerguide/broker-instance-sizes.html)
- [MSK Serverless](https://docs.aws.amazon.com/msk/latest/developerguide/serverless.html)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on how to contribute to this project.

## License

This sample code is made available under the MIT-0 license. See the LICENSE file for details.

## Disclaimer

This tool is provided as a sample for educational and demonstration purposes. While it follows AWS best practices, it should be reviewed and tested in your environment before use in production. AWS does not provide official support for this sample code.

## Support

For issues, questions, or contributions:
- GitHub Issues: [Report a bug](https://github.com/hermes-pimentel/msk-health-check/issues)
- Documentation: [Wiki](https://github.com/hermes-pimentel/msk-health-check/wiki)

## Changelog

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
