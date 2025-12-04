# Security Policy

## Reporting a Vulnerability

If you discover a potential security issue in this project, we ask that you notify AWS Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). Please do **not** create a public GitHub issue.

## Security Best Practices

When using this tool, please follow these security best practices:

### AWS Credentials
- Never commit AWS credentials to the repository
- Use IAM roles when running on EC2 or ECS
- Use temporary credentials via AWS STS when possible
- Follow the principle of least privilege for IAM permissions

### Required IAM Permissions
The tool requires the following minimal IAM permissions:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
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

### Data Handling
- The tool only reads metrics and configuration data
- No data is sent to external services
- PDF reports are generated locally
- Ensure proper file permissions on generated reports

### Network Security
- The tool uses AWS SDK which communicates over HTTPS
- No inbound network connections are required
- Outbound connections are only to AWS API endpoints

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 1.0.x   | :white_check_mark: |

## Security Updates

Security updates will be released as patch versions. Please keep your installation up to date.
