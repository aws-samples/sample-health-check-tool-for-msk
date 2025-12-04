# Deployment Guide

This guide covers different deployment options for the MSK Health Check Report tool.

## Table of Contents
- [Local Installation](#local-installation)
- [Docker Deployment](#docker-deployment)
- [AWS Lambda Deployment](#aws-lambda-deployment)
- [CI/CD Integration](#cicd-integration)

## Local Installation

### Prerequisites
- Python 3.8 or higher
- pip package manager
- AWS credentials configured

### Step-by-Step Installation

1. **Clone the repository**
```bash
git clone https://github.com/yourusername/msk-health-check.git
cd msk-health-check
```

2. **Create virtual environment (recommended)**
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
```

4. **Install the package**
```bash
pip install -e .
```

5. **Verify installation**
```bash
msk-health-check --help
```

### AWS Credentials Setup

**Option 1: AWS CLI Configuration**
```bash
aws configure
```

**Option 2: Environment Variables**
```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-east-1
```

**Option 3: IAM Role (EC2/ECS)**
- Attach IAM role with required permissions to your instance

## Docker Deployment

### Build Docker Image

Create a `Dockerfile`:
```dockerfile
FROM python:3.9-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY . .
RUN pip install -e .

# Create output directory
RUN mkdir -p /reports

ENTRYPOINT ["msk-health-check"]
```

Build the image:
```bash
docker build -t msk-health-check:latest .
```

### Run with Docker

```bash
docker run --rm \
  -v ~/.aws:/root/.aws:ro \
  -v $(pwd)/reports:/reports \
  msk-health-check:latest \
  --region us-east-1 \
  --cluster-arn arn:aws:kafka:us-east-1:123456789012:cluster/my-cluster/uuid \
  --output-dir /reports
```

### Docker Compose

Create `docker-compose.yml`:
```yaml
version: '3.8'

services:
  msk-health-check:
    build: .
    volumes:
      - ~/.aws:/root/.aws:ro
      - ./reports:/reports
    environment:
      - AWS_REGION=us-east-1
    command: >
      --region us-east-1
      --cluster-arn arn:aws:kafka:us-east-1:123456789012:cluster/my-cluster/uuid
      --output-dir /reports
```

Run:
```bash
docker-compose up
```

## AWS Lambda Deployment

### Package for Lambda

1. **Create deployment package**
```bash
# Create package directory
mkdir lambda-package
cd lambda-package

# Install dependencies
pip install -r ../requirements.txt -t .

# Copy application code
cp -r ../msk_health_check .

# Create Lambda handler
cat > lambda_handler.py << 'EOF'
import json
import os
from msk_health_check.cli import main

def lambda_handler(event, context):
    """
    Lambda handler for MSK Health Check.
    
    Event format:
    {
        "region": "us-east-1",
        "cluster_arn": "arn:aws:kafka:...",
        "output_bucket": "my-reports-bucket"
    }
    """
    region = event['region']
    cluster_arn = event['cluster_arn']
    output_bucket = event.get('output_bucket')
    
    # Run health check
    # Implementation depends on your requirements
    
    return {
        'statusCode': 200,
        'body': json.dumps('Health check completed')
    }
EOF

# Create ZIP package
zip -r ../msk-health-check-lambda.zip .
```

2. **Create Lambda function**
```bash
aws lambda create-function \
  --function-name msk-health-check \
  --runtime python3.9 \
  --role arn:aws:iam::123456789012:role/lambda-msk-health-check \
  --handler lambda_handler.lambda_handler \
  --zip-file fileb://msk-health-check-lambda.zip \
  --timeout 900 \
  --memory-size 1024
```

3. **Required IAM Role Permissions**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "kafka:DescribeClusterV2",
        "cloudwatch:GetMetricStatistics",
        "cloudwatch:GetMetricWidgetImage",
        "s3:PutObject",
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "*"
    }
  ]
}
```

### EventBridge Scheduled Execution

Create a rule to run weekly:
```bash
aws events put-rule \
  --name msk-health-check-weekly \
  --schedule-expression "cron(0 9 ? * MON *)"

aws events put-targets \
  --rule msk-health-check-weekly \
  --targets "Id"="1","Arn"="arn:aws:lambda:us-east-1:123456789012:function:msk-health-check"
```

## CI/CD Integration

### GitHub Actions

Create `.github/workflows/msk-health-check.yml`:
```yaml
name: MSK Health Check

on:
  schedule:
    - cron: '0 9 * * 1'  # Every Monday at 9 AM
  workflow_dispatch:

jobs:
  health-check:
    runs-on: ubuntu-latest
    
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.9'
      
      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install -e .
      
      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v2
        with:
          aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
          aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          aws-region: us-east-1
      
      - name: Run health check
        run: |
          msk-health-check \
            --region us-east-1 \
            --cluster-arn ${{ secrets.MSK_CLUSTER_ARN }} \
            --output-dir ./reports
      
      - name: Upload report
        uses: actions/upload-artifact@v3
        with:
          name: msk-health-report
          path: ./reports/*.pdf
      
      - name: Upload to S3
        run: |
          aws s3 cp ./reports/*.pdf s3://my-reports-bucket/msk-health-checks/
```

### GitLab CI

Create `.gitlab-ci.yml`:
```yaml
stages:
  - health-check

msk-health-check:
  stage: health-check
  image: python:3.9
  
  before_script:
    - pip install -r requirements.txt
    - pip install -e .
  
  script:
    - msk-health-check
        --region $AWS_REGION
        --cluster-arn $MSK_CLUSTER_ARN
        --output-dir ./reports
    - aws s3 cp ./reports/*.pdf s3://my-reports-bucket/msk-health-checks/
  
  artifacts:
    paths:
      - reports/*.pdf
    expire_in: 30 days
  
  only:
    - schedules
```

### Jenkins Pipeline

Create `Jenkinsfile`:
```groovy
pipeline {
    agent any
    
    triggers {
        cron('0 9 * * 1')  // Every Monday at 9 AM
    }
    
    environment {
        AWS_REGION = 'us-east-1'
        MSK_CLUSTER_ARN = credentials('msk-cluster-arn')
    }
    
    stages {
        stage('Setup') {
            steps {
                sh 'pip install -r requirements.txt'
                sh 'pip install -e .'
            }
        }
        
        stage('Health Check') {
            steps {
                sh '''
                    msk-health-check \
                        --region ${AWS_REGION} \
                        --cluster-arn ${MSK_CLUSTER_ARN} \
                        --output-dir ./reports
                '''
            }
        }
        
        stage('Archive') {
            steps {
                archiveArtifacts artifacts: 'reports/*.pdf'
                sh 'aws s3 cp ./reports/*.pdf s3://my-reports-bucket/msk-health-checks/'
            }
        }
    }
    
    post {
        always {
            cleanWs()
        }
    }
}
```

## Production Considerations

### Security
- Use IAM roles instead of access keys when possible
- Store sensitive data in AWS Secrets Manager or Parameter Store
- Enable CloudTrail logging for audit
- Use VPC endpoints for AWS API calls

### Monitoring
- Set up CloudWatch alarms for Lambda failures
- Monitor execution time and memory usage
- Track report generation success/failure rates

### Cost Optimization
- Use Lambda for scheduled execution (pay per use)
- Store reports in S3 with lifecycle policies
- Use S3 Intelligent-Tiering for cost savings

### Scalability
- Process multiple clusters in parallel
- Use SQS for queue-based processing
- Implement retry logic with exponential backoff

## Troubleshooting

### Common Deployment Issues

**Issue: Lambda timeout**
- Increase timeout to 900 seconds (15 minutes)
- Increase memory to 1024 MB or higher

**Issue: Package too large for Lambda**
- Use Lambda layers for dependencies
- Remove unnecessary files from package
- Consider using EFS for large dependencies

**Issue: Permission denied errors**
- Verify IAM role has all required permissions
- Check resource-based policies
- Ensure cross-account access is configured correctly

## Support

For deployment issues:
- Check logs in CloudWatch Logs
- Review IAM permissions
- Verify network connectivity
- Contact support team
