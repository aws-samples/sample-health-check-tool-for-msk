"""CLI entry point for MSK Health Check Report."""

import argparse
import sys
import logging
import os
from datetime import datetime
from typing import Optional


def parse_arguments() -> argparse.Namespace:
    """Parse and validate command-line arguments."""
    parser = argparse.ArgumentParser(
        description="MSK Health Check Report - Analyze AWS MSK cluster health and generate PDF report",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --region us-east-1 --cluster-arn arn:aws:kafka:us-east-1:123456789012:cluster/my-cluster/uuid
  %(prog)s --region us-west-2 --cluster-arn arn:aws:kafka:us-west-2:123456789012:cluster/prod-cluster/uuid --output-dir ./reports --debug
        """
    )
    
    parser.add_argument(
        '--region',
        required=True,
        help='AWS region where the MSK cluster is located (e.g., us-east-1)'
    )
    
    parser.add_argument(
        '--cluster-arn',
        required=True,
        help='ARN of the MSK cluster to analyze'
    )
    
    parser.add_argument(
        '--output-dir',
        default='.',
        help='Directory to save the PDF report (default: current directory)'
    )
    
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging'
    )
    
    parser.add_argument(
        '--log-file',
        help='Path to log file (default: log to console only)'
    )
    
    return parser.parse_args()


def main() -> int:
    """
    Entry point for CLI application.
    Returns: Exit code (0 for success, non-zero for errors)
    """
    args = parse_arguments()
    
    # Configure logging
    from .logging_config import setup_logging
    setup_logging(debug=args.debug, log_file=args.log_file)
    
    logger = logging.getLogger(__name__)
    logger.info("MSK Health Check Report starting...")
    logger.info(f"Region: {args.region}, Cluster ARN: {args.cluster_arn}")
    
    try:
        # Import modules
        from .validators import validate_region, validate_arn, verify_cluster_exists
        from .aws_clients import create_aws_clients
        from .cluster_info import get_cluster_info
        from .metrics_collector import collect_metrics
        from .analyzer import analyze_metrics
        from .recommendations import generate_recommendations
        from .visualizations import create_charts
        from .pdf_builder import build_pdf_report, ReportContent, generate_output_filename
        
        # Validate inputs
        logger.info("Validating inputs...")
        region_result = validate_region(args.region)
        if not region_result.is_valid:  # nosemgrep: is-function-without-parentheses
            logger.error(region_result.error_message)
            return 1
        
        arn_result = validate_arn(args.cluster_arn)
        if not arn_result.is_valid:  # nosemgrep: is-function-without-parentheses
            logger.error(arn_result.error_message)
            return 1
        
        # Create AWS clients
        logger.info("Creating AWS clients...")
        clients = create_aws_clients(args.region)
        
        # Verify cluster exists
        logger.info("Verifying cluster exists...")
        exists_result = verify_cluster_exists(clients.msk_client, args.cluster_arn)
        if not exists_result.is_valid:  # nosemgrep: is-function-without-parentheses
            logger.error(exists_result.error_message)
            return 1
        
        # Get cluster info
        logger.info("Retrieving cluster information...")
        cluster_info = get_cluster_info(clients.msk_client, args.cluster_arn)
        
        # Collect metrics
        logger.info("Collecting metrics from CloudWatch...")
        
        # Calculate available days based on cluster age
        days_back = 30  # Default
        if cluster_info.creation_time:
            from datetime import datetime, timezone
            now = datetime.now(timezone.utc)
            cluster_age_days = (now - cluster_info.creation_time).days
            if cluster_age_days < 30:
                days_back = max(1, cluster_age_days)  # At least 1 day
                logger.info(f"Cluster is {cluster_age_days} days old, collecting {days_back} days of metrics")
        
        metrics = collect_metrics(clients.cloudwatch_client, args.cluster_arn, cluster_info.broker_count, cluster_info.cluster_type, days_back)
        logger.info(f"Collected {len(metrics.metrics)} metric types")
        
        # Analyze metrics
        logger.info("Analyzing metrics...")
        analysis = analyze_metrics(cluster_info, metrics)
        logger.info(f"Analysis complete: {len(analysis.findings)} findings, health score: {analysis.overall_health_score}")
        
        # Generate recommendations
        logger.info("Generating recommendations...")
        recommendations = generate_recommendations(analysis)
        logger.info(f"Generated {len(recommendations)} recommendations")
        
        # Create visualizations
        logger.info("Creating visualizations...")
        charts = create_charts(clients.cloudwatch_client, cluster_info, metrics)
        logger.info(f"Created {len(charts)} charts")
        
        # Build PDF report
        logger.info("Building PDF report...")
        report_content = ReportContent(
            cluster_info=cluster_info,
            analysis=analysis,
            recommendations=recommendations,
            charts=charts,
            generation_time=datetime.utcnow()
        )
        
        output_filename = generate_output_filename(args.cluster_arn, datetime.utcnow())
        output_path = os.path.join(args.output_dir, output_filename)
        
        build_pdf_report(report_content, output_path)
        
        logger.info(f"Report generated successfully: {output_path}")
        print(f"\n✓ Report generated: {output_path}")
        print(f"  Health Score: {analysis.overall_health_score}/100")
        print(f"  Findings: {len(analysis.findings)}")
        print(f"  Recommendations: {len(recommendations)}")
        
        return 0
        
    except Exception as e:
        logger.exception(f"Error generating report: {e}")
        return 4


if __name__ == '__main__':
    sys.exit(main())
