"""CLI entry point for MSK Health Check Report."""

import argparse
import logging
import os
import sys
from datetime import datetime, timezone

EXIT_OK = 0
EXIT_INPUT = 1
EXIT_AUTH = 2
EXIT_PERMISSION = 3
EXIT_IO = 4


def parse_arguments(argv=None) -> argparse.Namespace:
    """Parse and validate command-line arguments."""
    parser = argparse.ArgumentParser(
        description="MSK Health Check Report - analyse an Amazon MSK cluster against AWS best practices and quotas "
                    "and generate a PDF report with a machine-readable manifest",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --region us-east-1 --cluster-arn arn:aws:kafka:us-east-1:123456789012:cluster/my-cluster/uuid
  %(prog)s --region us-west-2 --cluster-arn arn:aws:kafka:...:cluster/prod/uuid --output-dir ./reports --workload production
  %(prog)s --region us-east-1 --cluster-arn arn:... --compare-with ./reports/msk_health_check_prod_20260801_120000.json
  %(prog)s --region us-east-1 --cluster-arn arn:... --redact
        """
    )
    parser.add_argument('--region', required=True, help='AWS region of the cluster (e.g. us-east-1)')
    parser.add_argument('--cluster-arn', required=True, help='ARN of the MSK cluster to analyse')
    parser.add_argument('--output-dir', default='.', help='Directory for the PDF and JSON manifest (default: current directory)')
    parser.add_argument('--days', type=int, default=None,
                        help='Metrics window in days, 1-30 (default: 30, shortened automatically for younger clusters)')
    parser.add_argument('--workload', choices=['production', 'non-production'], default='production',
                        help='Adjusts the severity of resilience checks (2 AZs, storage auto scaling, broker logs)')
    parser.add_argument('--redact', action='store_true', help='Mask the account id, ARN and cluster name in the outputs')
    parser.add_argument('--compare-with', metavar='MANIFEST_JSON',
                        help='Manifest of a previous run; the report gains a section with new, resolved and persisting findings')
    parser.add_argument('--no-manifest', action='store_true', help='Do not write the JSON manifest next to the PDF')
    parser.add_argument('--no-network', action='store_true',
                        help='Skip the lookup of the recommended Kafka version on the AWS documentation site')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--log-file', help='Path to a log file (default: console only)')
    args = parser.parse_args(argv)
    if args.days is not None and not 1 <= args.days <= 30:
        parser.error('--days must be between 1 and 30')
    return args


def main(argv=None) -> int:
    """Entry point. Returns the process exit code."""
    args = parse_arguments(argv)

    from .logging_config import setup_logging
    setup_logging(debug=args.debug, log_file=args.log_file)
    logger = logging.getLogger(__name__)
    logger.info("MSK Health Check Report starting")
    logger.info(f"Region: {args.region}, cluster ARN: {args.cluster_arn}")

    try:
        from botocore.exceptions import ClientError, NoCredentialsError
        from .validators import validate_region, validate_arn, verify_cluster_exists
        from .aws_clients import create_aws_clients
        from .cluster_info import get_cluster_info
        from .metrics_collector import collect_metrics
        from .analyzer import analyze_metrics
        from .recommendations import generate_recommendations
        from .visualizations import create_charts
        from .manifest import build_manifest, write_manifest, load_manifest, compare_runs
        from .pdf_builder import build_pdf_report, ReportContent, generate_output_filename

        region_result = validate_region(args.region)
        if not region_result.is_valid:  # nosemgrep: is-function-without-parentheses
            logger.error(region_result.error_message)
            return EXIT_INPUT
        arn_result = validate_arn(args.cluster_arn)
        if not arn_result.is_valid:  # nosemgrep: is-function-without-parentheses
            logger.error(arn_result.error_message)
            return EXIT_INPUT

        previous = None
        if args.compare_with:
            try:
                previous = load_manifest(args.compare_with)
            except (OSError, ValueError) as e:
                logger.error(f"Cannot read the previous manifest: {e}")
                return EXIT_INPUT

        try:
            clients = create_aws_clients(args.region)
        except NoCredentialsError:
            logger.error("AWS credentials not found. Configure credentials (aws configure / aws sso login) and retry.")
            return EXIT_AUTH

        exists_result = verify_cluster_exists(clients.msk_client, args.cluster_arn)
        if not exists_result.is_valid:  # nosemgrep: is-function-without-parentheses
            logger.error(exists_result.error_message)
            message = exists_result.error_message or ''
            if 'AccessDenied' in message or 'not authorized' in message:
                return EXIT_PERMISSION
            if 'ExpiredToken' in message or 'InvalidClientTokenId' in message or 'UnrecognizedClientException' in message:
                return EXIT_AUTH
            return EXIT_INPUT

        logger.info("Retrieving cluster information")
        cluster_info = get_cluster_info(clients.msk_client, args.cluster_arn, clients.autoscaling_client)

        days_back = args.days or 30
        if cluster_info.creation_time:
            age_days = (datetime.now(timezone.utc) - cluster_info.creation_time).days
            if age_days < days_back:
                days_back = max(1, age_days)
                logger.info(f"Cluster is {age_days} days old; collecting {days_back} day(s) of metrics")

        logger.info("Collecting metrics from CloudWatch")
        metrics = collect_metrics(clients.cloudwatch_client, args.cluster_arn, cluster_info.broker_count,
                                  cluster_info.cluster_type, days_back,
                                  monitoring_level=cluster_info.enhanced_monitoring_level,
                                  auth_methods=cluster_info.authentication_methods)
        logger.info(f"Collected {len(metrics.metrics)} metric types; {len(metrics.not_published)} not published")

        logger.info("Analysing metrics and configuration")
        analysis = analyze_metrics(cluster_info, metrics, workload=args.workload, allow_network=not args.no_network)
        recommendations = generate_recommendations(analysis)
        logger.info(f"{analysis.checks_assessed}/{analysis.checks_total} checks assessed, status {analysis.overall_status}, "
                    f"score {analysis.overall_health_score}, {len(recommendations)} recommendations")

        logger.info("Rendering charts")
        charts = create_charts(clients.cloudwatch_client, cluster_info, metrics)
        logger.info(f"Created {len(charts)} charts")

        comparison = compare_runs(previous, analysis) if previous else None
        generated_at = datetime.now(timezone.utc)
        pdf_name = generate_output_filename(args.cluster_arn, generated_at, 'pdf', args.redact)
        json_name = generate_output_filename(args.cluster_arn, generated_at, 'json', args.redact)
        os.makedirs(args.output_dir, exist_ok=True)
        pdf_path = os.path.join(args.output_dir, pdf_name)
        json_path = os.path.join(args.output_dir, json_name)

        content = ReportContent(cluster_info=cluster_info, analysis=analysis, recommendations=recommendations, charts=charts,
                                generation_time=generated_at, redact=args.redact, comparison=comparison,
                                manifest_filename='' if args.no_manifest else json_name)
        logger.info("Building PDF report")
        build_pdf_report(content, pdf_path)
        if not args.no_manifest:
            write_manifest(build_manifest(analysis, recommendations, generated_at, pdf_name, args.redact, comparison), json_path)

        print(f"\nReport generated: {pdf_path}")
        if not args.no_manifest:
            print(f"Manifest:         {json_path}")
        print(f"  Status:          {analysis.overall_status} (score {analysis.overall_health_score}/100)")
        print(f"  Checks:          {analysis.checks_assessed}/{analysis.checks_total} assessed")
        print(f"  Findings:        {sum(1 for f in analysis.findings if f.severity.value == 'critical')} critical, "
              f"{sum(1 for f in analysis.findings if f.severity.value == 'warning')} warning")
        print(f"  Recommendations: {len(recommendations)}")
        return EXIT_OK

    except ClientError as e:
        code = e.response.get('Error', {}).get('Code', '')
        logger.exception(f"AWS API error: {code}")
        if code in ('AccessDenied', 'AccessDeniedException', 'UnauthorizedOperation'):
            return EXIT_PERMISSION
        if code in ('ExpiredToken', 'ExpiredTokenException', 'InvalidClientTokenId', 'UnrecognizedClientException'):
            return EXIT_AUTH
        return EXIT_IO
    except OSError as e:
        logger.exception(f"File system error: {e}")
        return EXIT_IO
    except Exception as e:
        logger.exception(f"Error generating report: {e}")
        return EXIT_IO


if __name__ == '__main__':
    sys.exit(main())
