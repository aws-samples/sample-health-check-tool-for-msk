"""PDF report builder module."""

from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from typing import Dict, List

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle, PageBreak, KeepTogether
from reportlab.lib.enums import TA_CENTER, TA_LEFT

from .analyzer import AnalysisResult, Finding, Severity
from .cluster_info import ClusterInfo
from .metrics_collector import MetricData
from .recommendations import Recommendation


@dataclass
class ReportContent:
    """Complete report data."""
    cluster_info: ClusterInfo
    analysis: AnalysisResult
    recommendations: List[Recommendation]
    charts: List  # List of ChartImage from visualizations module
    generation_time: datetime


def build_pdf_report(content: ReportContent, output_path: str) -> None:
    """
    Build complete PDF report.
    
    Args:
        content: Report content to include
        output_path: Path to save PDF file
    """
    doc = SimpleDocTemplate(
        output_path, 
        pagesize=letter,
        leftMargin=0.75*inch,
        rightMargin=0.75*inch,
        topMargin=0.75*inch,
        bottomMargin=0.75*inch
    )
    story = []
    styles = getSampleStyleSheet()
    
    # Define consistent color scheme (Rule of 3)
    PRIMARY_COLOR = colors.HexColor('#0066cc')    # Professional blue
    SECONDARY_COLOR = colors.HexColor('#6c757d')  # Neutral gray
    ACCENT_COLOR = colors.HexColor('#28a745')     # Success green
    
    # Custom styles with better hierarchy
    title_style = ParagraphStyle(
        'CustomTitle', 
        parent=styles['Heading1'], 
        fontSize=28,
        textColor=PRIMARY_COLOR,
        spaceAfter=12,
        spaceBefore=0,
        alignment=TA_CENTER,
        fontName='Helvetica-Bold'
    )
    
    subtitle_style = ParagraphStyle(
        'Subtitle',
        parent=styles['Normal'],
        fontSize=12,
        textColor=SECONDARY_COLOR,
        alignment=TA_CENTER,
        spaceAfter=30
    )
    
    # Title Page
    story.append(Spacer(1, 2*inch))
    story.append(Paragraph('Amazon MSK', title_style))
    story.append(Paragraph('Operational Review Report', title_style))
    story.append(Spacer(1, 0.3*inch))
    story.append(Paragraph(f'Cluster: {content.cluster_info.name}', subtitle_style))
    story.append(Paragraph(f'Generated: {content.generation_time.strftime("%B %d, %Y")}', subtitle_style))
    story.append(PageBreak())
    
    # Disclaimer
    story.extend(create_disclaimer_section(styles))
    story.append(PageBreak())
    
    # Table of Contents
    story.extend(create_table_of_contents(styles))
    story.append(PageBreak())
    
    # Executive Summary
    story.extend(create_executive_summary(content, styles))
    story.append(PageBreak())
    
    # Cluster Details & Findings
    story.extend(create_summary_section(content))
    story.append(PageBreak())
    
    # Metrics Analysis
    story.append(Paragraph('<b>Detailed Metrics Analysis</b>', styles['Heading1']))
    story.append(Spacer(1, 0.3*inch))
    
    charts_dict = {chart.metric_name: chart.image_data for chart in content.charts}
    
    # Show all metrics that have charts
    for metric_name, chart_data in charts_dict.items():
        metric_list = content.analysis.metrics.metrics.get(metric_name, [])
        findings = [f for f in content.analysis.findings if f.metric_name == metric_name]
        recs = [r for r in content.recommendations if r.finding in findings]
        
        # Use first metric (cluster-level or first broker) for display
        metric = metric_list[0] if metric_list and isinstance(metric_list, list) else (metric_list if metric_list else None)
        story.extend(create_metric_section(metric_name, metric, metric_list, findings, recs, chart_data))
        story.append(PageBreak())
    
    # Configuration findings (no metrics/charts)
    config_findings = [f for f in content.analysis.findings if f.metric_name not in charts_dict]
    if config_findings:
        story.append(Paragraph('<b>Configuration Analysis</b>', styles['Heading1']))
        story.append(Spacer(1, 0.2*inch))
        
        for finding in config_findings:
            recs = [r for r in content.recommendations if r.finding == finding]
            story.extend(create_config_finding_section(finding, recs, styles))
            story.append(Spacer(1, 0.3*inch))
    
    # Consolidated Recommendations Table
    if content.recommendations:
        story.append(PageBreak())
        story.append(Paragraph('<b>Summary - Key Recommendations</b>', styles['Heading1']))
        story.append(Spacer(1, 0.2*inch))
        story.extend(create_recommendations_table(content.recommendations, styles))
    
    # Important Links
    story.append(PageBreak())
    story.extend(create_important_links_section(styles))
    
    doc.build(story)


def create_table_of_contents(styles) -> List:
    """Create table of contents."""
    elements = []
    
    elements.append(Paragraph('<b>Table of Contents</b>', styles['Heading1']))
    elements.append(Spacer(1, 0.3*inch))
    
    toc_style = ParagraphStyle(
        'TOC',
        parent=styles['Normal'],
        fontSize=11,
        leading=18,
        leftIndent=0,
        spaceAfter=6
    )
    
    toc_items = [
        ('1.', 'Executive Summary'),
        ('2.', 'Cluster Details'),
        ('3.', 'Findings Summary'),
        ('4.', 'Detailed Metrics Analysis'),
        ('5.', 'Configuration Analysis'),
        ('6.', 'Recommendations'),
        ('7.', 'References & Resources'),
    ]
    
    for number, title in toc_items:
        elements.append(Paragraph(f'<b>{number}</b> {title}', toc_style))
    
    return elements


def create_executive_summary(content: ReportContent, styles) -> List:
    """Create executive summary with key findings and recommendations."""
    elements = []
    
    elements.append(Paragraph('<b>Executive Summary</b>', styles['Heading1']))
    elements.append(Spacer(1, 0.3*inch))
    
    # Overview paragraph
    # Calculate actual metrics period
    metrics_days = (content.analysis.metrics.end_time - content.analysis.metrics.start_time).days
    
    # Build period description
    if metrics_days < 30:
        period_text = f"over a {metrics_days}-day period (cluster is newer than 30 days)"
    else:
        period_text = "over a 30-day period"
    
    overview = f"""This operational review analyzes the health and performance of the Amazon MSK cluster 
    <b>{content.cluster_info.name}</b> {period_text}. The cluster is running Kafka version 
    {content.cluster_info.kafka_version} with {content.cluster_info.broker_count} broker nodes 
    on {content.cluster_info.instance_type} instances."""
    
    elements.append(Paragraph(overview, styles['Normal']))
    elements.append(Spacer(1, 0.2*inch))
    
    # Health Score
    score = content.analysis.overall_health_score
    score_color = '#28a745' if score >= 80 else '#ffc107' if score >= 60 else '#dc3545'
    score_status = 'Healthy' if score >= 80 else 'Needs Attention' if score >= 60 else 'Critical'
    
    elements.append(Paragraph(f'<b>Overall Health Score:</b> <font color="{score_color}"><b>{score:.0f}/100</b></font> ({score_status})', styles['Normal']))
    elements.append(Spacer(1, 0.2*inch))
    
    # Key Findings
    critical_findings = [f for f in content.analysis.findings if f.severity == Severity.CRITICAL]
    warning_findings = [f for f in content.analysis.findings if f.severity == Severity.WARNING]
    
    elements.append(Paragraph('<b>Key Findings:</b>', styles['Heading3']))
    elements.append(Spacer(1, 0.1*inch))
    
    if critical_findings:
        elements.append(Paragraph(f'• <font color="#dc3545"><b>{len(critical_findings)} Critical Issues</b></font> requiring immediate attention', styles['Normal']))
    if warning_findings:
        elements.append(Paragraph(f'• <font color="#ffc107"><b>{len(warning_findings)} Warnings</b></font> that should be addressed', styles['Normal']))
    if not critical_findings and not warning_findings:
        elements.append(Paragraph('• <font color="#28a745"><b>No critical issues detected</b></font> - cluster is operating within recommended parameters', styles['Normal']))
    
    elements.append(Spacer(1, 0.2*inch))
    
    # Top Recommendations
    if content.recommendations:
        elements.append(Paragraph('<b>Top Recommendations:</b>', styles['Heading3']))
        elements.append(Spacer(1, 0.1*inch))
        
        top_recs = sorted(content.recommendations, key=lambda r: r.priority)[:3]
        for i, rec in enumerate(top_recs, 1):
            elements.append(Paragraph(f'{i}. {rec.action}', styles['Normal']))
            elements.append(Spacer(1, 0.05*inch))
    
    elements.append(Spacer(1, 0.2*inch))
    
    # Analysis Scope
    elements.append(Paragraph('<b>Analysis Scope:</b>', styles['Heading3']))
    elements.append(Spacer(1, 0.1*inch))
    
    scope_text = f"""This review analyzed {len(content.analysis.metrics.metrics)} metric types collected over {metrics_days} days, 
    covering reliability, performance, capacity, and configuration best practices. The analysis includes automated 
    checks against AWS MSK best practices and industry standards."""
    
    elements.append(Paragraph(scope_text, styles['Normal']))
    
    return elements


def create_overview_section(styles) -> List:
    """Create overview section explaining the report purpose."""
    elements = []
    
    elements.append(Paragraph('<b>Overview</b>', styles['Heading2']))
    
    overview_text = """Amazon MSK Operational Review is a proactive, targeted operational review based on your MSK cluster 
    against design and configuration best practices. This review provides prescriptive and actionable recommendations 
    for optimizing the MSK cluster configurations for reliability, security, performance and cost optimization."""
    
    elements.append(Paragraph(overview_text, styles['Normal']))
    elements.append(Spacer(1, 0.2*inch))
    
    # Analysis categories
    elements.append(Paragraph('<b>Analysis Performed</b>', styles['Normal']))
    elements.append(Spacer(1, 0.1*inch))
    
    elements.append(Paragraph('<b>Reliability & Availability</b>', styles['Normal']))
    checks = [
        ('Active Controller', 'Ensures exactly one controller is active in the cluster'),
        ('Offline Partitions', 'Detects partitions without an active leader'),
        ('Under-Replicated Partitions', 'Identifies partitions below minimum in-sync replicas'),
        ('Partition Distribution', 'Validates balanced partition distribution across brokers'),
        ('Leader Distribution', 'Checks for balanced leadership across brokers'),
    ]
    for name, desc in checks:
        elements.append(Paragraph(f'• <b>{name}:</b> {desc}', styles['Normal']))
    elements.append(Spacer(1, 0.1*inch))
    
    elements.append(Paragraph('<b>Performance & Capacity</b>', styles['Normal']))
    checks = [
        ('CPU Usage', 'Monitors total CPU (User + System) against 60% threshold'),
        ('Memory Usage', 'Tracks heap memory after GC against 60% threshold'),
        ('Disk Usage', 'Monitors storage utilization and auto-scaling configuration'),
        ('Network Throughput', 'Validates traffic against instance-specific limits'),
        ('Partition Capacity', 'Checks partition count against broker capacity limits'),
        ('Connection Count', 'Monitors client connections against instance limits'),
        ('Connection Churn', 'Detects excessive connection creation/close rates'),
    ]
    for name, desc in checks:
        elements.append(Paragraph(f'• <b>{name}:</b> {desc}', styles['Normal']))
    elements.append(Spacer(1, 0.1*inch))
    
    elements.append(Paragraph('<b>Configuration & Best Practices</b>', styles['Normal']))
    checks = [
        ('Kafka Version', 'Validates cluster runs AWS recommended version'),
        ('Enhanced Monitoring', 'Checks if PER_BROKER monitoring is enabled'),
        ('Encryption', 'Verifies encryption in-transit and at-rest configuration'),
        ('Multi-AZ', 'Confirms deployment across multiple availability zones'),
        ('Intelligent Rebalancing', 'Validates rebalancing for MSK Express clusters'),
    ]
    for name, desc in checks:
        elements.append(Paragraph(f'• <b>{name}:</b> {desc}', styles['Normal']))
    
    elements.append(Spacer(1, 0.15*inch))
    elements.append(Paragraph(
        'Refer <a href="https://docs.aws.amazon.com/msk/latest/developerguide/bestpractices.html" color="blue">Amazon MSK Best Practices</a> for more details.',
        styles['Normal']
    ))
    
    return elements


def create_summary_section(content: ReportContent) -> List:
    """
    Create executive summary section.
    
    Args:
        content: Report content
        
    Returns:
        List of reportlab flowables
    """
    elements = []
    styles = getSampleStyleSheet()
    
    # Extract Account ID and Region from ARN
    arn_parts = content.cluster_info.arn.split(':')
    account_id = arn_parts[4] if len(arn_parts) > 4 else 'N/A'
    region = arn_parts[3] if len(arn_parts) > 3 else 'N/A'
    
    # Cluster info - keep together on one page
    cluster_details = []
    cluster_details.append(Paragraph('<b>Cluster Details</b>', styles['Heading2']))
    
    # Create small font style for ARN
    small_style = ParagraphStyle('Small', parent=styles['Normal'], fontSize=8, leading=10)
    
    info_data = [
        ['Cluster Name', content.cluster_info.name],
        ['Cluster ARN', Paragraph(content.cluster_info.arn, small_style)],
        ['Account ID', account_id],
        ['Region', region],
        ['MSK Version', content.cluster_info.kafka_version],
        ['Instance Type', content.cluster_info.instance_type],
        ['No. of Broker Nodes', str(content.cluster_info.broker_count)],
        ['EBS Volume Size Per Broker', f'{content.cluster_info.ebs_volume_size} GB' if content.cluster_info.ebs_volume_size else 'N/A'],
        ['Enhanced Monitoring', content.cluster_info.enhanced_monitoring_level],
        ['Encryption In Transit - Client Broker', content.cluster_info.encryption_in_transit_type],
        ['Encryption In Transit - In Cluster', 'True' if content.cluster_info.encryption_in_transit else 'False'],
        ['Encryption At Rest', 'True' if content.cluster_info.encryption_at_rest else 'False'],
        ['Cluster State', content.cluster_info.cluster_state],
        ['Health Score', f'{content.analysis.overall_health_score:.1f}/100'],
    ]
    
    info_table = Table(info_data, colWidths=[2.5*inch, 3.5*inch])
    info_table.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTNAME', (1, 0), (1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 10),
        ('TOPPADDING', (0, 0), (-1, -1), 10),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#e9ecef')),
        ('TEXTCOLOR', (0, 0), (0, -1), colors.HexColor('#495057')),
        ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#dee2e6')),
    ]))
    cluster_details.append(info_table)
    
    # Wrap in KeepTogether to prevent page breaks
    elements.append(KeepTogether(cluster_details))
    elements.append(Spacer(1, 0.3*inch))
    
    # Findings summary by severity - as table
    elements.append(Paragraph('<b>Findings Summary</b>', styles['Heading2']))
    elements.append(Spacer(1, 0.1*inch))
    
    critical_findings = [f for f in content.analysis.findings if f.severity == Severity.CRITICAL]
    warning_findings = [f for f in content.analysis.findings if f.severity == Severity.WARNING]
    info_findings = [f for f in content.analysis.findings if f.severity == Severity.INFORMATIONAL]
    healthy_findings = [f for f in content.analysis.findings if f.severity == Severity.HEALTHY]
    
    summary_data = [
        ['Severity', 'Count', 'Status'],
        ['Critical', str(len(critical_findings)), '🔴' if len(critical_findings) > 0 else '✓'],
        ['Warning', str(len(warning_findings)), '🟠' if len(warning_findings) > 0 else '✓'],
        ['Informational', str(len(info_findings)), '🔵' if len(info_findings) > 0 else '✓'],
        ['Healthy', str(len(healthy_findings)), '✓'],
        ['Total', str(len(content.analysis.findings)), ''],
    ]
    
    summary_table = Table(summary_data, colWidths=[2*inch, 1.5*inch, 1*inch])
    summary_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#0066cc')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTNAME', (0, 1), (0, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('ALIGN', (1, 0), (-1, -1), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 10),
        ('TOPPADDING', (0, 0), (-1, -1), 10),
        ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#dee2e6')),
        ('BACKGROUND', (0, 1), (-1, 1), colors.HexColor('#f8d7da') if len(critical_findings) > 0 else colors.HexColor('#d4edda')),
        ('BACKGROUND', (0, 2), (-1, 2), colors.HexColor('#fff3cd') if len(warning_findings) > 0 else colors.HexColor('#d4edda')),
        ('BACKGROUND', (0, 3), (-1, 3), colors.HexColor('#d1ecf1')),
        ('BACKGROUND', (0, 4), (-1, 4), colors.HexColor('#d4edda')),
        ('BACKGROUND', (0, 5), (-1, 5), colors.HexColor('#e9ecef')),
        ('FONTNAME', (0, 5), (-1, 5), 'Helvetica-Bold'),
    ]))
    elements.append(summary_table)
    elements.append(Spacer(1, 0.3*inch))
    
    # List critical findings
    if critical_findings:
        elements.append(Paragraph('<font color="#dc3545"><b>Critical Issues:</b></font>', styles['Heading3']))
        for f in critical_findings:
            elements.append(Paragraph(f'• {f.title}', styles['Normal']))
        elements.append(Spacer(1, 0.1*inch))
    
    # List warnings
    if warning_findings:
        elements.append(Paragraph('<font color="#f57c00"><b>Warnings:</b></font>', styles['Heading3']))
        for f in warning_findings:
            elements.append(Paragraph(f'• {f.title}', styles['Normal']))
        elements.append(Spacer(1, 0.1*inch))
    
    # List informational
    if info_findings:
        elements.append(Paragraph('<font color="#1976d2"><b>Informational:</b></font>', styles['Heading3']))
        for f in info_findings:
            elements.append(Paragraph(f'• {f.title}', styles['Normal']))
        elements.append(Spacer(1, 0.1*inch))
    
    # List healthy
    if healthy_findings:
        elements.append(Paragraph('<font color="#388e3c"><b>Healthy:</b></font>', styles['Heading3']))
        for f in healthy_findings:
            elements.append(Paragraph(f'• {f.title}', styles['Normal']))
        elements.append(Spacer(1, 0.1*inch))
    
    return elements


def create_metric_section(
    metric_name: str,
    metric: 'MetricData',
    metric_list: List,
    findings: List[Finding],
    recommendations: List[Recommendation],
    chart_data: bytes
) -> List:
    """
    Create detailed section for a single metric.
    
    Args:
        metric_name: Name of the metric
        metric: Primary metric data (for display) - can be None
        metric_list: List of all MetricData (per-broker or cluster)
        findings: Related findings
        recommendations: Related recommendations
        chart_data: Chart image bytes
        
    Returns:
        List of reportlab flowables
    """
    elements = []
    styles = getSampleStyleSheet()
    
    # Metric title
    elements.append(Paragraph(f'<b>{metric_name}</b>', styles['Heading2']))
    
    # Metric description
    description = _get_metric_description(metric_name)
    elements.append(Paragraph(description, styles['Normal']))
    elements.append(Spacer(1, 0.1*inch))
    
    # Statistics table - only if we have metrics
    if metric or metric_list:
        # Statistics table - show per-broker if multiple brokers
        if isinstance(metric_list, list) and len(metric_list) > 1:
            # Per-broker statistics
            stats_data = [['Broker', 'Min', 'Max', 'Avg']]
            for m in metric_list:
                broker_label = f"Broker {m.broker_id}" if m.broker_id else "Cluster"
                stats_data.append([
                    broker_label,
                    f"{m.statistics['min']:.2f}",
                    f"{m.statistics['max']:.2f}",
                    f"{m.statistics['avg']:.2f}"
                ])
            
            stats_table = Table(stats_data, colWidths=[1.5*inch, 1.2*inch, 1.2*inch, 1.2*inch])
        elif metric:
            # Single metric statistics
            stats_data = [
                ['Statistic', 'Value'],
                ['Minimum', f"{metric.statistics['min']:.2f}"],
                ['Maximum', f"{metric.statistics['max']:.2f}"],
                ['Average', f"{metric.statistics['avg']:.2f}"],
                ['P95', f"{metric.statistics['p95']:.2f}"],
                ['P99', f"{metric.statistics['p99']:.2f}"],
            ]
            stats_table = Table(stats_data, colWidths=[1.5*inch, 1.5*inch])
        else:
            stats_table = None
        
        if stats_table:
            stats_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 9),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ]))
            elements.append(stats_table)
            elements.append(Spacer(1, 0.2*inch))
    
    # Chart
    from io import BytesIO
    chart_buffer = BytesIO(chart_data)
    img = Image(chart_buffer, width=6*inch, height=2.4*inch)
    elements.append(img)
    elements.append(Spacer(1, 0.2*inch))
    
    # Status and Findings
    if findings:
        elements.append(Paragraph('<b>Status:</b>', styles['Heading3']))
        for finding in findings:
            severity_color = {
                Severity.CRITICAL: '#d32f2f',
                Severity.WARNING: '#f57c00',
                Severity.INFORMATIONAL: '#1976d2',
                Severity.HEALTHY: '#388e3c'
            }.get(finding.severity, '#000000')
            
            elements.append(Paragraph(
                f'<font color="{severity_color}"><b>{finding.severity.value.upper()}: {finding.title}</b></font>',
                styles['Normal']
            ))
            elements.append(Paragraph(finding.description, styles['Normal']))
            elements.append(Spacer(1, 0.1*inch))
    else:
        # No findings - show as healthy
        elements.append(Paragraph('<b>Status:</b>', styles['Heading3']))
        elements.append(Paragraph(
            '<font color="#388e3c"><b>HEALTHY: Metric within normal parameters</b></font>',
            styles['Normal']
        ))
        elements.append(Spacer(1, 0.1*inch))
    
    # Recommendations
    if recommendations:
        elements.append(Paragraph('<b>Recommendations:</b>', styles['Heading3']))
        for rec in recommendations:
            elements.append(Paragraph(f'<b>Action:</b> {rec.action}', styles['Normal']))
            if rec.impact:
                elements.append(Paragraph(f'<b>Impact if not addressed:</b> <i>{rec.impact}</i>', styles['Normal']))
            if rec.rationale:
                elements.append(Paragraph(f'<b>Rationale:</b> <i>{rec.rationale}</i>', styles['Normal']))
            elements.append(Spacer(1, 0.05*inch))
    
    elements.append(Spacer(1, 0.3*inch))
    
    return elements


def generate_output_filename(cluster_arn: str, timestamp: datetime) -> str:
    """
    Generate descriptive filename for PDF.
    
    Args:
        cluster_arn: Cluster ARN
        timestamp: Generation timestamp
        
    Returns:
        Filename string
    """
    cluster_name = cluster_arn.split('/')[-2]
    date_str = timestamp.strftime('%Y%m%d_%H%M%S')
    return f'msk_health_check_{cluster_name}_{date_str}.pdf'


def _get_metric_description(metric_name: str) -> str:
    """Get human-readable description for metric."""
    descriptions = {
        # Standard metrics
        'ActiveControllerCount': 'Number of active controllers in the cluster. Should always be exactly 1. If this drops to 0, the cluster cannot process metadata changes.',
        'GlobalPartitionCount': 'Total number of partitions across all topics in the cluster (excluding replicas). High partition counts can impact broker performance.',
        'GlobalTopicCount': 'Total number of topics in the cluster. Helps understand cluster utilization and organization.',
        'OfflinePartitionsCount': 'Number of partitions that are offline and unavailable. Should always be 0 for a healthy cluster.',
        'CpuUser': 'Percentage of CPU time spent in user space. High values indicate heavy application workload.',
        'CpuSystem': 'Percentage of CPU time spent in kernel space. High values may indicate I/O or network bottlenecks.',
        'CpuUsage': 'Combined CPU usage (User + System). Should remain below 60% for optimal performance.',
        'MemoryUsed': 'Amount of memory currently in use by the broker. Monitor to prevent out-of-memory issues.',
        'MemoryFree': 'Amount of free memory available to the broker.',
        'HeapMemoryAfterGC': 'Percentage of heap memory in use after garbage collection. High values indicate memory pressure.',
        'KafkaDataLogsDiskUsed': 'Percentage of disk space used for Kafka data logs. Should stay below 80% to prevent broker failures.',
        'LeaderCount': 'Number of partition leaders on each broker. Should be balanced across brokers for optimal performance.',
        'PartitionCount': 'Total number of partitions per broker including replicas. High counts can impact performance.',
        'ClientConnectionCount': 'Number of active client connections to the cluster. Monitor against instance connection limits.',
        'ConnectionCount': 'Total number of connections to the broker (including inter-broker connections). Monitor for connection exhaustion.',
        'ConnectionCreationRate': 'Rate of new connections being created per second. High rates indicate missing connection pooling, short timeouts, or client instability. IAM auth is limited to 100 connections/sec.',
        'UnderMinIsrPartitionCount': 'Number of partitions with in-sync replicas below the minimum. Indicates replication lag and potential data loss risk.',
        'BytesInPerSec': 'Rate of data being written to the cluster in bytes per second. Indicates producer throughput.',
        'BytesOutPerSec': 'Rate of data being read from the cluster in bytes per second. Indicates consumer throughput.',
        # Express metrics
        'ClusterActiveConnectionCount': 'Total number of active client connections to the MSK Express cluster. Monitor for connection patterns and scaling needs.',
        'ClusterBytesInPerSec': 'Rate of data being written to the MSK Express cluster in bytes per second. Indicates overall producer throughput.',
        'ClusterBytesOutPerSec': 'Rate of data being read from the MSK Express cluster in bytes per second. Indicates overall consumer throughput.',
        'ClusterMessagesInPerSec': 'Rate of messages being written to the MSK Express cluster per second. Indicates message volume.',
    }
    return descriptions.get(metric_name, f'Metric: {metric_name}')


def create_recommendations_table(recommendations: List[Recommendation], styles) -> List:
    """Create consolidated recommendations table."""
    elements = []
    
    # Sort by priority
    sorted_recs = sorted(recommendations, key=lambda r: r.priority)
    
    # Create table data with Paragraphs for proper wrapping
    table_data = [[
        Paragraph('<b>Severity</b>', styles['Normal']),
        Paragraph('<b>Observation</b>', styles['Normal']),
        Paragraph('<b>Impact</b>', styles['Normal']),
        Paragraph('<b>Solution</b>', styles['Normal'])
    ]]
    
    row_idx = 1
    for rec in sorted_recs:
        severity_map = {
            1: 'CRITICAL',
            2: 'HIGH',
            3: 'MEDIUM',
            4: 'LOW',
            5: 'INFO'
        }
        severity = severity_map.get(rec.priority, 'MEDIUM')
        
        table_data.append([
            Paragraph(severity, styles['Normal']),
            Paragraph(rec.finding.title, styles['Normal']),
            Paragraph(rec.impact, styles['Normal']),
            Paragraph(rec.action, styles['Normal'])
        ])
        row_idx += 1
    
    rec_table = Table(table_data, colWidths=[0.8*inch, 1.8*inch, 2*inch, 2*inch])
    
    # Base style with consistent colors
    table_style = [
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#0066cc')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 8),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ('TOPPADDING', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#dee2e6')),
    ]
    
    # Add row colors based on severity
    for idx, rec in enumerate(sorted_recs, start=1):
        if rec.priority == 1:  # CRITICAL
            table_style.append(('BACKGROUND', (0, idx), (-1, idx), colors.HexColor('#f8d7da')))
        elif rec.priority == 2:  # HIGH
            table_style.append(('BACKGROUND', (0, idx), (-1, idx), colors.HexColor('#fff3cd')))
        elif rec.priority == 3:  # MEDIUM
            table_style.append(('BACKGROUND', (0, idx), (-1, idx), colors.HexColor('#d1ecf1')))
    
    rec_table.setStyle(TableStyle(table_style))
    
    elements.append(rec_table)
    return elements


def create_important_links_section(styles) -> List:
    """Create References & Resources section."""
    elements = []
    
    elements.append(Paragraph('<b>References & Resources</b>', styles['Heading1']))
    elements.append(Spacer(1, 0.3*inch))
    
    links = [
        ('Best Practices for Standard brokers', 'https://docs.aws.amazon.com/msk/latest/developerguide/bestpractices.html'),
        ('Best practices for Express brokers', 'https://docs.aws.amazon.com/msk/latest/developerguide/bestpractices-express.html'),
        ('Best practices for Apache Kafka clients', 'https://docs.aws.amazon.com/msk/latest/developerguide/bestpractices-kafka-client.html'),
        ('Amazon MSK Serverless', 'https://docs.aws.amazon.com/msk/latest/developerguide/serverless.html'),
        ('Amazon MSK Tiered Storage', 'https://docs.aws.amazon.com/msk/latest/developerguide/msk-tiered-storage.html'),
        ('Safely remove Kafka brokers', 'https://docs.aws.amazon.com/msk/latest/developerguide/msk-remove-broker.html'),
        ('Monitoring with CloudWatch', 'https://docs.aws.amazon.com/msk/latest/developerguide/monitoring.html'),
        ('Security in Amazon MSK', 'https://docs.aws.amazon.com/msk/latest/developerguide/security.html'),
    ]
    
    for title, url in links:
        elements.append(Paragraph(f'• <a href="{url}" color="blue">{title}</a>', styles['Normal']))
    
    return elements


def create_disclaimer_section(styles) -> List:
    """Create disclaimer section."""
    elements = []
    
    elements.append(Paragraph('<b>Important Notice</b>', styles['Heading1']))
    elements.append(Spacer(1, 0.2*inch))
    
    disclaimer_text = """
    This health check report provides automated analysis based on general AWS best practices 
    and standard metric thresholds. The findings and recommendations are synthetic in nature 
    and should be interpreted within the context of your specific environment.
    """
    elements.append(Paragraph(disclaimer_text, styles['Normal']))
    elements.append(Spacer(1, 0.2*inch))
    
    elements.append(Paragraph('<b>Key Considerations:</b>', styles['Heading2']))
    elements.append(Spacer(1, 0.1*inch))
    
    considerations = [
        ('<b>Context Matters</b>: Metric patterns that appear concerning in isolation may be normal '
         'for your workload characteristics, application behavior, and usage patterns.'),
        ('<b>Holistic Analysis Required</b>: Effective troubleshooting and optimization require '
         'correlating multiple metrics, understanding application architecture, and considering business requirements.')
    ]
    
    for consideration in considerations:
        elements.append(Paragraph(f'• {consideration}', styles['Normal']))
        elements.append(Spacer(1, 0.1*inch))
    
    return elements


def create_config_finding_section(finding: Finding, recommendations: List[Recommendation], styles) -> List:
    """Create section for configuration finding (no metric/chart)."""
    elements = []
    
    severity_color = {
        Severity.CRITICAL: '#d32f2f',
        Severity.WARNING: '#f57c00',
        Severity.INFORMATIONAL: '#1976d2',
        Severity.HEALTHY: '#388e3c'
    }.get(finding.severity, '#000000')
    
    elements.append(Paragraph(f'<b>{finding.metric_name}</b>', styles['Heading2']))
    elements.append(Paragraph(
        f'<font color="{severity_color}"><b>{finding.severity.value.upper()}: {finding.title}</b></font>',
        styles['Normal']
    ))
    elements.append(Paragraph(finding.description, styles['Normal']))
    elements.append(Spacer(1, 0.1*inch))
    
    if recommendations:
        elements.append(Paragraph('<b>Recommendations:</b>', styles['Heading3']))
        for rec in recommendations:
            elements.append(Paragraph(f'<b>Action:</b> {rec.action}', styles['Normal']))
            if rec.impact:
                elements.append(Paragraph(f'<b>Impact if not addressed:</b> <i>{rec.impact}</i>', styles['Normal']))
            if rec.rationale:
                elements.append(Paragraph(f'<b>Rationale:</b> <i>{rec.rationale}</i>', styles['Normal']))
            elements.append(Spacer(1, 0.05*inch))
    
    return elements
