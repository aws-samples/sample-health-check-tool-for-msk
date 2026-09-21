"""Machine-readable manifest of a health check run, plus comparison between runs.

The manifest is written next to the PDF. It records the window actually observed, the
metrics collected or missing, every check with its evidence, the scores and the
recommendations, so a run can be audited or diffed against a later one.
"""

import hashlib
import json
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

from . import __version__
from .analyzer import AnalysisResult, Severity
from .cluster_info import ClusterInfo
from .recommendations import Recommendation
from . import reference as ref

MANIFEST_SCHEMA = 1


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat(timespec='seconds') if value.tzinfo else value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value):
        return asdict(value)
    if hasattr(value, 'tolist'):
        return value.tolist()
    if isinstance(value, float) and value != value:  # NaN
        return None
    return str(value)


def redacted_name(name: str) -> str:
    digest = hashlib.sha256(name.encode('utf-8')).hexdigest()[:8]
    return f'cluster-{digest}'


def redact_arn(arn: str) -> str:
    parts = arn.split(':')
    if len(parts) > 5:
        parts[4] = '************'
        resource = parts[5].split('/')
        if len(resource) >= 3:
            resource[1] = redacted_name(resource[1])
            resource[2] = '****'
        parts[5] = '/'.join(resource)
    return ':'.join(parts)


def cluster_summary(ci: ClusterInfo, redact: bool = False) -> Dict[str, Any]:
    return {
        'name': redacted_name(ci.name) if redact else ci.name,
        'arn': redact_arn(ci.arn) if redact else ci.arn,
        'account_id': '************' if redact else ci.account_id,
        'region': ci.region,
        'cluster_type': ci.cluster_type,
        'broker_size': ci.instance_type,
        'broker_family': ci.instance_family,
        'broker_count': ci.broker_count,
        'availability_zones': ci.availability_zones,
        'kafka_version': ci.kafka_version,
        'kafka_version_status': ci.kafka_version_status,
        'metadata_mode': ci.metadata_mode,
        'authentication': ci.authentication_methods,
        'encryption_client_broker': ci.encryption_in_transit_type,
        'encryption_in_cluster': ci.in_cluster_encryption,
        'encryption_at_rest_kms_key': ('****' if redact else ci.kms_key_arn),
        'public_access': ci.public_access,
        'enhanced_monitoring': ci.enhanced_monitoring_level,
        'logging_destinations': ci.logging_destinations,
        'ebs_volume_size_gib': ci.ebs_volume_size,
        'storage_mode': ci.storage_mode,
        'storage_autoscaling': ci.storage_autoscaling,
        'storage_autoscaling_detail': ci.storage_autoscaling_detail,
        'provisioned_throughput_enabled': ci.provisioned_throughput_enabled,
        'intelligent_rebalancing_status': ci.rebalancing_status,
        'state': ci.cluster_state,
        'created_at': ci.creation_time,
    }


def build_manifest(analysis: AnalysisResult, recommendations: List[Recommendation], generated_at: datetime,
                   pdf_filename: str, redact: bool = False, comparison: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    m = analysis.metrics
    coverage = {
        name: {
            'series': len(items),
            'coverage_pct': round(min(i.coverage_pct for i in items), 1) if items else 0.0,
            'effective_statistic': items[0].effective_stat if items else None,
            'period_seconds': items[0].period_seconds if items else m.period_seconds,
        }
        for name, items in sorted(m.metrics.items())
    }
    findings = []
    for f in analysis.findings:
        findings.append({
            'check_id': f.check_id, 'metric': f.metric_name, 'severity': f.severity.value, 'category': f.category.value,
            'title': f.title, 'description': f.description, 'observed': f.observed, 'threshold': f.threshold,
            'current_value': f.current_value, 'threshold_value': f.threshold_value, 'confidence': f.confidence,
            'source': f.source_url, 'affected_brokers': f.affected_brokers, 'section': f.section,
            'reason_not_assessed': f.reason or None, 'evidence': f.evidence,
        })
    recs = [{
        'check_id': r.finding.check_id, 'priority': r.priority, 'priority_label': r.priority_label,
        'severity': r.finding.severity.value, 'action': r.action, 'confirm': r.confirm, 'verify': r.verify,
        'impact': r.impact, 'rationale': r.rationale, 'documentation': r.documentation_links,
        'affected_brokers': r.affected_brokers,
    } for r in recommendations]
    manifest = {
        'schema': MANIFEST_SCHEMA,
        'tool': {'name': 'msk-health-check', 'version': __version__, 'rules_version': ref.RULES_VERSION},
        'generated_at': generated_at,
        'pdf': pdf_filename,
        'redacted': redact,
        'workload_profile': analysis.workload,
        'cluster': cluster_summary(analysis.cluster_info, redact),
        'window': {
            'start': m.start_time, 'end': m.end_time,
            'days': round((m.end_time - m.start_time).total_seconds() / 86400, 2),
            'period_seconds': m.period_seconds, 'timezone': 'UTC',
        },
        'metrics': {
            'attempted': m.attempted_metrics, 'collected': coverage, 'not_published': m.not_published,
            'collection_errors': m.collection_errors, 'partial': m.partial_metrics,
            'discovery_available': m.discovery_available,
        },
        'summary': {
            'status': analysis.overall_status, 'score': analysis.overall_health_score,
            'category_scores': analysis.category_scores, 'checks_total': analysis.checks_total,
            'checks_assessed': analysis.checks_assessed,
            'counts': {s.value: sum(1 for f in analysis.findings if f.severity == s) for s in Severity},
        },
        'kafka_version_reference': analysis.version_reference,
        'findings': findings,
        'recommendations': recs,
    }
    if comparison:
        manifest['comparison'] = comparison
    return manifest


def write_manifest(manifest: Dict[str, Any], path: str) -> None:
    with open(path, 'w', encoding='utf-8') as fh:
        json.dump(manifest, fh, indent=2, default=_json_default)


def load_manifest(path: str) -> Dict[str, Any]:
    with open(path, 'r', encoding='utf-8') as fh:
        data = json.load(fh)
    if data.get('schema') != MANIFEST_SCHEMA:
        raise ValueError(f'Unsupported manifest schema {data.get("schema")} (expected {MANIFEST_SCHEMA})')
    return data


def compare_runs(previous: Dict[str, Any], analysis: AnalysisResult) -> Dict[str, Any]:
    """Diff a previous manifest against the current analysis."""
    prev_findings = {f['check_id']: f for f in previous.get('findings', []) if f.get('check_id')}
    curr = {f.check_id: f for f in analysis.findings if f.check_id}
    actionable = {Severity.CRITICAL.value, Severity.HIGH.value, Severity.WARNING.value}
    new, resolved, changed, unchanged = [], [], [], []
    for cid, f in curr.items():
        p = prev_findings.get(cid)
        if p is None:
            if f.severity.value in actionable:
                new.append({'check_id': cid, 'severity': f.severity.value, 'title': f.title})
            continue
        if p['severity'] != f.severity.value:
            changed.append({'check_id': cid, 'from': p['severity'], 'to': f.severity.value, 'title': f.title})
        elif f.severity.value in actionable:
            unchanged.append({'check_id': cid, 'severity': f.severity.value, 'title': f.title})
    for cid, p in prev_findings.items():
        if cid not in curr and p['severity'] in actionable:
            resolved.append({'check_id': cid, 'severity': p['severity'], 'title': p['title']})
    for c in changed:
        if c['from'] in actionable and c['to'] not in actionable:
            resolved.append({'check_id': c['check_id'], 'severity': c['from'], 'title': c['title']})
    prev_summary = previous.get('summary', {})
    return {
        'previous_generated_at': previous.get('generated_at'),
        'previous_window': previous.get('window'),
        'previous_status': prev_summary.get('status'),
        'previous_score': prev_summary.get('score'),
        'score_delta': round(analysis.overall_health_score - float(prev_summary.get('score') or 0), 1),
        'new': new, 'resolved': resolved, 'changed': changed, 'persisting': unchanged,
        'previous_cluster': previous.get('cluster', {}).get('name'),
    }
