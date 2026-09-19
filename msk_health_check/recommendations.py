"""Recommendation generation module.

One recommendation per finding that needs action. Each recommendation separates what was
observed from the causes compatible with it, says how to confirm the cause and how to verify
the improvement, and keeps the documentation links that justify it.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from .analyzer import AnalysisResult, Finding, Severity
from . import reference as ref


@dataclass
class Recommendation:
    """Actionable recommendation with context."""
    finding: Finding
    action: str
    rationale: str
    impact: str                       # impact of not acting
    documentation_links: List[str]
    priority: int                     # 1 (highest) to 5 (lowest)
    estimated_impact: str             # expected improvement
    confirm: str = ''                 # how to confirm the cause before acting
    verify: str = ''                  # how to verify the improvement afterwards
    affected_brokers: List[str] = field(default_factory=list)

    @property
    def severity_label(self) -> str:
        return self.finding.severity.value.upper()

    @property
    def priority_label(self) -> str:
        return {1: 'P1 - immediate', 2: 'P2 - this week', 3: 'P3 - this month', 4: 'P4 - next maintenance window',
                5: 'P5 - optional'}.get(self.priority, 'P3 - this month')


def _links(*keys: str) -> List[str]:
    return [ref.DOCS[k] for k in keys if k in ref.DOCS]


# Keyed by check_id. Text describes what the operation requires; the reader judges the effort.
TEMPLATES: Dict[str, Dict[str, object]] = {
    'active_controller': dict(
        action='Correlate the controller gaps with broker restarts (MSK console events, broker logs) and check that clients tolerate leadership moves.',
        rationale='Controller gaps normally coincide with broker replacements or patching; gaps without a maintenance event point to controller instability.',
        impact='While no controller is active, leader elections and topic operations stall.',
        confirm='Compare the timestamps in the evidence with cluster operations and broker log entries about controller election.',
        verify='ActiveControllerCount (Minimum, 1-minute period) stays at 1 across the next window.',
        links=_links('troubleshooting', 'best_practices')),
    'offline_partitions': dict(
        action='Identify the affected topics with kafka-topics.sh --describe --unavailable-partitions, restore replicas, and raise the replication factor of topics that use RF 1.',
        rationale='A partition goes offline when every replica is unavailable; replication factor 1 makes any broker restart an outage for that partition.',
        impact='Producers and consumers of offline partitions fail until a leader is available.',
        confirm='List topics with replication factor below 3 and check broker events at the time of the last occurrence.',
        verify='OfflinePartitionsCount (Maximum) stays at 0 through a rolling operation such as a patching cycle.',
        links=_links('best_practices', 'troubleshooting')),
    'under_min_isr': dict(
        action='Check the health of the follower brokers (CPU, disk, network) and confirm that min.insync.replicas is at most RF - 1.',
        rationale='Partitions drop below min ISR when followers cannot keep up or a broker is down; a minISR equal to RF turns every broker restart into unavailability for acks=all producers.',
        impact='Producers with acks=all receive NotEnoughReplicas errors; durability is reduced.',
        confirm='Correlate UnderMinIsrPartitionCount with UnderReplicatedPartitions and broker restarts on the affected brokers.',
        verify='UnderMinIsrPartitionCount returns to 0 and stays there outside maintenance windows.',
        links=_links('best_practices', 'troubleshooting')),
    'under_replicated': dict(
        action='Compare replication lag with the load on the follower brokers; if under-replication persists outside maintenance, add capacity or rebalance leaders.',
        rationale='Followers fall behind when the broker hosting them is saturated (CPU, disk throughput, network).',
        impact='Reduced redundancy; a second failure can take partitions offline or cause data loss.',
        confirm='Check CPU, KafkaDataLogsDiskUsed and BytesInPerSec on the affected brokers during the episodes.',
        verify='UnderReplicatedPartitions is 0 for at least 95% of the buckets in the window.',
        links=_links('best_practices')),
    'disk_usage': dict(
        action='Increase broker storage (or enable storage auto scaling), reduce retention (log.retention.hours / retention.bytes) or delete unused topics.',
        rationale='AWS recommends acting when KafkaDataLogsDiskUsed reaches 85%; a full volume stops the broker.',
        impact='Broker failure and offline partitions when the data volume fills.',
        confirm='Review topic retention settings and the growth projection in the evidence.',
        verify='Peak KafkaDataLogsDiskUsed stays below 75% and the growth projection exceeds 90 days.',
        links=_links('best_practices', 'storage_manual', 'storage_autoscaling')),
    'availability_zones': dict(
        action='Create a replacement cluster in 3 AZs (AZ count cannot be changed on an existing cluster) and migrate with MSK Replicator or MirrorMaker 2.',
        rationale='AWS recommends 3 AZs so that a single AZ event leaves a majority of replicas in sync.',
        impact='An AZ event can leave partitions with a single in-sync replica or offline.',
        confirm='Confirm the workload criticality and RTO/RPO with the application owners.',
        verify='The new cluster reports 3 client subnets in distinct AZs.',
        links=_links('best_practices')),
    'storage_autoscaling': dict(
        action='Register the cluster with Application Auto Scaling for broker storage and attach a target-tracking policy (for example 60% disk utilisation).',
        rationale='Automatic storage expansion removes the manual step between a disk alarm and a broker failure.',
        impact='Manual intervention required when disk grows; a missed alarm leads to a stopped broker.',
        confirm='Check that the disk growth projection justifies automation for this cluster.',
        verify='describe-scalable-targets returns a target for the cluster and a policy with the chosen target value.',
        links=_links('storage_autoscaling')),
    'kafka_version': dict(
        action='Plan an in-place upgrade to the reference version, testing client compatibility first.',
        rationale='Newer versions carry fixes and features; deprecated versions stop receiving them.',
        impact='Missing fixes and, for deprecated versions, end of support.',
        confirm='Review the release notes between the current and the reference version for client-facing changes.',
        verify='DescribeClusterV2 reports the new KafkaVersion and ListKafkaVersions marks it ACTIVE.',
        links=_links('kafka_versions', 'version_upgrade')),
    'intelligent_rebalancing': dict(
        action='Enable intelligent rebalancing (UpdateRebalancing) so MSK redistributes partitions automatically.',
        rationale='Express clusters can rebalance partitions without manual reassignment.',
        impact='Partition skew persists until corrected manually.',
        confirm='Check the partition balance finding for current skew.',
        verify='Rebalancing status is ACTIVE and PartitionCount per broker converges.',
        links=_links('best_practices_express')),
    'cpu_total': dict(
        action='Move to the next broker size (AWS preferred option) or add brokers and spread partitions of the busiest topics onto them.',
        rationale='AWS recommends keeping CPU User + System under 60% so that maintenance and failover do not add latency.',
        impact='Produce and consume latency grows with CPU; a broker restart during peak overloads the remaining brokers.',
        confirm='Check whether the busiest brokers also lead more partitions (leader balance) or receive more traffic (traffic balance): if so, rebalancing may be enough.',
        verify='P95 of CPU (User + System) below 60% on every broker over the next window.',
        links=_links('best_practices', 'update_broker_type', 'update_broker_count')),
    'heap_after_gc': dict(
        action='Move to a broker size with more memory; if transactions are used, lower transactional.id.expiration.ms; reduce partitions per broker.',
        rationale='Heap that stays above 60% after garbage collection indicates the JVM is close to its limit.',
        impact='Long GC pauses, request timeouts and broker restarts.',
        confirm='Correlate heap usage with partition count per broker and with the use of transactions.',
        verify='P95 of HeapMemoryAfterGC below 60% on every broker.',
        links=_links('best_practices')),
    'throughput_in': dict(
        action='Add brokers and spread the partitions of the busiest topics, or move to a larger broker size.',
        rationale='Throughput above the sustained limit degrades latency; at the quota MSK throttles clients.',
        impact='Higher produce latency, then throttled producers.',
        confirm='Check the traffic balance finding: if one broker carries most of the traffic, rebalancing partitions may be enough.',
        verify='P95 BytesInPerSec per broker below the sustained limit.',
        links=_links('best_practices_express', 'quotas', 'update_broker_count')),
    'throughput_out': dict(
        action='Add brokers and spread partitions, move to a larger size, or reduce consumer fan-out (fewer consumer groups reading the same data).',
        rationale='Egress above the sustained limit degrades latency; at the quota MSK throttles clients.',
        impact='Higher consume latency, then throttled consumers.',
        confirm='Identify the consumer groups responsible for most egress and whether they are duplicated.',
        verify='P95 BytesOutPerSec per broker below the sustained limit.',
        links=_links('best_practices_express', 'quotas')),
    'partition_capacity': dict(
        action='Add brokers (then reassign partitions) or move to a larger broker size; remove unused topics.',
        rationale='Partition counts above the maximum block configuration updates and can drop metrics; above the recommended value performance depends on traffic per partition.',
        impact='Blocked cluster operations and degraded performance.',
        confirm='List topics by partition count and identify unused or over-partitioned topics.',
        verify='PartitionCount per broker below the recommended value.',
        links=_links('best_practices', 'update_broker_count', 'kafka_reassign')),
    'partition_balance': dict(
        action='Reassign partitions with Cruise Control or kafka-reassign-partitions.sh (at most 10 partitions per call on Standard brokers, 20 on Express).',
        rationale='Even partition placement lets every broker share the load and reach its limits at the same time.',
        impact='The most loaded broker reaches CPU, disk or network limits first.',
        confirm='Check which topics have replicas concentrated on the hottest broker.',
        verify='PartitionCount of the hottest broker within 10% of the mean.',
        links=_links('cruise_control', 'kafka_reassign')),
    'leader_balance': dict(
        action='Run a preferred leader election (kafka-leader-election.sh --election-type PREFERRED) or reassign partitions so that leaders spread evenly.',
        rationale='Leaders do the produce and consume work; a broker with more leaders carries more load.',
        impact='Uneven CPU and network use across brokers.',
        confirm='Compare LeaderCount with CPU per broker.',
        verify='LeaderCount of the hottest broker within 10% of the mean.',
        links=_links('cruise_control')),
    'cpu_balance': dict(
        action='Rebalance leaders and partitions toward the less loaded brokers; check for keyed topics whose hot keys land on one partition.',
        rationale='Uneven CPU indicates uneven leadership or traffic; AWS recommends Cruise Control for continuous balancing.',
        impact='One broker hits the 60% CPU limit while others idle.',
        confirm='Compare CPU with LeaderCount and BytesInPerSec per broker.',
        verify='CPU of the hottest broker within 20% of the mean.',
        links=_links('best_practices', 'cruise_control')),
    'bytes_in_balance': dict(
        action='Reassign partitions of the topics that concentrate traffic on the hottest broker; review partitioning keys.',
        rationale='Traffic skew usually follows partition placement or hot keys.',
        impact='The hottest broker reaches network limits first.',
        confirm='Use per-topic metrics (PER_TOPIC_PER_BROKER) or client-side metrics to find the topics behind the skew.',
        verify='BytesInPerSec of the hottest broker within 20% of the mean.',
        links=_links('cruise_control')),
    'bytes_out_balance': dict(
        action='Reassign partitions of the topics that concentrate consumer reads; consider rack-aware fetching for cross-AZ consumers.',
        rationale='Egress skew follows leader placement of the most consumed partitions.',
        impact='The hottest broker reaches egress limits first.',
        confirm='Identify the consumer groups and topics reading from the hottest broker.',
        verify='BytesOutPerSec of the hottest broker within 20% of the mean.',
        links=_links('cruise_control')),
    'messages_balance': dict(
        action='Reassign partitions of the topics concentrating message intake; review partitioning keys.',
        rationale='Message skew follows partition placement or hot keys.',
        impact='Uneven CPU across brokers.',
        confirm='Compare MessagesInPerSec with LeaderCount per broker.',
        verify='MessagesInPerSec of the hottest broker within 20% of the mean.',
        links=_links('cruise_control')),
    'connection_balance': dict(
        action='Check that clients bootstrap with brokers from every AZ and spread connections; rebalance leaders if connections follow leadership.',
        rationale='Connections concentrate on a broker when clients pin to it or when it leads most partitions.',
        impact='Connection and CPU pressure on one broker.',
        confirm='Compare ConnectionCount with LeaderCount and review client bootstrap strings.',
        verify='ConnectionCount of the hottest broker within 25% of the mean.',
        links=_links('client_best_practices')),
    'client_connections': dict(
        action='Reuse producer and consumer instances (connection pooling) and review clients that open a connection per request; raise listener.name.client_iam.max.connections only after checking broker memory.',
        rationale='IAM listeners refuse connections beyond the per-broker quota.',
        impact='New clients cannot connect once the quota is reached.',
        confirm='Identify the applications with the most connections (client logs or per-Client-Authentication breakdown in the evidence).',
        verify='Peak ClientConnectionCount per broker below 80% of the quota.',
        links=_links('quotas', 'iam_access')),
    'connection_creation_rate': dict(
        action='Use long-lived producers and consumers, set reconnect.backoff.ms and reconnect.backoff.max.ms, and fix clients that reconnect on every request or restart in loops.',
        rationale='IAM listeners accept a limited number of new connections per second per broker; each new connection also costs CPU for TLS and authentication.',
        impact='Refused connections (IAMTooManyConnections) and CPU spent on handshakes.',
        confirm='Find the clients behind the reconnections in broker logs (authentication entries) or client logs.',
        verify='P95 ConnectionCreationRate below 70% of the quota and IAMTooManyConnections at 0.',
        links=_links('quotas', 'client_best_practices')),
    'enhanced_monitoring': dict(
        action='Consider PER_BROKER enhanced monitoring for the connection-rate and throttling checks (paid CloudWatch metrics).',
        rationale='PER_BROKER adds ConnectionCreationRate, IAMTooManyConnections and throttle metrics.',
        impact='Connection-rate throttling cannot be detected from metrics.',
        confirm='Check the list of checks limited by the current level in the evidence.',
        verify='ConnectionCreationRate appears in CloudWatch for every broker.',
        links=_links('monitoring')),
    'authentication': dict(
        action='Migrate clients to IAM, SASL/SCRAM or mTLS and disable the unauthenticated listener.',
        rationale='Unauthenticated listeners let any client with network access read and write data.',
        impact='Unauthorised access to data and topics.',
        confirm='Use ClientConnectionCount with the Client Authentication dimension to see which clients still use the unauthenticated listener.',
        verify='DescribeClusterV2 shows Unauthenticated.Enabled = false.',
        links=_links('authentication')),
    'encryption_in_transit': dict(
        action='Move remaining clients to TLS and set client-broker encryption to TLS only.',
        rationale='Plaintext listeners expose data and credentials on the network.',
        impact='Data and credentials readable on the network.',
        confirm='Identify clients still connecting to the plaintext port (9092).',
        verify='EncryptionInTransit.ClientBroker = TLS.',
        links=_links('encryption')),
    'encryption_in_cluster': dict(
        action='Enable in-cluster encryption (requires a new cluster; the setting cannot be changed in place).',
        rationale='Replication traffic between brokers is otherwise unencrypted.',
        impact='Replication traffic readable inside the VPC.',
        confirm='Confirm compliance requirements for intra-VPC encryption.',
        verify='EncryptionInTransit.InCluster = true on the replacement cluster.',
        links=_links('encryption')),
    'public_access': dict(
        action='Review security group rules and IAM policies or ACLs for the public listeners; disable public access if it is no longer needed.',
        rationale='Public listeners widen the attack surface even with authentication.',
        impact='Exposure to internet-originated connection attempts.',
        confirm='List the principals that connect from outside the VPC.',
        verify='Only intended principals appear in authentication logs.',
        links=_links('public_access')),
    'broker_logging': dict(
        action='Enable broker log delivery to CloudWatch Logs, S3 or Firehose.',
        rationale='Broker logs are the primary source for investigating authentication failures, leader elections and disk errors.',
        impact='Incidents cannot be investigated after the fact.',
        confirm='Decide the destination and retention according to compliance needs.',
        verify='LoggingInfo shows at least one destination enabled.',
        links=_links('logging')),
    'graviton': dict(
        action='Compare the price of the Graviton equivalent for this Region and validate with a load test before updating the broker size.',
        rationale='AWS positions M7g brokers as better price-performance than M5.',
        impact='Potentially higher compute cost than necessary.',
        confirm='Check the MSK pricing page for both sizes in this Region.',
        verify='Cluster reports the Graviton broker size with equal or better latency in the load test.',
        links=_links('graviton', 'pricing', 'update_broker_type')),
    'right_sizing': dict(
        action='Evaluate a smaller broker size (or fewer brokers, keeping at least 3 and replication factor 3) if the observed window represents the steady state.',
        rationale='Every utilisation dimension is far below the broker size limits.',
        impact='Compute cost above what the workload requires.',
        confirm='Compare the window with seasonal peaks and planned growth before changing size.',
        verify='After the change, CPU P95 stays below 60% and throughput below the sustained limit.',
        links=_links('broker_sizes', 'update_broker_type')),
}


def _priority(finding: Finding) -> int:
    base = {Severity.CRITICAL: 1, Severity.WARNING: 2, Severity.INFORMATIONAL: 4}.get(finding.severity, 5)
    # Balance findings are a means to an end: lower priority unless a capacity check is also failing
    if finding.check_id.endswith('_balance') and base == 2:
        base = 3
    if finding.check_id in ('graviton', 'right_sizing', 'enhanced_monitoring', 'public_access'):
        base = 5 if finding.severity == Severity.INFORMATIONAL else base
    if finding.confidence == 'low' and base < 3:
        base += 1
    return base


def create_recommendation_for_finding(finding: Finding) -> Optional[Recommendation]:
    """Create the recommendation for one finding, or None when no action is needed."""
    if finding.severity in (Severity.HEALTHY, Severity.NOT_ASSESSED):
        return None
    template = TEMPLATES.get(finding.check_id) or TEMPLATES.get(finding.metric_name)
    if not template:
        template = dict(action=f'Review "{finding.title}" against the linked documentation.', rationale=finding.description,
                        impact='May affect cluster reliability or performance.', confirm='', verify='',
                        links=[finding.source_url] if finding.source_url else _links('best_practices'))
    return Recommendation(
        finding=finding,
        action=str(template['action']),
        rationale=str(template['rationale']),
        impact=str(template['impact']),
        documentation_links=list(template.get('links') or []),
        priority=_priority(finding),
        estimated_impact=str(template.get('verify', '')),
        confirm=str(template.get('confirm', '')),
        verify=str(template.get('verify', '')),
        affected_brokers=list(finding.affected_brokers),
    )


def generate_recommendations(analysis: AnalysisResult) -> List[Recommendation]:
    """One recommendation per actionable finding, ordered by priority then severity."""
    recommendations: List[Recommendation] = []
    for finding in analysis.findings:
        rec = create_recommendation_for_finding(finding)
        if rec:
            recommendations.append(rec)
    severity_rank = {Severity.CRITICAL: 0, Severity.WARNING: 1, Severity.INFORMATIONAL: 2}
    recommendations.sort(key=lambda r: (r.priority, severity_rank.get(r.finding.severity, 3), r.finding.title))
    return recommendations
