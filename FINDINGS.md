# Findings catalog

Every check the report can produce, with the statistic it reads, the thresholds it applies, the
severity it assigns and where the threshold comes from. The same catalog drives the analysis
(`msk_health_check/analyzer.py`), the reference values (`msk_health_check/reference.py`), the
recommendations (`msk_health_check/recommendations.py`) and the PDF.

## Check states

| State | Meaning | Effect on score |
|-------|---------|-----------------|
| CRITICAL | Operational impact now or imminent: data unavailability, quota reached, or a published limit exceeded in a sustained way | category score x 0.60 |
| HIGH | Serious posture or resilience gap that does not by itself make the cluster unhealthy (unauthenticated listener, plaintext client traffic, 2 AZs on a production Standard cluster) | category score x 0.70 |
| WARNING | Approaching a limit, resilience gap, or a limit exceeded episodically | category score x 0.85 |
| INFORMATIONAL | Context or an optional improvement; not a defect | none |
| HEALTHY | Check passed | none |
| NOT ASSESSED | The check could not run (metric not published, permission missing, broker size not in the catalog); the reason is printed | none, counted in coverage |

The overall status is bounded by the worst finding: any CRITICAL finding gives "Critical", any HIGH
or WARNING gives "Needs Attention", otherwise "Healthy". A HIGH finding never labels the cluster as
Critical: an unauthenticated listener or a plaintext port may be a deliberate choice for an isolated
network, and the label describes operational health. The score (0-100) is the weighted average of
the category scores: Reliability 35%, Performance 30%, Security 20%, Cost 15%.

Confidence: `high` for published quotas and best-practice values, `medium` for heuristics applied to
published metrics (imbalance, episodic peaks), `low` for tool guidelines without a published quota
(Standard broker network throughput, right-sizing).

## Statistics used

| Metric family | Statistic read | Why |
|---------------|----------------|-----|
| CPU, heap, disk, bytes/s, messages/s | hourly `Average`; P95 of hourly values; `Maximum` sample kept as "peak" | utilisation over time; the 1-minute peak is shown but the rule uses P95 |
| CpuUser + CpuSystem | series aligned by timestamp, then summed | percentiles of the sum, not the sum of percentiles |
| OfflinePartitionsCount, UnderMinIsrPartitionCount, UnderReplicatedPartitions | hourly `Maximum` | a short event inside an hour must not be averaged away |
| ActiveControllerCount | hourly `Minimum` | detects a controller gap inside an hour |
| ClientConnectionCount, ConnectionCount, ConnectionCreationRate | hourly `Sum` / 60 | MSK publishes one sample per network processor per minute; the broker total is the Sum per minute (see the quotas page) |

Connection metrics published with the `Client Authentication` dimension are queried per listener and
summed; the IAM listener alone is compared with the IAM quota.

## Reliability and availability (35%)

| Check id | Source / metric | Rule | Severity | Confidence |
|----------|-----------------|------|----------|------------|
| active_controller | ActiveControllerCount (Minimum) | minimum < 1 in any hour | CRITICAL | high |
| | | maximum >= 2 | WARNING | high |
| offline_partitions | OfflinePartitionsCount (Maximum) | any value > 0 | CRITICAL | high |
| under_min_isr | UnderMinIsrPartitionCount per broker (Maximum) | > 0 in the latest hour | CRITICAL | high |
| | | > 0 earlier in the window | WARNING | medium |
| under_replicated | UnderReplicatedPartitions per broker (Maximum) | > 0 now, or in >= 5% of hours | WARNING | high |
| | | brief episodes | INFORMATIONAL | medium |
| disk_usage (Standard) | KafkaDataLogsDiskUsed per broker | peak >= 85% (AWS action threshold) | CRITICAL | high |
| | | peak >= 75% (tool headroom) | WARNING | high |
| | | growth projection: days until 85% from a linear fit of the window | shown in the text | |
| availability_zones (Standard) | client subnets (MSK requires one AZ per subnet; clusters span 2 or 3 AZs) | 2 AZs on a production cluster (AWS recommends 3) | HIGH | high |
| | | 2 AZs, non-production | INFORMATIONAL | high |
| | Express brokers always span 3 AZs | | HEALTHY | |
| storage_autoscaling (Standard) | Application Auto Scaling scalable target + policy for `kafka` | no target or no policy | WARNING (production) / INFORMATIONAL | high |
| | | permission missing | NOT ASSESSED | |
| kafka_version | ListKafkaVersions status; documented recommended version when reachable | status DEPRECATED | WARNING | high |
| | | two or more minor versions behind the reference | WARNING | high (docs) / medium (catalog) |
| | | one minor version behind | INFORMATIONAL | |
| intelligent_rebalancing (Express) | DescribeClusterV2 `Provisioned.Rebalancing.Status` | not ACTIVE | INFORMATIONAL | high |
| | | field absent (old SDK) | NOT ASSESSED | |

## Performance and capacity (30%)

| Check id | Source / metric | Rule | Severity | Confidence |
|----------|-----------------|------|----------|------------|
| cpu_total | CpuUser + CpuSystem per broker (aligned) | P95 >= 60% | CRITICAL | high |
| | | some hours >= 60%, P95 below | WARNING | medium |
| heap_after_gc | HeapMemoryAfterGC per broker | P95 >= 60% | CRITICAL | high |
| | | some hours >= 60% | WARNING | medium |
| throughput_in / throughput_out | BytesInPerSec / BytesOutPerSec per broker | Express: 1-minute peak >= 90% of the throttle quota | CRITICAL | high |
| | | P95 >= sustained limit (Express: published; Standard: tool guideline) | WARNING | high / low |
| | | broker size not in catalog | NOT ASSESSED | |
| partition_capacity | PartitionCount per broker (latest hour) | > maximum for the size | CRITICAL | high |
| | | > recommended for the size | WARNING | high |
| client_connections | ClientConnectionCount per broker (Sum/min); IAM listener | peak >= 3000 (IAM quota) | CRITICAL | high |
| | | peak >= 80% of the quota | WARNING | high |
| | | non-IAM listeners (no enforced quota) | INFORMATIONAL | medium |
| connection_creation_rate (PER_BROKER) | ConnectionCreationRate per broker (Sum/min); IAMTooManyConnections | IAMTooManyConnections > 0 | CRITICAL | high |
| | | P95 >= quota (100/s; 4/s on kafka.t3.small) | CRITICAL | high |
| | | P95 >= 70% of the quota | WARNING | high |
| | | metric not published (DEFAULT monitoring) | NOT ASSESSED | |
| *_balance (cpu, partition, leader, bytes_in, bytes_out, messages, connection) | per-broker means (latest hour for counts) | hottest broker above the mean by more than 10% (partitions, leaders), 20% (CPU, traffic) or 25% (connections), above an activity floor | WARNING | medium |
| enhanced_monitoring | EnhancedMonitoring | DEFAULT: lists the checks not assessed because of the level and what PER_BROKER adds; always recommends one level above DEFAULT (priority P2) | INFORMATIONAL | high |

Imbalance is not evaluated below the activity floor (for example 1 MB/s, 100 msg/s, 30% CPU per
broker on average), because a skew on an idle cluster has no operational effect.

## Security (20%)

| Check id | Source | Rule | Severity |
|----------|--------|------|----------|
| authentication | ClientAuthentication | unauthenticated listener enabled | HIGH |
| encryption_in_transit | EncryptionInTransit.ClientBroker | PLAINTEXT | HIGH |
| | | TLS_PLAINTEXT | WARNING |
| encryption_in_cluster | EncryptionInTransit.InCluster | false | WARNING |
| encryption_at_rest | EncryptionAtRest | always on in MSK; KMS key shown | HEALTHY |
| public_access | ConnectivityInfo.PublicAccess | SERVICE_PROVIDED_EIPS | INFORMATIONAL |
| broker_logging | LoggingInfo.BrokerLogs | no destination enabled | WARNING (production) / INFORMATIONAL |

## Cost optimisation (15%)

| Check id | Source | Rule | Severity | Confidence |
|----------|--------|------|----------|------------|
| graviton | broker size | x86 size with a Graviton counterpart in MSK | INFORMATIONAL | medium |
| right_sizing | cpu_total, throughput, partition_capacity | every assessed utilisation dimension below 20-30% of the size limits | INFORMATIONAL | low |

No cost percentage is asserted: the report points to the pricing page for the Region instead.

## Reference values

Partition counts per broker (recommended / maximum) and the Express throughput table (sustained /
throttle quota) are copied from the AWS documentation into `msk_health_check/reference.py`
together with the IAM connection quotas. Standard broker network values in that file are guidelines
shipped with this tool and are labelled as such in the report. Update the file when AWS publishes
new broker sizes; an unknown size makes the size-dependent checks report NOT ASSESSED rather than
guess.
