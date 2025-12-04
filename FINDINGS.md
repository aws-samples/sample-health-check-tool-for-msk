# Findings Documentation

This document describes all findings that the MSK Health Check Report can detect, their severity levels, and recommended actions.

## Table of Contents
- [Reliability & Availability](#reliability--availability)
- [Performance & Capacity](#performance--capacity)
- [Security](#security)
- [Cost Optimization](#cost-optimization)

## Reliability & Availability

### Active Controller Count

**Metric:** `ActiveControllerCount`  
**Expected:** Exactly 1  
**Severity:** CRITICAL

**Description:**  
The Kafka cluster must have exactly one active controller at all times. The controller is responsible for managing partition leadership and cluster metadata.

**Findings:**
- **No Active Controller** (CRITICAL): Cluster has 0 controllers - cluster cannot process metadata changes
- **Multiple Controllers** (CRITICAL): Cluster has >1 controllers - split-brain scenario
- **Healthy** (HEALTHY): Exactly 1 controller active

**Recommendations:**
- Investigate cluster controller stability
- Check broker logs for controller election issues
- Restart brokers if needed
- Review network connectivity between brokers

---

### Offline Partitions

**Metric:** `OfflinePartitionsCount`  
**Expected:** 0  
**Severity:** CRITICAL

**Description:**  
Offline partitions indicate data unavailability. Partitions go offline when they have no active leader.

**Findings:**
- **Offline Partitions Detected** (CRITICAL): One or more partitions are offline
- **Healthy** (HEALTHY): No offline partitions

**Recommendations:**
- Investigate and recover offline partitions immediately
- Check broker health and logs
- Verify replication factor configuration
- Review under-replicated partitions

---

### Under-Replicated Partitions

**Metric:** `UnderMinIsrPartitionCount`  
**Expected:** 0  
**Severity:** CRITICAL (current) / WARNING (historical)

**Description:**  
Partitions with fewer in-sync replicas than the minimum ISR setting are at risk of data loss.

**Findings:**
- **Under-Replicated Partitions Now** (CRITICAL): Current partitions below minimum ISR (immediate risk)
- **Under-Replicated Partitions (30 days)** (WARNING): Partitions below minimum ISR in last 30 days
- **Healthy** (HEALTHY): All partitions meet minimum ISR

**Recommendations:**
- Investigate broker health and network connectivity
- Check replication lag
- Review min.insync.replicas configuration
- Ensure sufficient broker capacity

---

### Partition Distribution

**Metric:** `PartitionCount` (per broker)  
**Expected:** Balanced across brokers (±10%)  
**Severity:** WARNING

**Description:**  
Uneven partition distribution can lead to hotspots and unbalanced load.

**Findings:**
- **Partition Imbalance** (WARNING): >10% deviation from average
- **Healthy** (HEALTHY): Partitions balanced across brokers

**Recommendations:**
- Rebalance partitions using Cruise Control
- Use kafka-reassign-partitions tool
- Enable intelligent rebalancing (Express clusters)
- Plan partition distribution for new topics

---

### Leader Distribution

**Metric:** `LeaderCount` (per broker)  
**Expected:** Balanced across brokers (±10%)  
**Severity:** WARNING

**Description:**  
Uneven leader distribution causes unbalanced CPU and network load.

**Findings:**
- **Leader Imbalance** (WARNING): >10% deviation from average
- **Healthy** (HEALTHY): Leaders balanced across brokers

**Recommendations:**
- Run preferred leader election
- Use Cruise Control for rebalancing
- Check auto.leader.rebalance.enable setting
- Review partition assignment strategy

---

## Performance & Capacity

### CPU Usage

**Metrics:** `CpuUser`, `CpuSystem`  
**Expected:** Total (User + System) < 60%  
**Severity:** WARNING

**Description:**  
AWS recommends keeping total CPU usage below 60% to handle operational events like broker failures and rolling upgrades.

**Findings:**
- **High CPU Usage** (WARNING): Total CPU ≥60%
- **Healthy** (HEALTHY): Total CPU <60%

**Recommendations:**
- Scale up to larger instance type
- Add more brokers to distribute load
- Review producer/consumer configurations
- Optimize topic configurations (compression, batch size)

---

### Memory Usage

**Metric:** `HeapMemoryAfterGC`  
**Expected:** <60%  
**Severity:** WARNING

**Description:**  
High heap memory after garbage collection indicates memory pressure and can cause performance degradation.

**Findings:**
- **High Heap Memory** (WARNING): Heap after GC ≥60%
- **Healthy** (HEALTHY): Heap after GC <60%

**Recommendations:**
- Tune JVM heap settings
- Scale up to instance type with more memory
- Reduce transactional.id.expiration.ms (if using transactions)
- Review memory-intensive configurations

---

### Disk Usage (Standard Only)

**Metric:** `KafkaDataLogsDiskUsed`  
**Expected:** <70%  
**Severity:** CRITICAL (>80%) / WARNING (70-80%)

**Description:**  
Monitors disk usage on Standard brokers with storage growth projection. Calculates linear growth rate from historical data and projects when disk will reach 80% capacity.

**Findings:**
- **High Disk Usage** (CRITICAL): Disk usage ≥80%, immediate action required
- **Elevated Disk Usage** (WARNING): Disk usage 70-80%, includes projection if < 30 days until full
- **Healthy** (HEALTHY): Disk usage <70%, includes projection if < 90 days until full

**Storage Growth Projection:**
- Calculates growth rate from time series data
- Projects days until 80% capacity
- Helps plan capacity expansion proactively

**Recommendations:**
- **Critical**: Immediately increase EBS volume size or enable auto-scaling
- **Warning**: Plan storage expansion within projected timeframe
- **Healthy**: Monitor growth trends, review retention policies
- Enable storage auto-scaling for automatic expansion

---

### Network Throughput

**Metric:** `BytesInPerSec`, `BytesOutPerSec`  
**Expected:** <70% of instance network capacity  
**Severity:** WARNING (≥70%)

**Description:**  
Monitors network throughput against instance-specific limits. Express clusters have higher limits than Standard. Alerts at 70% to provide time for action before throttling.

**Instance Limits:**
- **Standard M7g**: 10-125 MB/s (symmetric ingress/egress)
- **Express M7g**: 23.4-750 MB/s ingress, 58.5-1875 MB/s egress (asymmetric)

**Findings:**
- **High Network Utilization** (WARNING): ≥70% of capacity, risk of throttling
- **Healthy** (HEALTHY): <70% of capacity

**Recommendations:**
- Upgrade to larger instance type for higher network capacity
- Optimize data transfer patterns
- Implement compression
- Review consumer/producer configurations

**Metrics:** `BytesInPerSec`, `BytesOutPerSec`  
**Expected:** Within instance network limits  
**Severity:** WARNING

**Description:**  
Network throughput exceeding instance limits causes throttling and performance degradation.

**Findings:**
- **Network Limit Exceeded** (WARNING): Throughput exceeds instance capacity
- **Network Imbalance** (WARNING): >20% deviation between brokers (ignored if <10 MB/s)
- **Healthy** (HEALTHY): Within limits and balanced

**Recommendations:**
- Upgrade to instance type with higher network capacity
- Add more brokers to distribute traffic
- Enable compression
- Review producer/consumer batch sizes

---

### Message Distribution

**Metric:** `MessagesInPerSec`  
**Expected:** Balanced across brokers (±10%)  
**Severity:** WARNING

**Description:**  
Uneven message distribution indicates partition imbalance and can cause broker hotspots.

**Findings:**
- **Message Imbalance** (WARNING): >10% deviation from average
- **Healthy** (HEALTHY): Messages balanced across brokers

**Recommendations:**
- **Express with intelligent rebalancing**: Automatic rebalancing will handle this
- **Express without rebalancing or Standard**: Use Cruise Control or manual reassignment
- Review partition key distribution
- Consider increasing partition count for high-throughput topics

---

### Partition Capacity

**Metric:** `PartitionCount` (per broker)  
**Expected:** Within instance limits  
**Severity:** CRITICAL/WARNING

**Description:**  
Each instance type has partition capacity limits. Exceeding these degrades performance. Express clusters have higher limits than Standard.

**Instance Limits:**
- **Standard M5/M7g**: 1000-16000 partitions per broker (based on instance size)
- **Express M7g**: 1500-32000 partitions per broker (higher than Standard)

**Examples:**
- kafka.m7g.large: 1000 partitions
- express.m7g.large: 1500 partitions (50% more)
- express.m7g.16xlarge: 32000 partitions

**Findings:**
- **Exceeded Capacity** (CRITICAL): >100% of capacity (over limit)
- **Near Capacity** (WARNING): 90-100% of capacity
- **Partition Imbalance** (WARNING): Uneven distribution with available capacity
- **Healthy** (HEALTHY): <90% of capacity

**Recommendations:**
- **Critical**: Immediately add more brokers or upgrade instance type
- **Warning**: Plan capacity expansion soon
- **Imbalance**: Rebalance partitions using Cruise Control, kafka-reassign-partitions, or enable intelligent rebalancing (Express)
- **Healthy**: Monitor growth trends

---

### Client Connection Count

**Metric:** `ClientConnectionCount`  
**Expected:** Within instance limits  
**Severity:** WARNING

**Description:**  
Monitors active client connections to each broker. Each instance type has connection limits. Exceeding these prevents new clients from connecting.

**Findings:**
- **High Connection Count** (WARNING): >80% of limit
- **Healthy** (HEALTHY): <80% of limit

**Recommendations:**
- Implement connection pooling in clients
- Scale up to instance type with higher limits
- Review client connection management
- Close idle connections

---

### Total Connection Count

**Metric:** `ConnectionCount`  
**Expected:** Balanced across brokers (±15%)  
**Severity:** WARNING

**Description:**  
Monitors total connections including client connections and inter-broker connections. Imbalance may indicate uneven load distribution or configuration issues.

**Findings:**
- **Connection Imbalance** (WARNING): >15% deviation between brokers
- **Healthy** (HEALTHY): Connections balanced across brokers

**Recommendations:**
- Review partition distribution
- Check client connection patterns
- Implement connection pooling
- Monitor for connection leaks in applications

---

## Security

### Encryption In-Transit

**Configuration:** `encryption_in_transit_type`  
**Expected:** TLS or TLS_PLAINTEXT  
**Severity:** CRITICAL

**Description:**  
Encryption in-transit protects data from eavesdropping and tampering.

**Findings:**
- **No Encryption** (CRITICAL): PLAINTEXT only
- **Partial Encryption** (WARNING): TLS_PLAINTEXT (mixed mode)
- **Encrypted** (HEALTHY): TLS only

**Recommendations:**
- Enable TLS encryption for all client connections
- Migrate from PLAINTEXT to TLS
- Update client configurations to use TLS

---

### Encryption At-Rest

**Configuration:** `encryption_at_rest`  
**Expected:** Enabled  
**Severity:** WARNING

**Description:**  
Encryption at-rest protects data stored on EBS volumes.

**Findings:**
- **Not Encrypted** (WARNING): Encryption at-rest disabled
- **Encrypted** (HEALTHY): Encryption at-rest enabled

**Recommendations:**
- Enable encryption at-rest for compliance
- Use AWS KMS for key management
- Note: Cannot be enabled on existing clusters (requires recreation)

---

### Authentication

**Configuration:** `authentication_type`  
**Expected:** IAM or mTLS  
**Severity:** CRITICAL

**Description:**  
Authentication prevents unauthorized access to the cluster.

**Findings:**
- **No Authentication** (CRITICAL): Unauthenticated access enabled
- **Authenticated** (HEALTHY): IAM or mTLS enabled

**Recommendations:**
- Enable IAM authentication for AWS-native access control
- Enable mTLS for certificate-based authentication
- Disable unauthenticated access
- Review client access patterns

---

### Enhanced Monitoring

**Configuration:** `enhanced_monitoring_level`  
**Expected:** PER_BROKER  
**Severity:** INFORMATIONAL

**Description:**  
Enhanced monitoring provides detailed per-broker metrics for troubleshooting.

**Findings:**
- **Basic Monitoring** (INFORMATIONAL): DEFAULT level only
- **Enhanced Monitoring** (HEALTHY): PER_BROKER or PER_TOPIC_PER_BROKER

**Recommendations:**
- Enable PER_BROKER monitoring for production clusters
- Consider PER_TOPIC_PER_BROKER for detailed analysis
- Note: Additional CloudWatch costs apply

---

## Cost Optimization

### Instance Type

**Configuration:** `instance_type`  
**Expected:** Graviton-based (M7g)  
**Severity:** INFORMATIONAL

**Description:**  
Graviton-based instances offer better price-performance than x86 instances.

**Findings:**
- **Non-Graviton Instance** (INFORMATIONAL): Using M5 or other x86 instances
- **Graviton Instance** (HEALTHY): Using M7g instances

**Recommendations:**
- Migrate to M7g instances for ~20% cost savings
- Test workload compatibility with Graviton
- Plan migration during maintenance window

---

### Storage Auto-Scaling

**Configuration:** `storage_auto_scaling`  
**Expected:** Enabled (Standard only)  
**Severity:** INFORMATIONAL

**Description:**  
Storage auto-scaling automatically increases EBS volume size when needed.

**Findings:**
- **Auto-Scaling Disabled** (INFORMATIONAL): Manual storage management required
- **Auto-Scaling Enabled** (HEALTHY): Automatic storage expansion

**Recommendations:**
- Enable storage auto-scaling to prevent disk space issues
- Set appropriate target utilization (80%)
- Monitor auto-scaling events
- Note: Only applies to Standard clusters

---

### Kafka Version

**Configuration:** `kafka_version`  
**Expected:** AWS recommended version  
**Severity:** WARNING

**Description:**  
Running outdated Kafka versions exposes clusters to security vulnerabilities and missing features.

**Findings:**
- **Outdated Version** (WARNING): Not running AWS recommended version
- **Current Version** (HEALTHY): Running recommended version

**Recommendations:**
- Upgrade to AWS recommended Kafka version
- Review release notes for breaking changes
- Test upgrade in non-production environment
- Plan rolling upgrade during maintenance window

---

### Availability Zones

**Configuration:** `availability_zones`  
**Expected:** 3 AZs  
**Severity:** WARNING

**Description:**  
Multi-AZ deployment provides fault tolerance and high availability.

**Findings:**
- **Single AZ** (CRITICAL): No fault tolerance
- **Two AZs** (WARNING): Limited fault tolerance
- **Three AZs** (HEALTHY): Full fault tolerance

**Recommendations:**
- Deploy across 3 availability zones for production
- Note: Cannot be changed on existing clusters
- Plan for multi-AZ in new cluster deployments

---

### Intelligent Rebalancing (Express)

**Configuration:** `intelligent_rebalancing_enabled`  
**Expected:** Enabled (Express only)  
**Severity:** INFORMATIONAL

**Description:**  
Intelligent rebalancing automatically maintains balanced partition distribution in Express clusters.

**Findings:**
- **Rebalancing Disabled** (INFORMATIONAL): Manual rebalancing required
- **Rebalancing Enabled** (HEALTHY): Automatic rebalancing active

**Recommendations:**
- Enable intelligent rebalancing for Express clusters
- Reduces operational overhead
- Maintains optimal performance automatically

---

## Severity Levels

### CRITICAL
- Immediate action required
- Impacts availability or data integrity
- Can cause service disruption or data loss
- Examples: No controller, offline partitions, no authentication

### WARNING
- Action recommended within 1 week
- Impacts performance or reliability
- Can lead to issues if not addressed
- Examples: High CPU, memory pressure, outdated version

### INFORMATIONAL
- Optional improvements
- Cost optimization opportunities
- Best practice recommendations
- Examples: Enhanced monitoring, Graviton migration, auto-scaling

### HEALTHY
- No action required
- Operating within recommended parameters
- Meets AWS best practices

---

## Score Impact

Each finding impacts the health score based on its category and severity:

**Category Weights:**
- Reliability: 35%
- Performance: 30%
- Security: 20%
- Cost Optimization: 15%

**Severity Impact (per finding):**
- CRITICAL: -40% of category score
- WARNING: -15% of category score
- INFORMATIONAL: -5% of category score

**Example:**
- 1 CRITICAL in Reliability: ~86/100 (35% × 60% = 21 points lost)
- 2 WARNINGs in Performance: ~91/100 (30% × 27.75% = 8.3 points lost)
- Multiple issues compound multiplicatively
