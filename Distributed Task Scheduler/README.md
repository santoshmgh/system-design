# Distributed Job Scheduler — Mental Model

> **Problem:** Design a distributed job scheduler at 10,000 QPS with fault tolerance, at-least-once execution, and status querying.
> **Level:** Senior (L5) | **Role:** Engineering Manager
> **Confirmed Salesforce ask**

---

## 1.Requirments

### Functional 
- **Create the task**
- **Cancel the task**
- **Execute the task**
- **Schedule the task**
- **Status of the task**

### Non Functional 
- **Availability**
- **Durability**
- **Scalability**
- **Fault Tolerant**
- **Bounded amout of time**


## 1. Domain

### Actors
- **Client / UI** — submits jobs
- **Scheduler Service** — decides when to publish jobs to Kafka
- **Worker Pods** — consume from Kafka, execute jobs

### Job Object
| Field | Description |
|---|---|
| `taskId` | Unique identifier |
| `taskName` | Human readable name |
| `taskType` | ONE_TIME · RECURRING |
| `taskSchedule` | Cron expression or timestamp |
| `taskTenant` | Tenant identifier — Salesforce org |
| `taskStatus` | Current state |
| `taskCurrentRetryCount` | Attempts so far |
| `taskMaxRetryCount` | Max attempts before DLQ |
| `lastFailedTime` | Timestamp of last failure |

### Configuration (system-level)
| Parameter | Description |
|---|---|
| `maxRetryCount` | System default retry limit |
| `jobTimeoutDuration` | Per jobType SLA |
| `priorityLevels` | Per tenant priority |
| `workerPoolSize` | K8s pod count |

### State
```
PENDING → SCHEDULED → RUNNING → SUCCESS
                              → TIMEOUT  → RETRYING
                              → ERRORED  → RETRYING (retryCount not exhausted)
                                         → FAILED   (retryCount exhausted) → DLQ
```

### Events
- `submitJob(jobId)` — client submits job
- `executeJob(jobId)` — worker executes job
- `retryJob(jobId)` — retry after failure
- `jobStatus(jobId)` — query current status

### Transitions
```
submitJob()   → PENDING
PENDING       → SCHEDULED (Scheduler picks up)
SCHEDULED     → RUNNING   (Worker consumes from Kafka)
RUNNING       → SUCCESS
RUNNING       → TIMEOUT   → RETRYING
RUNNING       → ERRORED   → RETRYING (retryCount < maxRetryCount)
                          → FAILED   (retryCount == maxRetryCount) → DLQ
RETRYING      → RUNNING   (retry attempt)
RETRYING      → FAILED    (exhausted)
```

---

## 2. HLD

### NFR
| Dimension | Target |
|---|---|
| Scale | 10M jobs/day · 115 TPS avg · peak 1000 TPS |
| Latency | Per jobType SLA · submission < 15 min · real-time < 5s · batch < 1hr |
| Job Processing Availability | 99.99% · Active-Active both regions |
| Job Scheduling Availability | 99.9% · Active-Passive acceptable |
| MTTD | Processing < 1 min · Scheduling < 5 min |
| MTTR | Processing < 5 min · Scheduling < 15 min |
| Failover | Workers Active-Active · Scheduler Active-Passive |
| Cross-region | Kafka MirrorMaker 2 · MongoDB Global DB |

> **Key insight:** Job Processing and Job Scheduling have different criticality — different SLAs, different failover strategies. Don't treat them the same.

### Components
```
Client / UI
  → API Gateway              (rate limiting per tenantId + Auth)
  → Load Balancer
  → Scheduler Service        (decides when · publishes to Kafka · Active-Passive)
  → Kafka Main Topic         (partitioned by tenantId · at-least-once)
  → Worker Pods (K8s)        (consume · execute · commit offset on success)
  → Kafka DLQ Topic          (failed jobs after maxRetry)
  → DLQ Worker Pods          (separate consumer group · alert + manual retry)
  → MongoDB Global DB        (job state · source of truth · sharded by jobId)
  → Redis                    (read-through cache for status · write-through for state)
  → MirrorMaker 2            (cross-region Kafka replication · DR)
```

### Data Storage

**MongoDB — source of truth**
```
Shard key:        jobId           ← uniform distribution
Secondary index:  tenantId        ← tenant-level queries
Secondary index:  status          ← find all FAILED/PENDING jobs
Document:         full Job object
```

**Redis — caching layer**
```
Job status   → read-through  (TTL 5 sec · staleness acceptable)
Job state    → write-through (MongoDB + Redis on every transition)
Lock state   → no cache      (Redis primary only · noeviction)
```

**Caching pattern reference:**
| Pattern | Use when | Risk |
|---|---|---|
| Read-through | Reads frequent · staleness ok | Stale data |
| Write-through | Reads must be fresh · writes affordable | Higher write latency |
| Write-behind | Write speed critical · loss acceptable | Data loss on crash |

**Kafka — job queue**
```
Main Topic:  partitioned by tenantId (premium tenants = dedicated partitions)
DLQ Topic:   separate topic · separate consumer group
Retention:   7 days
Replication: MirrorMaker 2 → secondary region
```

**Redis eviction policy: `noeviction`** for lock state · `volatile-lru` for status cache

### End-to-End Flow

#### ✅ Path 1 — Happy path (one-time job)
```
Client submits → API Gateway → Scheduler Service
→ MongoDB: status=PENDING
→ Scheduler publishes to Kafka Main Topic at scheduled time
→ Worker consumes → status=RUNNING → executes
→ SUCCESS → MongoDB: status=SUCCESS → commit Kafka offset
→ Redis cache updated
```

#### 🔄 Path 2 — Failure + retry + success
```
Worker consumes → executes → FAILS
→ don't commit Kafka offset
→ MongoDB: status=RETRYING · retryCount++
→ Kafka redelivers after visibilityTimeout
→ Worker retries → SUCCESS → commit offset
→ MongoDB: status=SUCCESS
```

#### ❌ Path 3 — Exhausted retries → DLQ
```
retryCount == maxRetryCount
→ publish to Kafka DLQ Topic
→ MongoDB: status=FAILED
→ DLQ Worker consumes → alerts ops → manual review
→ Splunk alert fired
```

#### 🔁 Path 4 — Recurring job
```
Worker executes → SUCCESS → commit offset
→ Scheduler detects jobType=RECURRING
→ re-publishes to Kafka based on jobSchedule (cron)
→ repeat from Path 1
```

#### ⚠️ Path 5 — Infrastructure failure
```
Kafka down:
→ Scheduler cannot publish → jobs stay PENDING
→ alert fires · MTTD < 1 min
→ MirrorMaker promotes secondary Kafka
→ Scheduler republishes → workers resume

MongoDB down:
→ workers cannot update status → retry with backoff
→ Redis cache serves reads during outage
→ writes queued → flush on recovery
```

---

## 3. LLD

### API Design

```
POST /jobs                    → submitJob()
GET  /jobs/{jobId}/status     → jobStatus()
POST /jobs/{jobId}/retry      → retryJob()
GET  /jobs?tenantId=X&status=Y → list jobs by tenant + status
```

**Request — submit job:**
```json
{
  "jobName": "monthly-invoice",
  "jobType": "RECURRING",
  "jobSchedule": "0 0 1 * *",
  "jobTenant": "org-salesforce-123",
  "maxRetryCount": 3,
  "payload": { ... }
}
```

**Response — 202 Accepted (async):**
```json
{
  "jobId": "job-uuid-123",
  "status": "PENDING",
  "estimatedStart": "2026-06-08T10:00:00Z"
}
```

**Status codes:**
```
202 — job accepted (async submission)
200 — status retrieved
404 — job not found
429 — rate limit per tenantId
503 — scheduler unavailable
```

### Class Structure

```java
// Job.java
public class Job {
    private String jobId;
    private String jobName;
    private JobType jobType;          // ONE_TIME, RECURRING
    private String jobSchedule;       // cron or timestamp
    private String jobTenant;
    private JobStatus status;
    private int currentRetryCount;
    private int maxRetryCount;
    private long lastFailedTime;
    private Map<String, Object> payload;
}

// SchedulerService.java
public class SchedulerService {
    public void schedule(Job job);           // publish to Kafka at right time
    public void reschedule(Job job);         // for recurring jobs
    public List<Job> getPendingJobs();       // poll MongoDB for due jobs
}

// WorkerService.java
public class WorkerService {
    public void execute(Job job);            // consume from Kafka · execute
    public void onSuccess(Job job);          // commit offset · update status
    public void onFailure(Job job);          // don't commit · retryCount++
    public void onExhausted(Job job);        // publish to DLQ
}

// JobStatusService.java
public class JobStatusService {
    public JobStatus getStatus(String jobId); // Redis cache → MongoDB
    public void updateStatus(String jobId, JobStatus status); // write-through
}
```

### Algorithm — Job Execution

```java
void execute(Job job) {
    // idempotency check
    if (job.status == SUCCESS) {
        commitOffset();
        return;
    }

    try {
        // execute job logic
        runJob(job.payload);
        onSuccess(job);
    } catch (RetryableException e) {
        onFailure(job, true);
    } catch (NonRetryableException e) {
        onFailure(job, false);
    } catch (TimeoutException e) {
        onFailure(job, true);  // timeout = retryable
    }
}

void onFailure(Job job, boolean retryable) {
    if (!retryable || job.currentRetryCount >= job.maxRetryCount) {
        publishToDLQ(job);
        updateStatus(job, FAILED);
    } else {
        job.currentRetryCount++;
        updateStatus(job, RETRYING);
        // don't commit offset → Kafka redelivers
    }
}
```

---

## 4. Production

> **Pre-submission checklist:**
> 1. What breaks?
> 2. How do I know it broke?
> 3. How long until fixed?
> 4. What does the system do while broken?
> 5. How does this behave with 10,000 Salesforce tenants simultaneously?

### Failures
| Failure | Behaviour |
|---|---|
| Kafka down | Jobs stay PENDING · MirrorMaker promotes secondary · alert fires |
| MongoDB down | Redis serves reads · writes queued · flush on recovery |
| Worker pod crash | K8s restarts pod · Kafka redelivers uncommitted message |
| Scheduler down | Active-Passive failover · jobs stay PENDING during gap |
| DLQ full | Alert ops · pause DLQ consumer · investigate |
| Job timeout | Treated as retryable · retryCount++ · Kafka redelivers |

### Observability
**Metrics — CloudWatch:**
- Job throughput per tenantId (jobs/sec)
- Job success rate · failure rate · DLQ rate
- Kafka consumer lag per partition (processing backlog)
- Worker pod CPU/memory (K8s auto-scaling trigger)
- MongoDB query latency (p50, p99)
- Redis cache hit rate

**Logs — Splunk:**
- Every job state transition with jobId + tenantId + duration
- DLQ events — alert ops immediately
- Retry events — high retry rate = downstream issue signal
- Worker crashes with stack trace

**Alerts:**
- Kafka consumer lag > threshold → scale workers
- DLQ rate spike → page on-call
- Job failure rate > 5% → investigate
- MongoDB latency spike → check indexes
- Worker pod OOM → K8s resource limits

**Tenant isolation — Salesforce lens:**
- Kafka partitioned by tenantId — noisy tenant doesn't affect others
- Separate worker pod groups per premium tenant tier
- Rate limiting per tenantId at API Gateway
- MongoDB sharded by jobId — no cross-tenant query bleed
- Priority queue: premium tenants get dedicated partitions

### Scaling
- **Workers** — K8s HPA on Kafka consumer lag metric
- **Scheduler** — scale on job submission rate
- **MongoDB** — horizontal sharding by jobId
- **Kafka** — add partitions on throughput increase
- **Redis** — Redis Cluster if status query volume grows

---

## Score (L5 EM bar)

| Layer | Score | Key gap |
|---|---|---|
| Domain | 8.5/10 | Rich job object · good state machine · timeout→retry correct |
| HLD | 9/10 | Processing vs scheduling SLA split — exceptional EM thinking |
| LLD | 8/10 | Idempotency + retryable vs non-retryable distinction strong |
| API | 8/10 | 202 Accepted for async submission — correct |
| Production | 8.5/10 | Led well · Kafka lag as scaling trigger · tenant isolation |
| **Overall** | **8.4/10** | Consistent with trend · NFR split was L5+ signal |

### Trend
7.2 → 7.8 → 8.1 → 8.4 → 8.4

> Key Salesforce differentiator: always ask
> *"How does this behave with 10,000 tenants simultaneously?"*
