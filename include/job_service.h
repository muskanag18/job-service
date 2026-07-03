#pragma once

#include <map>
#include <optional>
#include <queue>
#include <string>
#include <unordered_map>
#include <vector>

#include "clock.h"
#include "handler.h"
#include "types.h"

class JobService {
public:
    explicit JobService(IClock& clock);

    JobId submit(const JobSpec& spec);
    void registerWorker(const std::string& worker_id,
                        const std::vector<std::string>& types);
    void registerHandler(const std::string& type, JobHandler* handler);

    std::optional<LeasedJob> pull(const std::string& worker_id,
                                    const std::vector<std::string>& types);
    bool renewLease(const JobId& job_id, const std::string& worker_id);
    CompleteStatus complete(const JobId& job_id,
                            const std::string& worker_id,
                            Result result);
    void cancel(const JobId& job_id);

    void tick();

    std::map<std::string, std::map<int, int>> pendingCountsByTypeAndPriority() const;
    int inFlightCount() const;
    int dlqSize() const;
    std::vector<DlqEntry> listDlq() const;
    WorkerStats workerStats(const std::string& worker_id) const;

private:
    struct JobRecord {
        JobId id;
        std::string type;
        Payload payload;
        int priority = 5;
        int max_retries = 3;
        int lease_duration_ms = 30000;
        JobState state = JobState::Pending;
        int64_t sequence_number = 0;
        int failure_count = 0;
        int64_t retry_at_ms = 0;
        std::string failure_reason;

        std::optional<std::string> worker_id;
        int64_t lease_expiry_ms = 0;
        int lease_generation = 0;
    };

    struct PendingEntry {
        JobId id;
        int priority = 0;
        int64_t sequence = 0;
    };

    struct PendingCompare {
        bool operator()(const PendingEntry& a, const PendingEntry& b) const {
            if (a.priority != b.priority) {
                return a.priority < b.priority;
            }
            return a.sequence > b.sequence;
        }
    };

    struct RetryEntry {
        int64_t retry_at_ms = 0;
        JobId id;
    };

    struct RetryCompare {
        bool operator()(const RetryEntry& a, const RetryEntry& b) const {
            return a.retry_at_ms > b.retry_at_ms;
        }
    };

    struct WorkerInfo {
        std::vector<std::string> types;
        std::optional<JobId> current_lease;
        int success_count = 0;
        int failure_count = 0;
    };

    IClock& clock_;
    int64_t next_sequence_ = 0;
    int64_t next_job_number_ = 0;

    std::priority_queue<PendingEntry, std::vector<PendingEntry>, PendingCompare> pending_queue_;
    std::priority_queue<RetryEntry, std::vector<RetryEntry>, RetryCompare> retry_queue_;
    std::unordered_map<JobId, JobRecord> jobs_;
    std::vector<DlqEntry> dlq_;
    std::unordered_map<std::string, JobHandler*> handlers_;
    std::unordered_map<std::string, WorkerInfo> workers_;

    JobId generateJobId();
    void validateSpec(const JobSpec& spec) const;
    int clampLeaseDuration(int lease_duration_ms) const;
    int64_t computeBackoffMs(int failure_count) const;
    void enqueuePending(const JobId& job_id);
    void moveToDlq(JobRecord& job, const std::string& reason);
    void clearWorkerLease(const std::string& worker_id, const JobId& job_id);
    bool workerHandlesType(const std::vector<std::string>& types,
                           const std::string& job_type) const;
};
