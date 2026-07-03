#include "job_service.h"

#include <algorithm>
#include <cmath>
#include <sstream>
#include <stdexcept>
using namespace std;

namespace {

constexpr int kMinLeaseDurationMs = 1000;
constexpr int kMaxLeaseDurationMs = 300000;
constexpr int kDefaultLeaseDurationMs = 30000;
constexpr int kMinPriority = 0;
constexpr int kMaxPriority = 9;
constexpr int kMaxBackoffMs = 300000;

}  // namespace

JobService::JobService(IClock& clock) : clock_(clock) {}

JobId JobService::generateJobId() {
    ++next_job_number_;
    ostringstream oss;
    oss << "job-" << next_job_number_;
    return oss.str();
}

void JobService::validateSpec(const JobSpec& spec) const {
    if (spec.type.empty()) {
        throw ValidationError("job type must not be empty");
    }
    if (spec.priority < kMinPriority || spec.priority > kMaxPriority) {
        throw ValidationError("priority must be in range 0-9");
    }
    if (spec.max_retries < 0) {
        throw ValidationError("max_retries must be >= 0");
    }
}

int JobService::clampLeaseDuration(int lease_duration_ms) const {
    return clamp(lease_duration_ms, kMinLeaseDurationMs, kMaxLeaseDurationMs);
}

int64_t JobService::computeBackoffMs(int failure_count) const {
    const double delay = 1000.0 * pow(2.0, failure_count);
    return static_cast<int64_t>(min(delay, static_cast<double>(kMaxBackoffMs)));
}

void JobService::enqueuePending(const JobId& job_id) {
    const auto it = jobs_.find(job_id);
    if (it == jobs_.end()) {
        return;
    }

    JobRecord& job = it->second;
    job.state = JobState::Pending;
    pending_queue_.push(PendingEntry{job.id, job.priority, job.sequence_number});
}

void JobService::moveToDlq(JobRecord& job, const string& reason) {
    job.state = JobState::Dlq;
    job.failure_reason = reason;
    dlq_.push_back(DlqEntry{
        job.id,
        job.type,
        job.payload,
        job.failure_count,
        reason,
    });
}

void JobService::clearWorkerLease(const string& worker_id, const JobId& job_id) {
    const auto worker_it = workers_.find(worker_id);
    if (worker_it == workers_.end()) {
        return;
    }
    if (worker_it->second.current_lease == job_id) {
        worker_it->second.current_lease.reset();
    }
}

bool JobService::workerHandlesType(const vector<string>& types,
                                   const string& job_type) const {
    return find(types.begin(), types.end(), job_type) != types.end();
}

JobId JobService::submit(const JobSpec& spec) {
    validateSpec(spec);

    JobRecord job;
    job.id = generateJobId();
    job.type = spec.type;
    job.payload = spec.payload;
    job.priority = spec.priority;
    job.max_retries = spec.max_retries;
    job.lease_duration_ms = clampLeaseDuration(
        spec.lease_duration_ms > 0 ? spec.lease_duration_ms : kDefaultLeaseDurationMs);
    job.sequence_number = ++next_sequence_;
    job.state = JobState::Pending;

    const JobId job_id = job.id;
    jobs_.emplace(job_id, move(job));
    enqueuePending(job_id);
    return job_id;
}

void JobService::registerWorker(const string& worker_id,
                                const vector<string>& types) {
    workers_[worker_id].types = types;
}

void JobService::registerHandler(const string& type, JobHandler* handler) {
    handlers_[type] = handler;
}

optional<LeasedJob> JobService::pull(const string& worker_id,
                                          const vector<string>& types) {
    vector<PendingEntry> deferred;

    while (!pending_queue_.empty()) {
        PendingEntry entry = pending_queue_.top();
        pending_queue_.pop();

        const auto it = jobs_.find(entry.id);
        if (it == jobs_.end() || it->second.state != JobState::Pending) {
            continue;
        }

        JobRecord& job = it->second;
        if (!workerHandlesType(types, job.type)) {
            deferred.push_back(entry);
            continue;
        }

        for (const PendingEntry& deferred_entry : deferred) {
            pending_queue_.push(deferred_entry);
        }

        const int64_t now = clock_.nowMs();
        job.state = JobState::Claimed;
        job.worker_id = worker_id;
        job.lease_generation += 1;
        job.lease_expiry_ms = now + job.lease_duration_ms;

        auto worker_it = workers_.find(worker_id);
        if (worker_it != workers_.end()) {
            worker_it->second.current_lease = job.id;
        }

        return LeasedJob{
            job.id,
            job.type,
            job.payload,
            job.lease_generation,
        };
    }

    for (const PendingEntry& deferred_entry : deferred) {
        pending_queue_.push(deferred_entry);
    }
    return nullopt;
}

bool JobService::renewLease(const JobId& job_id, const string& worker_id) {
    const auto it = jobs_.find(job_id);
    if (it == jobs_.end() || it->second.state != JobState::Claimed) {
        return false;
    }

    JobRecord& job = it->second;
    if (!job.worker_id || *job.worker_id != worker_id) {
        return false;
    }

    job.lease_expiry_ms = clock_.nowMs() + job.lease_duration_ms;
    return true;
}

CompleteStatus JobService::complete(const JobId& job_id,
                                    const string& worker_id,
                                    Result result) {
    const auto it = jobs_.find(job_id);
    if (it == jobs_.end()) {
        return CompleteStatus::JobNotFound;
    }

    JobRecord& job = it->second;
    if (job.state != JobState::Claimed || !job.worker_id || *job.worker_id != worker_id) {
        return CompleteStatus::LeaseLost;
    }

    clearWorkerLease(worker_id, job_id);

    auto worker_it = workers_.find(worker_id);
    WorkerInfo* worker_info = worker_it != workers_.end() ? &worker_it->second : nullptr;

    if (result == Result::Success) {
        job.state = JobState::Succeeded;
        if (worker_info) {
            worker_info->success_count += 1;
        }
        return CompleteStatus::Ok;
    }

    if (result == Result::PermanentFailure) {
        job.failure_count += 1;
        if (worker_info) {
            worker_info->failure_count += 1;
        }
        moveToDlq(job, "permanent failure");
        return CompleteStatus::Ok;
    }

    job.failure_count += 1;
    if (worker_info) {
        worker_info->failure_count += 1;
    }

    if (job.failure_count > job.max_retries) {
        moveToDlq(job, "max retries exhausted");
        return CompleteStatus::Ok;
    }

    job.state = JobState::RetryScheduled;
    job.retry_at_ms = clock_.nowMs() + computeBackoffMs(job.failure_count);
    job.worker_id.reset();
    retry_queue_.push(RetryEntry{job.retry_at_ms, job.id});
    return CompleteStatus::Ok;
}

void JobService::cancel(const JobId& job_id) {
    const auto it = jobs_.find(job_id);
    if (it == jobs_.end()) {
        // cout<<"Job not found."<<endl;
        return;
    }
    if (it->second.state == JobState::Pending || it->second.state == JobState::RetryScheduled) {
        // cout<<"Job cancelled in state: "<<it->second.state<<endl;
        JobRecord& job = it->second;
        job.state = JobState::Cancelled;
        return;
    }

    if (it->second.state == JobState::Claimed || it->second.state == JobState::Succeeded || it->second.state == JobState::Dlq || it->second.state == JobState::Cancelled) {
        // cout<<"Job cannot be cancelled in" << it->second.state << " state."<<endl;
        throw ValidationError("Job cannot be cancelled.");
    }
    return ;
}

void JobService::tick() {
    const int64_t now = clock_.nowMs();

    vector<JobId> expired;
    for (const auto& [job_id, job] : jobs_) {
        if (job.state == JobState::Claimed && now >= job.lease_expiry_ms) {
            expired.push_back(job_id);
        }
    }

    for (const JobId& job_id : expired) {
        JobRecord& job = jobs_.at(job_id);
        if (job.worker_id) {
            clearWorkerLease(*job.worker_id, job_id);
        }
        job.worker_id.reset();
        enqueuePending(job_id);
    }

    vector<RetryEntry> deferred_retries;
    while (!retry_queue_.empty()) {
        RetryEntry entry = retry_queue_.top();
        retry_queue_.pop();

        const auto it = jobs_.find(entry.id);
        if (it == jobs_.end() || it->second.state != JobState::RetryScheduled) {
            continue;
        }

        if (entry.retry_at_ms > now) {
            deferred_retries.push_back(entry);
            continue;
        }

        enqueuePending(entry.id);
    }

    for (const RetryEntry& entry : deferred_retries) {
        retry_queue_.push(entry);
    }
}

map<string, map<int, int>>
JobService::pendingCountsByTypeAndPriority() const {
    map<string, map<int, int>> counts;
    for (const auto& [_, job] : jobs_) {
        if (job.state == JobState::Pending) {
            counts[job.type][job.priority] += 1;
        }
    }
    return counts;
}

int JobService::inFlightCount() const {
    int count = 0;
    for (const auto& [_, job] : jobs_) {
        if (job.state == JobState::Claimed) {
            count += 1;
        }
    }
    return count;
}

int JobService::dlqSize() const { return static_cast<int>(dlq_.size()); }

vector<DlqEntry> JobService::listDlq() const { return dlq_; }

WorkerStats JobService::workerStats(const string& worker_id) const {
    const auto it = workers_.find(worker_id);
    if (it == workers_.end()) {
        return WorkerStats{};
    }

    return WorkerStats{
        it->second.current_lease,
        it->second.success_count,
        it->second.failure_count,
    };
}
