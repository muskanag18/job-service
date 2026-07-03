#pragma once

#include <cstdint>
#include <map>
#include <stdexcept>
#include <optional>
#include <string>
#include <vector>

using JobId = std::string;
using Payload = std::map<std::string, std::string>;

enum class JobState {
    Pending,
    Claimed,
    RetryScheduled,
    Succeeded,
    Dlq,
    Cancelled
};

enum class Result {
    Success,
    TransientFailure,
    PermanentFailure
};

enum class CompleteStatus {
    Ok,
    LeaseLost,
    JobNotFound
};

struct JobSpec {
    std::string type;
    Payload payload;
    int priority = 5;
    int max_retries = 3;
    int lease_duration_ms = 30000;
};

struct LeasedJob {
    JobId id;
    std::string type;
    Payload payload;
    int lease_generation = 0;
};

struct DlqEntry {
    JobId id;
    std::string type;
    Payload payload;
    int attempt_count = 0;
    std::string failure_reason;
};

struct WorkerStats {
    std::optional<JobId> current_lease;
    int success_count = 0;
    int failure_count = 0;
};

class ValidationError : public std::runtime_error {
public:
    explicit ValidationError(const std::string& message)
        : std::runtime_error(message) {}
};
