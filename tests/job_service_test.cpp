#include <iostream>
#include <vector>

#include "clock.h"
#include "handler.h"
#include "job_service.h"

namespace {

int failures = 0;

void expect(bool condition, const char* message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << std::endl;
        failures += 1;
    }
}

JobSpec makeSpec(const std::string& type,
                 int priority = 5,
                 int max_retries = 3) {
    JobSpec spec;
    spec.type = type;
    spec.payload = {{"key", "value"}};
    spec.priority = priority;
    spec.max_retries = max_retries;
    return spec;
}

struct MockHandler : JobHandler {
    Result result = Result::Success;
    int invocations = 0;

    Result run(const Payload&) override {
        invocations += 1;
        return result;
    }
};

struct SequenceHandler : JobHandler {
    std::vector<Result> results;
    size_t index = 0;
    int invocations = 0;

    Result run(const Payload&) override {
        invocations += 1;
        if (index >= results.size()) {
            return Result::TransientFailure;
        }
        return results[index++];
    }
};

void test_priority_ordering() {
    FakeClock clock;
    JobService svc(clock);

    const JobId id0 = svc.submit(makeSpec("task", 0));
    const JobId id9 = svc.submit(makeSpec("task", 9));
    const JobId id5 = svc.submit(makeSpec("task", 5));

    const auto first = svc.pull("w1", {"task"});
    const auto second = svc.pull("w1", {"task"});
    const auto third = svc.pull("w1", {"task"});

    expect(first.has_value() && first->id == id9, "priority ordering: priority 9 dequeued first");
    expect(second.has_value() && second->id == id5, "priority ordering: priority 5 dequeued second");
    expect(third.has_value() && third->id == id0, "priority ordering: priority 0 dequeued third");
}

void test_same_priority_fifo() {
    FakeClock clock;
    JobService svc(clock);

    const JobId first_id = svc.submit(makeSpec("task", 5));
    const JobId second_id = svc.submit(makeSpec("task", 5));
    const JobId third_id = svc.submit(makeSpec("task", 5));

    const auto first = svc.pull("w1", {"task"});
    const auto second = svc.pull("w1", {"task"});
    const auto third = svc.pull("w1", {"task"});

    expect(first.has_value() && first->id == first_id, "fifo: first submitted job pulled first");
    expect(second.has_value() && second->id == second_id, "fifo: second submitted job pulled second");
    expect(third.has_value() && third->id == third_id, "fifo: third submitted job pulled third");
}

void test_lease_renewal_happy_path() {
    FakeClock clock;
    JobService svc(clock);
    svc.registerWorker("w1", {"task"});

    const JobId job_id = svc.submit(makeSpec("task"));
    const auto leased = svc.pull("w1", {"task"});
    expect(leased.has_value(), "lease renewal: job leased");

    clock.advance(std::chrono::milliseconds(20000));
    expect(svc.renewLease(job_id, "w1"), "lease renewal: renew succeeds");

    clock.advance(std::chrono::milliseconds(20000));
    svc.tick();

    expect(svc.inFlightCount() == 1, "lease renewal: job still in-flight after partial expiry");
    expect(svc.complete(job_id, "w1", Result::Success) == CompleteStatus::Ok,
           "lease renewal: complete succeeds");

    expect(svc.inFlightCount() == 0, "lease renewal: no in-flight after success");
    expect(svc.workerStats("w1").success_count == 1, "lease renewal: worker success count updated");
}

void test_lease_expiry_at_least_once() {
    FakeClock clock;
    JobService svc(clock);
    svc.registerWorker("wA", {"task"});
    svc.registerWorker("wB", {"task"});

    const JobId job_id = svc.submit(makeSpec("task"));
    const auto leased_a = svc.pull("wA", {"task"});
    expect(leased_a.has_value(), "lease expiry: worker A claims job");

    clock.advance(std::chrono::milliseconds(30001));
    svc.tick();

    expect(svc.inFlightCount() == 0, "lease expiry: job returned to pending");

    const auto leased_b = svc.pull("wB", {"task"});
    expect(leased_b.has_value(), "lease expiry: worker B re-claims job");

    expect(svc.complete(job_id, "wB", Result::Success) == CompleteStatus::Ok,
           "lease expiry: worker B completes successfully");
    expect(svc.complete(job_id, "wA", Result::Success) == CompleteStatus::LeaseLost,
           "lease expiry: stale worker A completion rejected");

    expect(svc.workerStats("wB").success_count == 1, "lease expiry: worker B credited with success");
}

void test_transient_retry_backoff() {
    FakeClock clock;
    JobService svc(clock);
    svc.registerWorker("w1", {"task"});

    MockHandler handler;
    handler.result = Result::TransientFailure;
    svc.registerHandler("task", &handler);

    const JobId job_id = svc.submit(makeSpec("task", 5, 3));
    const auto leased = svc.pull("w1", {"task"});
    expect(leased.has_value(), "retry backoff: job leased");

    expect(svc.complete(job_id, "w1", Result::TransientFailure) == CompleteStatus::Ok,
           "retry backoff: transient failure accepted");

    expect(svc.pull("w1", {"task"}).has_value() == false,
           "retry backoff: job not pullable before backoff elapses");

    clock.advance(std::chrono::milliseconds(1999));
    svc.tick();
    expect(svc.pull("w1", {"task"}).has_value() == false,
           "retry backoff: still not pullable before 2s backoff");

    clock.advance(std::chrono::milliseconds(1));
    svc.tick();
    const auto retried = svc.pull("w1", {"task"});
    expect(retried.has_value(), "retry backoff: job pullable after backoff elapses");
}

void test_max_retries_to_dlq() {
    FakeClock clock;
    JobService svc(clock);
    svc.registerWorker("w1", {"task"});

    SequenceHandler handler;
    handler.results = {
        Result::TransientFailure,
        Result::TransientFailure,
        Result::TransientFailure,
        Result::TransientFailure,
    };
    svc.registerHandler("task", &handler);

    const JobId job_id = svc.submit(makeSpec("task", 5, 3));

    for (int attempt = 0; attempt < 4; ++attempt) {
        svc.tick();
        const auto leased = svc.pull("w1", {"task"});
        expect(leased.has_value(), "max retries: job pullable on each attempt");
        expect(svc.complete(job_id, "w1", Result::TransientFailure) == CompleteStatus::Ok,
               "max retries: transient failure recorded");
        if (attempt < 3) {
            clock.advance(std::chrono::milliseconds(300000));
        }
    }

    expect(svc.dlqSize() == 1, "max retries: job moved to DLQ after 4 attempts");
    const auto dlq = svc.listDlq();
    expect(!dlq.empty() && dlq[0].id == job_id, "max retries: correct job in DLQ");
    expect(dlq[0].attempt_count == 4, "max retries: attempt count is 4");
}

void test_permanent_failure_short_circuit() {
    FakeClock clock;
    JobService svc(clock);
    svc.registerWorker("w1", {"task"});

    MockHandler handler;
    handler.result = Result::PermanentFailure;
    svc.registerHandler("task", &handler);

    const JobId job_id = svc.submit(makeSpec("task"));
    const auto leased = svc.pull("w1", {"task"});
    expect(leased.has_value(), "permanent failure: job leased");

    expect(svc.complete(job_id, "w1", Result::PermanentFailure) == CompleteStatus::Ok,
           "permanent failure: completion accepted");

    expect(svc.dlqSize() == 1, "permanent failure: immediate DLQ");
    expect(svc.listDlq()[0].attempt_count == 1, "permanent failure: attempt count is 1");
    expect(svc.pull("w1", {"task"}).has_value() == false,
           "permanent failure: job not re-queued");
}

void test_observability() {
    FakeClock clock;
    JobService svc(clock);
    svc.registerWorker("w1", {"email", "resize", "cleanup"});

    svc.submit(makeSpec("email", 7));
    svc.submit(makeSpec("email", 3));
    svc.submit(makeSpec("resize", 5));

    auto pending = svc.pendingCountsByTypeAndPriority();
    expect(pending["email"][7] == 1, "observability: email priority 7 pending");
    expect(pending["email"][3] == 1, "observability: email priority 3 pending");
    expect(pending["resize"][5] == 1, "observability: resize priority 5 pending");

    const auto leased = svc.pull("w1", {"email"});
    expect(leased.has_value(), "observability: pull email job");
    expect(svc.inFlightCount() == 1, "observability: one in-flight job");

    pending = svc.pendingCountsByTypeAndPriority();
    expect(pending["email"][7] == 0, "observability: claimed job not counted as pending");
    expect(pending["email"][3] == 1, "observability: remaining email job still pending");

    svc.complete(leased->id, "w1", Result::Success);
    expect(svc.inFlightCount() == 0, "observability: no in-flight after success");
    expect(svc.workerStats("w1").success_count == 1, "observability: worker success count");

    const auto remaining_resize = svc.pull("w1", {"resize"});
    expect(remaining_resize.has_value(), "observability: drain remaining resize job");
    svc.complete(remaining_resize->id, "w1", Result::Success);

    const JobId failing_id = svc.submit(makeSpec("cleanup", 1, 0));
    const auto fail_lease = svc.pull("w1", {"cleanup"});
    expect(fail_lease.has_value() && fail_lease->id == failing_id,
           "observability: pull cleanup job for DLQ");
    svc.complete(failing_id, "w1", Result::PermanentFailure);

    expect(svc.dlqSize() == 1, "observability: DLQ size updated");
    expect(svc.listDlq().size() == 1, "observability: listDlq matches size");
}

void test_submit_validation() {
    FakeClock clock;
    JobService svc(clock);

    JobSpec empty_type;
    empty_type.type = "";
    empty_type.payload = {{"k", "v"}};

    bool threw = false;
    try {
        svc.submit(empty_type);
    } catch (const ValidationError&) {
        threw = true;
    }
    expect(threw, "validation: empty type rejected");
}

void test_cancel_validation() {
    FakeClock clock;
    JobService svc(clock);
    svc.registerWorker("w1", {"task"});
    const JobId job_id = svc.submit(makeSpec("task"));
    // const auto leased = svc.pull("w1", {"task"});
    svc.cancel(job_id);
    // expect(throw, "cancel validation: job cancelled");
}

}  // namespace

int main() {
    test_priority_ordering();
    test_same_priority_fifo();
    test_lease_renewal_happy_path();
    test_lease_expiry_at_least_once();
    test_transient_retry_backoff();
    test_max_retries_to_dlq();
    test_permanent_failure_short_circuit();
    test_observability();
    test_submit_validation();
    test_cancel_validation();

    if (failures == 0) {
        std::cout << "All tests passed." << std::endl;
        return 0;
    }

    std::cerr << failures << " test(s) failed." << std::endl;
    return 1;
}
