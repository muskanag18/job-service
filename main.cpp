#include <iostream>

#include "clock.h"
#include "handler.h"
#include "job_service.h"
using namespace std;

namespace {
struct EmailHandler : JobHandler {
    Result run(const Payload& payload) override {
        cout << "Sending email to " << payload.at("to") << endl;
        return Result::Success;
    }
};
}  // namespace

int main() {
    FakeClock clock;
    JobService svc(clock);

    svc.registerWorker("worker-1", {"send_email"});
    EmailHandler email_handler;
    svc.registerHandler("send_email", &email_handler);

    JobSpec spec;
    spec.type = "send_email";
    spec.payload = {{"to", "user@example.com"}};
    spec.priority = 7;
    spec.max_retries = 3;

    const JobId job_id = svc.submit(spec);
    cout << "Submitted job: " << job_id << endl;

    svc.tick();
    if (auto job = svc.pull("worker-1", {"send_email"})) {
        cout << "Worker claimed job: " << job->id << endl;
        svc.complete(job->id, "worker-1", Result::Success);
        cout << "Job completed successfully." << endl;
    }

    cout<<"Cancelling job: "<<job_id<<endl;
    svc.cancel(job_id);

    const WorkerStats stats = svc.workerStats("worker-1");
    cout << "Worker successes: " << stats.success_count << endl;
    cout << "DLQ size: " << svc.dlqSize() << endl;

    return 0;
}
