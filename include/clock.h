#pragma once

#include <chrono>

class IClock {
public:
    virtual ~IClock() = default;
    virtual int64_t nowMs() const = 0;
};

class SystemClock : public IClock {
public:
    int64_t nowMs() const override {
        using namespace std::chrono;
        return duration_cast<milliseconds>(
                   system_clock::now().time_since_epoch())
            .count();
    }
};

class FakeClock : public IClock {
public:
    explicit FakeClock(int64_t start_ms = 0) : now_ms_(start_ms) {}

    int64_t nowMs() const override { return now_ms_; }

    void advance(std::chrono::milliseconds delta) {
        now_ms_ += delta.count();
    }

    void set(int64_t ms) { now_ms_ = ms; }

private:
    int64_t now_ms_;
};
