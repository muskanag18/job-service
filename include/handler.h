#pragma once

#include "types.h"

class JobHandler {
public:
    virtual ~JobHandler() = default;
    virtual Result run(const Payload& payload) = 0;
};
