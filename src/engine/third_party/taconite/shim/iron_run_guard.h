// SPDX-License-Identifier: Apache-2.0
#pragma once

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#include <cstdlib>
#endif

namespace iron_run_guard {

#ifdef _WIN32
[[noreturn]] inline void retire()
{
    // A submitted run may still own buffers: skip all native and DLL teardown.
    TerminateProcess(GetCurrentProcess(), 75);
    std::abort();
}
#endif

template<class Operation>
auto invoke(Operation&& operation) -> decltype(operation())
{
#ifdef _WIN32
    try {
        return operation();
    } catch (...) {
        retire();
    }
#else
    return operation();
#endif
}

template<class Wait, class State>
State wait(Wait&& operation, State completed)
{
    return invoke([&] {
        const State state = operation();
#ifdef _WIN32
        if (state != completed)
            retire();
#else
        (void)completed;
#endif
        return state;
    });
}

} // namespace iron_run_guard
