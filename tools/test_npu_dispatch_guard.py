"""Compile the native dispatch guard and verify retirement before teardown."""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
import unittest


ROOT = Path(__file__).resolve().parents[1]
SHIM = ROOT / 'src/engine/third_party/taconite/shim'
CHILD = r'''
#include <fstream>
#include <stdexcept>
#include <string>

// The negative control changes only this disposable fixture, never the header.
#ifdef WINSEARCH_TEST_UNPROTECTED
namespace iron_run_guard {
template<class Operation> auto invoke(Operation operation) -> decltype(operation()) {
    return operation();
}
template<class Wait, class State> State wait(Wait operation, State) {
    return operation();
}
}
#else
#include "iron_run_guard.h"
#endif

const char* marker = nullptr;
void record(const char* value) {
    if (marker) std::ofstream(marker, std::ios::app) << value << '\n';
}
struct Sentinel {
    const char* name;
    ~Sentinel() { record(name); }
} global_sentinel{"global"};

struct FakeRun {
    std::string fault;
    ~FakeRun() { record("run"); }
    void start() {
        if (fault == "start") throw std::runtime_error("launch failed after submission");
    }
    int wait() {
        if (fault == "wait") throw std::runtime_error("in-flight wait failed");
        if (fault == "unknown") throw 1;
        return fault == "terminal" ? 2 : 0;
    }
};

int main(int argc, char** argv) {
    if (argc != 3) return 90;
    marker = argv[2];
    const std::string mode = argv[1];
    try {
        Sentinel stack{"stack"};
        if (mode == "prelaunch") throw std::runtime_error("invalid input before launch");
        const auto separator = mode.find('-');
        if (mode.substr(0, separator) == "async") {
            auto* run = new FakeRun{mode.substr(separator + 1)};
            iron_run_guard::invoke([&] { run->start(); });
            const auto state = iron_run_guard::wait([&] { return run->wait(); }, 0);
            delete run;
            return state == 0 ? 0 : 93;
        }
        FakeRun run{mode.substr(separator + 1)};
        iron_run_guard::invoke([&] { run.start(); });
        const auto state = iron_run_guard::wait([&] { return run.wait(); }, 0);
        return state == 0 ? 0 : 93;
    } catch (...) {
        return 92;
    }
}
'''


def msvc_environment():
    """Find the installed x64 compiler and SDK without changing machine state."""
    environment = os.environ.copy()
    compiler = shutil.which('cl.exe')
    if compiler and environment.get('INCLUDE') and environment.get('LIB'):
        return compiler, environment
    program_files = Path(os.environ.get('ProgramFiles(x86)', r'C:\Program Files (x86)'))
    vswhere = program_files / 'Microsoft Visual Studio/Installer/vswhere.exe'
    installation = Path(subprocess.check_output([
        str(vswhere), '-latest', '-products', '*', '-requires',
        'Microsoft.VisualStudio.Component.VC.Tools.x86.x64', '-property', 'installationPath',
    ], text=True).strip())
    version = lambda path: tuple(int(part) for part in path.name.split('.'))
    tools = max((installation / 'VC/Tools/MSVC').iterdir(), key=version)
    sdk = program_files / 'Windows Kits/10'
    sdk_version = max((sdk / 'Include').iterdir(), key=version).name
    binary = tools / 'bin/Hostx64/x64'
    environment['PATH'] = str(binary) + os.pathsep + environment.get('PATH', '')
    environment['INCLUDE'] = os.pathsep.join(str(path) for path in [
        tools / 'include', *(sdk / 'Include' / sdk_version / name for name in ['ucrt', 'shared', 'um']),
    ])
    environment['LIB'] = os.pathsep.join(str(path) for path in [
        tools / 'lib/x64', *(sdk / 'Lib' / sdk_version / name / 'x64' for name in ['ucrt', 'um']),
    ])
    return str(binary / 'cl.exe'), environment


@unittest.skipUnless(os.name == 'nt', 'Windows forced retirement contract')
class DispatchGuardTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary = tempfile.TemporaryDirectory(prefix='winsearch-dispatch-guard-')
        cls.addClassCleanup(cls.temporary.cleanup)
        cls.directory = Path(cls.temporary.name)
        source = cls.directory / 'child.cpp'
        source.write_text(CHILD, encoding='utf-8')
        cls.binary = cls.directory / 'child.exe'
        compiler, environment = msvc_environment()
        command = [compiler, '/nologo', '/std:c++17', '/EHsc', '/MT', '/W4',
                   '/I' + str(SHIM), str(source), '/Fe' + str(cls.binary)]
        if os.environ.get('WINSEARCH_TEST_UNPROTECTED') == '1':
            command.append('/DWINSEARCH_TEST_UNPROTECTED')
        result = subprocess.run(command, cwd=cls.directory, env=environment,
                                capture_output=True, text=True, timeout=60)
        if result.returncode:
            raise RuntimeError(result.stdout + result.stderr)

    def child(self, case):
        marker = self.directory / (case + '.teardown')
        started = time.monotonic()
        result = subprocess.run([str(self.binary), case, str(marker)], timeout=5)
        self.assertLess(time.monotonic() - started, 5)
        return result.returncode, marker.read_text() if marker.exists() else ''

    def test_native_failures_retire_before_stack_run_or_global_destructors(self):
        for route in ['sync', 'async']:
            for fault in ['start', 'wait', 'terminal', 'unknown']:
                with self.subTest(route=route, fault=fault):
                    code, teardown = self.child(route + '-' + fault)
                    self.assertEqual(code, 75)
                    self.assertEqual(teardown, '')

    def test_success_preserves_normal_native_teardown(self):
        for route in ['sync', 'async']:
            with self.subTest(route=route):
                code, teardown = self.child(route + '-success')
                self.assertEqual(code, 0)
                self.assertEqual(teardown.splitlines(), ['run', 'stack', 'global'])

    def test_prelaunch_validation_remains_recoverable(self):
        code, teardown = self.child('prelaunch')
        self.assertEqual(code, 92)
        self.assertEqual(teardown.splitlines(), ['stack', 'global'])


if __name__ == '__main__':
    unittest.main()
