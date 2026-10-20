# Microsoft C++ runtime in the engine-only payload

The engine and llama.cpp DLLs import the Microsoft C++ v14 runtime. The
engine-only assembler retains the complete **release x64 CRT** file set from the
pinned Microsoft Visual Studio redistributable VSIX in `msvc-runtime.json`.
It verifies the archive hash before reading any member. Debug/nonredistributable
files, ARM64 files, installers and build tools are excluded. Assembly does not
install or update the machine's runtime.

The pin comes from the Microsoft Visual Studio package metadata for
`Microsoft.VC.14.51.CRT.Redist.X64.base`, version `14.51.36231`. The explicit input
may be that package's cached `payload.vsix`, or the identical archive downloaded
from the immutable Microsoft URL recorded in the pin. Do not copy arbitrary
DLLs from `System32`, a different application, or the developer PATH.

These binaries are Microsoft software, subject to the applicable Visual Studio
license and redistribution terms. Preserve this notice with the payload.
Redistribution eligibility and final release licensing remain part of the
release review; a successful local package test does not establish that review.

Official references:

- [Redistribute Visual C++ files](https://learn.microsoft.com/en-us/cpp/windows/redistributing-visual-cpp-files)
- [Deployment methods and application-local DLLs](https://learn.microsoft.com/en-us/cpp/windows/choosing-a-deployment-method)
- [Supported runtime versions and toolset compatibility](https://learn.microsoft.com/en-us/cpp/windows/latest-supported-vc-redist)

Application-local runtime security updates require rebuilding, verifying and
shipping a new pinned payload. Windows 11 supplies the Universal CRT and Windows
API libraries; they are not copied into this package.
