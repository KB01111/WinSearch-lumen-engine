# Pinned decoder redistribution

Binary pin, archive SHA-256 and required shared DLLs: ffmpeg.json.
This is BtbN's Windows x64 LGPL shared 9.0 build. Its archive LICENSE.txt
contains LGPLv3; retain it and the accompanying GPLv3 text. The extraction
worker invokes the executables with argument arrays; it does not link a
Rust binding or modify the libraries. Do not prohibit replacement or reverse
engineering of these components.

Source revisions verified through the projects' GitHub APIs:

- FFmpeg: https://github.com/FFmpeg/FFmpeg/tree/2a571b606854520cf89804d8030c8b328e621689
- Build recipes, patches and dependency scripts:
  https://github.com/BtbN/FFmpeg-Builds/tree/6c9aec5fc9a72ec3abedd1fa84db141fa18cf52b
- Build target/variant/addin: win64 lgpl-shared 9.0; the recipe's build.sh and
  makeimage.sh take separate arguments "win64", "lgpl-shared", "9.0".
- Provider: https://github.com/BtbN/FFmpeg-Builds
- Upstream license guidance: https://ffmpeg.org/legal.html

The final release assembly must retain this document, the binary's exact
-version/-buildconf output, all relevant dependency licenses, corresponding
source and patches/build instructions for the distributed binary and its
bundled libraries. Include a downloadable corresponding-source archive next
to any published binary. These upstream links alone do not establish that
release gate. Public binary/source publication is a separate release action.

The selected monthly asset is retained for two years under the provider's
retention policy; daily assets last fourteen days. Cache the hash-verified
archive for reproducible packaging. Renew the explicit manifest through a
reviewed dependency change; never silently resolve "latest".
