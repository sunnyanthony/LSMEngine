# Internal io_uring dependency

This is third-party code, not an LSMEngine implementation. It is kept inside
the IO adapter so dependency types cannot escape the filesystem boundary.

Source: `github.com/iceber/iouring-go` at
`002cfd2e2a9021c13192f3bd4d9023374e38c93f`, with the `utils.go` compatibility
patch from `github.com/royalcat/iouring-go` at
`286062ac1b239b234ff900720ba925e7693dd609` (upstream pull request 31).
The patch replaces private syscall linknames rejected by Go 1.23 with explicit
socket address conversions. Those conversions derive from the Go standard
library; see LICENSE-Go. The upstream library's MIT license is in LICENSE.

Only root production Go files and the syscall subpackage are copied. Internal
imports are relocated and files are formatted with gofmt. Upstream examples,
module files and tests are not copied;
LSMEngine maintains adapter and compatibility tests separately. Do not expose
this package as a new public API or add unrelated upstream changes here.

Local correction: Connect retains the separately allocated raw socket address
until request completion; retaining only the original Sockaddr is insufficient.

Additional local corrections use syscall.Mmap-owned byte slices and
unsafe.Add/Slice for queue views instead of uintptr/reflect.SliceHeader
conversions. Shared mappings cover the larger of SQ and CQ. Partial mapping
initialization is cleaned up, including closing the ring descriptor on failure.
Reserved ABI fields remain as blank padding; an unused unregister helper and
an impossible unsigned-negative syscall result check were removed.

Repeated real-ring tests need adequate locked-memory allowance on older kernels.
On the development Linux arm64 Docker host, 100 strict read/write iterations
passed with an 8 MiB container memlock limit; the default 64 KiB limit produced
intermittent ENOMEM. ENOMEM is not treated as a passing test or skip.

A module-level replace was deliberately avoided: consumers of LSMEngine do not
inherit replacement directives. Keeping this compatibility copy internal also
avoids disabling the Go linker's checks or removing the Linux backend.

When updating, compare against the pinned sources, retain both license notices,
and verify Linux linking, address conversion tests, actual backend IO where
the kernel permits it, and compilation from a separate consuming module.
