//go:build linux

package iouring

import (
	"runtime"
	"syscall"
	"testing"
	"unsafe"

	iouring_syscall "lsmengine/internal/lsm/iofs/internal/iouring/syscall"
)

func TestConnectRetainsRawAddress(t *testing.T) {
	data := &UserData{request: &request{}}
	// Drop the preparation closure before GC, as happens after submission.
	func() {
		prep, err := Connect(3, &syscall.SockaddrInet4{Port: 12345, Addr: [4]byte{127, 0, 0, 1}})
		if err != nil {
			t.Fatal(err)
		}
		prep(&iouring_syscall.SubmissionQueueEntry64{}, data)
	}()
	runtime.GC()
	var raw unsafe.Pointer
	for _, held := range data.holds {
		if ptr, ok := held.(unsafe.Pointer); ok {
			raw = ptr
		}
	}
	if raw == nil {
		t.Fatal("Connect did not retain raw sockaddr until completion")
	}
	addr := (*syscall.RawSockaddrInet4)(raw)
	port := (*[2]byte)(unsafe.Pointer(&addr.Port))
	if addr.Family != syscall.AF_INET || addr.Addr != [4]byte{127, 0, 0, 1} || *port != [2]byte{0x30, 0x39} {
		t.Fatalf("retained address changed: %+v", addr)
	}
	runtime.KeepAlive(data)
}
