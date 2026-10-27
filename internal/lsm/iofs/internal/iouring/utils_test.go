//go:build linux

package iouring

import (
	"reflect"
	"strings"
	"syscall"
	"testing"
	"unsafe"
)

func TestSockaddrRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		addr syscall.Sockaddr
		size uint32
	}{
		{"ipv4", &syscall.SockaddrInet4{Port: 12345, Addr: [4]byte{127, 0, 0, 1}}, syscall.SizeofSockaddrInet4},
		{"ipv6", &syscall.SockaddrInet6{Port: 65535, ZoneId: 7, Addr: [16]byte{15: 1}}, syscall.SizeofSockaddrInet6},
		{"unix", &syscall.SockaddrUnix{Name: "/tmp/socket"}, 14},
		{"abstract", &syscall.SockaddrUnix{Name: "@socket"}, 9},
		{"abstract-max", &syscall.SockaddrUnix{Name: "@" + strings.Repeat("x", 107)}, 110},
		{"netlink", &syscall.SockaddrNetlink{Family: syscall.AF_NETLINK, Pid: 42, Groups: 3}, syscall.SizeofSockaddrNetlink},
		{"packet", &syscall.SockaddrLinklayer{Ifindex: 2, Protocol: 8, Halen: 6, Addr: [8]byte{1, 2, 3, 4, 5, 6}}, syscall.SizeofSockaddrLinklayer},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ptr, size, err := sockaddr(tc.addr)
			if err != nil || size != tc.size {
				t.Fatalf("sockaddr: size=%d err=%v; want %d", size, err, tc.size)
			}
			// Copy into a full-sized buffer before decoding, as the kernel does.
			var raw syscall.RawSockaddrAny
			copy(unsafe.Slice((*byte)(unsafe.Pointer(&raw)), int(unsafe.Sizeof(raw))), unsafe.Slice((*byte)(ptr), int(size)))
			got, err := anyToSockaddr(&raw)
			if err != nil || !reflect.DeepEqual(got, tc.addr) {
				t.Fatalf("round trip: got %#v, %v; want %#v", got, err, tc.addr)
			}
		})
	}
}

func TestSockaddrInvalidInput(t *testing.T) {
	for _, addr := range []syscall.Sockaddr{
		nil,
		&syscall.SockaddrInet4{Port: -1},
		&syscall.SockaddrInet6{Port: 65536},
		&syscall.SockaddrUnix{Name: strings.Repeat("x", 108)},
		&syscall.SockaddrUnix{Name: "@" + strings.Repeat("x", 108)},
		&syscall.SockaddrLinklayer{Ifindex: -1},
	} {
		ptr, size, err := sockaddr(addr)
		if ptr != nil || size != 0 || err != syscall.EINVAL {
			t.Errorf("sockaddr(%#v) = %v, %d, %v", addr, ptr, size, err)
		}
	}
	var raw syscall.RawSockaddrAny
	if _, err := anyToSockaddr(&raw); err != syscall.EAFNOSUPPORT {
		t.Fatalf("unsupported family: %v", err)
	}
}
