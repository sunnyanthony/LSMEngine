//go:build linux

package iouring

import (
	"errors"
	"syscall"
	"testing"
)

func TestNotificationFailureCleansUnstartedRing(t *testing.T) {
	for _, stage := range []string{"event-create", "event-register", "poller-register"} {
		t.Run(stage, func(t *testing.T) {
			fd, err := syscall.Open("/dev/null", syscall.O_RDONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			iour := &IOURing{fd: fd, eventfd: -1, closed: make(chan struct{})}
			t.Cleanup(func() { _ = iour.closeUnstarted() })
			sentinel := errors.New("injected " + stage)
			eventFD := -1
			err = iour.initializeNotifications(func() error {
				if stage == "event-create" {
					return sentinel
				}
				var err error
				eventFD, err = syscall.Open("/dev/null", syscall.O_RDONLY, 0)
				if err != nil {
					return err
				}
				iour.eventfd = eventFD
				if stage == "event-register" {
					return sentinel
				}
				return nil
			}, func() error { return sentinel })
			if !errors.Is(err, sentinel) {
				t.Fatalf("lost initialization error: %v", err)
			}
			for _, closedFD := range []int{fd, eventFD} {
				if closedFD < 0 {
					continue
				}
				var stat syscall.Stat_t
				if err := syscall.Fstat(closedFD, &stat); err != syscall.EBADF {
					t.Fatalf("descriptor %d not closed: %v", closedFD, err)
				}
			}
		})
	}
}
