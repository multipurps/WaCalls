//go:build !windows

package workerhost

import (
	"log/slog"
	"os/exec"
	"syscall"
	"time"
)

// Own process group so a SIGKILL of a stuck worker also takes down anything
// it spawned, and so a Ctrl-C aimed at the manager is not delivered to
// workers behind its back (the manager shuts them down in order instead).
func sysProcAttr() *syscall.SysProcAttr { return &syscall.SysProcAttr{Setpgid: true} }

func terminateProcess(cmd *exec.Cmd, exited <-chan error, grace time.Duration, log *slog.Logger, why string) {
	if cmd.Process == nil {
		return
	}
	log.Info("stopping worker", "pid", cmd.Process.Pid, "why", why)
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
	select {
	case <-exited:
		return
	case <-time.After(grace):
	}
	log.Warn("worker ignored SIGTERM; killing", "pid", cmd.Process.Pid)
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	<-exited
}
