//go:build windows

package workerhost

import (
	"log/slog"
	"os/exec"
	"syscall"
	"time"
)

func sysProcAttr() *syscall.SysProcAttr { return &syscall.SysProcAttr{} }

// Windows has no SIGTERM for child processes; dev-only fallback.
func terminateProcess(cmd *exec.Cmd, exited <-chan error, grace time.Duration, log *slog.Logger, why string) {
	if cmd.Process == nil {
		return
	}
	log.Info("stopping worker", "pid", cmd.Process.Pid, "why", why)
	_ = cmd.Process.Kill()
	<-exited
}
