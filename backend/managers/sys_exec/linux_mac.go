//go:build !windows
// +build !windows

package sys_exec

import (
	"os/exec"
	"syscall"
	"time"
)

// termGracePeriod 是发送 SIGTERM 后等待进程组自行退出的宽限期
const termGracePeriod = 3 * time.Second

// pollInterval 是轮询进程是否退出的间隔
const pollInterval = 50 * time.Millisecond

func BuildCmd(cmdStr string) *exec.Cmd {
	return exec.Command("sh", "-c", cmdStr)
}

func Setpgid(cmd *exec.Cmd) {
	if cmd == nil {
		return
	}
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	} else {
		cmd.SysProcAttr.Setpgid = true
	}
}

// KillProcess 终止任务的整个进程组（Setpgid 保证后代与外壳同组）：
// 先 SIGTERM 给进程清理机会，宽限期后 SIGKILL 兜底，
// 进程组已不存在（ESRCH）视为成功，保证重复取消幂等。
func KillProcess(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	pgid := cmd.Process.Pid

	if err := syscall.Kill(-pgid, syscall.SIGTERM); err != nil && err != syscall.ESRCH {
		return err
	}
	if waitProcessExit(cmd.Process.Pid, termGracePeriod) {
		return nil
	}
	if err := syscall.Kill(-pgid, syscall.SIGKILL); err != nil && err != syscall.ESRCH {
		return err
	}
	return nil
}

// waitProcessExit 轮询进程是否已退出（signal 0 只探测、不回收，
// 回收仍由执行方的 cmd.Wait 统一完成，避免两处抢尸）。
func waitProcessExit(pid int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(pid, 0); err == syscall.ESRCH {
			return true
		}
		time.Sleep(pollInterval)
	}
	return false
}
