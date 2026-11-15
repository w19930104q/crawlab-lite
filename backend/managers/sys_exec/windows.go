//go:build windows
// +build windows

package sys_exec

import (
	"os/exec"
	"strconv"
)

func BuildCmd(cmdStr string) *exec.Cmd {
	return exec.Command("cmd", "/C", cmdStr)
}

// Setpgid 在 Windows 上通过 CREATE_NEW_PROCESS_GROUP 让任务命令成为
// 独立进程组的根，后续 taskkill /T 才能沿进程树终止其全部后代，
// 而不是只杀掉 cmd.exe 外壳、留下继续写结果的脚本子进程。
func Setpgid(cmd *exec.Cmd) {
	if cmd == nil {
		return
	}
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = sysProcAttrNewProcessGroup()
	} else {
		setNewProcessGroup(cmd.SysProcAttr)
	}
}

// KillProcess 终止 cmd 的整棵进程树（/T）并强制结束（/F）。
// 进程已经退出时 taskkill 会返回错误，此处将其视为成功，保证重复取消幂等。
func KillProcess(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	pid := cmd.Process.Pid
	kill := exec.Command("taskkill", "/F", "/T", "/PID", strconv.Itoa(pid))
	if err := kill.Run(); err != nil {
		if isProcessAlreadyGone(err) {
			return nil
		}
		return err
	}
	return nil
}
