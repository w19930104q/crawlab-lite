//go:build windows
// +build windows

package sys_exec

import (
	"os/exec"
	"syscall"
)

// CREATE_NEW_PROCESS_GROUP，见 Windows API 文档
const createNewProcessGroup = 0x00000200

func sysProcAttrNewProcessGroup() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{CreationFlags: createNewProcessGroup}
}

func setNewProcessGroup(attr *syscall.SysProcAttr) {
	attr.CreationFlags |= createNewProcessGroup
}

// isProcessAlreadyGone 判断 taskkill 失败是否因为进程已经退出。
// taskkill 对不存在的 PID 固定返回退出码 128，与系统语言无关。
func isProcessAlreadyGone(err error) bool {
	if exitErr, ok := err.(*exec.ExitError); ok {
		return exitErr.ExitCode() == 128
	}
	return false
}
