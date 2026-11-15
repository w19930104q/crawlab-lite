package managers

import (
	"crawlab-lite/constants"
	"crawlab-lite/managers/sys_exec"
	"os/exec"
	"sync"
	"time"

	"github.com/apex/log"
)

// reapTimeout 是取消任务后等待进程（及其后代）退出的最长时间。
// 超过该时间仍未退出时，调用方按失败处理，不会无限阻塞。
const reapTimeout = 10 * time.Second

// runningTask 记录一个正在执行（PROCESSING/RUNNING）的任务与其进程树的归属关系，
// 是取消操作能够定位并终止目标任务后代进程、并等待收口的唯一入口。
type runningTask struct {
	taskId string
	cmd    *exec.Cmd

	// cancel 保证取消信号只生效一次（重复取消幂等）
	cancelOnce sync.Once
	cancelErr  error
	cancelled  bool

	// done 在进程被 Wait 回收后关闭，取消方据此等待收口
	doneOnce sync.Once
	done     chan struct{}
}

func (rt *runningTask) markDone() {
	rt.doneOnce.Do(func() {
		close(rt.done)
	})
}

// taskRegistry 维护内存中"任务 -> 进程树"的归属关系。
// 任务进入执行流程时登记，进程被 Wait 回收后注销；
// 任务从未进入执行流程（PENDING）时这里没有记录，由数据库终态负责。
type taskRegistry struct {
	mu      sync.RWMutex
	entries map[string]*runningTask
}

var runningRegistry = &taskRegistry{
	entries: make(map[string]*runningTask),
}

// register 登记一个即将执行的任务。重复登记同一任务时返回已存在的记录。
func (r *taskRegistry) register(taskId string, cmd *exec.Cmd) *runningTask {
	r.mu.Lock()
	defer r.mu.Unlock()
	if rt, ok := r.entries[taskId]; ok {
		return rt
	}
	rt := &runningTask{
		taskId: taskId,
		cmd:    cmd,
		done:   make(chan struct{}),
	}
	r.entries[taskId] = rt
	return rt
}

func (r *taskRegistry) unregister(taskId string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.entries, taskId)
}

func (r *taskRegistry) get(taskId string) *runningTask {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.entries[taskId]
}

// cancelResult 描述一次取消请求的结果。
type cancelResult struct {
	// managed 为 true 表示任务正由本进程执行，取消信号已送达或已标记取消；
	// 为 false 表示任务不在执行流程中（排队中或已终态），仅需处理数据库终态。
	managed bool
	// processStarted 表示取消时进程已经启动，调用方应等待进程树收口
	processStarted bool
	// reaped 在进程已启动时有效，表示进程树是否已在超时时间内退出并被回收。
	reaped bool
	err    error
}

// requestCancel 请求取消目标任务：只结束该任务登记的进程及其后代，并等待进程收口。
// 对同一任务的重复取消直接返回首次结果，保证幂等。
func (r *taskRegistry) requestCancel(taskId string) cancelResult {
	rt := r.get(taskId)
	if rt == nil {
		return cancelResult{managed: false}
	}

	startedBefore := rt.cmd != nil && rt.cmd.Process != nil

	rt.cancelOnce.Do(func() {
		rt.cancelled = true
		if rt.cmd == nil || rt.cmd.Process == nil {
			// 进程尚未启动，等待执行方自行检查 cancelled 标记后退出
			return
		}
		log.Infof("task %s cancelling, killing process tree (pid=%d)", taskId, rt.cmd.Process.Pid)
		if err := sys_exec.KillProcess(rt.cmd); err != nil {
			rt.cancelErr = err
			log.Errorf("task %s kill process error: %s", taskId, err.Error())
		}
	})

	if rt.cancelErr != nil {
		return cancelResult{managed: true, processStarted: startedBefore, err: rt.cancelErr}
	}

	// 进程尚未启动时无需等待进程回收，执行方会在准备阶段检查取消标记后自行收口
	if !startedBefore {
		return cancelResult{managed: true, processStarted: false}
	}

	// 等待执行方 Wait 回收进程，避免只杀掉外壳而留下孤儿进程
	timer := time.NewTimer(reapTimeout)
	defer timer.Stop()
	select {
	case <-rt.done:
		return cancelResult{managed: true, processStarted: true, reaped: true}
	case <-timer.C:
		return cancelResult{managed: true, processStarted: true, reaped: false,
			err: errCancelReapTimeout(taskId)}
	}
}

// isCancelled 供执行方在启动进程前的各个准备阶段检查取消标记。
func (r *taskRegistry) isCancelled(taskId string) bool {
	rt := r.get(taskId)
	return rt != nil && rt.cancelled
}

func errCancelReapTimeout(taskId string) error {
	return &cancelReapTimeoutError{taskId: taskId}
}

type cancelReapTimeoutError struct {
	taskId string
}

func (e *cancelReapTimeoutError) Error() string {
	return "wait task process exit timeout: " + e.taskId
}

// isTerminalStatus 判断任务状态是否为不可再变更的终态。
func isTerminalStatus(status constants.TaskStatus) bool {
	switch status {
	case constants.TaskStatusFinished,
		constants.TaskStatusError,
		constants.TaskStatusCancelled:
		return true
	default:
		return false
	}
}
