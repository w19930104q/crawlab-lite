package managers

import (
	"bufio"
	"crawlab-lite/constants"
	"crawlab-lite/dao"
	"crawlab-lite/database"
	"crawlab-lite/models"
	"errors"
	"os/exec"
	"sync"
	"time"

	"github.com/apex/log"
)

// taskOutcome 表示一次任务执行的终态归类
type taskOutcome int

const (
	outcomeFinished  taskOutcome = iota // 进程正常退出
	outcomeError                        // 进程异常退出或启动失败
	outcomeCancelled                    // 任务被取消
	outcomeRetry                        // 调度准备阶段的临时失败，回到 PENDING 等待重试
)

// CancelRunningTasks 在服务启动时恢复上次未收口的任务：
// RUNNING/PROCESSING 一律置为 CANCELLED（原进程已随服务退出，无法再归属），
// PENDING 保持可调度状态。
func CancelRunningTasks() (err error) {
	if err := dao.WriteTx(database.MainDB, func(tx dao.Tx) error {
		tasks, err := tx.SelectAllTasks()
		if err != nil {
			return err
		}
		for _, task := range tasks {
			if task.Status == constants.TaskStatusRunning ||
				task.Status == constants.TaskStatusProcessing {
				applyCancelled(task)
				if err = tx.UpdateTask(task); err != nil {
					return err
				}
			}
			// PENDING 维持原样，等待执行器重新调度
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

// CancelTask 请求取消任务并等待进程树收口。
//   - 排队中（PENDING）：直接在数据库落 CANCELLED 终态；
//   - 执行中（PROCESSING/RUNNING）：向登记的进程树发终止信号并等待回收；
//   - 已终态：幂等返回，不做任何变更。
//
// 返回的 result 仅在数据库发生终态写入时非空。
func CancelTask(taskId string) (result *models.Task, err error) {
	// 先读当前状态。终态任务直接幂等返回。
	var task *models.Task
	if err = dao.ReadTx(database.MainDB, func(tx dao.Tx) error {
		task, err = tx.SelectTask(mustParseUUID(taskId))
		return err
	}); err != nil {
		return nil, err
	}
	if task == nil {
		return nil, errors.New("task not found")
	}
	if isTerminalStatus(task.Status) {
		return nil, nil
	}

	// 执行中的任务交给注册表：终止目标任务的进程及其后代并等待收口，
	// 终态由执行方统一写回，避免多个写者互相覆盖。
	cr := runningRegistry.requestCancel(taskId)
	if cr.err != nil {
		return nil, cr.err
	}
	if cr.managed {
		// 进程已收口，执行方可能正处在"回收后写库"的瞬间，短暂等待终态可见
		if waitTaskTerminalInDB(taskId, time.Second) {
			return nil, nil
		}
		// 兜底：执行方未能及时写库（例如进程启动前就被取消），由这里补终态
		return finalizeCancelledIfActive(taskId)
	}

	// 未进入执行流程（PENDING，或正处于 pop 与 register 之间的 PROCESSING）：
	// 直接落 CANCELLED 终态；PROCESSING 的兜底写库只允许覆盖非终态。
	return finalizeCancelledIfActive(taskId)
}

// finalizeCancelledIfActive 将仍处于非终态的任务写为 CANCELLED。
// 对已经被执行方写成其他终态的任务不做覆盖，返回 nil。
func finalizeCancelledIfActive(taskId string) (*models.Task, error) {
	var result *models.Task
	err := dao.WriteTx(database.MainDB, func(tx dao.Tx) error {
		task, err := tx.SelectTask(mustParseUUID(taskId))
		if err != nil {
			return err
		}
		if task == nil {
			return errors.New("task not found")
		}
		if isTerminalStatus(task.Status) {
			return nil
		}
		applyCancelled(task)
		if _, err := tx.UpdateTaskIfStatus(task,
			constants.TaskStatusPending,
			constants.TaskStatusProcessing,
			constants.TaskStatusRunning,
		); err != nil {
			return err
		}
		result = task
		return nil
	})
	return result, err
}

// waitTaskTerminalInDB 轮询等待任务在数据库中进入终态
func waitTaskTerminalInDB(taskId string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	id := mustParseUUID(taskId)
	for time.Now().Before(deadline) {
		var terminal bool
		_ = dao.ReadTx(database.MainDB, func(tx dao.Tx) error {
			task, err := tx.SelectTask(id)
			if err != nil {
				return err
			}
			if task != nil && isTerminalStatus(task.Status) {
				terminal = true
			}
			return nil
		})
		if terminal {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

// markRunningCAS 仅当数据库中任务仍为 PROCESSING 时才置为 RUNNING。
// 若任务已被取消（PENDING 阶段的 CANCELLED 终态），返回 updated=false，
// 执行方据此放弃启动进程。
func markRunningCAS(task *models.Task) (updated bool, err error) {
	task.StartTs = time.Now()
	task.Status = constants.TaskStatusRunning
	task.WaitDuration = task.StartTs.Sub(task.CreateTs).Seconds()
	err = dao.WriteTx(database.MainDB, func(tx dao.Tx) error {
		updated, err = tx.UpdateTaskIfStatus(task, constants.TaskStatusProcessing)
		return err
	})
	return updated, err
}

// finalizeTask 统一写回任务终态，保证状态、耗时字段与 BoltDB 记录一致。
// 终态写入带 CAS 保护：数据库中已是终态（例如取消兜底先落了 CANCELLED）时
// 不再覆盖，保证同一任务的终态严格只生效一次。
func finalizeTask(task *models.Task, outcome taskOutcome, errMsg string) error {
	finishTs := time.Now()
	task.FinishTs = finishTs

	if outcome == outcomeRetry {
		// 临时失败：回到 PENDING，不写结束时间，等待后续调度重试。
		// 仅允许覆盖 PROCESSING，避免覆盖准备阶段并发到达的 CANCELLED。
		task.Status = constants.TaskStatusPending
		if errMsg != "" {
			log.Errorf("task %s prepare failed, will retry: %s", task.Id, errMsg)
		}
		return updateTaskIfStatus(task, constants.TaskStatusProcessing)
	}

	switch outcome {
	case outcomeFinished:
		task.Status = constants.TaskStatusFinished
		task.Error = ""
	case outcomeError:
		task.Status = constants.TaskStatusError
		if errMsg != "" {
			task.Error = errMsg
		}
	case outcomeCancelled:
		applyCancelled(task)
		task.FinishTs = finishTs
	}

	if !task.StartTs.IsZero() {
		task.RuntimeDuration = task.FinishTs.Sub(task.StartTs).Seconds()
	}
	task.TotalDuration = task.FinishTs.Sub(task.CreateTs).Seconds()

	// 仅当任务尚未进入终态时写入，已被其他路径（如取消兜底）落终态则放弃覆盖
	return updateTaskIfStatus(task,
		constants.TaskStatusPending,
		constants.TaskStatusProcessing,
		constants.TaskStatusRunning,
	)
}

// updateTaskIfStatus 带状态前置条件的持久化，条件不满足时静默跳过
func updateTaskIfStatus(task *models.Task, expect ...constants.TaskStatus) error {
	return dao.WriteTx(database.MainDB, func(tx dao.Tx) error {
		_, err := tx.UpdateTaskIfStatus(task, expect...)
		return err
	})
}

// applyCancelled 把内存中的任务对象置为取消终态并计算耗时，不触碰数据库
func applyCancelled(task *models.Task) {
	task.Status = constants.TaskStatusCancelled
	task.FinishTs = time.Now()
	if !task.StartTs.IsZero() {
		task.RuntimeDuration = task.FinishTs.Sub(task.StartTs).Seconds()
	}
	task.TotalDuration = task.FinishTs.Sub(task.CreateTs).Seconds()
}

func popPendingTask() (task *models.Task, err error) {
	if err := dao.WriteTx(database.MainDB, func(tx dao.Tx) error {
		if task, err = tx.SelectFirstTaskWhereStatus(constants.TaskStatusPending); err != nil {
			return err
		}
		if task == nil {
			return nil
		}
		task.Status = constants.TaskStatusProcessing
		if err = tx.UpdateTask(task); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return task, nil
}

// taskLogWriters 管理 stdout/stderr 两个日志采集 goroutine 的生命周期，
// 进程回收后必须等待它们把管道中的残余日志全部落库，保证日志与任务终态一致。
type taskLogWriters struct {
	wg sync.WaitGroup
}

func recordTaskLog(cmd *exec.Cmd, task *models.Task) (*taskLogWriters, error) {
	// get stdout reader
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, errors.New("get stdout error: " + err.Error())
	}
	stdoutScanner := bufio.NewScanner(stdout)

	// get stderr scanner
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, errors.New("get stderr error: " + err.Error())
	}
	stderrScanner := bufio.NewScanner(stderr)

	writers := &taskLogWriters{}

	// scan stdout and record
	writers.wg.Add(1)
	go func() {
		defer writers.wg.Done()
		for stdoutScanner.Scan() {
			text := stdoutScanner.Text()
			if err := saveTaskLog(&models.TaskLog{
				TaskId:   task.Id,
				LineText: text,
				LogStd:   constants.TaskLogStdOut,
				CreateTs: time.Now(),
			}); err != nil {
				break
			}
		}
	}()

	// scan stderr and record
	writers.wg.Add(1)
	go func() {
		defer writers.wg.Done()
		for stderrScanner.Scan() {
			text := stderrScanner.Text()
			if err := saveTaskLog(&models.TaskLog{
				TaskId:   task.Id,
				LineText: text,
				LogStd:   constants.TaskLogStdErr,
				CreateTs: time.Now(),
			}); err != nil {
				break
			}
		}
	}()

	return writers, nil
}

// wait 阻塞直到两个采集 goroutine 都把管道数据写入 BoltDB
func (w *taskLogWriters) wait() {
	w.wg.Wait()
}

// appendTaskLog 追加一条由执行器自身产生的日志（取消、准备失败等），
// 让任务状态与日志记录保持一致
func appendTaskLog(task *models.Task, std constants.TaskLogStd, text string) {
	if err := saveTaskLog(&models.TaskLog{
		TaskId:   task.Id,
		LineText: text,
		LogStd:   std,
		CreateTs: time.Now(),
	}); err != nil {
		log.Errorf("task %s save log error: %s", task.Id, err.Error())
	}
}

// 保存日志
func saveTaskLog(taskLog *models.TaskLog) (err error) {
	if err := dao.WriteTx(database.LogDB, func(tx dao.Tx) error {
		if err := tx.InsertTaskLog(taskLog); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func removeTasksOlderThan(days int) (err error) {
	if err := dao.WriteTx(database.MainDB, func(tx dao.Tx) error {
		tasks, err := tx.SelectAllTasks()
		if err != nil {
			return err
		}
		if len(tasks) == 0 {
			return nil
		}
		if err := dao.WriteTx(database.LogDB, func(tx2 dao.Tx) error {
			expireDate := time.Now().AddDate(0, 0, -days)
			for _, task := range tasks {
				if task.CreateTs.Before(expireDate) {
					// 先终止仍在执行的任务进程
					if !isTerminalStatus(task.Status) {
						if _, err := CancelTask(task.Id.String()); err != nil {
							return err
						}
					}

					// 删除任务
					if err = tx.DeleteTask(task.Id); err != nil {
						return err
					}

					// 删除日志
					if err = tx2.DeleteAllTaskLogs(task.Id); err != nil {
						return err
					}
				}
			}
			return nil
		}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	return nil
}

func removeTaskLogsOlderThan(days int) (err error) {
	if err := dao.ReadTx(database.MainDB, func(tx dao.Tx) error {
		tasks, err := tx.SelectAllTasks()
		if err != nil {
			return err
		}
		if len(tasks) == 0 {
			return nil
		}
		if err := dao.WriteTx(database.LogDB, func(tx2 dao.Tx) error {
			for _, task := range tasks {
				if err := tx2.DeleteTaskLogsOlderThan(task.Id, days); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	return nil
}
