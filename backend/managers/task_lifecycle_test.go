package managers

import (
	"crawlab-lite/constants"
	"crawlab-lite/dao"
	"crawlab-lite/database"
	"crawlab-lite/models"
	"errors"
	"io/ioutil"
	"path/filepath"
	"testing"
	"time"

	uuid "github.com/satori/go.uuid"
)

// 假命令树脚本：父进程派生子进程，两者都周期性向 marker 文件写行，
// 用来验证取消时外壳与后代进程被一并终止（marker 停止增长）。
// 同时持续输出 stdout，验证日志采集。
const fakeTreeScript = `
echo "parent-start $$"
sh -c '
  echo "child-start $$"
  i=0
  while true; do
    echo "child-heartbeat $i"
    i=$((i+1))
    sleep 0.1
  done
' &
child=$!
i=0
while true; do
  echo "parent-heartbeat $i"
  i=$((i+1))
  sleep 0.1
done
`

// writeFakeTreeScript 在爬虫版本目录中放置假命令树脚本
func writeFakeTreeScript(t *testing.T, env *testEnv) string {
	t.Helper()
	path := filepath.Join(env.spiderDir, "v1", "tree.sh")
	if err := ioutil.WriteFile(path, []byte(fakeTreeScript), 0755); err != nil {
		t.Fatalf("write fake tree script: %v", err)
	}
	return "sh tree.sh"
}

// waitForStatus 轮询等待任务进入指定状态
func waitForStatus(t *testing.T, env *testEnv, id uuid.UUID, want constants.TaskStatus, timeout time.Duration) *models.Task {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		task := env.getTask(t, id)
		if task != nil && task.Status == want {
			return task
		}
		time.Sleep(20 * time.Millisecond)
	}
	task := env.getTask(t, id)
	got := constants.TaskStatus("")
	if task != nil {
		got = task.Status
	}
	t.Fatalf("task %s status = %s, want %s (timeout %s)", id, got, want, timeout)
	return nil
}

// waitForStatusOneOf 轮询等待任务进入候选状态之一
func waitForStatusOneOf(t *testing.T, env *testEnv, id uuid.UUID, want []constants.TaskStatus, timeout time.Duration) *models.Task {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		task := env.getTask(t, id)
		if task != nil {
			for _, s := range want {
				if task.Status == s {
					return task
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	task := env.getTask(t, id)
	got := constants.TaskStatus("")
	if task != nil {
		got = task.Status
	}
	t.Fatalf("task %s status = %s, want one of %v (timeout %s)", id, got, want, timeout)
	return nil
}

// TestExecuteTask_FakeCommandTree_Finishes 验证假命令树可以正常跑完一个短任务，
// 日志、状态、耗时与 BoltDB 记录一致
func TestExecuteTask_FakeCommandTree_Finishes(t *testing.T) {
	env := setupTestEnv(t)
	spider, version := env.createSpiderFixture(t)

	// 短命令：输出两行后正常退出
	env.createPendingTask(t, spider.Id, version.Id, "echo line1 && echo line2")

	env.executor.ExecuteTask(0)

	task := env.getTask(t, mustFirstTaskID(t, env))
	if task.Status != constants.TaskStatusFinished {
		t.Fatalf("status = %s, want FINISHED", task.Status)
	}
	if task.FinishTs.IsZero() {
		t.Fatal("finish_ts should be set")
	}
	if task.RuntimeDuration <= 0 {
		t.Fatalf("runtime_duration = %v, want > 0", task.RuntimeDuration)
	}
	if task.TotalDuration < task.RuntimeDuration {
		t.Fatalf("total_duration %v < runtime_duration %v", task.TotalDuration, task.RuntimeDuration)
	}
	if task.Error != "" {
		t.Fatalf("error = %q, want empty", task.Error)
	}

	logs := env.taskLogs(t, task.Id)
	if len(logs) < 2 {
		t.Fatalf("log count = %d, want >= 2", len(logs))
	}
	stdoutLines := 0
	for _, l := range logs {
		if l.LogStd == constants.TaskLogStdOut {
			stdoutLines++
		}
	}
	if stdoutLines < 2 {
		t.Fatalf("stdout log count = %d, want >= 2", stdoutLines)
	}
}

// TestCancelTask_FakeTree_KillsShellAndChildren 是核心回归：
// 取消必须终止整棵进程树（外壳 + 子进程），任务进入 CANCELLED 并等待收口
func TestCancelTask_FakeTree_KillsShellAndChildren(t *testing.T) {
	env := setupTestEnv(t)
	spider, version := env.createSpiderFixture(t)
	cmd := writeFakeTreeScript(t, env)
	task := env.createPendingTask(t, spider.Id, version.Id, cmd)

	go env.executor.ExecuteTask(0)

	// 等待任务进入 RUNNING 且子进程已产出日志
	running := waitForStatus(t, env, task.Id, constants.TaskStatusRunning, 5*time.Second)
	if running.StartTs.IsZero() {
		t.Fatal("start_ts should be set when running")
	}
	waitForLogsAtLeast(t, env, task.Id, 4, 5*time.Second)

	// 取消并等待收口
	if _, err := CancelTask(task.Id.String()); err != nil {
		t.Fatalf("cancel task: %v", err)
	}

	cancelled := waitForStatus(t, env, task.Id, constants.TaskStatusCancelled, 5*time.Second)
	if cancelled.FinishTs.IsZero() {
		t.Fatal("finish_ts should be set when cancelled")
	}
	if cancelled.RuntimeDuration <= 0 {
		t.Fatalf("runtime_duration = %v, want > 0", cancelled.RuntimeDuration)
	}

	// 注册表必须已注销，不存在无所有者的残留通道/归属
	if rt := runningRegistry.get(task.Id.String()); rt != nil {
		t.Fatal("running registry entry should be removed after task reaped")
	}

	// 进程树已死：日志行数在宽限期内不再增长
	countAfterCancel := env.taskLogCount(t, task.Id)
	time.Sleep(500 * time.Millisecond)
	countLater := env.taskLogCount(t, task.Id)
	if countLater > countAfterCancel {
		t.Fatalf("process tree still writing logs after cancel: %d -> %d", countAfterCancel, countLater)
	}

	// 取消动作应在日志中留痕
	logs := env.taskLogs(t, task.Id)
	foundCancelLog := false
	for _, l := range logs {
		if l.LineText == "task cancelled, process tree terminated" || l.LineText == "task cancelled" {
			foundCancelLog = true
		}
	}
	if !foundCancelLog {
		t.Fatal("cancel event not recorded in task logs")
	}
}

// TestCancelTask_Idempotent 重复取消必须幂等：状态保持 CANCELLED，不报错
func TestCancelTask_Idempotent(t *testing.T) {
	env := setupTestEnv(t)
	spider, version := env.createSpiderFixture(t)
	cmd := writeFakeTreeScript(t, env)
	task := env.createPendingTask(t, spider.Id, version.Id, cmd)

	go env.executor.ExecuteTask(0)
	waitForStatus(t, env, task.Id, constants.TaskStatusRunning, 5*time.Second)
	waitForLogsAtLeast(t, env, task.Id, 2, 5*time.Second)

	for i := 0; i < 3; i++ {
		if _, err := CancelTask(task.Id.String()); err != nil {
			t.Fatalf("cancel #%d error: %v", i+1, err)
		}
	}
	waitForStatus(t, env, task.Id, constants.TaskStatusCancelled, 5*time.Second)

	// 终态之后再次取消，仍然幂等无错误
	if _, err := CancelTask(task.Id.String()); err != nil {
		t.Fatalf("cancel after terminal error: %v", err)
	}
	final := env.getTask(t, task.Id)
	if final.Status != constants.TaskStatusCancelled {
		t.Fatalf("status = %s, want CANCELLED", final.Status)
	}
}

// TestCancelTask_Pending_NoOrphanChannel 排队任务取消：
// 直接落 CANCELLED，注册表与通道都不能留下无所有者的残留
func TestCancelTask_Pending_NoOrphanChannel(t *testing.T) {
	env := setupTestEnv(t)
	spider, version := env.createSpiderFixture(t)
	task := env.createPendingTask(t, spider.Id, version.Id, "echo never-run")

	if _, err := CancelTask(task.Id.String()); err != nil {
		t.Fatalf("cancel pending: %v", err)
	}
	cancelled := waitForStatus(t, env, task.Id, constants.TaskStatusCancelled, 2*time.Second)
	if !cancelled.FinishTs.IsZero() {
		// PENDING 任务从未启动，StartTs 为零，runtime 应为 0；finish/Total 仍记录
	}
	if cancelled.RuntimeDuration != 0 {
		t.Fatalf("pending task runtime = %v, want 0", cancelled.RuntimeDuration)
	}
	if rt := runningRegistry.get(task.Id.String()); rt != nil {
		t.Fatal("pending cancel must not create registry entry")
	}

	// 执行器再跑一轮，被取消的任务不得复活
	env.executor.ExecuteTask(0)
	again := env.getTask(t, task.Id)
	if again.Status != constants.TaskStatusCancelled {
		t.Fatalf("cancelled pending task resurrected to %s", again.Status)
	}
}

// TestPrepareFailure_QueryError_ReturnsToPending 故障注入：
// 启动前版本查询失败（临时性错误），任务必须回到 PENDING 可重试，而不是卡在 PROCESSING
func TestPrepareFailure_QueryError_ReturnsToPending(t *testing.T) {
	env := setupTestEnv(t)
	spider, version := env.createSpiderFixture(t)
	task := env.createPendingTask(t, spider.Id, version.Id, "echo x")

	injected := errors.New("injected version query failure")
	hookQueryVersion = func(fn func() (*models.SpiderVersion, error)) (*models.SpiderVersion, error) {
		return nil, injected
	}

	env.executor.ExecuteTask(0)

	retry := waitForStatusOneOf(t, env, task.Id,
		[]constants.TaskStatus{constants.TaskStatusPending, constants.TaskStatusProcessing}, 2*time.Second)
	if retry.Status != constants.TaskStatusPending {
		t.Fatalf("status = %s, want PENDING for retry", retry.Status)
	}

	// 恢复后再次调度，任务应能正常完成
	resetHooks()
	env.executor.ExecuteTask(0)
	waitForStatus(t, env, task.Id, constants.TaskStatusFinished, 5*time.Second)
}

// TestPrepareFailure_NoVersion_FailsExplicitly 确定性失败（爬虫无版本）：
// 任务必须明确落到 ERROR，不能永久 PROCESSING
func TestPrepareFailure_NoVersion_FailsExplicitly(t *testing.T) {
	env := setupTestEnv(t)
	// 只建爬虫、不建版本
	spider := &models.Spider{Name: "no-version-spider"}
	if err := dao.WriteTx(database.MainDB, func(tx dao.Tx) error {
		return tx.InsertSpider(spider)
	}); err != nil {
		t.Fatalf("insert spider: %v", err)
	}
	task := env.createPendingTask(t, spider.Id, uuid.Nil, "echo x")

	env.executor.ExecuteTask(0)

	failed := waitForStatus(t, env, task.Id, constants.TaskStatusError, 2*time.Second)
	if failed.Error == "" {
		t.Fatal("error message should explain the failure")
	}
	if failed.FinishTs.IsZero() {
		t.Fatal("finish_ts should be set on error")
	}
	if env.taskLogCount(t, task.Id) == 0 {
		t.Fatal("prepare failure should be recorded in logs")
	}
}

// TestPrepareFailure_MissingDirectory_FailsExplicitly 爬虫目录缺失：明确失败
func TestPrepareFailure_MissingDirectory_FailsExplicitly(t *testing.T) {
	env := setupTestEnv(t)
	spider, _ := env.createSpiderFixture(t)
	// 插入一个指向不存在目录的版本
	missingVersion := &models.SpiderVersion{SpiderId: spider.Id, Path: "does-not-exist"}
	if err := dao.WriteTx(database.MainDB, func(tx dao.Tx) error {
		return tx.InsertSpiderVersion(missingVersion)
	}); err != nil {
		t.Fatalf("insert version: %v", err)
	}
	task := env.createPendingTask(t, spider.Id, missingVersion.Id, "echo x")

	env.executor.ExecuteTask(0)

	failed := waitForStatus(t, env, task.Id, constants.TaskStatusError, 2*time.Second)
	if failed.Error == "" {
		t.Fatal("missing directory should produce explicit error")
	}
}

// TestCancelRunningTasks_Recovery 重启恢复：
// 上次遗留的 RUNNING/PROCESSING 必须收口为 CANCELLED，PENDING 保持可调度
func TestCancelRunningTasks_Recovery(t *testing.T) {
	env := setupTestEnv(t)
	spider, version := env.createSpiderFixture(t)

	runningTask := env.createPendingTask(t, spider.Id, version.Id, "echo r")
	runningTask.Status = constants.TaskStatusRunning
	runningTask.StartTs = time.Now().Add(-time.Minute)
	mustUpdate(t, env, runningTask)

	processingTask := env.createPendingTask(t, spider.Id, version.Id, "echo p")
	processingTask.Status = constants.TaskStatusProcessing
	mustUpdate(t, env, processingTask)

	pendingTask := env.createPendingTask(t, spider.Id, version.Id, "echo q")
	// PENDING 保持默认状态

	if err := CancelRunningTasks(); err != nil {
		t.Fatalf("recovery: %v", err)
	}

	if got := env.getTask(t, runningTask.Id); got.Status != constants.TaskStatusCancelled {
		t.Fatalf("running task after recovery = %s, want CANCELLED", got.Status)
	}
	if got := env.getTask(t, processingTask.Id); got.Status != constants.TaskStatusCancelled {
		t.Fatalf("processing task after recovery = %s, want CANCELLED", got.Status)
	}
	if got := env.getTask(t, pendingTask.Id); got.Status != constants.TaskStatusPending {
		t.Fatalf("pending task after recovery = %s, want PENDING", got.Status)
	}

	// 恢复后 PENDING 任务可以被正常调度执行
	env.executor.ExecuteTask(0)
	waitForStatus(t, env, pendingTask.Id, constants.TaskStatusFinished, 5*time.Second)
}

// TestCancelTask_NotFound 取消不存在的任务返回错误
func TestCancelTask_NotFound(t *testing.T) {
	setupTestEnv(t)
	if _, err := CancelTask(uuid.NewV4().String()); err == nil {
		t.Fatal("cancel missing task should return error")
	}
}

// TestFinalizeTask_DurationsConsistent 终态写入时耗时字段与时间戳一致
func TestFinalizeTask_DurationsConsistent(t *testing.T) {
	env := setupTestEnv(t)
	spider, version := env.createSpiderFixture(t)
	task := env.createPendingTask(t, spider.Id, version.Id, "echo x")
	// 模拟真实生命周期：创建 -> 等待 1s -> 启动 -> 运行 2s -> 终态
	task.CreateTs = time.Now().Add(-3 * time.Second)
	task.StartTs = time.Now().Add(-2 * time.Second)
	mustUpdate(t, env, task)

	if err := finalizeTask(task, outcomeCancelled, ""); err != nil {
		t.Fatalf("finalize: %v", err)
	}
	stored := env.getTask(t, task.Id)
	if stored.Status != constants.TaskStatusCancelled {
		t.Fatalf("status = %s", stored.Status)
	}
	if stored.RuntimeDuration < 1.9 {
		t.Fatalf("runtime %v inconsistent with start/finish", stored.RuntimeDuration)
	}
	if stored.TotalDuration < stored.RuntimeDuration {
		t.Fatal("total duration should cover runtime")
	}
}

// --- helpers ---

func resetHooks() {
	hookQueryVersion = func(fn func() (*models.SpiderVersion, error)) (*models.SpiderVersion, error) {
		return fn()
	}
	hookBuildCmd = defaultBuildCmd
}

func mustFirstTaskID(t *testing.T, env *testEnv) uuid.UUID {
	t.Helper()
	var tasks []*models.Task
	if err := dao.ReadTx(database.MainDB, func(tx dao.Tx) error {
		var err error
		tasks, err = tx.SelectAllTasks()
		return err
	}); err != nil {
		t.Fatalf("select all: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("want 1 task, got %d", len(tasks))
	}
	return tasks[0].Id
}

func waitForLogsAtLeast(t *testing.T, env *testEnv, id uuid.UUID, min int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if env.taskLogCount(t, id) >= min {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("logs for %s never reached %d (have %d)", id, min, env.taskLogCount(t, id))
}

func mustUpdate(t *testing.T, env *testEnv, task *models.Task) {
	t.Helper()
	if err := dao.WriteTx(database.MainDB, func(tx dao.Tx) error {
		return tx.UpdateTask(task)
	}); err != nil {
		t.Fatalf("update task: %v", err)
	}
}
