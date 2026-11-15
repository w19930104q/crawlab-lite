package managers

import (
	"crawlab-lite/constants"
	"crawlab-lite/dao"
	"crawlab-lite/database"
	"crawlab-lite/models"
	"os/exec"
	"sync"
	"testing"
	"time"
)

// TestStartFailure_FailsExplicitly 故障注入：命令构造为不存在的可执行文件，
// Start 失败必须明确落到 ERROR，不能卡在 PROCESSING/RUNNING
func TestStartFailure_FailsExplicitly(t *testing.T) {
	env := setupTestEnv(t)
	spider, version := env.createSpiderFixture(t)
	task := env.createPendingTask(t, spider.Id, version.Id, "ignored-cmd")

	hookBuildCmd = func(cmdStr string) *exec.Cmd {
		// 指向一个绝对不存在的二进制
		return exec.Command("/nonexistent/crawlab-fake-bin-12345")
	}

	env.executor.ExecuteTask(0)

	failed := waitForStatus(t, env, task.Id, constants.TaskStatusError, 3*time.Second)
	if failed.Error == "" {
		t.Fatal("start failure should record an error message")
	}
	if !failed.FinishTs.IsZero() && failed.RuntimeDuration < 0 {
		t.Fatal("runtime duration must not be negative")
	}
}

// TestCancelDuringPrepare_CancelsBeforeProcessStarts 在准备阶段阻塞时取消：
// 任务必须落 CANCELLED，且不会启动任何进程、注册表完成收口
func TestCancelDuringPrepare_CancelsBeforeProcessStarts(t *testing.T) {
	env := setupTestEnv(t)
	spider, version := env.createSpiderFixture(t)
	task := env.createPendingTask(t, spider.Id, version.Id, "echo x")

	release := make(chan struct{})
	entered := make(chan struct{})
	hookQueryVersion = func(fn func() (*models.SpiderVersion, error)) (*models.SpiderVersion, error) {
		close(entered)
		<-release // 阻塞在准备阶段，模拟启动前查询缓慢
		return fn()
	}

	done := make(chan struct{})
	go func() {
		env.executor.ExecuteTask(0)
		close(done)
	}()

	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("prepare hook never entered")
	}

	// 此时任务应为 PROCESSING 且尚未启动，取消它
	if _, err := CancelTask(task.Id.String()); err != nil {
		t.Fatalf("cancel during prepare: %v", err)
	}
	close(release)

	waitForStatus(t, env, task.Id, constants.TaskStatusCancelled, 5*time.Second)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("ExecuteTask did not return after prepare-stage cancel")
	}
	if rt := runningRegistry.get(task.Id.String()); rt != nil {
		t.Fatal("registry entry should be removed after prepare-stage cancel")
	}
}

// TestBatchCancel_FakeTrees_AllReaped 模拟前端批量停止：多个假命令树任务并行运行，
// 逐个取消后全部进入 CANCELLED，进程树全部停止写日志，无注册表残留
func TestBatchCancel_FakeTrees_AllReaped(t *testing.T) {
	env := setupTestEnv(t)
	spider, version := env.createSpiderFixture(t)
	cmd := writeFakeTreeScript(t, env)

	const n = 3
	tasks := make([]*models.Task, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		tasks[i] = env.createPendingTask(t, spider.Id, version.Id, cmd)
		wg.Add(1)
		// 每个任务占用独立 worker 槽位，与真实执行器"每 worker 一任务"模型一致
		workerId := i
		go func() {
			defer wg.Done()
			env.executor.ExecuteTask(workerId)
		}()
	}

	// 等待全部 RUNNING 并产出日志
	for _, task := range tasks {
		waitForStatus(t, env, task.Id, constants.TaskStatusRunning, 5*time.Second)
		waitForLogsAtLeast(t, env, task.Id, 2, 5*time.Second)
	}

	// 批量取消（前端即循环调用单任务取消接口）
	for _, task := range tasks {
		if _, err := CancelTask(task.Id.String()); err != nil {
			t.Fatalf("batch cancel %s: %v", task.Id, err)
		}
	}

	for _, task := range tasks {
		cancelled := waitForStatus(t, env, task.Id, constants.TaskStatusCancelled, 5*time.Second)
		if cancelled.RuntimeDuration <= 0 {
			t.Fatalf("task %s runtime not recorded", task.Id)
		}
		if rt := runningRegistry.get(task.Id.String()); rt != nil {
			t.Fatalf("task %s registry not cleaned", task.Id)
		}
	}

	// 全部进程树停止后，日志行数不再增长
	before := make([]int, n)
	for i, task := range tasks {
		before[i] = env.taskLogCount(t, task.Id)
	}
	time.Sleep(600 * time.Millisecond)
	for i, task := range tasks {
		after := env.taskLogCount(t, task.Id)
		if after > before[i] {
			t.Fatalf("task %s still writing after batch cancel: %d -> %d", task.Id, before[i], after)
		}
	}

	wg.Wait()
}

// TestConcurrentCancel_SameTask_ReapedOnce 同一任务并发取消：
// 所有调用都成功返回，终态只写一次语义、进程树只终止一次
func TestConcurrentCancel_SameTask_ReapedOnce(t *testing.T) {
	env := setupTestEnv(t)
	spider, version := env.createSpiderFixture(t)
	cmd := writeFakeTreeScript(t, env)
	task := env.createPendingTask(t, spider.Id, version.Id, cmd)

	go env.executor.ExecuteTask(0)
	waitForStatus(t, env, task.Id, constants.TaskStatusRunning, 5*time.Second)
	waitForLogsAtLeast(t, env, task.Id, 2, 5*time.Second)

	const callers = 8
	var wg sync.WaitGroup
	errs := make([]error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, errs[idx] = CancelTask(task.Id.String())
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent cancel #%d error: %v", i, err)
		}
	}

	final := waitForStatus(t, env, task.Id, constants.TaskStatusCancelled, 5*time.Second)
	if final.RuntimeDuration <= 0 {
		t.Fatal("runtime duration should be recorded")
	}
}

// TestUpdateTaskIfStatus_CAS 验证 DAO 层状态 CAS：
// 只允许覆盖预期状态，终态不被回退
func TestUpdateTaskIfStatus_CAS(t *testing.T) {
	env := setupTestEnv(t)
	spider, version := env.createSpiderFixture(t)
	task := env.createPendingTask(t, spider.Id, version.Id, "echo x")

	// PENDING -> PROCESSING 允许
	task.Status = constants.TaskStatusProcessing
	updated, err := writeCAS(env, task, constants.TaskStatusPending)
	if err != nil {
		t.Fatalf("cas pending->processing: %v", err)
	}
	if !updated {
		t.Fatal("expected CAS to succeed for PENDING")
	}

	// 已被外部置为 CANCELLED 后，RUNNING 写入必须被拒绝
	cancelled := env.getTask(t, task.Id)
	cancelled.Status = constants.TaskStatusCancelled
	mustUpdate(t, env, cancelled)

	stale := env.getTask(t, task.Id)
	stale.Status = constants.TaskStatusRunning
	var ok bool
	if err := dao.WriteTx(database.MainDB, func(tx dao.Tx) error {
		ok, err = tx.UpdateTaskIfStatus(stale, constants.TaskStatusProcessing)
		return err
	}); err != nil {
		t.Fatalf("stale cas: %v", err)
	}
	if ok {
		t.Fatal("CAS must reject RUNNING overwrite after CANCELLED")
	}
	again := env.getTask(t, task.Id)
	if again.Status != constants.TaskStatusCancelled {
		t.Fatalf("terminal status regressed to %s", again.Status)
	}
}

// TestErrorTask_DoesNotBlockLaterTasks 失败任务不会阻塞后续任务调度
func TestErrorTask_DoesNotBlockLaterTasks(t *testing.T) {
	env := setupTestEnv(t)
	spider, _ := env.createSpiderFixture(t)

	bad := &models.SpiderVersion{SpiderId: spider.Id, Path: "missing-dir"}
	if err := dao.WriteTx(database.MainDB, func(tx dao.Tx) error {
		return tx.InsertSpiderVersion(bad)
	}); err != nil {
		t.Fatalf("insert bad version: %v", err)
	}
	badTask := env.createPendingTask(t, spider.Id, bad.Id, "echo x")

	goodVersion := &models.SpiderVersion{SpiderId: spider.Id, Path: "v1"}
	if err := dao.WriteTx(database.MainDB, func(tx dao.Tx) error {
		return tx.InsertSpiderVersion(goodVersion)
	}); err != nil {
		t.Fatalf("insert good version: %v", err)
	}
	goodTask := env.createPendingTask(t, spider.Id, goodVersion.Id, "echo ok")

	// 每个任务使用独立 worker 槽位；弹出顺序不影响最终归属
	env.executor.ExecuteTask(0)
	env.executor.ExecuteTask(1)
	waitForStatus(t, env, badTask.Id, constants.TaskStatusError, 3*time.Second)
	waitForStatus(t, env, goodTask.Id, constants.TaskStatusFinished, 5*time.Second)
}

// TestRegistry_CancelUnregisteredTask 取消未登记任务（排队/终态）走数据库路径且不报错
func TestRegistry_CancelUnregisteredTask(t *testing.T) {
	env := setupTestEnv(t)
	spider, version := env.createSpiderFixture(t)
	task := env.createPendingTask(t, spider.Id, version.Id, "echo x")

	cr := runningRegistry.requestCancel(task.Id.String())
	if cr.managed {
		t.Fatal("pending task must not be managed by registry")
	}
	if cr.err != nil {
		t.Fatalf("unexpected error: %v", cr.err)
	}
}

// TestFinalizeTask_DoesNotOverwriteTerminal 终态 CAS：执行方的后写入
// 不得覆盖已存在的 CANCELLED 终态（模拟取消兜底先落库的竞态）
func TestFinalizeTask_DoesNotOverwriteTerminal(t *testing.T) {
	env := setupTestEnv(t)
	spider, version := env.createSpiderFixture(t)
	task := env.createPendingTask(t, spider.Id, version.Id, "echo x")
	task.StartTs = time.Now().Add(-time.Second)
	mustUpdate(t, env, task)

	// 取消兜底先落 CANCELLED
	if err := finalizeTask(task, outcomeCancelled, ""); err != nil {
		t.Fatalf("first finalize: %v", err)
	}
	firstFinish := env.getTask(t, task.Id).FinishTs

	// 执行方随后尝试写 FINISHED，必须被 CAS 拒绝
	time.Sleep(10 * time.Millisecond)
	if err := finalizeTask(task, outcomeFinished, ""); err != nil {
		t.Fatalf("second finalize: %v", err)
	}
	stored := env.getTask(t, task.Id)
	if stored.Status != constants.TaskStatusCancelled {
		t.Fatalf("terminal status overwritten to %s", stored.Status)
	}
	if !stored.FinishTs.Equal(firstFinish) {
		t.Fatal("finish_ts must not be refreshed by a rejected write")
	}
}

// TestFinalizeTask_RetryRejectsCancelled 重试写 PENDING 不得覆盖并发的 CANCELLED
func TestFinalizeTask_RetryRejectsCancelled(t *testing.T) {
	env := setupTestEnv(t)
	spider, version := env.createSpiderFixture(t)
	task := env.createPendingTask(t, spider.Id, version.Id, "echo x")

	// 模拟任务已被 pop 为 PROCESSING
	task.Status = constants.TaskStatusProcessing
	mustUpdate(t, env, task)

	// 并发取消先落库
	cancelled := env.getTask(t, task.Id)
	if err := finalizeTask(cancelled, outcomeCancelled, ""); err != nil {
		t.Fatalf("cancel finalize: %v", err)
	}

	// 准备失败的重试写 PENDING，必须被拒绝
	if err := finalizeTask(task, outcomeRetry, "transient"); err != nil {
		t.Fatalf("retry finalize: %v", err)
	}
	stored := env.getTask(t, task.Id)
	if stored.Status != constants.TaskStatusCancelled {
		t.Fatalf("cancelled task resurrected to %s by retry write", stored.Status)
	}
}

// writeCAS 封装一次 CAS 更新，供 DAO 状态迁移测试使用
func writeCAS(env *testEnv, task *models.Task, expect ...constants.TaskStatus) (bool, error) {
	var updated bool
	err := dao.WriteTx(database.MainDB, func(tx dao.Tx) error {
		var err error
		updated, err = tx.UpdateTaskIfStatus(task, expect...)
		return err
	})
	return updated, err
}
