package managers

import (
	"crawlab-lite/constants"
	"crawlab-lite/dao"
	"crawlab-lite/database"
	"crawlab-lite/managers/sys_exec"
	"crawlab-lite/models"
	"crawlab-lite/utils"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	linq "github.com/ahmetb/go-linq/v3"
	"github.com/apex/log"
	"github.com/robfig/cron/v3"
	uuid "github.com/satori/go.uuid"
	"github.com/spf13/viper"
)

// 任务执行器
var Executor *executor

type executor struct {
	cron *cron.Cron
}

// 任务执行锁
var lockList sync.Map

// 故障注入钩子（测试用），默认不改变任何行为
var (
	// hookQueryVersion 包装爬虫版本查询，可注入查询失败
	hookQueryVersion = func(fn func() (*models.SpiderVersion, error)) (*models.SpiderVersion, error) {
		return fn()
	}
	// hookBuildCmd 包装命令构造，可注入启动失败；
	// defaultBuildCmd 是未注入故障时的命令构造函数，测试重置钩子使用
	hookBuildCmd    = sys_exec.BuildCmd
	defaultBuildCmd = sys_exec.BuildCmd
)

// 启动任务执行器
func (ex *executor) Start() error {
	// 启动cron服务
	ex.cron.Start()

	// 加入执行器到定时任务
	spec := "0/1 * * * * *" // 每秒执行一次
	for i := 0; i < viper.GetInt("task.workers"); i++ {
		workerId := i

		// 初始化任务锁
		lockList.Store(workerId, false)

		// 加入定时任务
		if _, err := ex.cron.AddFunc(spec, func() {
			ex.ExecuteTask(workerId)
		}); err != nil {
			return err
		}
	}

	return nil
}

// 执行任务
func (ex *executor) ExecuteTask(workerId int) {
	if flag, ok := lockList.Load(workerId); ok {
		if flag.(bool) {
			return
		}
	}

	// 上锁
	lockList.Store(workerId, true)

	// 解锁（延迟执行）
	defer func() {
		lockList.Delete(workerId)
		lockList.Store(workerId, false)
	}()

	// 开始计时
	tic := time.Now()

	// 获取任务
	task, err := popPendingTask()
	if err != nil {
		log.Errorf("[WorkerId %d] execute task, query task error: %s", workerId, err.Error())
		return
	}
	if task == nil {
		return
	}

	// 登记任务归属，之后取消请求可以定位到该任务的进程树
	rt := runningRegistry.register(task.Id.String(), nil)
	defer runningRegistry.unregister(task.Id.String())

	// 调度准备阶段：任何一步失败都不能让任务永久停在 PROCESSING
	cwd, outcome, errMsg := ex.prepareTask(workerId, task)

	// 准备阶段被取消：直接落取消终态
	if runningRegistry.isCancelled(task.Id.String()) {
		if err := finalizeTask(task, outcomeCancelled, ""); err != nil {
			log.Errorf(logPrefix(workerId, task.Id)+"save cancelled task error: %s", err.Error())
		}
		rt.markDone()
		return
	}

	if outcome == outcomeError {
		// 确定性失败（爬虫无版本、目录缺失）：明确失败
		appendTaskLog(task, constants.TaskLogStdErr, errMsg)
		if err := finalizeTask(task, outcomeError, errMsg); err != nil {
			log.Errorf(logPrefix(workerId, task.Id)+"save error task error: %s", err.Error())
		}
		rt.markDone()
		return
	}
	if outcome == outcomeRetry {
		// 临时性失败（数据库查询等）：回到 PENDING 可重试
		appendTaskLog(task, constants.TaskLogStdErr, errMsg)
		if err := finalizeTask(task, outcomeRetry, errMsg); err != nil {
			log.Errorf(logPrefix(workerId, task.Id)+"reset retry task error: %s", err.Error())
		}
		rt.markDone()
		return
	}

	// 开始执行任务
	log.Info(logPrefix(workerId, task.Id) + "start task")

	// CAS 置 RUNNING：若任务已被取消，放弃启动进程
	updated, err := markRunningCAS(task)
	if err != nil {
		log.Errorf(logPrefix(workerId, task.Id)+"save running task error: %s", err.Error())
		_ = finalizeTask(task, outcomeRetry, err.Error())
		rt.markDone()
		return
	}
	if !updated {
		log.Infof(logPrefix(workerId, task.Id) + "task already cancelled before running")
		_ = finalizeTask(task, outcomeCancelled, "")
		rt.markDone()
		return
	}

	// 执行 Shell 命令，终态由进程执行结果或取消信号决定
	outcome, errMsg = ex.runTaskProcess(cwd, task, workerId, rt)

	// 进程启动后、写库前又收到取消信号时，以取消终态为准
	if runningRegistry.isCancelled(task.Id.String()) {
		outcome = outcomeCancelled
		errMsg = ""
	}

	if err := finalizeTask(task, outcome, errMsg); err != nil {
		log.Errorf(logPrefix(workerId, task.Id)+"save task error: %s", err.Error())
	}

	// 结束计时
	toc := time.Now()
	duration := toc.Sub(tic).Seconds()
	log.Infof(logPrefix(workerId, task.Id)+"finished with status %s, elapsed: %s sec",
		task.Status, strconv.FormatFloat(duration, 'f', 6, 64))
}

// prepareTask 完成进程启动前的全部准备工作，返回工作目录；
// 失败时同时返回应归入的终态（outcomeError / outcomeRetry）与错误信息。
func (ex *executor) prepareTask(workerId int, task *models.Task) (
	cwd string, outcome taskOutcome, errMsg string) {

	// 获取爬虫版本
	var (
		version *models.SpiderVersion
		err     error
	)
	if task.SpiderVersionId != uuid.Nil {
		version, err = hookQueryVersion(func() (*models.SpiderVersion, error) {
			return querySpiderVersionById(task.SpiderId, task.SpiderVersionId)
		})
	} else {
		version, err = hookQueryVersion(func() (*models.SpiderVersion, error) {
			return queryLatestSpiderVersion(task.SpiderId)
		})
	}
	if err != nil {
		log.Errorf(logPrefix(workerId, task.Id)+"execute task, query spider version error: %s", err.Error())
		return "", outcomeRetry, "query spider version error: " + err.Error()
	}
	if version == nil {
		msg := "execute task, query spider version error: spider does not have any version"
		log.Error(logPrefix(workerId, task.Id) + msg)
		return "", outcomeError, msg
	}

	// 工作目录
	cwd = filepath.Join(viper.GetString("spider.path"), version.Path)

	// 检查爬虫目录是否存在
	if exist := utils.PathExist(cwd); !exist {
		msg := "spider directory not exist: " + cwd
		log.Error(logPrefix(workerId, task.Id) + msg)
		return "", outcomeError, msg
	}

	return cwd, outcomeFinished, ""
}

// runTaskProcess 构造并启动进程、同步等待回收。
// 取消由 runningRegistry 驱动：终止整棵进程树，cmd.Wait 由本函数统一执行，
// 保证外壳与后代进程都被回收，不会留下孤儿进程。
func (ex *executor) runTaskProcess(cwd string, task *models.Task, workerId int, rt *runningTask) (outcome taskOutcome, errMsg string) {
	cmdStr := task.Cmd

	log.Debugf(logPrefix(workerId, task.Id)+"cwd: %s", cwd)
	log.Debugf(logPrefix(workerId, task.Id)+"cmd: %s", cmdStr)

	// 生成执行命令（保持原有命令：sh -c / cmd /C）
	cmd := hookBuildCmd(cmdStr)
	cmd.Dir = cwd
	rt.cmd = cmd

	// 记录任务生成的日志
	logWriters, err := recordTaskLog(cmd, task)
	if err != nil {
		log.Error(logPrefix(workerId, task.Id) + err.Error())
		return outcomeError, err.Error()
	}

	// 设置环境变量
	setEnv(cmd, task)

	// 独立进程组/作业，保证 kill 可以覆盖所有子进程
	sys_exec.Setpgid(cmd)

	// 启动进程
	if err := cmd.Start(); err != nil {
		log.Error(logPrefix(workerId, task.Id) + err.Error())
		// Start 失败时管道会被 exec 关闭，等待采集 goroutine 收口后再返回
		logWriters.wait()
		return outcomeError, "start task error: " + err.Error()
	}

	log.Debugf(logPrefix(workerId, task.Id)+"process started, pid: %d", cmd.Process.Pid)

	// 启动与取消可能并发：若启动完成前取消信号已到，立即补发终止
	if runningRegistry.isCancelled(task.Id.String()) && cmd.Process != nil {
		if err := sys_exec.KillProcess(cmd); err != nil {
			log.Errorf(logPrefix(workerId, task.Id)+"kill after start error: %s", err.Error())
		}
	}

	// 同步等待进程退出（唯一的 Wait 调用方）
	waitErr := cmd.Wait()

	// 等待残余日志全部落库后再通知取消方收口，保证日志与终态一致
	logWriters.wait()

	// 进程已回收，通知取消方收口完成
	rt.markDone()

	if waitErr != nil {
		log.Errorf(logPrefix(workerId, task.Id)+"wait process finish error: %s", waitErr.Error())

		// 取消导致的终止归入 CANCELLED，其余非零退出归入 ERROR
		if runningRegistry.isCancelled(task.Id.String()) {
			appendTaskLog(task, constants.TaskLogStdOut, "task cancelled, process tree terminated")
			return outcomeCancelled, ""
		}
		if exitError, ok := waitErr.(*exec.ExitError); ok {
			log.Errorf(logPrefix(workerId, task.Id)+"exit error, exit code: %d", exitError.ExitCode())
		}
		return outcomeError, waitErr.Error()
	}

	if runningRegistry.isCancelled(task.Id.String()) {
		// 进程恰好正常退出，但取消信号已发出，仍按取消语义收口
		appendTaskLog(task, constants.TaskLogStdOut, "task cancelled")
		return outcomeCancelled, ""
	}
	return outcomeFinished, ""
}

// 初始化
func InitTaskExecutor() error {
	Executor = &executor{
		cron: cron.New(cron.WithSeconds()),
	}
	if err := Executor.Start(); err != nil {
		return err
	}
	return nil
}

// ExecutorStepForTest 供其他包的测试单步驱动一次任务调度，不依赖 cron
func ExecutorStepForTest(workerId int) {
	(&executor{}).ExecuteTask(workerId)
}

func querySpiderVersionById(spiderId uuid.UUID, versionId uuid.UUID) (version *models.SpiderVersion, err error) {
	if err := dao.ReadTx(database.MainDB, func(tx dao.Tx) error {
		if version, err = tx.SelectSpiderVersion(spiderId, versionId); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return version, nil
}

func queryLatestSpiderVersion(spiderId uuid.UUID) (version *models.SpiderVersion, err error) {
	if err := dao.ReadTx(database.MainDB, func(tx dao.Tx) error {
		versions, err := tx.SelectAllSpiderVersions(spiderId)
		if err != nil {
			return err
		}
		versionI := linq.From(versions).OrderByDescendingT(func(version *models.SpiderVersion) int64 {
			return version.CreateTs.UnixNano()
		}).First()
		if versionI != nil {
			version = versionI.(*models.SpiderVersion)
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return version, nil
}

// 设置环境变量
func setEnv(cmd *exec.Cmd, task *models.Task) {
	// 读取系统环境变量（靠后的重复变量会被忽略）
	cmd.Env = append(cmd.Env, os.Environ()...)
}

func logPrefix(workerId int, taskId uuid.UUID) string {
	return fmt.Sprintf("[Worker(%d) Task(%s)] ", workerId, taskId)
}
