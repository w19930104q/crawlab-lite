package managers

import (
	"crawlab-lite/dao"
	"crawlab-lite/database"
	"crawlab-lite/models"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	uuid "github.com/satori/go.uuid"
	"github.com/spf13/viper"
	bolt "go.etcd.io/bbolt"
)

// testEnv 持有一套隔离的 BoltDB、爬虫目录与测试数据
type testEnv struct {
	dir       string
	spiderDir string
	mainDB    *bolt.DB
	logDB     *bolt.DB
	executor  *executor
}

func setupTestEnv(t *testing.T) *testEnv {
	t.Helper()

	dir, err := ioutil.TempDir("", "crawlab-test-")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	t.Cleanup(func() {
		if database.MainDB != nil {
			_ = database.MainDB.Close()
		}
		if database.LogDB != nil {
			_ = database.LogDB.Close()
		}
		_ = os.RemoveAll(dir)
		// 重置全局状态，避免用例间互相污染
		database.MainDB = nil
		database.LogDB = nil
		resetHooks()
		resetRegistry()
		resetWorkerLocks()
	})

	dbDir := filepath.Join(dir, "db")
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		t.Fatalf("mkdir db: %v", err)
	}
	mainDB, err := bolt.Open(filepath.Join(dbDir, "main.db"), 0666, nil)
	if err != nil {
		t.Fatalf("open main db: %v", err)
	}
	logDB, err := bolt.Open(filepath.Join(dbDir, "log.db"), 0666, nil)
	if err != nil {
		t.Fatalf("open log db: %v", err)
	}
	database.MainDB = mainDB
	database.LogDB = logDB

	spiderDir := filepath.Join(dir, "spiders")
	if err := os.MkdirAll(spiderDir, 0755); err != nil {
		t.Fatalf("mkdir spiders: %v", err)
	}

	// viper 配置（不依赖 config.yml）
	viper.Set("spider.path", spiderDir)
	viper.Set("task.workers", 1)

	resetHooks()
	resetRegistry()
	resetWorkerLocks()

	return &testEnv{
		dir:       dir,
		spiderDir: spiderDir,
		mainDB:    mainDB,
		logDB:     logDB,
		executor:  &executor{},
	}
}

func resetRegistry() {
	runningRegistry.mu.Lock()
	runningRegistry.entries = make(map[string]*runningTask)
	runningRegistry.mu.Unlock()
}

// resetWorkerLocks 清空全局 worker 锁，避免上一个用例残留的锁状态影响调度
func resetWorkerLocks() {
	lockList.Range(func(key, _ interface{}) bool {
		lockList.Delete(key)
		return true
	})
}

// createSpiderFixture 写入一个爬虫及其版本，版本目录指向真实存在的目录
func (e *testEnv) createSpiderFixture(t *testing.T) (*models.Spider, *models.SpiderVersion) {
	t.Helper()
	spider := &models.Spider{Name: "test-spider"}
	version := &models.SpiderVersion{Path: "v1"}
	versionDir := filepath.Join(e.spiderDir, version.Path)
	if err := os.MkdirAll(versionDir, 0755); err != nil {
		t.Fatalf("mkdir version dir: %v", err)
	}
	if err := dao.WriteTx(e.mainDB, func(tx dao.Tx) error {
		if err := tx.InsertSpider(spider); err != nil {
			return err
		}
		version.SpiderId = spider.Id
		return tx.InsertSpiderVersion(version)
	}); err != nil {
		t.Fatalf("insert spider fixture: %v", err)
	}
	return spider, version
}

// createPendingTask 写入一条 PENDING 任务
func (e *testEnv) createPendingTask(t *testing.T, spiderID, versionID uuid.UUID, cmd string) *models.Task {
	t.Helper()
	task := &models.Task{
		SpiderId:        spiderID,
		SpiderVersionId: versionID,
		Cmd:             cmd,
	}
	if err := dao.WriteTx(e.mainDB, func(tx dao.Tx) error {
		return tx.InsertTask(task)
	}); err != nil {
		t.Fatalf("insert task: %v", err)
	}
	return task
}

func (e *testEnv) getTask(t *testing.T, id uuid.UUID) *models.Task {
	t.Helper()
	var task *models.Task
	if err := dao.ReadTx(e.mainDB, func(tx dao.Tx) error {
		var err error
		task, err = tx.SelectTask(id)
		return err
	}); err != nil {
		t.Fatalf("select task: %v", err)
	}
	return task
}

func (e *testEnv) taskLogCount(t *testing.T, id uuid.UUID) int {
	t.Helper()
	var count int
	if err := dao.ReadTx(e.logDB, func(tx dao.Tx) error {
		var err error
		count, err = tx.CountTaskLogs(id)
		return err
	}); err != nil {
		t.Fatalf("count logs: %v", err)
	}
	return count
}

func (e *testEnv) taskLogs(t *testing.T, id uuid.UUID) []*models.TaskLog {
	t.Helper()
	var logs []*models.TaskLog
	if err := dao.ReadTx(e.logDB, func(tx dao.Tx) error {
		var err error
		logs, err = tx.SelectAllTaskLogs(id)
		return err
	}); err != nil {
		t.Fatalf("select logs: %v", err)
	}
	return logs
}
