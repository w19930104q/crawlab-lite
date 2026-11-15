package api

import (
	"crawlab-lite/constants"
	"crawlab-lite/dao"
	"crawlab-lite/database"
	"crawlab-lite/managers"
	"crawlab-lite/models"
	"crawlab-lite/results"
	"crawlab-lite/routes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	uuid "github.com/satori/go.uuid"
	"github.com/spf13/viper"
	bolt "go.etcd.io/bbolt"
)

// initCancelAPITest 搭建隔离的 BoltDB + 路由环境，并登录获取 token
func initCancelAPITest(t *testing.T) (app *gin.Engine, token, spiderDir string, cleanup func()) {
	t.Helper()
	gin.SetMode(gin.TestMode)

	dir, err := ioutil.TempDir("", "crawlab-api-test-")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	spiderDir = filepath.Join(dir, "spiders")
	dbDir := filepath.Join(dir, "db")
	if err := os.MkdirAll(spiderDir, 0755); err != nil {
		t.Fatalf("mkdir spiders: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(spiderDir, "v1"), 0755); err != nil {
		t.Fatalf("mkdir version: %v", err)
	}
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

	// 执行器与登录所需的配置项
	viper.Set("spider.path", spiderDir)
	viper.Set("task.workers", 1)
	viper.Set("user.username", "admin")
	viper.Set("user.password", "admin")
	viper.Set("server.secret", "test-secret")

	app = gin.New()
	routes.InitRoutes(app)

	// 登录
	var tok string
	loginReq, _ := PostJson("/login", map[string]string{
		"username": "admin",
		"password": "admin",
	})
	w := httptest.NewRecorder()
	app.ServeHTTP(w, loginReq)
	if w.Code != http.StatusOK {
		t.Fatalf("login failed: code=%d body=%s", w.Code, w.Body.String())
	}
	resp := GetResponse(w.Body)
	var ok bool
	tok, ok = resp.Data.(string)
	if !ok || tok == "" {
		t.Fatalf("login token missing: %v", resp.Data)
	}

	cleanup = func() {
		_ = mainDB.Close()
		_ = logDB.Close()
		_ = os.RemoveAll(dir)
		database.MainDB = nil
		database.LogDB = nil
	}
	return app, tok, spiderDir, cleanup
}

func apiFixtureSpider(t *testing.T) (*models.Spider, *models.SpiderVersion) {
	t.Helper()
	spider := &models.Spider{Name: "api-spider"}
	version := &models.SpiderVersion{Path: "v1"}
	if err := dao.WriteTx(database.MainDB, func(tx dao.Tx) error {
		if err := tx.InsertSpider(spider); err != nil {
			return err
		}
		version.SpiderId = spider.Id
		return tx.InsertSpiderVersion(version)
	}); err != nil {
		t.Fatalf("insert fixture: %v", err)
	}
	return spider, version
}

func apiCreateTask(t *testing.T, app *gin.Engine, token string, form map[string]interface{}) results.Task {
	t.Helper()
	req, err := PostJsonWithToken("/tasks", form, token)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	w := httptest.NewRecorder()
	app.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("create task: code=%d body=%s", w.Code, w.Body.String())
	}
	resp := GetResponse(w.Body)
	data, err := json.Marshal(resp.Data)
	if err != nil {
		t.Fatalf("marshal data: %v", err)
	}
	var task results.Task
	if err := json.Unmarshal(data, &task); err != nil {
		t.Fatalf("unmarshal task: %v", err)
	}
	return task
}

func apiCancelTask(t *testing.T, app *gin.Engine, token, taskID string) (int, *results.ResponseBody) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, "/tasks/"+taskID+"/cancel", nil)
	if err != nil {
		t.Fatalf("build cancel request: %v", err)
	}
	req.Header.Set(constants.AuthHeader, token)
	w := httptest.NewRecorder()
	app.ServeHTTP(w, req)
	return w.Code, GetResponse(w.Body)
}

func apiGetTask(t *testing.T, app *gin.Engine, token, taskID string) results.Task {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, "/tasks/"+taskID, nil)
	if err != nil {
		t.Fatalf("build get request: %v", err)
	}
	req.Header.Set(constants.AuthHeader, token)
	w := httptest.NewRecorder()
	app.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("get task: code=%d body=%s", w.Code, w.Body.String())
	}
	resp := GetResponse(w.Body)
	data, _ := json.Marshal(resp.Data)
	var task results.Task
	if err := json.Unmarshal(data, &task); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return task
}

func loadTaskFromDB(t *testing.T, id uuid.UUID) *models.Task {
	t.Helper()
	var task *models.Task
	if err := dao.ReadTx(database.MainDB, func(tx dao.Tx) error {
		var err error
		task, err = tx.SelectTask(id)
		return err
	}); err != nil {
		t.Fatalf("load task: %v", err)
	}
	return task
}

// TestAPICancel_PendingTask_KeepsStatusName 取消排队任务：
// API 状态名保持 "CANCELLED"，重复取消幂等
func TestAPICancel_PendingTask_KeepsStatusName(t *testing.T) {
	app, token, _, cleanup := initCancelAPITest(t)
	defer cleanup()
	spider, version := apiFixtureSpider(t)

	created := apiCreateTask(t, app, token, map[string]interface{}{
		"spider_id":         spider.Id.String(),
		"spider_version_id": version.Id.String(),
		"cmd":               "echo never",
	})

	code, resp := apiCancelTask(t, app, token, created.Id.String())
	if code != http.StatusOK || resp.Code != http.StatusOK {
		t.Fatalf("cancel response: http=%d code=%d msg=%s", code, resp.Code, resp.Message)
	}
	task := apiGetTask(t, app, token, created.Id.String())
	if task.Status != constants.TaskStatusCancelled {
		t.Fatalf("status = %q, want CANCELLED", task.Status)
	}

	// 重复取消必须幂等：仍返回 200，状态不变
	code2, resp2 := apiCancelTask(t, app, token, created.Id.String())
	if code2 != http.StatusOK {
		t.Fatalf("idempotent cancel http=%d msg=%s", code2, resp2.Message)
	}
	again := apiGetTask(t, app, token, created.Id.String())
	if again.Status != constants.TaskStatusCancelled {
		t.Fatalf("status after second cancel = %q", again.Status)
	}

	// BoltDB 记录与 API 一致
	dbTask := loadTaskFromDB(t, created.Id)
	if dbTask.Status != constants.TaskStatusCancelled {
		t.Fatalf("db status = %q, want CANCELLED", dbTask.Status)
	}
	if dbTask.FinishTs.IsZero() {
		t.Fatal("finish_ts must be persisted")
	}
}

// TestAPICancel_RunningTask_ReapsTree 取消运行中的假命令树：
// 进程树收口，终态 CANCELLED，耗时字段落库
func TestAPICancel_RunningTask_ReapsTree(t *testing.T) {
	app, token, spiderDir, cleanup := initCancelAPITest(t)
	defer cleanup()
	spider, version := apiFixtureSpider(t)

	script := `
sh -c 'i=0; while true; do echo child $i; i=$((i+1)); sleep 0.1; done' &
i=0
while true; do echo parent $i; i=$((i+1)); sleep 0.1; done
`
	if err := ioutil.WriteFile(filepath.Join(spiderDir, "v1", "tree.sh"), []byte(script), 0755); err != nil {
		t.Fatalf("write script: %v", err)
	}

	created := apiCreateTask(t, app, token, map[string]interface{}{
		"spider_id":         spider.Id.String(),
		"spider_version_id": version.Id.String(),
		"cmd":               "sh tree.sh",
	})

	// 单步驱动执行器把任务跑到 RUNNING（不启动 cron 调度器）
	go managers.ExecutorStepForTest(0)

	// 等待任务被调度运行
	waitDBStatus(t, created.Id, constants.TaskStatusRunning, 5*time.Second)

	code, _ := apiCancelTask(t, app, token, created.Id.String())
	if code != http.StatusOK {
		t.Fatalf("cancel http=%d", code)
	}

	final := waitDBStatus(t, created.Id, constants.TaskStatusCancelled, 5*time.Second)
	if final.RuntimeDuration <= 0 {
		t.Fatalf("runtime duration = %v, want > 0", final.RuntimeDuration)
	}

	// 取消后日志停止增长，证明进程树已终止
	var countAfter, countLater int
	_ = dao.ReadTx(database.LogDB, func(tx dao.Tx) error {
		countAfter, _ = tx.CountTaskLogs(created.Id)
		return nil
	})
	time.Sleep(500 * time.Millisecond)
	_ = dao.ReadTx(database.LogDB, func(tx dao.Tx) error {
		countLater, _ = tx.CountTaskLogs(created.Id)
		return nil
	})
	if countLater > countAfter {
		t.Fatalf("process tree alive after cancel: logs %d -> %d", countAfter, countLater)
	}
}

// waitDBStatus 轮询数据库中的任务状态
func waitDBStatus(t *testing.T, id uuid.UUID, want constants.TaskStatus, timeout time.Duration) *models.Task {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		task := loadTaskFromDB(t, id)
		if task != nil && task.Status == want {
			return task
		}
		time.Sleep(20 * time.Millisecond)
	}
	task := loadTaskFromDB(t, id)
	got := constants.TaskStatus("")
	if task != nil {
		got = task.Status
	}
	t.Fatalf("task %s status=%s want %s", id, got, want)
	return nil
}
