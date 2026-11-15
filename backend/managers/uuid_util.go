package managers

import uuid "github.com/satori/go.uuid"

// mustParseUUID 解析任务 ID，内部调用方的 ID 均来自数据库/参数校验，
// 解析失败时返回 Nil UUID，由后续 SelectTask 走 not found 分支。
func mustParseUUID(id string) uuid.UUID {
	parsed, err := uuid.FromString(id)
	if err != nil {
		return uuid.Nil
	}
	return parsed
}
