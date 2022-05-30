package models

import (
	"encoding/json"
	"fmt"
	"gorm.io/gorm"
	"jingrongshuan/rongan-fnotify/meta"
	"time"
)

// BackupTaskModel备份任务
type BackupTaskModel struct {
	ID      int64      `gorm:"column:id;primaryKey;AUTO_INCREMENT;"`
	Trigger int64      `gorm:"column:trigger"`           // 触发原因：1手动，2自动，0未知原因
	ConfID  int64      `gorm:"column:conf_id"`           // 同步配置ID
	Start   *time.Time `gorm:"column:start_time"`        // 开始时间
	End     *time.Time `gorm:"column:end_time"`          // 结束时间
	Status  string     `gorm:"column:status"`            // 实时状态
	Success bool       `gorm:"column:success;default:f"` // 是否成功
	ExtInfo string     `gorm:"column:ext_info"`          // 扩展参数，JSON格式
}

type BackupExt struct {
	Handler string `json:"handler"`
}

func (_ BackupTaskModel) TableName() string {
	return ModelDefaultSchema + ".backup_task"
}

func (t *BackupTaskModel) String() string {
	return fmt.Sprintf("<BackupTaskModel(ID=%v, Conf=%v, StartWithRetry=%v>", t.ID, t.ConfID, t.Start.Format(meta.TimeFMT))
}

func (t *BackupTaskModel) BackupExtInfos() (be BackupExt, err error) {
	err = json.Unmarshal([]byte(t.ExtInfo), &be)
	return be, err
}

func CreateBackupTaskModel(db *gorm.DB, btm *BackupTaskModel) (err error) {
	r := db.Create(btm)
	return r.Error
}

func QueryBackupTaskByConfID(db *gorm.DB, cid int64) (b BackupTaskModel, err error) {
	r := db.Model(&BackupTaskModel{}).Where("conf_id=?", cid).Take(&b)
	return b, r.Error
}

func UpdateBackupTaskStatusByConfID(db *gorm.DB, cid int64, status string) (err error) {
	return db.Model(&BackupTaskModel{}).Where("conf_id = ?", cid).Updates(
		map[string]interface{}{"status": status}).Error
}
