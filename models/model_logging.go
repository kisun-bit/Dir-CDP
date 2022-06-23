package models

import (
	"gorm.io/gorm"
	"jingrongshuan/rongan-fnotify/meta"
	"time"
)

type Logging struct {
	Id         int64      `gorm:"column:id;primaryKey;AUTO_INCREMENT" json:"id"`
	ConfID     int64      `gorm:"column:conf_id;index" json:"conf_id"`
	TaskID     int64      `gorm:"column:task_id;index" json:"task_id"`
	TaskType   string     `gorm:"column:task_type" json:"task_type"`
	Key        string     `gorm:"column:key;size:16" json:"key"`
	TaskStatus string     `gorm:"column:task_status;size:16" json:"task_status"`
	Message    string     `gorm:"column:message;" json:"message"`
	JsonDetail string     `gorm:"column:json_detail;type:text" json:"json_detail"`
	Level      int        `gorm:"column:level;" json:"level"`
	Time       *time.Time `gorm:"column:time" json:"time"`
}

func (Logging) TableName() string {
	return ModelDefaultSchema + ".log"
}

func NewLog(db *gorm.DB, conf, task int64, type_, key, status, message, detail string, level int) (err error) {
	current := time.Now()
	return db.Model(&Logging{}).Create(&Logging{
		ConfID:     conf,
		TaskID:     task,
		TaskType:   type_,
		Key:        key,
		TaskStatus: status,
		Message:    message,
		JsonDetail: detail,
		Level:      level,
		Time:       &current,
	}).Error
}

func DeleteCDPStartLogsByTime(db *gorm.DB, conf, time_ int64) (err error) {
	return db.Where(
		"conf_id = ? AND time < ? AND key = ?",
		conf, time.Unix(time_, 0), `''`).Delete(&Logging{}).Error
}

func DeleteCDPIOLogsByTime(db *gorm.DB, conf, time_ int64) (err error) {
	return db.Where(
		"conf_id = ? AND time < ? AND key = ?",
		conf, time.Unix(time_, 0), meta.RuntimeIOBackup).Delete(&Logging{}).Error
}
