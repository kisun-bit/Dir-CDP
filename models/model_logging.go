package models

import "time"

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
