package models

import (
	"encoding/json"
	"fmt"
	"gorm.io/gorm"
	"jingrongshuan/rongan-fnotify/meta"
	"time"
)

// RestoreTaskModel还原任务
type RestoreTaskModel struct {
	ID           int64      `gorm:"column:id;primaryKey;AUTO_INCREMENT;"`
	ConfID       int64      `gorm:"column:conf_id"`           // 同步配置ID
	Start        *time.Time `gorm:"column:start_time"`        // 开始时间
	End          *time.Time `gorm:"column:end_time"`          // 结束时间
	RestoreBytes int64      `gorm:"column:restore_bytes"`     // 已经还原的数据量
	Status       string     `gorm:"column:status"`            // 实时状态
	Success      bool       `gorm:"column:success;default:f"` // 是否成功
	Client       int64      `gorm:"column:client"`            // 恢复目标客户端
	/*ExtInfo 扩展参数的格式 JSON
	{
		"restore_dir": "str, 恢复目标路径",
	    "fileset":     "str, 指定恢复的文件集，以@jrsa@作为分割符",
	    "starttime":   "str, 指定开始时间恢复，若此项存在endtime不存在，则表示恢复starttime之前的所有数据",
		"endtime":     "str, 指定结束时间恢复，若此项存在starttime不存在，则表示恢复endtime之后的所有数据",
	    "include":     "str, 恢复白名单"
	    "exclude":     "str, 恢复黑名单"
	}
	*/
	ExtInfo string `gorm:"column:ext_info"` // 扩展参数，JSON格式
}

func (_ RestoreTaskModel) TableName() string {
	return ModelDefaultSchema + ".restore_task"
}

func (t *RestoreTaskModel) String() string {
	return fmt.Sprintf("<RestoreTaskModel(ID=%v, Conf=%v, Start=%v>", t.ID, t.ConfID, t.Start.Format(meta.TimeFMT))
}

func QueryRestoreTaskByID(db *gorm.DB, task int64) (t RestoreTaskModel, err error) {
	r := db.Model(&RestoreTaskModel{}).Where("id=?", task).Take(&t)
	return t, r.Error
}

type RestoreFilter struct {
	RestoreDir string `json:"restore_dir"`
	Fileset    string `json:"fileset"`
	Starttime  string `json:"starttime"`
	Endtime    string `json:"endtime"`
	Include    string `json:"include"`
	Exclude    string `json:"exclude"`
}

func QueryRestoreFilterArgs(db *gorm.DB, task int64) (f RestoreFilter, err error) {
	r_, err := QueryRestoreTaskByID(db, task)
	if err != nil {
		return f, err
	}
	err = json.Unmarshal([]byte(r_.ExtInfo), &f)
	return f, err
}
