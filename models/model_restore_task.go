package models

import (
	"encoding/json"
	"errors"
	"fmt"
	"gorm.io/gorm"
	"jingrongshuan/rongan-fnotify/meta"
	"strings"
	"time"
)

// RestoreTaskModel还原任务
type RestoreTaskModel struct {
	ID           int64      `gorm:"column:id;primaryKey;AUTO_INCREMENT;"`
	ConfID       int64      `gorm:"column:conf_id"`           // 同步配置ID
	Start        *time.Time `gorm:"column:start_time"`        // 开始时间
	End          *time.Time `gorm:"column:end_time"`          // 结束时间
	RestoreFiles int64      `gorm:"column:restore_files"`     // 依据还原的文件
	RestoreBytes int64      `gorm:"column:restore_bytes"`     // 已经还原的数据量
	Cancel       bool       `gorm:"column:cancel"`            // 是否取消
	Status       string     `gorm:"column:status"`            // 实时状态
	Success      bool       `gorm:"column:success;default:f"` // 是否成功
	Client       int64      `gorm:"column:client"`            // 恢复目标客户端
	/*ExtInfo 扩展参数的格式 JSON
	{
		"restore_map": "str, 恢复目标路径",
	    "fileset":     "str, 指定恢复的文件集，以@jrsa@作为分割符",
	    "starttime":   "str, 指定开始时间恢复，若此项存在endtime不存在，则表示恢复starttime之前的所有数据",
		"endtime":     "str, 指定结束时间恢复，若此项存在starttime不存在，则表示恢复endtime之后的所有数据",
	    "include":     "str, 恢复白名单"
	    "exclude":     "str, 恢复黑名单"
	    "when_same"    "str, 同名文件如何处理，overwrite或ignore"
	    "threads":     "int, 恢复线程数"
	}

	"restore_map": "str, 恢复目标路径",结构说明：
	[
	    {
	        "Target": "D:\\tmp\\backup_origin\\2",
	        "bucket": "",
	        "origin": "D:\\tmp\\backup\\map2",
	        "recursion": true,
	        "depth": -1
	    },
	    {
	        "Target": "D:\\tmp\\backup_origin\\1",
	        "bucket": "",
	        "origin": "D:\\tmp\\backup\\map1",
	        "recursion": true,
	        "depth": -1
	    }
	]
	*/
	ExtInfo     string         `gorm:"column:ext_info"` // 扩展参数，JSON格式
	ExtInfoJson RestoreExtInfo `gorm:"-"`
}

type RestoreDirMap struct {
	LocalDir                       string `json:"origin"`    // 本地恢复目录
	StorageBucket                  string `json:"bucket"`    // 目标桶
	StoragePrefixForBucketOrRemote string `json:"Target"`    // 目标存储的前缀、目录
	StorageVolume                  string `json:"volume"`    // 目标卷
	LocalEnableRecursion           bool   `json:"recursion"` // 是否支持递归
	LocalEnabledDepth              int    `json:"depth"`     // 指定备恢复深度
}

type RestoreExtInfo struct {
	RestoreMap []RestoreDirMap `json:"restore_map"`
	Fileset    string          `json:"fileset"`
	Starttime  string          `json:"starttime"`
	Endtime    string          `json:"endtime"`
	Include    string          `json:"include"`
	Exclude    string          `json:"exclude"`
	WhenSame   string          `json:"when_same"`
	Threads    int             `json:"threads"`
}

func (_ RestoreTaskModel) TableName() string {
	return ModelDefaultSchema + ".restore_task"
}

func (t *RestoreTaskModel) String() string {
	return fmt.Sprintf("<RestoreTaskModel(ID=%v, Conf=%v, Start=%v>",
		t.ID, t.ConfID, t.Start.Format(meta.TimeFMT))
}

func (t *RestoreTaskModel) LoadJsonFields() (err error) {
	if err = t.loadExtInfo(); err != nil {
		return
	}
	return
}

func (t *RestoreTaskModel) loadExtInfo() (err error) {
	return json.Unmarshal([]byte(t.ExtInfo), &t.ExtInfoJson)
}

func (t *RestoreTaskModel) SpecifyLocalDirAndBucket(storage string) (local, bucket string, err error) {
	var item RestoreDirMap
	for _, dm := range t.ExtInfoJson.RestoreMap {
		if strings.HasPrefix(storage, dm.StoragePrefixForBucketOrRemote) && len(dm.StoragePrefixForBucketOrRemote) > len(item.StoragePrefixForBucketOrRemote) {
			item = dm
		}
	}
	if item.LocalDir == meta.UnsetStr {
		err = errors.New("failed to match storage address")
		return
	}
	return item.LocalDir, item.StorageBucket, err
}

func IsRestoreEnable(db *gorm.DB, task int64) (_ bool, err error) {
	var c RestoreTaskModel
	r := db.Model(&RestoreTaskModel{}).Where("id = ?", task).Take(&c)
	if r.Error != nil {
		return false, r.Error
	}
	return !c.Cancel, r.Error
}

func QueryRestoreTaskByID(db *gorm.DB, task int64) (t RestoreTaskModel, err error) {
	r := db.Model(&RestoreTaskModel{}).Where("id=?", task).Take(&t)
	return t, r.Error
}

func QueryAllRestoreTasks(db *gorm.DB) (cs []RestoreTaskModel, err error) {
	r := db.Model(&RestoreTaskModel{}).Where("end_time is NULL").Find(&cs)
	return cs, r.Error
}

func UpdateRestoreTask(db *gorm.DB, task int64, status string) (err error) {
	return db.Model(&RestoreTaskModel{}).Where("id = ?", task).Updates(
		map[string]interface{}{"status": status}).Error
}

func UpdateRestoreTaskProgress(db *gorm.DB, task, files, bytes int64) (err error) {
	return db.Model(&RestoreTaskModel{}).Where("id = ?", task).Updates(
		map[string]interface{}{"restore_bytes": bytes, "restore_files": files}).Error
}

func EndRestoreTask(db *gorm.DB, task int64, success bool) (err error) {
	return db.Model(&RestoreTaskModel{}).Where("id = ?", task).Updates(
		map[string]interface{}{
			"status":   meta.RESTOREFINISH,
			"end_time": time.Now(),
			"success":  success},
	).Error
}
