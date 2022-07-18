package models

import (
	"fmt"
	"gorm.io/gorm"
	"time"
)

type Snapshot struct {
	ID     int64      `gorm:"column:id;primaryKey;AUTO_INCREMENT;"`
	Ver    string     `gorm:"column:version;"` // 一个版本下面存在多个卷的快照
	Time   *time.Time `gorm:"column:time"`
	Snap   string     `gorm:"column:snapshot;type:text;"`
	Type   string     `gorm:"column:type;type:text"`
	Target int64      `gorm:"column:target;"`
	Origin int64      `gorm:"column:origin;"`
	Config int64      `gorm:"column:config"`
	Task   int64      `gorm:"column:task;"`
	Ext    string     `gorm:"column:ext;type:text;"`
}

func (_ Snapshot) TableName() string {
	return ModelDefaultSchema + ".snapshot"
}

func (c *Snapshot) String() string {
	return fmt.Sprintf("<Snapshot(ID=%v>", c.ID)
}

//func QueryLastedSnapshot(db *gorm.DB, config, task int64) (_ string, err error) {
//	var s Snapshot
//	err = db.Model(&Snapshot{}).Where(
//		"config=? AND task=?", config, task).Order("time DESC").First(&s).Error
//	return s.Ver, err
//}

func QueryNeedlessSnapshots(db *gorm.DB, config, task, keep int64) (ss []Snapshot, err error) {
	r := db.Model(&Snapshot{}).Where(
		"config=? AND task=?", config, task).Order("id DESC").Offset(int(keep)).Find(&ss)
	return ss, r.Error
}

func QuerySnapshotByVersion(db *gorm.DB, version string) (s Snapshot, err error) {
	err = db.Model(&Snapshot{}).Where(
		"version = ?", version).First(&s).Error
	return
}

func DeleteSnapshotByID(db *gorm.DB, id int64) (err error) {
	return db.Where("id = ?", id).Delete(&Snapshot{}).Error
}

func QuerySnapshotsAfterVersion(db *gorm.DB, config, id int64) (ss []Snapshot, err error) {
	r := db.Model(&Snapshot{}).Where("config = ? AND id >= ?", config, id).Find(&ss)
	err = r.Error
	return
}

func DeleteSnapshotsAfterID(db *gorm.DB, config, id int64) (err error) {
	return db.Where("id >= ? AND config = ?", id, config).Delete(&Snapshot{}).Error
}

func CountSnapshots(db *gorm.DB, config, task int64) (count int64, err error) {
	err = db.Model(&Snapshot{}).Where(
		"config=? AND task=?", config, task).Count(&count).Error
	return
}

func CreateSnapshot(db *gorm.DB, snap Snapshot) (err error) {
	return db.Model(&Snapshot{}).Create(&snap).Error
}

func UpdateSnapByID(db *gorm.DB, id int64, snap string) (err error) {
	return db.Model(&Snapshot{}).Where("id = ?", id).Updates(
		map[string]interface{}{"snapshot": snap}).Error
}
