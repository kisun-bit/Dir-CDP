package logic

import (
	"fmt"
	"gorm.io/gorm"
	"jingrongshuan/rongan-fnotify/meta"
	"jingrongshuan/rongan-fnotify/models"
	"time"
)

type LogRecycle struct {
	ConfigID, TaskID int64
	KeepDays         int
	DB               *gorm.DB
	Stop             bool
}

func NewLogRecycle(config, task int64, keep int, db *gorm.DB) (lr *LogRecycle) {
	lr = new(LogRecycle)
	lr.ConfigID, lr.TaskID, lr.KeepDays, lr.DB = config, task, keep, db
	return
}

func (lr *LogRecycle) Str() string {
	return fmt.Sprintf("<LogRecycle(Config=%v, TaskID=%v, KeepDays=%v>",
		lr.ConfigID, lr.TaskID, lr.KeepDays)
}

func (lr *LogRecycle) Start() {
	lr.logic()
}

func (lr *LogRecycle) SetStop() {
	if lr.Stop {
		return
	}
	lr.Stop = true
}

func (lr *LogRecycle) logic() {
	defer func() {
		logger.Fmt.Infof("%v.logic exit...", lr.Str())
	}()

	if lr.KeepDays == meta.UnsetInt || lr.KeepDays == 0 {
		logger.Fmt.Infof("%v.logic permanent log", lr.Str())
		return
	}

	d, err := time.ParseDuration(fmt.Sprintf("-%vh", 24*lr.KeepDays))
	if err != nil {
		logger.Fmt.Infof("%v.logic ParseDuration Err=%v", lr.Str(), err)
		return
	}

	for {
		if lr.Stop {
			break
		}
		time.Sleep(meta.OneDay)
		validTime := time.Now().Add(d)
		if err = models.DeleteCDPIOLogsByTime(lr.DB, lr.ConfigID, validTime.Unix()); err != nil {
			logger.Fmt.Warnf("%v.DeleteCDPIOLogsByTime Err=%v", lr.Str(), err)
		}
		if err = models.DeleteCDPSnapLogsByTime(lr.DB, lr.ConfigID, validTime.Unix()); err != nil {
			logger.Fmt.Warnf("%v.DeleteCDPSnapLogsByTime Err=%v", lr.Str(), err)
		}
	}
}
