package logic

import (
	"github.com/kr/pretty"
	"gorm.io/gorm"
	"jingrongshuan/rongan-fnotify/meta"
	"jingrongshuan/rongan-fnotify/models"
	"sync/atomic"
	"time"
)

type Progress struct {
	ProcessedSize   int64         `json:"processed_size"`
	ProcessedNumber int64         `json:"processed_number"`
	Interval        time.Duration `json:"interval"`
	TaskID          int64         `json:"task_id"`
	Type            meta.TaskType
	Server          string
	DB              *gorm.DB `json:"db"`
	stop            bool
}

func NewProgress(interval time.Duration, task int64, db *gorm.DB, server string, type_ meta.TaskType) *Progress {
	p := &Progress{
		ProcessedSize:   0,
		ProcessedNumber: 0,
		Interval:        interval,
		TaskID:          task,
		DB:              db,
		Server:          server,
		Type:            type_,
	}
	p.setStart()
	return p
}

func (p *Progress) setStart() {
	if p.Type == meta.TaskTypeRestore {
		t, err := models.QueryRestoreTaskByID(p.DB, p.TaskID)
		if err != nil {
			return
		}
		p.ProcessedSize = t.RestoreBytes
		p.ProcessedNumber = t.RestoreFiles
	} else {
		t, err := models.QueryBackupTaskByID(p.DB, p.TaskID)
		if err != nil {
			return
		}
		p.ProcessedSize = t.Bytes
		p.ProcessedNumber = t.Files
	}
}

func (p *Progress) AddSize(delta int64) {
	atomic.AddInt64(&p.ProcessedSize, delta)
}

func (p *Progress) AddNum(delta int64) {
	atomic.AddInt64(&p.ProcessedNumber, delta)
}

func (p *Progress) LoadSize() int64 {
	return atomic.LoadInt64(&p.ProcessedSize)
}

func (p *Progress) LoadNum() int64 {
	return atomic.LoadInt64(&p.ProcessedNumber)
}

func (p *Progress) String() string {
	return pretty.Sprint(p)
}

func (p *Progress) Stop() {
	p.stop = true
}

func (p *Progress) Gather() {
	for true {
		if p.stop {
			break
		}
		if err := p.UploadTaskProcess(); err != nil {
			logger.Error("Progress.Gather update err. stop processing info")
			break
		}
		time.Sleep(p.Interval)
	}
}

func (p *Progress) UploadTaskProcess() (err error) {
	if p.Type == meta.TaskTypeRestore {
		return models.UpdateRestoreTaskProgress(p.DB, p.TaskID, p.LoadNum(), p.LoadSize())
	}
	return models.UpdateBackupTaskProgress(p.DB, p.TaskID, p.LoadNum(), p.LoadSize())
}
