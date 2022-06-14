package logic

import (
	"fmt"
	"jingrongshuan/rongan-fnotify/meta"
	"jingrongshuan/rongan-fnotify/models"
	"time"
)

// hangEventMonitor 任务终止事件监视器
type hangEventMonitor struct {
	task  interface{}
	type_ meta.TaskType
}

func NewMonitorWithBackup(cdp *CDPExecutor) *hangEventMonitor {
	hem := new(hangEventMonitor)
	hem.task = cdp
	hem.type_ = meta.TaskTypeBackup
	return hem
}

func NewMonitorWithRestore(restore *RestoreTask) *hangEventMonitor {
	hem := new(hangEventMonitor)
	hem.task = restore
	hem.type_ = meta.TaskTypeRestore
	return hem
}

func (h *hangEventMonitor) monitor() {
	logger.Fmt.Infof("%v.monitor 任务监控器已启动, 正在捕捉中断事件...", h.Str())

	defer func() {
		logger.Fmt.Infof("%v.monitor【终止】", h.Str())
	}()

	var (
		ok  bool
		err error
	)

	for {
		ticker := time.NewTicker(meta.DefaultRetryTimeInterval)

		// TODO 可考虑用任务接口改写...
		for range ticker.C {
			if h.type_ == meta.TaskTypeBackup {
				ok, err = models.IsEnable(
					h.CDPExecutorObj().DBDriver.DB, h.CDPExecutorObj().confObj.ID)
			} else if h.type_ == meta.TaskTypeRestore {
				ok, err = models.IsRestoreCancel(
					h.RestoreObj().DBDriver.DB, h.RestoreObj().taskObj.ID)
			}

			if !ok && err == nil {
				logger.Fmt.Infof("%v.monitor !!!!!!!!!!!!!!!!! 【取消事件】", h.Str())
				if h.type_ == meta.TaskTypeBackup {
					h.CDPExecutorObj().exitWhenErr(
						ExitErrCodeUserCancel, ErrByCode(ExitErrCodeUserCancel))
				} else if h.type_ == meta.TaskTypeRestore {
					h.RestoreObj().exitWhenErr(
						ExitErrCodeUserCancel, ErrByCode(ExitErrCodeUserCancel))
				}
			} else if err != nil {
				logger.Fmt.Infof("%v.monitor !!!!!!!!!!!!!!!!! 【备份服务器连接失败】", h.Str())
				if h.type_ == meta.TaskTypeBackup {
					h.CDPExecutorObj().exitWhenErr(
						ExitErrCodeTargetConn, ErrByCode(ExitErrCodeTargetConn))
				} else if h.type_ == meta.TaskTypeRestore {
					h.RestoreObj().exitWhenErr(
						ExitErrCodeTargetConn, ErrByCode(ExitErrCodeTargetConn))
				}
			} else if ok {
				continue
			}
			ticker.Stop()
		}
	}
}

func (h *hangEventMonitor) CDPExecutorObj() *CDPExecutor {
	c, ok := h.task.(*CDPExecutor)
	if !ok {
		panic("hangEventMonitor. failed to convert to CDPExecutorObj")
	}
	return c
}

func (h *hangEventMonitor) RestoreObj() *RestoreTask {
	r, ok := h.task.(*RestoreTask)
	if !ok {
		panic("hangEventMonitor. failed to convert to RestoreTask")
	}
	return r
}

func (h *hangEventMonitor) Str() string {
	return fmt.Sprintf("%v.<hangEventMonitor-Thread>", h.CDPExecutorObj().Str())
}
