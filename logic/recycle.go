package logic

import "jingrongshuan/rongan-fnotify/models"

// 空间回收器
type ResourceCollector struct {
	configObj *models.ConfigModel
	taskObj   *models.BackupTaskModel
}

func NewResourceCollector(config *models.ConfigModel, task *models.BackupTaskModel) *ResourceCollector {
	return &ResourceCollector{
		configObj: config,
		taskObj:   task,
	}
}

func (rc *ResourceCollector) Start() {
	// TODO 空间回收逻辑
}
