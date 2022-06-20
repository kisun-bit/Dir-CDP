package models

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/thoas/go-funk"
	"gorm.io/gorm"
	"jingrongshuan/rongan-fnotify/meta"
	"strings"
)

type ConfigModelExtInfo struct {
	ServerAddress string `json:"server_address"`
}

type ConfigModelTimeStrategy struct {
	Pre   int    `json:"days"`
	After int    `json:"daysLast"`
	Type  string `json:"timeType"`
}

type ConfigModelTarget struct {
	TargetType string `json:"target_type"`
	TargetID   int64  `json:"target_id"`
}

type BackupDirMap struct {
	LocalOrigin                    string `json:"origin"`    // 本地备份源目录
	StorageBucket                  string `json:"bucket"`    // 目标桶
	StoragePrefixForBucketOrRemote string `json:"target"`    // 目标存储的前缀、目录
	StorageVolume                  string `json:"volume"`    // 目标卷
	LocalEnableRecursion           bool   `json:"recursion"` // 是否支持递归
	LocalEnabledDepth              int    `json:"depth"`     // 指定备份深度
}

// ConfigModel 用于监控同步配置
type ConfigModel struct {
	ID     int64  `gorm:"column:id;primaryKey;AUTO_INCREMENT;"`
	Name   string `gorm:"column:name;type:text"`
	Desc   string `gorm:"column:desc;type:text"`
	Origin int64  `gorm:"column:origin"` // 同步源机ID
	Server int64  `gorm:"column:server"` // 备份服务器ID
	/*XXX DirsMapping 源目录与目标目录的映射关系

	支持多个目录，其结构如下
	[
		{
			"origin": "/opt/dir1",
			"bucket": "",
			"target": "/opt/tmp",
			"recursion": true,
			"depth": -1,
		},
		{
			"origin": "/opt/dir1",
			"bucket": "",
			"target": "/opt/tmp",
			"recursion": false,
			"depth": -1,
		},
	]
	*/
	DirsMapping string `gorm:"column:dirs_mapping;type:text"`
	Include     string `gorm:"column:include;type:text"` // 监控白名单，需要监控哪些类型文件
	Exclude     string `gorm:"column:exclude;type:text"` // 监控黑名单，排除监控哪些类型文件
	/*XXX TimeStrategy 备份时间策略

	支持下述4种策略
	- 所有时间点(type=no_limit)
	- x天以前的(pre=x, type=pre)
	- 最近x天的(pre=y, type=after)
	- 最近x天到y天的(pre=x, after=y, type=center)

	结构
	{
		"pre": 3,
		"after": 4,
		"type": "pre|center|after|no_limit"
	}
	*/
	TimeStrategy  string `gorm:"column:time_strategy;type:text"`  // 备份最近多久的文件, -1表示无限制
	RetainSecs    int64  `gorm:"column:keep_secs;default:-1"`     // 保留期限，-1表示无限制
	Cores         int64  `gorm:"column:cores;default:1"`          // 并发数，默认1
	EnableVersion bool   `gorm:"column:enable_version;default:t"` // 启用版本, 默认为True
	Enable        bool   `gorm:"column:enable;default:f"`         // 启用配置
	/* XXX Target现行逻辑说明
	target 配置目标目录时，可以指定下述几种类型：
	1. 云存储（AWS S3）
	{
	    "target_type": "S3",
	    "target_id": 1
	}
	2. 异机(HOST)
	{
	    "target_type": "HOST",
	    "target_id": 2
	}
	*/
	Target string `gorm:"column:target;type:text"` // 同步目标信息
	/*XXX ExtInfo 扩展参数
	{
		"server_address": "192.168.1.90"
	}
	*/
	ExtInfo string `gorm:"column:ext_info;type:text"` // 扩展参数，JSON格式

	OriginHostJson   Client                  `gorm:"-"`
	TargetHostJson   Client                  `gorm:"-"`
	S3ConfJson       S3Conf                  `gorm:"-"`
	ExtInfoJson      ConfigModelExtInfo      `gorm:"-"`
	TimeStrategyJson ConfigModelTimeStrategy `gorm:"-"`
	TargetJson       ConfigModelTarget       `gorm:"-"`
	DirsMappingJson  []BackupDirMap          `gorm:"-"`
}

func (_ ConfigModel) TableName() string {
	return ModelDefaultSchema + ".config"
}

func (c *ConfigModel) String() string {
	return fmt.Sprintf("<ConfigModel(ID=%v>", c.ID)
}

func (c *ConfigModel) LoadsJsonFields(db *gorm.DB) (err error) {
	if err = c.loadOriginHostJson(db); err != nil {
		logger.Fmt.Errorf("loadOriginHostJson ERR=%v", err)
		return
	}
	if err = c.loadTargetJson(); err != nil {
		logger.Fmt.Errorf("loadTargetJson ERR=%v", err)
		return
	}
	if err = c.loadTargetHostJson(db); err != nil {
		logger.Fmt.Errorf("loadTargetHostJson ERR=%v", err)
		return
	}
	if err = c.loadExtInfoJson(); err != nil {
		logger.Fmt.Errorf("loadExtInfoJson ERR=%v", err)
		return
	}
	if err = c.loadTimeStrategyJson(); err != nil {
		logger.Fmt.Errorf("loadTimeStrategyJson ERR=%v", err)
		return
	}
	if err = c.loadDirsMappingJson(); err != nil {
		logger.Fmt.Errorf("loadDirsMappingJson ERR=%v", err)
		return
	}
	if err = c.loadS3ConfJson(db); err != nil {
		logger.Fmt.Errorf("loadS3ConfJson ERR=%v", err)
		return
	}
	return
}

func (c *ConfigModel) loadOriginHostJson(db *gorm.DB) (err error) {
	c.OriginHostJson, err = QueryClientInfoByID(db, c.Origin)
	return
}

func (c *ConfigModel) loadTargetHostJson(db *gorm.DB) (err error) {
	if c.TargetJson.TargetType != meta.WatchingConfTargetHost {
		return nil
	}
	c.TargetHostJson, err = QueryClientInfoByID(db, c.TargetJson.TargetID)
	return
}

func (c *ConfigModel) loadExtInfoJson() (err error) {
	return json.Unmarshal([]byte(c.ExtInfo), &c.ExtInfoJson)
}

func (c *ConfigModel) loadTimeStrategyJson() (err error) {
	return json.Unmarshal([]byte(c.TimeStrategy), &c.TimeStrategyJson)
}

func (c *ConfigModel) loadTargetJson() (err error) {
	return json.Unmarshal([]byte(c.Target), &c.TargetJson)
}

func (c *ConfigModel) loadDirsMappingJson() (err error) {
	return json.Unmarshal([]byte(c.DirsMapping), &c.DirsMappingJson)
}

func (c *ConfigModel) loadS3ConfJson(db *gorm.DB) (err error) {
	if c.TargetJson.TargetType == meta.WatchingConfTargetHost {
		return nil
	}
	c.S3ConfJson, err = QueryS3ConfByID(db, c.TargetJson.TargetID)
	return
}

func (c *ConfigModel) UniqBuckets() (bs []string) {
	tmp := make([]string, 0)
	for _, v := range c.DirsMappingJson {
		tmp = append(tmp, v.StorageBucket)
	}
	return funk.UniqString(tmp)
}

func (c *ConfigModel) UniqDirs() (ods []string) {
	tmp := make([]string, 0)
	for _, v := range c.DirsMappingJson {
		tmp = append(tmp, v.LocalOrigin)
	}
	return funk.UniqString(tmp)
}

func (c *ConfigModel) SpecifyTarget(path string) (origin, bucket, volume, prefix string, err error) {
	var item BackupDirMap
	for _, dm := range c.DirsMappingJson {
		if strings.HasPrefix(path, dm.LocalOrigin) && len(dm.LocalOrigin) > len(item.LocalOrigin) {
			item = dm
		}
	}
	if item.LocalOrigin == meta.UnsetStr {
		err = errors.New("failed to match origin path")
		return
	}
	return item.LocalOrigin, item.StorageBucket, item.StorageVolume, item.StoragePrefixForBucketOrRemote, err
}

func (c *ConfigModel) SpecifyTargetWhenHost(path string) (origin, remote string, err error) {
	o, _, _, p, e := c.SpecifyTarget(path)
	if err = e; err != nil {
		return
	}
	return o, p, nil
}

func QueryConfigByID(db *gorm.DB, cid int64) (c ConfigModel, err error) {
	r := db.Model(&ConfigModel{}).Where("id = ?", cid).Take(&c)
	return c, r.Error
}

func IsEnable(db *gorm.DB, confID int64) (_ bool, err error) {
	var c ConfigModel
	r := db.Model(&ConfigModel{}).Where("id = ?", confID).Take(&c)
	if r.Error != nil {
		return false, r.Error
	}
	return c.Enable, nil
}

func QueryConfig(db *gorm.DB, conf int64) (c ConfigModel, err error) {
	r := db.Model(&ConfigModel{}).Where("id = ?", conf).Take(&c)
	return c, r.Error
}

func QueryAllEnabledConfigs(db *gorm.DB) (cs []ConfigModel, err error) {
	r := db.Model(&ConfigModel{}).Where("enable = ?", "t").Find(&cs)
	return cs, r.Error
}

func EnableConfig(db *gorm.DB, conf int64) (err error) {
	r := db.Model(&ConfigModel{}).Where("id = ?", conf).Updates(
		map[string]interface{}{"enable": true})
	return r.Error
}
