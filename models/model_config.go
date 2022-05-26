package models

import (
	"encoding/json"
	"fmt"
	"gorm.io/gorm"
	"jingrongshuan/rongan-fnotify/meta"
	"strings"
)

// ConfigModel 用于监控同步配置
type ConfigModel struct {
	ID         int64  `gorm:"column:id;primaryKey;AUTO_INCREMENT;"`
	Origin     int64  `gorm:"column:origin"`                // 同步源机ID
	Server     int64  `gorm:"column:server"`                // 备份服务器ID
	Dir        string `gorm:"column:dir;type:text"`         // 监控源目录
	Include    string `gorm:"column:include;type:text"`     // 监控白名单，需要监控哪些类型文件
	Exclude    string `gorm:"column:exclude;type:text"`     // 监控黑名单，排除监控哪些类型文件
	Recursion  bool   `gorm:"column:recursion;default:t"`   // 是否递归监控
	ValidDays  int64  `gorm:"column:valid_days;default:-1"` // 备份最近多久的文件, -1表示无限制
	Depth      int64  `gorm:"column:depth;default:-1"`      // 监控目录深度，仅在Recursion为true时有用，默认为-1(无限制)
	Compress   bool   `gorm:"column:compress;default:f"`    // 是否开启压缩
	RetainSecs int64  `gorm:"column:retainsecs;default:-1"` // 保留期限，-1表示无限制
	Cores      int64  `gorm:"column:cores;default:1"`       // 并发数，默认1
	Enable     bool   `gorm:"column:enable;default:f"`      // 启用配置
	/* XXX Target现行逻辑说明
	target 配置目标目录时，可以指定下述几种类型：
	1. 云存储（AWS S3）
	{
	    "target_type": "S3",
	    "target_conf": {
			"access_key": "access_key",
			"secret_key": "secret_key",
			"endpoint": "www.endpoint.com"，
			"region": "us-east-1",
			"ssl": false,
			"bucket": "bucket-name"
			"path": true,
		}
	}
	2. 异机(HOST)
	{
	    "target_type": "HOST",
	    "target_conf": {
			"client_id": -1,
	        "remote_path": "C:\\backup\\"
		}
	}
	TODO support more storage type...
	*/
	Target  string `gorm:"column:target"`   // 同步目标信息
	ExtInfo string `gorm:"column:ext_info"` // 扩展参数，JSON格式
}

func (_ ConfigModel) TableName() string {
	return ModelDefaultSchema + ".config"
}

func (c *ConfigModel) String() string {
	return fmt.Sprintf("<ConfigModel(ID=%v, Dir=%v>", c.ID, c.Dir)
}

func QueryConfigByID(db *gorm.DB, cid int64) (c ConfigModel, err error) {
	r := db.Model(&ConfigModel{}).Where("id = ?", cid).Take(&c)
	return c, r.Error
}

func Is2Host(config ConfigModel) bool {
	return strings.Contains(config.Target, fmt.Sprintf(`"%s"`, meta.WatchingConfTargetHost))
}

func Is2S3(config ConfigModel) bool {
	return strings.Contains(config.Target, fmt.Sprintf(`"%s"`, meta.WatchingConfTargetS3))
}

type ClientInfo struct {
	ID      int64  `json:"id"`
	Address string `json:"address"`
	Type    string `json:"type"`
}

func QueryTargetHostInfoByConf(db *gorm.DB, conf int64) (c ClientInfo, err error) {
	var tch TargetHost
	if tch, err = QueryTargetConfHostByConf(db, conf); err != nil {
		return
	}
	return QueryHostInfoByHostID(db, tch.TargetConfHost.ClientID)
}

func QueryHostInfoByHostID(db *gorm.DB, id int64) (c ClientInfo, err error) {
	if r := db.Raw(
		fmt.Sprintf("SELECT id, address, type FROM web.t_client WHERE id = %v;", id)).Scan(&c); r.Error != nil {
		return c, r.Error
	}
	return
}

type TargetS3 struct {
	TargetType   string       `json:"target_type"`
	TargetConfS3 TargetConfS3 `json:"target_conf"`
}

type TargetConfS3 struct {
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
	Endpoint  string `json:"endpoint"`
	Region    string `json:"region"`
	Bucket    string `json:"bucket"`
	SSL       bool   `json:"ssl"`
	Path      bool   `json:"path"`
}

type TargetHost struct {
	TargetType     string         `json:"target_type"`
	TargetConfHost TargetConfHost `json:"target_conf"`
}

type TargetConfHost struct {
	ClientID   int64  `json:"client_id"`
	RemotePath string `json:"remote_path"`
}

func QueryTargetConfS3ByConf(db *gorm.DB, conf int64) (tcs TargetS3, err error) {
	config, err := QueryConfig(db, conf)
	if err != nil {
		return tcs, err
	}
	err = json.Unmarshal([]byte(config.Target), &tcs)
	return
}

func QueryTargetConfHostByConf(db *gorm.DB, conf int64) (tch TargetHost, err error) {
	config, err := QueryConfig(db, conf)
	if err != nil {
		return tch, err
	}
	err = json.Unmarshal([]byte(config.Target), &tch)
	return
}

func IsEnable(db *gorm.DB, confID int64) (_ bool, err error) {
	var c ConfigModel
	r := db.Model(&ConfigModel{}).Where("id = ?", confID).Take(&c)
	if r.Error != nil {
		//logger.Fmt.Warnf("IsEnable err=%v", r.Error)
		return false, r.Error
	}
	return c.Enable, r.Error
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

type ServerAddress struct {
	ServerAddress string `json:"server_address"`
}
