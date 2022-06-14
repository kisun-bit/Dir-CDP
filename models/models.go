package models

import (
	"fmt"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gLogger "gorm.io/gorm/logger"
	"jingrongshuan/rongan-fnotify/logging"
	"jingrongshuan/rongan-fnotify/meta"
	"strconv"
	"strings"
)

const ModelDefaultSchema = "fsnotify"

var logger = logging.Logger

// DBProxy 初始化文件流水的版本数据库
// 每一个ConfigModel对应一个桶（FileFlow_%2d表），即通过conf_id分表
type DBProxy struct {
	DB *gorm.DB
}

func NewDBInstanceByIP(ip string) (db *gorm.DB, err error) {
	return gorm.Open(
		postgres.Open(fmt.Sprintf(meta.DatabaseDriverTemplate, ip)),
		&gorm.Config{Logger: gLogger.Default.LogMode(gLogger.Silent)},
	)
}

func NewDBProxy(ip string) (fvb *DBProxy, err error) {
	fvb = new(DBProxy)
	if fvb.DB, err = NewDBInstanceByIP(ip); err != nil {
		logger.Fmt.Errorf("NewDBProxy gorm.Open err=%v", err)
		return
	}
	if err = fvb.initSchema(); err != nil {
		logger.Fmt.Errorf("NewDBProxy initSchema err=%v", err)
		return
	}
	if err = fvb.migrate(); err != nil {
		logger.Fmt.Errorf("NewDBProxy migrate err=%v", err)
		return
	}
	if err = fvb.sharding(); err != nil {
		logger.Fmt.Errorf("NewDBProxy sharding err=%v", err)
		return
	}
	return
}

func (fvb *DBProxy) migrate() (err error) {
	if err = fvb.DB.AutoMigrate(&ConfigModel{}); err != nil {
		return
	}
	if err = fvb.DB.AutoMigrate(&BackupTaskModel{}); err != nil {
		return
	}
	if err = fvb.DB.AutoMigrate(&RestoreTaskModel{}); err != nil {
		return
	}
	if err = fvb.DB.AutoMigrate(&Logging{}); err != nil {
		return
	}
	return
}

func (fvb *DBProxy) initSchema() (err error) {
	return fvb.DB.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", ModelDefaultSchema)).Error
}

// sharding 实现分表功能，每启用一个监控同步配置，便会新生成一个File表和一个Dir表
func (fvb *DBProxy) sharding() (err error) {
	cos, err := fvb.queryConfigObjects()
	if err != nil {
		logger.Fmt.Errorf("DBProxy.sharding queryConfigObjects err=%v", err)
		return
	}
	for _, c := range cos {
		if err = fvb.RegisterEventFileModel(c.ID); err != nil {
			return
		}
		if err = fvb.RegisterEventDirModel(c.ID); err != nil {
			return
		}
	}
	//middleware := sharding.Register(sharding.Config{
	//	ShardingKey:         "conf_id",
	//	NumberOfShards:      2147483647,
	//	PrimaryKeyGenerator: sharding.PKSnowflake,
	//}, "event_file")
	//if err = fvb.DB.Use(middleware); err != nil {
	//	logger.Fmt.Errorf("DBProxy.sharding use sharding-middleware err=%v", err)
	//	return
	//}
	return
}

func (fvb *DBProxy) RegisterEventFileModel(conf int64) (err error) {
	defer func() {
		if err != nil {
			if err_ := DeleteFileFlowByConfID(fvb.DB, conf); err_ != nil {
				logger.Fmt.Errorf("DBProxy.RegisterEventFileModel err=%v", err_)
			}
		}
	}()

	table := "event_file" + "_" + strconv.FormatInt(conf, 10)
	sql := strings.ReplaceAll(FileFlowCreateDDL, "event_file", table)
	if r := fvb.DB.Exec(sql); r.Error != nil {
		logger.Fmt.Errorf("DBProxy.RegisterEventFileModel failed to exec `%s` err=%v", sql, r.Error)
		return r.Error
	}
	return nil
}

func (fvb *DBProxy) RegisterEventDirModel(conf int64) (err error) {
	defer func() {
		if err != nil {
			if err_ := DeleteDirByConfID(fvb.DB, conf); err_ != nil {
				logger.Fmt.Errorf("DBProxy.RegisterEventDirModel err=%v", err_)
			}
		}
	}()

	table := "event_dir" + "_" + strconv.FormatInt(conf, 10)
	sql := strings.ReplaceAll(DirCreateDDL, "event_dir", table)
	if r := fvb.DB.Exec(sql); r.Error != nil {
		logger.Fmt.Errorf("DBProxy.RegisterEventDirModel failed to exec `%s` err=%v", sql, r.Error)
		return r.Error
	}
	return nil
}

func (fvb *DBProxy) queryConfigObjects() (cos []ConfigModel, err error) {
	r := fvb.DB.Model(&ConfigModel{}).Where("enable = ?", "t").Find(&cos)
	return cos, r.Error
}
