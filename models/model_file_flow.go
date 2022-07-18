package models

import (
	"database/sql"
	"fmt"
	"gorm.io/gorm"
	"jingrongshuan/rongan-fnotify/meta"
	"jingrongshuan/rongan-fnotify/nt_notify"
	"path/filepath"
	"strings"
	"time"
)

// event_file表将以config表ID进行分表处理
// 其模板建表语句如下：
var FileFlowCreateDDL = `
CREATE SEQUENCE IF NOT EXISTS "fsnotify".event_file_id_seq
    INCREMENT 1
    START 1
    MINVALUE 1
    MAXVALUE 93422337368375807
    CACHE 1;
CREATE TABLE IF NOT EXISTS "fsnotify"."event_file" (
		"id" int8 NOT NULL DEFAULT nextval( '"fsnotify".event_file_id_seq' :: regclass),
        "timestamp" int8 NOT NULL,
        "path" TEXT COLLATE "pg_catalog"."default" NOT NULL,
        "name" TEXT COLLATE "pg_catalog"."default" NOT NULL,
        "type" int8 NOT NULL DEFAULT 8,
        "mode" int8,
        "size" int8,
        "create" int8,
        "event" VARCHAR ( 32 ) COLLATE "pg_catalog"."default",
        "conf_id" int8,
        "version" TEXT COLLATE "pg_catalog"."default" NOT NULL,
        "parent" TEXT COLLATE "pg_catalog"."default" NOT NULL,
        "storage" TEXT COLLATE "pg_catalog"."default" NOT NULL,
        "status" VARCHAR ( 16 ) COLLATE "pg_catalog"."default",
        "tag" VARCHAR ( 32 ) COLLATE "pg_catalog"."default",
        "checksum" TEXT COLLATE "pg_catalog"."default",
        "ext_info" Text COLLATE "pg_catalog"."default",
        CONSTRAINT "event_file_pkey" PRIMARY KEY ( "id" )
);
ALTER TABLE "fsnotify"."event_file" OWNER TO "postgres";
CREATE INDEX IF NOT EXISTS "idx_fsnotify_event_file_path" ON "fsnotify"."event_file" USING btree ( "path" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST );
CREATE INDEX IF NOT EXISTS "idx_fsnotify_event_file_timestamp" ON "fsnotify"."event_file" USING btree ( "timestamp" "pg_catalog"."int8_ops" ASC NULLS LAST );
CREATE INDEX IF NOT EXISTS "idx_fsnotify_event_file_parent" ON "fsnotify"."event_file" USING btree ( "parent" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST );`

// EventFileModel 用于记录发生变更的文件项流水信息
type EventFileModel struct {
	nt_notify.FileInfo
	ID      int64
	Type    int    // 文件类型，1表示文件，2表示目录，4表示链接，8表示其他
	Mode    int64  // 文件权限，uint32类型
	Create  int64  // 创建时间戳，Win32捕捉变更事件的时间
	Event   string // 变更事件类型
	ConfID  int64  `json:"conf_id"` // 配置ID，表明此记录是基于哪一个配置所捕捉的
	Version string // 版本号，conf_id|full_path|file_type|size|mtime
	Tag     string // 若启动版本，该处值为时间标签（20220101094423）
	Parent  string // 父目录
	Status  string // 状态，WATCHED(被监控到)|SYNCING(正在上传)|ERROR|FINISHED
	Ext     string `json:"ext_info"` // 扩展字段
	Storage string // 存储路径
}

func (flow *EventFileModel) String() string {
	return fmt.Sprintf("<FileLogging(ID=%v, Flag=%v, Event=%v>", flow.ID, flow.Path, flow.Version)
}

func _eventFileTable(conf int64) string {
	return fmt.Sprintf(`"fsnotify"."event_file_%v"`, conf)
}

type FFID struct {
	ID int64 `json:"id"`
}

func CreateFileFlowModel(db *gorm.DB, conf int64, ff *EventFileModel) (err error) {
	sqlTmp := `insert into %v 
    ("timestamp", "path", "name", "type", "mode", "size", "create", "event", "conf_id", "version", "parent", "storage", "status", "tag")
    values (%v, '%v', '%v', %v, %v, %v, %v, '%v', %v, '%v', '%v', '%v', '%v', '%v') RETURNING id`
	sql_ := fmt.Sprintf(sqlTmp,
		_eventFileTable(conf),
		ff.Time,
		strings.ReplaceAll(ff.Path, `'`, `''`),
		strings.ReplaceAll(ff.Name, `'`, `''`),
		ff.Type,
		ff.Mode,
		ff.Size,
		ff.Create,
		ff.Event,
		ff.ConfID,
		strings.ReplaceAll(ff.Version, `'`, `''`),
		strings.ReplaceAll(ff.Parent, `'`, `''`),
		strings.ReplaceAll(ff.Storage, `'`, `''`),
		ff.Status,
		ff.Tag)
	i := new(FFID)
	err = db.Raw(sql_).Scan(&i).Error
	ff.ID = i.ID
	return err
}

func DeleteFileFlowByConfID(db *gorm.DB, conf int64) (err error) {
	t := fmt.Sprintf("event_file_%v", conf)
	for _, it := range []string{
		"idx_fsnotify_event_file_path",
		"idx_fsnotify_event_file_time",
		"idx_fsnotify_event_file_version",
		"idx_fsnotify_event_file_parent",
		"idx_fsnotify_event_file_type",
		"idx_fsnotify_event_file_storage",
	} {
		idx := strings.ReplaceAll(it, "event_file", t)
		if r := db.Exec(fmt.Sprintf(`DROP INDEX IF EXISTS %s ON "fsnotify"."event_file_%v"`,
			idx, conf)); r.Error != nil {
			return r.Error
		}
	}
	if r := db.Exec(`DROP TABLE IF EXISTS ` + t); r.Error != nil {
		return r.Error
	}
	s := fmt.Sprintf(`"fsnotify".event_file_%v_id_seq`, conf)
	if r := db.Exec(`DROP SEQUENCE IF EXISTS ` + s); r.Error != nil {
		return r.Error
	}
	return nil
}

func DeleteNotUploadFileFlows(db *gorm.DB, conf int64) (err error) {
	sql_ := fmt.Sprintf(`DELETE FROM %v WHERE conf_id=%v AND status != '%v'`,
		_eventFileTable(conf), conf, meta.FFStatusFinished)
	return db.Exec(sql_).Error
}

func UpdateFileFlowStatus(db *gorm.DB, conf, f int64, status string) (err error) {
	sql_ := fmt.Sprintf(`UPDATE %v SET status='%v' WHERE id=%v`, _eventFileTable(conf), status, f)
	return db.Exec(sql_).Error
}

type _exist struct {
	Exists string `json:"exists"`
}

func ExistedHistoryVersionFile(db *gorm.DB, conf int64, path string) bool {
	sql_ := fmt.Sprintf(`SELECT EXISTS(SELECT 1 FROM %v WHERE path='%v')`, _eventFileTable(conf), path)
	e := new(_exist)
	if db.Raw(sql_).Scan(e).Error != nil {
		return false
	}
	return e.Exists != "false"
}

func DeleteByPath(db *gorm.DB, conf int64, path string) (err error) {
	sql_ := fmt.Sprintf(`DELETE FROM %v WHERE path='%v'`, _eventFileTable(conf), path)
	return db.Exec(sql_).Error
}

func DeleteNoVersionFilesByPath(db *gorm.DB, conf int64, path string) (err error) {
	sql_ := fmt.Sprintf(`DELETE FROM %v WHERE path='%v' AND tag=''`, _eventFileTable(conf), path)
	return db.Exec(sql_).Error
}

func QueryLastSameNameFile(db *gorm.DB, conf int64, path string) (f EventFileModel, err error) {
	sql_ := fmt.Sprintf(`SELECT * FROM %v WHERE path='%v' ORDER BY id DESC LIMIT 1`,
		_eventFileTable(conf), strings.ReplaceAll(path, `'`, `''`))
	err = db.Raw(sql_).Scan(&f).Error
	return
}

func QueryRecursiveFilesIteratorInDir(db *gorm.DB, conf int64, dir string) (rows *sql.Rows, err error) {
	sql_ := fmt.Sprintf(`SELECT * FROM %v WHERE path LIKE "%v%%" AND type != 2`, _eventFileTable(conf), dir)
	return db.Raw(sql_).Rows()
}

func QueryFileByName(db *gorm.DB, conf int64, path string) (f EventFileModel, err error) {
	sql_ := fmt.Sprintf(`SELECT * FROM %v WHERE parent = '%v' AND name = '%v'`,
		_eventFileTable(conf), filepath.Dir(path), filepath.Base(path))
	err = db.Raw(sql_).Scan(&f).Error
	return
}

func QueryFileIteratorByTime(db *gorm.DB, conf int64, start, end *time.Time) (rows *sql.Rows, err error) {
	var sql_ string
	if start != nil && end != nil {
		sql_ = fmt.Sprintf(`SELECT * FROM %v WHERE timestamp BETWEEN "%v" AND "%v"`,
			_eventFileTable(conf), start.Unix(), end.Unix())
	} else if start != nil && end == nil {
		sql_ = fmt.Sprintf(`SELECT * FROM %v WHERE timestamp <= "%v"`,
			_eventFileTable(conf), start.Unix())
	} else if start == nil && end != nil {
		sql_ = fmt.Sprintf(`SELECT * FROM %v WHERE timestamp >= "%v"`,
			_eventFileTable(conf), end.Unix())
	} else {
		sql_ = fmt.Sprintf(`SELECT * FROM %v`,
			_eventFileTable(conf))
	}
	return db.Raw(sql_).Rows()
}

func QueryFileIteratorByConfig(db *gorm.DB, conf int64) (rows *sql.Rows, err error) {
	sql_ := fmt.Sprintf(`SELECT * FROM %v WHERE conf_id = %v`,
		_eventFileTable(conf), conf)
	return db.Raw(sql_).Rows()
}

func QueryNoVersionFilesByPath(db *gorm.DB, conf int64, path, name string) (fs []EventFileModel, err error) {
	sql_ := fmt.Sprintf(`SELECT * FROM %v where path = '%v' and name = '%v'`,
		_eventFileTable(conf), path, name)
	err = db.Raw(sql_).Scan(&fs).Error
	return
}

func IsEmptyTable(db *gorm.DB, conf int64) bool {
	sql_ := fmt.Sprintf(`SELECT EXISTS(SELECT * FROM %v LIMIT 1)`, _eventFileTable(conf))
	e := new(_exist)
	if db.Raw(sql_).Scan(e).Error != nil {
		return false
	}
	return e.Exists == "false"
}
