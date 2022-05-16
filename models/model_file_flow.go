package models

import (
	"database/sql"
	"fmt"
	"gorm.io/gorm"
	"jingrongshuan/rongan-fnotify/nt_notify"
	"path/filepath"
	"strings"
	"time"
)

// file_flow表将以config表ID进行分表处理
// 其模板建表语句如下：
var FileFlowCreateDDL = `
CREATE SEQUENCE IF NOT EXISTS "rongan_fnotify".file_flow_1_id_seq
    INCREMENT 1
    START 1
    MINVALUE 1
    MAXVALUE 93422337368375807
    CACHE 1;
CREATE TABLE IF NOT EXISTS "rongan_fnotify"."file_flow" (
		"id" int8 NOT NULL DEFAULT nextval( '"rongan_fnotify".file_flow_1_id_seq' :: regclass),
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
        CONSTRAINT "file_flow_1_pkey" PRIMARY KEY ( "id" )
);
ALTER TABLE "rongan_fnotify"."file_flow" OWNER TO "postgres";
CREATE INDEX IF NOT EXISTS "idx_rongan_fnotify_file_flow_1_path" ON "rongan_fnotify"."file_flow" USING btree ( "path" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST );
CREATE INDEX IF NOT EXISTS "idx_rongan_fnotify_file_flow_1_timestamp" ON "rongan_fnotify"."file_flow" USING btree ( "timestamp" "pg_catalog"."int8_ops" ASC NULLS LAST );
CREATE INDEX IF NOT EXISTS "idx_rongan_fnotify_file_flow_1_version" ON "rongan_fnotify"."file_flow" USING btree ( "version" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST );
CREATE INDEX IF NOT EXISTS "idx_rongan_fnotify_file_flow_1_parent" ON "rongan_fnotify"."file_flow" USING btree ( "parent" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST );
CREATE INDEX IF NOT EXISTS "idx_rongan_fnotify_file_flow_1_type" ON "rongan_fnotify"."file_flow" USING btree ( "type" "pg_catalog"."int8_ops" ASC NULLS LAST );
CREATE INDEX IF NOT EXISTS "idx_rongan_fnotify_file_flow_1_storage" ON "rongan_fnotify"."file_flow" USING btree ( "storage" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST );`

// FileFlowModel 用于记录发生变更的文件项流水信息
type FileFlowModel struct {
	nt_notify.FileInfo
	ID      int64
	Type    int    // 文件类型，1表示文件，2表示目录，4表示链接，8表示其他
	Mode    int    // 文件权限，uint32类型
	Create  int64  // 创建时间戳，Win32捕捉变更事件的时间
	Event   string // 变更事件类型
	ConfID  int64  `json:"conf_id"` // 配置ID，表明此记录是基于哪一个配置所捕捉的
	Version string // 版本号，conf_id|full_path|file_type|size|mtime
	Tag     string // 若启动版本，该处值为时间标签（20220101094423）
	Parent  string // 父目录
	Status  string // 状态，WATCHED(被监控到)|SYNCING(正在上传)|ERROR|FINISHED
	Storage string // 存储路径
}

func (flow *FileFlowModel) String() string {
	return fmt.Sprintf("<FileLogging(ID=%v, Path=%v, Event=%v>", flow.ID, flow.Path, flow.Version)
}

func _table(conf int64) string {
	return fmt.Sprintf(`"rongan_fnotify"."file_flow_%v"`, conf)
}

type FFID struct {
	ID int64 `json:"id"`
}

func CreateFileFlowModel(db *gorm.DB, conf int64, ff *FileFlowModel) (err error) {
	sqlTmp := `insert into %v 
    ("timestamp", "path", "name", "type", "mode", "size", "create", "event", "conf_id", "version", "parent", "storage", "status", "tag")
    values (%v, '%v', '%v', %v, %v, %v, %v, '%v', %v, '%v', '%v', '%v', '%v', '%v') RETURNING id`
	sql_ := fmt.Sprintf(sqlTmp,
		_table(conf),
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
	t := fmt.Sprintf("file_flow_%v", conf)
	for _, it := range []string{
		"idx_rongan_fnotify_file_flow_path",
		"idx_rongan_fnotify_file_flow_time",
		"idx_rongan_fnotify_file_flow_version",
		"idx_rongan_fnotify_file_flow_parent",
		"idx_rongan_fnotify_file_flow_type",
		"idx_rongan_fnotify_file_flow_storage",
	} {
		idx := strings.ReplaceAll(it, "file_flow", t)
		if r := db.Exec(fmt.Sprintf(`DROP INDEX IF EXISTS %s ON "rongan_fnotify"."file_flow_%v"`,
			idx, conf)); r.Error != nil {
			return r.Error
		}
	}
	if r := db.Exec(`DROP TABLE IF EXISTS ` + t); r.Error != nil {
		return r.Error
	}
	s := fmt.Sprintf(`"rongan_fnotify".file_flow_%v_id_seq`, conf)
	if r := db.Exec(`DROP SEQUENCE IF EXISTS ` + s); r.Error != nil {
		return r.Error
	}
	return nil
}

func DeleteAllFilesByConf(db *gorm.DB, conf int64) (err error) {
	sql_ := fmt.Sprintf(`DELETE FROM %v WHERE conf_id=%v`, _table(conf), conf)
	return db.Exec(sql_).Error
}

func UpdateFileFlowStatus(db *gorm.DB, conf, f int64, status string) (err error) {
	sql_ := fmt.Sprintf(`UPDATE %v SET status='%v' WHERE id=%v`, _table(conf), status, f)
	return db.Exec(sql_).Error
}

type _exist struct {
	Exists string `json:"exists"`
}

func ExistedHistoryVersionFile(db *gorm.DB, conf int64, path string) bool {
	sql_ := fmt.Sprintf(`SELECT EXISTS(SELECT 1 FROM %v WHERE path='%v')`, _table(conf), path)
	e := new(_exist)
	if db.Raw(sql_).Scan(e).Error != nil {
		return false
	}
	return e.Exists != "false"
}

func QueryLastSameNameFile(db *gorm.DB, conf int64, path string) (f FileFlowModel, err error) {
	sql_ := fmt.Sprintf(`SELECT * FROM %v WHERE path='%v' ORDER BY id DESC LIMIT 1`,
		_table(conf), strings.ReplaceAll(path, `'`, `''`))
	err = db.Raw(sql_).Scan(&f).Error
	return
}

func QueryLastSameNameFileExcludeSelf(db *gorm.DB, conf int64, path string) (f FileFlowModel, err error) {
	sql_ := fmt.Sprintf(`SELECT * FROM %v WHERE path='%v' ORDER BY id DESC LIMIT 1 OFFSET 1 `,
		_table(conf), strings.ReplaceAll(path, `'`, `''`))
	err = db.Raw(sql_).Scan(&f).Error
	return
}

func QueryRecursiveFilesIteratorInDir(db *gorm.DB, conf int64, dir string) (rows *sql.Rows, err error) {
	sql_ := fmt.Sprintf(`SELECT * FROM %v WHERE path LIKE "%v%%" AND type != 2`, _table(conf), dir)
	return db.Raw(sql_).Rows()
}

func QueryFileByName(db *gorm.DB, conf int64, path string) (f FileFlowModel, err error) {
	sql_ := fmt.Sprintf(`SELECT * FROM %v WHERE parent = '%v' AND name = '%v'`,
		_table(conf), filepath.Dir(path), filepath.Base(path))
	err = db.Raw(sql_).Scan(&f).Error
	return
}

func QueryFileIteratorByTime(db *gorm.DB, conf int64, start, end *time.Time) (rows *sql.Rows, err error) {
	var sql_ string
	if start != nil && end != nil {
		sql_ = fmt.Sprintf(`SELECT * FROM %v WHERE timestamp BETWEEN "%v" AND "%v"`,
			_table(conf), start.Unix(), end.Unix())
	} else if start != nil && end == nil {
		sql_ = fmt.Sprintf(`SELECT * FROM %v WHERE timestamp <= "%v"`,
			_table(conf), start.Unix())
	} else if start == nil && end != nil {
		sql_ = fmt.Sprintf(`SELECT * FROM %v WHERE timestamp >= "%v"`,
			_table(conf), end.Unix())
	} else {
		sql_ = fmt.Sprintf(`SELECT * FROM %v`,
			_table(conf))
	}
	return db.Raw(sql_).Rows()
}

func IsEmptyTable(db *gorm.DB, conf int64) bool {
	sql_ := fmt.Sprintf(`SELECT EXISTS(SELECT * FROM %v LIMIT 1)`, _table(conf))
	e := new(_exist)
	if db.Raw(sql_).Scan(e).Error != nil {
		return false
	}
	return e.Exists == "false"
}
