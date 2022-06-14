package models

import (
	"fmt"
	"gorm.io/gorm"
	"jingrongshuan/rongan-fnotify/meta"
	"jingrongshuan/rongan-fnotify/tools"
	"path/filepath"
	"strings"
)

type EventDirModel struct {
	ID      int64
	Path    string
	Name    string
	Parent  string
	ExtInfo string
}

// event_dir表将以config表ID进行分表处理
// 其模板建表语句如下：
var DirCreateDDL = `
CREATE SEQUENCE IF NOT EXISTS "fsnotify".event_dir_id_seq
    INCREMENT 1
    START 1
    MINVALUE 1
    MAXVALUE 93422337368375807
    CACHE 1;
CREATE TABLE IF NOT EXISTS "fsnotify"."event_dir" (
	"id" int8 NOT NULL DEFAULT nextval( '"fsnotify".event_dir_id_seq' :: regclass),
	"path" TEXT COLLATE "pg_catalog"."default",
	"name" TEXT COLLATE "pg_catalog"."default",
	"parent" TEXT COLLATE "pg_catalog"."default",
	"ext_info" TEXT COLLATE "pg_catalog"."default",
	CONSTRAINT "event_dir_pkey" PRIMARY KEY ( "id" ) 
);
ALTER TABLE "fsnotify"."event_dir" OWNER TO "postgres";
CREATE UNIQUE INDEX IF NOT EXISTS "idx_fsnotify_event_dir_path" ON "fsnotify"."event_dir" USING btree ( "path" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST );
CREATE INDEX IF NOT EXISTS "idx_fsnotify_event_dir_parent" ON "fsnotify"."event_dir" USING btree ( "parent" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST );
`

func DeleteDirByConfID(db *gorm.DB, conf int64) (err error) {
	t := fmt.Sprintf("event_event_dir_%v", conf)
	for _, it := range []string{
		"idx_fsnotify_event_dir_path",
		"idx_fsnotify_event_dir_parent",
	} {
		idx := strings.ReplaceAll(it, "event_dir", t)
		if r := db.Exec(fmt.Sprintf(`DROP INDEX IF EXISTS %s ON "fsnotify"."event_dir_%v"`,
			idx, conf)); r.Error != nil {
			return r.Error
		}
	}
	if r := db.Exec(`DROP TABLE IF EXISTS ` + t); r.Error != nil {
		return r.Error
	}
	s := fmt.Sprintf(`"fsnotify".event_dir_%v_id_seq`, conf)
	if r := db.Exec(`DROP SEQUENCE IF EXISTS ` + s); r.Error != nil {
		return r.Error
	}
	return nil
}

func _eventDirTable(conf int64) string {
	return fmt.Sprintf(`"fsnotify"."event_dir_%v"`, conf)
}

func CreateDirIfNotExists(db *gorm.DB, conf int64, path, ext string) (err error) {
	sqlTmp := `insert into %v ("path","name","parent","ext_info") 
    values ('%v', '%v', '%v', '%v') 
    ON CONFLICT (path) DO NOTHING`
	sql := fmt.Sprintf(sqlTmp,
		_eventDirTable(conf),
		strings.ReplaceAll(tools.CorrectDirWithPlatform(path, meta.IsWin), `'`, `''`),
		strings.ReplaceAll(filepath.Base(path), `'`, `''`),
		strings.ReplaceAll(tools.CorrectDirWithPlatform(filepath.Dir(path), meta.IsWin), `'`, `''`),
		strings.ReplaceAll(ext, `'`, `''`))
	return db.Exec(sql).Error
}

func QueryDirByPath(db *gorm.DB, conf int64, path string) (d EventDirModel, err error) {
	sql_ := fmt.Sprintf(`SELECT * FROM %v WHERE path='%v'`,
		_eventDirTable(conf), strings.ReplaceAll(path, `'`, `''`))
	err = db.Raw(sql_).Scan(&d).Error
	return
}
