package models

import (
	"encoding/json"
	"fmt"
	"gorm.io/gorm"
)

type S3ConfigField struct {
	Config string `json:"config"`
}

type S3Conf struct {
	AK       string `json:"access_key"`
	SK       string `json:"secret_key"`
	Endpoint string `json:"endpoint"`
	Region   string `json:"region"`
	SSL      bool   `json:"ssl"`
	Style    string `json:"addressing"`
}

func QueryS3ConfByID(db *gorm.DB, id int64) (sc S3Conf, err error) {
	var scf S3ConfigField
	sql := fmt.Sprintf("SELECT config FROM web.t_storage_archive WHERE id = %v;", id)
	if r := db.Debug().Raw(sql).Scan(&scf); r.Error != nil {
		return sc, r.Error
	}
	err = json.Unmarshal([]byte(scf.Config), &sc)
	return sc, err
}
