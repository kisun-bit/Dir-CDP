package models

import (
	"fmt"
	"gorm.io/gorm"
)

type Client struct {
	ID      int64  `json:"id"`
	Address string `json:"address"`
	Type    string `json:"type"`
}

func QueryClientInfoByID(db *gorm.DB, id int64) (c Client, err error) {
	if r := db.Raw(
		fmt.Sprintf("SELECT id, address, type FROM web.t_client WHERE id = %v;", id)).Scan(&c); r.Error != nil {
		return c, r.Error
	}
	return
}
