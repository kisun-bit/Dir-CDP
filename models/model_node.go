package models

import (
	"fmt"
	"gorm.io/gorm"
)

type ClientNode struct {
	ID   int64  `gorm:"column:id;primaryKey;AUTO_INCREMENT;"`
	Name string `gorm:"column:name;type:text"`
	Desc string `gorm:"column:desc;type:text"`
	OS   string `gorm:"column:os;type:text"`
	IP   string `gorm:"column:ip;type:text"`
	Port int64  `gorm:"column:port;"`
	Ext  string `gorm:"column:ext;type:text"`
}

func (_ ClientNode) TableName() string {
	return ModelDefaultSchema + ".node"
}

func (c *ClientNode) String() string {
	return fmt.Sprintf("<ClientNode(ID=%v, OS=%v, IP=%v>", c.ID, c.OS, c.IP)
}

func QueryClientNodeByID(db *gorm.DB, id int64) (c ClientNode, err error) {
	r := db.Model(&ClientNode{}).Where("id = ?", id).First(&c)
	return c, r.Error
}
