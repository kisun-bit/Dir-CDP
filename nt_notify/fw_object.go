package nt_notify

import (
	"fmt"
	"jingrongshuan/rongan-fnotify/meta"
	"os"
)

// FileInfo 文件基础信息，该结构体有两个引用点
// 一是作为FileFlow的内嵌模型属性；
// 二是作为由win32、inotify捕捉的文件信息结构体
type FileInfo struct {
	Time int64         `gorm:"column:timestamp;index;not null;"`     // 文件最后修改时间戳
	Path string        `gorm:"column:path;index;not null;type:text"` // 文件全路径, NT256,linux4096
	Name string        `gorm:"column:name;not null;type:text"`       // 文件名
	Type meta.FileType `gorm:"column:type;default:8"`                // 文件类型，1表示文件，2表示目录，4表示链接，8表示其他
	Mode os.FileMode   `gorm:"column:mode"`                          // 文件权限，uint32类型
	Size int64         `gorm:"column:size"`                          // 文件大小，单位Byte
}

type FileWatchingObj struct {
	FileInfo
	Event meta.Event
}

func (fwo *FileWatchingObj) String() string {
	return fmt.Sprintf(
		"<FileWatchingObj(name=%v, event=%v, path=%v)>", fwo.Name, meta.EventCode[fwo.Event], fwo.Path)
}

func GenerateVersion(confID int64, fwo FileWatchingObj) string {
	// conf_id|full_path|file_type|size|mtime
	return fmt.Sprintf("%v|%v|%v|%v|%v",
		confID, fwo.Path, fwo.Type, fwo.Size, fwo.Time)
}
