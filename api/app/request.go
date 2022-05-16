package app

import (
	"github.com/astaxie/beego/validation"
	"jingrongshuan/rongan-fnotify/logging"
)

// MarkErrors logs error logs
func MarkErrors(errors []*validation.Error) {
	for _, err := range errors {
		logging.Logger.Fmt.Info(err.Key, err.Message)
	}

	return
}
