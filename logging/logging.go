package logging

import (
	"github.com/kisun-bit/go-dr/src/log"
	"jingrongshuan/rongan-fnotify/meta"
)

var (
	LogDefaultName           = ""
	LogDefaultPath           = meta.ConfigSettings.Log
	LogDefaultRotateHours    = 24 * 30
	LogDefaultRetentionHours = 24 * 30 * 4
)

func GetDefaultLogger() *log.Logger {
	l, _ := log.NewRateFileLimitAgeSugaredLogger(
		LogDefaultName,
		LogDefaultPath,
		log.LDebug,
		log.EConsole,
		LogDefaultRotateHours,
		LogDefaultRetentionHours)
	return l
}

var Logger = GetDefaultLogger()
