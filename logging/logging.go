package logging

import (
	"github.com/kisun-bit/go-dr/src/log"
	"jingrongshuan/rongan-fnotify/meta"
)

var (
	LogDefaultName           = ""
	LogDefaultPath           = "C:\\rongan\\log\\fnotify.log"
	LogDefaultRotateHours    = 24 * 30
	LogDefaultRetentionHours = 24 * 30 * 4
)

func GetDefaultLogger() *log.Logger {
	if !meta.IsWin {
		LogDefaultPath = "/opt/rongan/log/fnotify.log"
	}

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
