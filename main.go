package main

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"jingrongshuan/rongan-fnotify/logging"
	"jingrongshuan/rongan-fnotify/logic"
	"jingrongshuan/rongan-fnotify/meta"
	"jingrongshuan/rongan-fnotify/router"
	"net/http"
	"time"
)

func StartHTTPService() (err error) {
	gin.SetMode(meta.AppMode)
	routersInit := router.InitRouter()
	endpoint := fmt.Sprintf(":%d", meta.AppPort)

	server := http.Server{
		Addr:           endpoint,
		Handler:        routersInit,
		ReadTimeout:    meta.AppReadTimeout,
		WriteTimeout:   meta.AppWriteTimeout,
		MaxHeaderBytes: 1 << 20,
	}
	logging.Logger.Fmt.Infof("[info] start http server listening %s", endpoint)
	if err = server.ListenAndServe(); err != nil {
		logging.Logger.Fmt.Error("[Error] Server err: ", err)
	}
	return
}

func StartReloadTask() {
	go func() {
		time.Sleep(12 * time.Second)
		logic.ReloadCDPTask()
	}()
}

// @title rongan-fnotify
// @version 1.0
// @description 文件级CDP、实时归档
// @termsOfService http://TODO

// @contact.name kisun-bit
// @contact.url https://github.com/kisun-bit
// @contact.email kisun168@aliyun.com

// @license.name Apache 2.0
// @license.url http://www.apache.org/licenses/LICENSE-2.0.html

// @host 0.0.0.0
// @BasePath C:\rongan\fnotify
func main() {
	var err error

	StartReloadTask()

	if err = StartHTTPService(); err != nil {
		return
	}
}
