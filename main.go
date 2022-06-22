package main

import (
	"context"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/kardianos/service"
	"jingrongshuan/rongan-fnotify/logging"
	"jingrongshuan/rongan-fnotify/logic"
	"jingrongshuan/rongan-fnotify/meta"
	"jingrongshuan/rongan-fnotify/router"
	"log"
	"net/http"
	"os"
	"time"
)

type program struct {
	Log service.Logger
	Srv *http.Server
	Cfg *service.Config
}

func (p *program) Start(s service.Service) error {
	defer func() {
		go p.run()
	}()
	return nil
}

func (p *program) run() {
	var err error

	go func() {
		time.Sleep(meta.DefaultReloadStartDuration)
		logic.ReloadCDPTask()
	}()

	gin.DisableConsoleColor()
	gin.SetMode(meta.ConfigSettings.Mode)
	routersInit := router.InitRouter()
	endpoint := fmt.Sprintf(":%d", meta.ConfigSettings.ServicePort)
	p.Srv = &http.Server{
		Addr:           endpoint,
		Handler:        routersInit,
		ReadTimeout:    meta.DefaultAppReadTimeout,
		WriteTimeout:   meta.DefaultAppWriteTimeout,
		MaxHeaderBytes: 1 << 20,
	}
	if err = p.Srv.ListenAndServe(); err != nil {
		log.Fatalf("fsnotify service serve ERR=%v", err)
	}
	return
}

func (p *program) Stop(s service.Service) error {
	var err error
	logging.Logger.Fmt.Infof("关闭服务")
	err = p.Srv.Shutdown(context.Background())
	if err != nil {
		os.Exit(1)
	}
	return err
}

func main() {
	svcConfig := &service.Config{
		Name:        meta.ConfigSettings.Name,
		DisplayName: meta.ConfigSettings.DisplayName,
		Description: meta.ConfigSettings.Description,
	}

	prg := &program{}
	s, err := service.New(prg, svcConfig)
	if err != nil {
		log.Fatal(err)
	}

	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "install":
			err := s.Install()
			if err != nil {
				log.Fatalf("Install service error:%s\n", err.Error())
			}
			fmt.Printf("服务已安装")
		case "uninstall":
			err := s.Uninstall()
			if err != nil {
				log.Fatalf("Uninstall service error:%s\n", err.Error())
			}
			fmt.Printf("服务已卸载")
		case "start":
			err := s.Start()
			if err != nil {
				log.Fatalf("Start service error:%s\n", err.Error())
			}
			fmt.Printf("服务已启动")
		case "stop":
			err := s.Stop()
			if err != nil {
				log.Fatalf("top service error:%s\n", err.Error())
			}
			fmt.Printf("服务已关闭")
		}
		return
	}

	if err = s.Run(); err != nil {
		log.Fatal(err)
	}
}
