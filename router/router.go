package router

import (
	"github.com/gin-gonic/gin"
	"jingrongshuan/rongan-fnotify/api/v1"
)

func InitRouter() *gin.Engine {
	r := gin.Default()
	r.Use(gin.Logger())
	r.Use(gin.Recovery())

	apiv1 := r.Group("/api/v1")
	{
		// 初始化数据库接口
		apiv1.GET("/db/init/:ip", v1.Init)
		// 启用监控配置
		apiv1.GET("/config/:ip/:id/enable", v1.EnableCDPConfig)
		// 开始恢复任务
		apiv1.GET("/restore/:id/start", v1.StartRestore)
		// 上传文件接口
		apiv1.POST("/upload", v1.Upload)
		// 下载文件接口
		apiv1.POST("/download/:b64", v1.Download)
		// NOTE. 更多接口可用studio中的接口来调度
	}

	return r
}
