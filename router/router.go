package router

import (
	"github.com/gin-gonic/gin"
	"github.com/unrolled/secure"
	"jingrongshuan/rongan-fnotify/api/v1"
)

func TlsHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		secureMiddleware := secure.New(secure.Options{
			SSLRedirect: true,
			SSLHost:     "localhost:8080",
		})
		err := secureMiddleware.Process(c.Writer, c.Request)

		// If there was an error, do not continue.
		if err != nil {
			return
		}

		c.Next()
	}
}

func InitRouter() *gin.Engine {
	r := gin.Default()
	r.Use(gin.Logger())
	r.Use(gin.Recovery())

	apiv1 := r.Group("/api/v1")
	{
		// 功能: 初始化数据库接口
		// 方法: GET
		// 参数(url-param):
		//      - {:ip} 备份服务器IP
		apiv1.GET("/db/init/:ip", v1.Init)
		// 功能: 启用监控配置
		// 方法: GET
		// 参数(url-param):
		//      - {:ip} 备份服务器IP
		//      - {:id} CDP配置ID
		apiv1.GET("/config/:ip/:id/enable", v1.EnableCDPConfig)
		// 功能: 开始恢复任务
		// 方法: GET
		// 参数(url-param):
		//      - {:ip} 备份服务器IP
		//      - {:id} 恢复配置ID
		apiv1.GET("/restore/:ip/:id/start", v1.StartRestore)
		// 功能: 上传文件接口
		// 方法: POST
		// 参数(Form):
		//      - filename 文件存储地址
		apiv1.POST("/upload", v1.Upload)
		// 功能: 下载文件接口
		// 方法: POST
		// 参数(Form):
		//       - file 文件存储地址
		//       - volume 卷路径(仅在基于linux实时任务产生的恢复任务中有效)
		apiv1.POST("/download", v1.Download)
		// 功能: 删除文件接口
		// 方法: POST
		// 参数(Form):
		//       - b64 文件存储地址
		apiv1.POST("/delete", v1.Delete)
		// 功能: 重命名文件接口
		// 方法: POST
		// 参数(Form):
		//       - old 文件存储地址(旧)
		//       - new 文件存储地址(新)
		apiv1.POST("/rename", v1.Rename)
		// 功能: 服务器IP修正
		// 方法: POST
		// 参数(Form):
		//       - old 备份服务器IP(旧)
		//       - new 备份服务器IP(新)
		apiv1.POST("/server/:old/:new", v1.ModifyServerIP)
		// 功能: 创建快照
		// 方法: POST
		// 参数(Form):
		//       - volume 卷(windows上为C:等驱动器号、linux上为设备路径)
		apiv1.POST("/create_snapshot", v1.CreateSnapshot)
		// 功能: 删除快照
		// 方法: POST
		// 参数(Form):
		//       - type 快照类型(windows上为vss、linux上为lvm)
		//       - snap 快照(windows上为vss快照ID、linux上为设备快照路径)
		apiv1.POST("/delete_snapshot", v1.DeleteSnapshot)
		// 功能: 回滚快照
		// 方法: POST
		// 参数(Form):
		//       - type 快照类型(windows上为vss、linux上为lvm)
		//       - snap 快照(windows上为vss快照ID、linux上为设备快照路径)
		apiv1.POST("/revert_snapshot", v1.RevertSnapshot)
		// 功能: 查询快照
		// 方法: POST
		// 参数(Form):
		//       - type 快照类型(windows上为vss、linux上为lvm)
		//       - snap 快照(windows上为vss快照ID、linux上为设备快照路径)
		apiv1.POST("/detail_snapshot", v1.DetailSnapshot)
		// 功能: 指定VSS存储
		// 方法: POST
		// 参数(Form):
		//       - for 启用vss的卷
		//       - on 存储vss增量数据的卷
		//       - size vss增量数据的卷的存储大小
		apiv1.POST("/add_storage", v1.AddShadowStorage)
		// 功能: 创建或修正目录
		// 方法: POST
		// 参数(Form):
		//       - path 目录路径
		//       - mode 目录权限
		apiv1.POST("/create_or_update_dir", v1.CreateOrUpdateDir)
		// 功能: 目标机连接共享目录，并映射为一个驱动器
		// 方法: POST
		// 参数(Form):
		//      - share_ip   源机IP
		//      - share_name 共享名称
		apiv1.POST("/smb/connect", v1.SMBConnect)
		// 功能: 目标机连接共享目录，并映射为一个驱动器
		// 方法: POST
		// 参数(Form):
		//      - share_ip   源机IP
		//      - share_name 共享名称
		apiv1.POST("/smb/disconnect", v1.SMBDisconnect)
		// 功能: 在共享端删除共享
		// 方法: POST
		// 参数(Form):
		//      - share_name 共享名称
		apiv1.POST("/smb/delete", v1.SMBDelete)
	}

	return r
}
