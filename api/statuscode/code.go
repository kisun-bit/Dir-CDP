package statuscode

const (
	SUCCESS              = 200
	ERROR                = 500
	INVALIDPARAMS        = 400
	BACKUPSERVERIPFAILED = 0x10001
	INITDBPROXYFAILED    = 0x10002
	QUERYCONFIGFAILED    = 0x10003
	INITCDPEXEFAILED     = 0x10004
	STARTCDPEXEFAILED    = 0x10005
	SYNCFILEFAILED       = 0x10006
	LACKFILENAME         = 0x10007
	EXISTSSAMENAMEFILE   = 0x10008
	WRITEIOFAILED        = 0x10009
	QUERYRESTOREFAILED   = 0x1000a
	INITRESTOREFAILED    = 0x1000b
	STARTESTOREFAILED    = 0x1000c
	RECORDSERVERFAILED   = 0x1000d
	LACKFILEPATH         = 0x1000e
	NOTEXISTSFILE        = 0x10010
	RENAMEERROR          = 0x10011
	DELFILEERROR         = 0x10012
	READIPSCONFERROR     = 0x10013
	ModifyIPSCONFERROR   = 0x10014
)

var MSGFlag = map[int]string{
	SUCCESS:              "成功",
	ERROR:                "失败",
	INVALIDPARAMS:        "请求错误",
	BACKUPSERVERIPFAILED: "获取备份服务器IP失败",
	INITDBPROXYFAILED:    "初始化数据库代理失败",
	QUERYCONFIGFAILED:    "查询监控同步配置失败",
	INITCDPEXEFAILED:     "初始化监控同步代理失败",
	STARTCDPEXEFAILED:    "启动监控同步代理失败",
	SYNCFILEFAILED:       "同步文件失败",
	LACKFILENAME:         "上传文件失败，缺少文件名",
	EXISTSSAMENAMEFILE:   "上传文件失败，已存在同名文件",
	WRITEIOFAILED:        "写入IO失败",
	QUERYRESTOREFAILED:   "查询恢复任务失败",
	INITRESTOREFAILED:    "初始化恢复任务失败",
	STARTESTOREFAILED:    "启动恢复任务失败",
	RECORDSERVERFAILED:   "缓存备份服务地址信息失败",
	LACKFILEPATH:         "缺少文件路径",
	NOTEXISTSFILE:        "文件不存在",
	RENAMEERROR:          "重命名失败",
	DELFILEERROR:         "删除文件失败",
	READIPSCONFERROR:     "读取本地备份服务器IP配置失败",
	ModifyIPSCONFERROR:   "修改本地备份服务器IP配置失败",
}
