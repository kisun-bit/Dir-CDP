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
	LackVolume           = 0x10015
	SysUnsupportedSnap   = 0x10016
	CreateVssSnapFailed  = 0x10017
	DetailVssSnapFailed  = 0x10018
	RevertVssSnapFailed  = 0x10019
	DeleteVssSnapFailed  = 0x10020
	FormatResponseErr    = 0x10021
	InitDBDriverFailed   = 0x10022
	QueryConfigFailed    = 0x10023
	InvalidRevertType    = 0x10024
	CorrectDirFailed     = 0x10025
	MallocDriveERR       = 0x10026
	SMBConnectERR        = 0x10027
	SMBDisConnectERR     = 0x10028
	SMBDelShareERR       = 0x10029
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
	LackVolume:           "未指定卷",
	SysUnsupportedSnap:   "系统不支持快照",
	CreateVssSnapFailed:  "创建VSS快照失败",
	DetailVssSnapFailed:  "查询VSS快照失败",
	RevertVssSnapFailed:  "回滚VSS快照失败",
	DeleteVssSnapFailed:  "删除VSS快照失败",
	FormatResponseErr:    "格式化响应失败",
	InitDBDriverFailed:   "初始化数据库驱动失败",
	QueryConfigFailed:    "查询配置失败",
	InvalidRevertType:    "不支持的恢复类型",
	CorrectDirFailed:     "创建目录或更改目录权限失败",
	MallocDriveERR:       "分配驱动器号失败",
	SMBConnectERR:        "连接共享文件夹失败",
	SMBDisConnectERR:     "断开共享文件夹连接失败",
	SMBDelShareERR:       "删除共享文件夹失败",
}
