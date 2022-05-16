package app

import (
	"github.com/gin-gonic/gin"
	"jingrongshuan/rongan-fnotify/api/statuscode"
)

type Gin struct {
	C *gin.Context
}

func (g *Gin) Response(httpCode, errCode int, data interface{}) {
	g.C.JSON(httpCode, Response{
		Code: errCode,
		Msg:  statuscode.MSGFlag[errCode],
		Data: data,
	})
}

type Response struct {
	Code int         `json:"code"`
	Msg  string      `json:"msg"`
	Data interface{} `json:"data"`
}

// Response setting gin.JSON
func Resp(c *gin.Context, httpCode, errCode int, data interface{}) {
	c.JSON(httpCode, Response{
		Code: errCode,
		Msg:  statuscode.MSGFlag[errCode],
		Data: data,
	})
}
