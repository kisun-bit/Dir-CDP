package app

import (
	"github.com/astaxie/beego/validation"
	"github.com/gin-gonic/gin"
	"jingrongshuan/rongan-fnotify/api/statuscode"
	"net/http"
)

// BindAndValid binds and validates data
func BindAndValid(c *gin.Context, form interface{}) (int, int) {
	err := c.Bind(form)
	if err != nil {
		return http.StatusBadRequest, statuscode.INVALIDPARAMS
	}

	valid := validation.Validation{}
	check, err := valid.Valid(form)
	if err != nil {
		return http.StatusInternalServerError, statuscode.ERROR
	}
	if !check {
		MarkErrors(valid.Errors)
		return http.StatusBadRequest, statuscode.INVALIDPARAMS
	}

	return http.StatusOK, statuscode.SUCCESS
}
