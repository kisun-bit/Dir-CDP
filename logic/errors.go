package logic

import (
	"errors"
	"fmt"
)

type ErrorCode int

var (
	ExitErrCodeUserCancel    ErrorCode = 0x00001
	ExitErrCodeServerConn    ErrorCode = 0x00002
	ExitErrCodeTargetConn    ErrorCode = 0x00003
	ExitErrCodeOriginErr     ErrorCode = 0x00004
	ExitErrCodeTaskReplicate ErrorCode = 0x00005
	ExitErrCodeLackHandler   ErrorCode = 0x00006
)

var ErrCodeMap = map[ErrorCode]error{
	ExitErrCodeUserCancel:    errors.New("用户取消"),
	ExitErrCodeServerConn:    errors.New("备份服务器连接异常"),
	ExitErrCodeTargetConn:    errors.New("目标存储服务器连接异常"),
	ExitErrCodeOriginErr:     errors.New("本地异常"),
	ExitErrCodeTaskReplicate: errors.New("重复任务"),
	ExitErrCodeLackHandler:   errors.New("缺少任务句柄"),
}

func ErrByCode(ec ErrorCode) error {
	return ErrCodeMap[ec]
}

func ExtendErr(ec ErrorCode, msg string) error {
	if msg == "" {
		return ErrByCode(ec)
	}
	return fmt.Errorf("%v(%s)", ErrByCode(ec), msg)
}
