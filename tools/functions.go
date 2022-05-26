package tools

import (
	"fmt"
	"github.com/gofrs/uuid"
	"github.com/journeymidnight/aws-sdk-go/aws"
	"github.com/journeymidnight/aws-sdk-go/aws/credentials"
	"github.com/journeymidnight/aws-sdk-go/aws/session"
	"github.com/journeymidnight/aws-sdk-go/service/s3"
	gos3 "github.com/kisun-bit/go-s3"
	"jingrongshuan/rongan-fnotify/meta"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// ConvertMode2FType os.FileMode转FileType
func ConvertMode2FType(mode os.FileMode) meta.FileType {
	if mode&os.ModeSymlink != 0 {
		return meta.FTLink
	} else if mode&os.ModeDir != 0 {
		return meta.FTDir
	} else if strings.HasPrefix(mode.String(), "-") {
		return meta.FTFile
	} else {
		return meta.FTOther // a、T、L、D、p、S、u、g、c、t?
	}
}

// ConvertToWin32Event 监控事件标记转Event
func ConvertToWin32Event(no uint32) meta.Event {
	if no > uint32(meta.Win32EventRenameTo) || no < uint32(meta.Win32EventCreate) {
		panic("invalid win32 event")
	}
	return meta.Event(no)
}

// ConvertToWin32Event 若输入IP为本机IP地址，则返回true
func IsLocalIP(ip string) (bool, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return false, err
	}
	for i := range addrs {
		intf, _, err := net.ParseCIDR(addrs[i].String())
		if err != nil {
			return false, err
		}
		if net.ParseIP(ip).Equal(intf) {
			return true, nil
		}
	}
	return false, nil
}

func NewS3Client(
	ak,
	sk,
	endpoint,
	region string,
	ssl,
	path bool,
) (*session.Session, gos3.S3Client) {
	S3Session := session.Must(
		session.NewSessionWithOptions(
			session.Options{
				Config: aws.Config{
					Credentials:      credentials.NewStaticCredentials(ak, sk, ""),
					DisableSSL:       aws.Bool(!ssl),
					Endpoint:         aws.String(endpoint),
					Region:           aws.String(region),
					S3ForcePathStyle: aws.Bool(path),
				},
			},
		),
	)
	s3client := s3.New(S3Session)
	return S3Session, gos3.S3Client{Client: s3client}
}

func GenerateS3Key(conf int64, dir int64) string {
	uid, err := uuid.NewV4()
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("%v/%v", conf, dir, uid.String())
}

func GenerateRemoteHostKey(conf int64, dir int64, remote string, targetWin bool) string {
	absRemote := filepath.Join(remote, strconv.FormatInt(conf, 10), strconv.FormatInt(dir, 10))
	uid, err := uuid.NewV4()
	if err != nil {
		panic(err)
	}
	path := filepath.Join(absRemote, uid.String())
	return CorrectPathWithPlatform(path, targetWin)
}

func String2Time(str string) (t time.Time) {
	loc, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		loc = time.FixedZone("CST", 8*3600)
	}
	t, err = time.ParseInLocation("2006-01-02 15:04:05", str, loc)
	if err != nil {
		panic(err)
	}
	return t
}

func IsWin(s string) bool {
	return strings.Contains(strings.ToLower(s), "win")
}

func IsLinux(s string) bool {
	return strings.Contains(strings.ToLower(s), "linux")
}

func ConvertModTime2VersionFlag(mod int64) (ver string) {
	ver = time.Unix(mod, 0).Format(meta.TimeFMT)
	ver = strings.ReplaceAll(ver, "-", "")
	ver = strings.ReplaceAll(ver, ":", "")
	ver = strings.ReplaceAll(ver, " ", "")
	return ver
}

func CorrectPathWithPlatform(path_ string, win bool) string {
	if win {
		path_ = strings.ReplaceAll(path_, "/", "\\")
		return strings.ReplaceAll(path_, `\\`, "\\")
	}
	path_ = strings.ReplaceAll(path_, `\`, "/")
	return strings.ReplaceAll(path_, `//`, "/")
}

func CorrectDirWithPlatform(dir string, win bool) string {
	dir = CorrectPathWithPlatform(dir, win)
	if win && !strings.HasSuffix(dir, "\\") {
		return dir + "\\"
	} else if !win && strings.HasSuffix(dir, "/") {
		return dir + "/"
	}
	return dir
}

func StringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}
