package tools

import (
	"fmt"
	"github.com/journeymidnight/aws-sdk-go/aws"
	"github.com/journeymidnight/aws-sdk-go/aws/credentials"
	"github.com/journeymidnight/aws-sdk-go/aws/session"
	"github.com/journeymidnight/aws-sdk-go/service/s3"
	gos3 "github.com/kisun-bit/go-s3"
	"jingrongshuan/rongan-fnotify/meta"
	"os"
	"path/filepath"
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

func GenerateS3Key(conf int64, root, local, prefix string) string {
	relative, err := filepath.Rel(root, local)
	if err != nil {
		return meta.UnsetStr
	}
	if prefix != meta.UnsetStr {
		return fmt.Sprintf("%s/%s", prefix, relative)
	}
	return fmt.Sprintf("notify%v/%v", conf, relative)
}

func GenerateRemoteHostKey(root, local, remote string, targetWin bool) string {
	relative, err := filepath.Rel(root, local)
	if err != nil {
		return meta.UnsetStr
	}
	if remote != meta.UnsetStr {
		return CorrectPathWithPlatform(filepath.Join(remote, relative), targetWin)
	}
	return meta.UnsetStr
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
