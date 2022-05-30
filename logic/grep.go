package logic

import (
	"github.com/ryanuber/go-glob"
	"strings"
)

const SEQ = "@jrsa@"

// filter 实现基于文件路径的黑白名单过滤
type Grep struct {
	Include []string
	Exclude []string
}

func NewGrep(include, exclude string) *Grep {
	g := new(Grep)
	if strings.TrimSpace(include) != "" {
		g.Include = strings.Split(include, SEQ)
	}
	if strings.TrimSpace(exclude) != "" {
		g.Include = strings.Split(exclude, SEQ)
	}
	return g
}

func (g *Grep) IsValidByGrep(path_ string) bool {
	if len(g.Include) == 0 && len(g.Exclude) == 0 { // 无黑名单，无白名单
		return true
	} else if len(g.Include) == 0 && len(g.Exclude) != 0 { // 有黑名单，无白名单
		return !g.IsExclude(path_)
	} else if len(g.Include) != 0 && len(g.Exclude) == 0 { // 无黑名单，有白名单
		return g.IsInclude(path_)
	} else { // 有黑名单，有白名单
		return g.IsInclude(path_) && !g.IsExclude(path_)
	}
}

func (g *Grep) IsInclude(path_ string) bool {
	return g._in(path_, g.Include)
}

func (g *Grep) IsExclude(path_ string) bool {
	return g._in(path_, g.Exclude)
}

func (g *Grep) _in(path_ string, filter []string) bool {
	if len(filter) == 0 {
		return false
	}
	for i := 0; i < len(filter); i++ {
		if glob.Glob(filter[i], path_) {
			return true
		}
	}
	return false
}
