package logic

import (
	"fmt"
	"io/ioutil"
	"jingrongshuan/rongan-fnotify/meta"
	"jingrongshuan/rongan-fnotify/nt_notify"
	"jingrongshuan/rongan-fnotify/tools"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Walker 实现了路径迭代器(比filepath.walk性能更好)
type Walker struct {
	Root        string
	Depth       int
	ThreadSize  int
	CurrentTime time.Time
	ValidDays   int
	Grep        Grep
	semaphore   chan struct{}
	wg          sync.WaitGroup
	pipe        chan nt_notify.FileWatchingObj
}

func NewWalker(
	root string, maxDepth, maxThreadsNum int, Include, Exclude string, now time.Time, validDays int) *Walker {
	w := new(Walker)

	w.Root, w.Depth, w.ThreadSize = root, maxDepth, maxThreadsNum
	w.pipe = make(chan nt_notify.FileWatchingObj, 100)
	w.semaphore = make(chan struct{}, w.ThreadSize)
	w.Grep = *NewGrep(Include, Exclude)
	w.CurrentTime = now
	w.ValidDays = validDays

	logger.Fmt.Infof("NewWalker -> %v", w.String())
	return w
}

func (w *Walker) Walk() (pq chan nt_notify.FileWatchingObj, err error) {
	logger.Info("Walker Walk. start")

	w.wg.Add(1)
	go w.walkDir(w.Root)

	go func() {
		w.wg.Wait()
		close(w.pipe) // 关闭通道不会影响从通道读数据
		logger.Info("Walker Walk. finished")
	}()
	return w.pipe, nil
}

func (w *Walker) walkDir(dir string) {
	defer w.wg.Done()
	if w.Depth != -1 && strings.Count(dir, meta.Sep) > w.Depth {
		logger.Fmt.Warnf("Walker walkDir. limit depth %d. ignore to walk %s", w.Depth, dir)
		return
	}
	for _, entry := range w.dirListObjs(dir) {
		path_, err := filepath.Abs(path.Join(dir, entry.Name()))
		if err != nil {
			logger.Fmt.Errorf("Walker walkDir. failed to get abs-path from %s", path_)
			continue
		}
		// ignore link file
		if entry.Mode()&os.ModeSymlink != 0 {
			logger.Fmt.Info("Walker walkDir. ignore to backup link file ", path_)
			continue
		}
		if entry.IsDir() {
			w.wg.Add(1)
			go w.walkDir(path_)
		} else {
			if w.Grep.IsValidByGrep(path_) {
				if w.ValidDays != -1 {
					if entry.ModTime().Add(time.Hour * 24 * time.Duration(w.ValidDays)).Before(w.CurrentTime) {
						logger.Fmt.Warnf("Walker walkDir. ignore to backup `%s` due to modify time", path_)
						continue
					}
				}
				fwo := nt_notify.FileWatchingObj{
					FileInfo: nt_notify.FileInfo{
						Time: entry.ModTime().Unix(),
						Path: path_,
						Name: filepath.Base(path_),
						Type: tools.ConvertMode2FType(entry.Mode()),
						Mode: entry.Mode(),
						Size: entry.Size(),
					},
					Event: meta.Win32EventFullScan,
				}
				w.pipe <- fwo
			}
		}
	}
}

func (w *Walker) dirListObjs(dir string) []os.FileInfo {
	w.semaphore <- struct{}{}
	defer func() { <-w.semaphore }()
	entries, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil
	}
	return entries
}

func (w *Walker) String() string {
	return fmt.Sprintf("<Walker: Root(%s) Depth(%d) Threads(%d) Include(%v) Exclude(%v)>",
		w.Root,
		w.Depth,
		w.ThreadSize,
		w.Grep.Include,
		w.Grep.Exclude)
}
