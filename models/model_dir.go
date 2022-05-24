package models

type DirectoryModel struct {
	ID      int64
	Path    string
	Name    string
	Parent  string
	ExtInfo string
}
