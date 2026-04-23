package web

import (
	"embed"
	"io/fs"
)

//go:embed static
var assets embed.FS

// Assets is the static file system rooted at the "static" subdirectory.
var Assets fs.FS

func init() {
	var err error
	Assets, err = fs.Sub(assets, "static")
	if err != nil {
		panic(err)
	}
}
