package logic

import (
	"errors"
	"fmt"
	"jingrongshuan/rongan-fnotify/tools"
)

func SetVolumeLabel(volume, label string) (err error) {
	args := fmt.Sprintf(`%s:%s`, volume, label)
	r, _, err := tools.Process("label", args)
	if r != 0 || err != nil {
		return errors.New("failed to set label")
	}
	return nil
}
