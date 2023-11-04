package pkg

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
)

func DirSize(dirPath string) (int64, error) {
	var size int64
	if err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	}); err != nil {
		return 0, err
	}
	return size, nil
}

func getDiskFreeSpace(path string) (uint64, error) {
	cmd := exec.Command("powershell", "Get-WmiObject Win32_LogicalDisk -Filter \"DeviceID='"+path+"'\" | Select-Object FreeSpace")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return 0, fmt.Errorf("failed to execute command: %v\n%s", err, out)
	}
	freeSpaceStr := string(out[len(out)-2 : len(out)-1])
	return parseSize(freeSpaceStr)
}

func parseSize(sizeStr string) (size uint64, err error) {
	sizeStr = sizeStr[:len(sizeStr)-1] // remove last char (MB or GB)
	size, err = strconv.ParseUint(sizeStr, 10, 64)
	switch sizeStr[len(sizeStr)-1:] {
	case "GB":
		size *= 1 << 30
	case "MB":
		size *= 1 << 20
	default:
		err = fmt.Errorf("unknown unit: %s", sizeStr[len(sizeStr)-1:])
	}
	return
}
