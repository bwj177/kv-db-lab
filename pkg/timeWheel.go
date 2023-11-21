package pkg

import (
	"github.com/xiaoxuxiansheng/timewheel"
	"time"
)

func InitTimeWheel() *timewheel.TimeWheel {
	timeWheel := timewheel.NewTimeWheel(10, 500*time.Millisecond)
	return timeWheel
}
