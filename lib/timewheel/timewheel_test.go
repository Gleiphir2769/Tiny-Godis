package timewheel

import (
	"fmt"
	"testing"
	"time"
)

func TestTimeWheel(t *testing.T) {
	check := false
	tw := Make(time.Second, 5)
	tw.Start()
	tw.AddJob("test", time.Second, func() {
		fmt.Println("come to test job")
		check = true
	})
	time.Sleep(time.Second * 2)
	if !check {
		t.Error("add task has some problem")
	}

	check2 := true
	tw.AddJob("test2", time.Second*2, func() {
		fmt.Println("come to test2 job")
		check2 = false
	})
	time.Sleep(time.Second)
	tw.RemoveJob("test2")
	time.Sleep(time.Second)
	if !check2 {
		t.Error("remove task has some problem")
	}
	tw.Stop()
}
