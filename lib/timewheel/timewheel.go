package timewheel

import (
	"Tiny-Godis/lib/logger"
	"container/list"
	"time"
)

type TimeWheel struct {
	interval time.Duration
	ticker   *time.Ticker
	slots    []*list.List

	timer          map[string]int
	currentPos     int
	slotNum        int
	addTaskChan    chan task
	removeTaskChan chan string
	stopChan       chan bool
}

type task struct {
	delay  time.Duration
	circle int
	key    string
	job    func()
}

func Make(interval time.Duration, slotNum int) *TimeWheel {
	tw := TimeWheel{
		interval:       interval,
		slots:          make([]*list.List, slotNum),
		timer:          make(map[string]int),
		slotNum:        slotNum,
		addTaskChan:    make(chan task),
		removeTaskChan: make(chan string),
		stopChan:       make(chan bool),
	}

	return &tw
}

func (tw *TimeWheel) initSlots() {
	for i := 0; i < tw.slotNum; i++ {
		tw.slots[i] = list.New()
	}
}

func (tw *TimeWheel) Start() {
	tw.ticker = time.NewTicker(tw.interval)
	tw.initSlots()
	go tw.start()
}

func (tw TimeWheel) Stop() {
	tw.stopChan <- true
}

func (tw *TimeWheel) AddJob(key string, dur time.Duration, job func()) {
	if dur < 0 {
		return
	}
	t := task{
		delay:  dur,
		circle: 0,
		key:    key,
		job:    job,
	}
	tw.addTaskChan <- t
}

func (tw TimeWheel) RemoveJob(key string) {
	if key == "" {
		return
	}
	tw.removeTaskChan <- key
}

func (tw *TimeWheel) start() {
	for {
		select {
		case <-tw.ticker.C:
			tw.tickerHandler()
		case task := <-tw.addTaskChan:
			tw.addTask(&task)
		case taskName := <-tw.removeTaskChan:
			tw.removeTask(taskName)
		case <-tw.stopChan:
			// todo: stop
			tw.ticker.Stop()
			return
		}
	}
}

func (tw *TimeWheel) tickerHandler() {
	taskList := tw.slots[tw.currentPos]
	for iterator := taskList.Front(); iterator != nil; {
		task := iterator.Value.(*task)
		if task.circle > 0 {
			task.circle--
			iterator = iterator.Next()
			continue
		}

		go func() {
			defer func() {
				if err := recover(); err != nil {
					logger.Error(err)
				}
			}()
			job := task.job
			job()
		}()

		next := iterator.Next()
		taskList.Remove(iterator)
		if task.key != "" {
			delete(tw.timer, task.key)
		}
		iterator = next
	}
	if tw.currentPos == tw.slotNum-1 {
		tw.currentPos = 0
	} else {
		tw.currentPos++
	}
}

func (tw *TimeWheel) addTask(task *task) {
	circle, pos := tw.getPosAndCircle(task.delay)
	task.circle = circle

	tw.slots[pos].PushBack(task)

	if task.key != "" {
		tw.timer[task.key] = pos
	}
}

func (tw *TimeWheel) getPosAndCircle(delay time.Duration) (pos int, circle int) {
	circle = int(delay.Seconds()) / int(tw.interval.Seconds()) / tw.slotNum
	pos = (tw.currentPos+int(delay.Seconds())/int(tw.interval.Seconds()))%tw.slotNum - 1
	return circle, pos
}

func (tw *TimeWheel) removeTask(key string) {
	position, ok := tw.timer[key]
	if !ok {
		return
	}
	taskList := tw.slots[position]
	for iter := taskList.Front(); iter != nil; {
		task := iter.Value.(*task)
		if task.key == key {
			taskList.Remove(iter)
			delete(tw.timer, key)
			break
		}
		iter = iter.Next()
	}
}
