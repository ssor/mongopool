package main

import (
	"fmt"
	"os"
	"os/signal"
	"sync/atomic"
	"time"

	"sync"

	"github.com/ssor/mongopool"
)

/*
	This example show a demo, how to do a series of db operation in serial
*/
type taskRunner func() error

var (
	mongoPool            *mongo_pool.MongoSessionPool
	mongoSessionMaxCount = 1 // assume that we just use one connection to  db

	mongoPoolLock sync.Mutex // only one test use mongo pool
)

func main() {
	initMongo()

	go failRunner(makeFailRunnerChan())

	go successRunner(makeSuccessRunnerChan())

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	// Block until a signal is received.
	<-c
	fmt.Println("[OK] Quit")

}

func makeFailRunnerChan() []taskRunner {
	// if we have two operation, the first cost long time, and this leads to the failure of the second
	taskRunnerFail := make([]taskRunner, 2)
	taskRunnerFail[0] = func() error {
		return getDataCostLong(mongoPool)
	}
	taskRunnerFail[1] = func() error {
		return getDataWaitShort(mongoPool)
	}
	return taskRunnerFail
}

func makeSuccessRunnerChan() []taskRunner {
	taskRunnerSuccess := make([]taskRunner, 2)

	// fill 2 tasks to do
	// the second task wait enough long so that it success after first task over
	taskRunnerSuccess[0] = func() error {
		return getDataCostLong(mongoPool)
	}
	taskRunnerSuccess[1] = func() error {
		return getDataWaitLong(mongoPool)
	}
	return taskRunnerSuccess
}

func failRunner(runners []taskRunner) {
	mongoPoolLock.Lock()
	fmt.Println(" ---> failRunner")
	defer func() {
		fmt.Println(" <--- failRunner")
		mongoPoolLock.Unlock()
	}()

	// count tasks executed
	var count int32
	defer func() {
		if count >= 3 {
			fmt.Println("[OK] yes, Only one task executed")
		} else {
			fmt.Println("[FAIL] should not all task executed")
		}
	}()

	chBreak := make(chan int)
	for _, runner := range runners {
		time.Sleep(300 * time.Millisecond) // make sure the second task do after the first
		go func(f taskRunner) {
			defer func() {
				if int(count) > 2 {
					close(chBreak)
				}
			}()
			doTask(f, &count)
		}(runner)
	}
	<-chBreak // wait, until all tasks over
}

func successRunner(runners []taskRunner) {
	mongoPoolLock.Lock()
	fmt.Println(" ---> successRunner")
	defer func() {
		fmt.Println(" <--- successRunner")
		mongoPoolLock.Unlock()
	}()

	// count tasks executed
	var count int32 // A flag indicate the result of task result
	defer func() {
		if count >= 2 {
			fmt.Println("[OK] yes, all task executed")
		} else {
			fmt.Println("[FAIL] Not all task executed")
		}
	}()

	chBreak := make(chan int)
	for _, runner := range runners {
		time.Sleep(1 * time.Second)
		f := runner
		go func() {
			defer func() {
				if int(count) >= 2 {
					close(chBreak)
				}
			}()
			doTask(f, &count)
		}()
	}
	<-chBreak
}

func doTask(f taskRunner, count *int32) {
	err := f()
	if err != nil {
		atomic.AddInt32(count, 2)
		fmt.Println(err)
		return
	}
	atomic.AddInt32(count, 1)
}

func initMongo() {
	mongoPool = mongo_pool.NewMongoSessionPool("127.0.0.1", mongoSessionMaxCount)
	mongoPool.Run()
}

type Data struct {
	ID    string `bson:"_id"`
	Value string `bson:"value"`
}

func getDataCostLong(pool *mongo_pool.MongoSessionPool) error {
	var err error
	session, err := pool.GetSessionTimeout(1 * time.Second)
	defer func() {
		pool.ReturnSession(session, err)
	}()
	if err != nil {
		return err
	}

	var data Data
	err = session.DB("testdb").C("testcol").Find(nil).One(&data)
	if err != nil {
		fmt.Println("getDataCostLong err: ", err)
		return err
	}
	fmt.Println("get Data 1 OK")
	time.Sleep(3 * time.Second) // simulate long time cost operation
	return nil
}

func getDataWaitLong(pool *mongo_pool.MongoSessionPool) error {
	var err error
	session, err := pool.GetSessionTimeout(5 * time.Second) // need at least 3 sconds
	defer func() {
		pool.ReturnSession(session, err)
	}()
	if err != nil {
		return err
	}

	var data Data
	err = session.DB("testdb").C("testcol").Find(nil).One(&data)
	if err != nil {
		fmt.Println("getDataWaitLong err: ", err)
		return err
	}
	fmt.Println("get Data 2 OK")

	return nil
}

func getDataWaitShort(pool *mongo_pool.MongoSessionPool) error {
	var err error
	session, err := pool.GetSessionTimeout(1 * time.Second)
	defer func() {
		pool.ReturnSession(session, err)
	}()
	if err != nil {
		return err
	}

	var data Data
	err = session.DB("testdb").C("testcol").Find(nil).One(&data)
	if err != nil {
		fmt.Println("getDataWaitShort err: ", err)
		return err
	}
	fmt.Println("get Data 3 OK")

	return nil
}
