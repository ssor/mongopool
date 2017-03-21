package main

import (
	"fmt"
	"os"
	"os/signal"
	"time"

	"gopkg.in/mgo.v2/bson"

	"github.com/ssor/mongopool"
)

/*
	This example show a demo to get mongo connection in time,
	if time too long, an error of timeout will be return
*/

var (
	mongoPool *mongo_pool.MongoSessionPool
)

func main() {
	initMongo()
	loopMongo()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	// Block until a signal is received.
	<-c
	fmt.Println("[OK] Quit")

}

func loopMongo() {
	go func() {
		for {
			time.Sleep(1000 * time.Millisecond)
			connToDB := func() {
				err := saveUserLoginInfoToDB(mongoPool)
				if err != nil {
					fmt.Println("*** save user err: ", err)
				} else {
					fmt.Println("[OK] save user to do success")
				}
			}
			go connToDB()
		}
	}()
}

func initMongo() {
	mongoPool = mongo_pool.NewMongoSessionPool("127.0.0.1", 2)
	mongoPool.Run()
}

func saveUserLoginInfoToDB(pool *mongo_pool.MongoSessionPool) error {
	var err error
	session, err := pool.GetSessionTimeout(1 * time.Second)
	defer func() {
		pool.ReturnSession(session, err)
	}()
	if err != nil {
		return err
	}
	now := time.Now().Format(time.RFC3339)
	_, err = session.DB("testdb").C("testcol").Upsert(bson.M{"_id": "testid"}, bson.M{"_id": "testid", "value": now})
	if err != nil {
		return err
	}
	time.Sleep(5 * time.Second)
	return nil
}
