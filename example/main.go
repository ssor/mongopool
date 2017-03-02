package main

import (
	"fmt"
	"time"

	"gopkg.in/mgo.v2/bson"

	"github.com/ssor/mongopool"

	"github.com/gin-gonic/gin"
)

var (
	Mongo_pool *mongo_pool.MongoSessionPool
)

func main() {
	InitMongo()
	loop_mongo()
	router := gin.Default()

	router.Run(":8090")
}

func loop_mongo() {

	go func() {
		for {
			time.Sleep(1000 * time.Millisecond)
			conn_to_db := func() {
				err := SaveUserLoginInfoToDB(Mongo_pool)
				if err != nil {
					fmt.Println("*** save user err: ", err)
				} else {
					fmt.Println("[OK] save user to do success")
				}
			}
			conn_to_db()
		}
	}()
}

func InitMongo() {
	Mongo_pool = mongo_pool.NewMongoSessionPool("127.0.0.1", 2)
	Mongo_pool.Run()
}

func SaveUserLoginInfoToDB(pool *mongo_pool.MongoSessionPool) error {
	var err error
	session, err := pool.GetSession()
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
	return nil
}
