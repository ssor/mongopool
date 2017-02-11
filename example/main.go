package main

import (
	"log"

	"gopkg.in/mgo.v2/bson"

	"github.com/ssor/mongopool"

	"time"

	"fmt"

	"github.com/gin-gonic/gin"
)

var (
	Mongo_pool *mongo_pool.MongoSessionPool
)

func main() {
	InitMongo()

	ticker := time.NewTicker(100 * time.Millisecond)
	// ticker := time.NewTicker(1 * time.Second)
	go func() {
		for {
			<-ticker.C
			conn_to_db := func() {
				err := SaveUserLoginInfoToDB(Mongo_pool)
				if err != nil {
					fmt.Println("*** ", err)
				}
			}
			for index := 0; index < 300; index++ {
				go conn_to_db()
			}
		}
	}()
	router := gin.Default()

	router.Run(":8090")
}

func InitMongo() {
	Mongo_pool = mongo_pool.NewMongoSessionPool("127.0.0.1", 3)
	Mongo_pool.Run()
}

var (
	count_connect_to_db = 0
)

func SaveUserLoginInfoToDB(mongo_pool *mongo_pool.MongoSessionPool) error {
	session, err := mongo_pool.GetSession()
	if err != nil {
		return err
	}
	defer mongo_pool.ReturnSession(session)

	_, err = session.DB("testdb").C("testcol").Upsert(bson.M{"_id": "testid"}, bson.M{"_id": "testid", "value": "123"})
	if err != nil {
		return err
	}
	count_connect_to_db += 1
	log.Println("mongo did some thing! ", count_connect_to_db)
	return nil
}
