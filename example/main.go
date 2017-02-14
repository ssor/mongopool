package main

import (
	"fmt"
	"log"
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

	ticker := time.NewTicker(10 * time.Millisecond)
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
}

func InitMongo() {
	Mongo_pool = mongo_pool.NewMongoSessionPool("127.0.0.1", 1)
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
