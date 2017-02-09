package main

import (
	"log"

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
	ticker := time.NewTicker(5 * time.Second)
	go func() {
		for {
			<-ticker.C
			err := SaveUserLoginInfoToDB(Mongo_pool)
			if err != nil {
				fmt.Println("*** ", err)
			}
		}
	}()
	router := gin.Default()

	router.Run(":8090")
}

func InitMongo() {
	Mongo_pool = mongo_pool.NewMongoSessionPool("127.0.0.1")
	Mongo_pool.Run()
}

func SaveUserLoginInfoToDB(mongo_pool *mongo_pool.MongoSessionPool) error {
	session, err := mongo_pool.GetSession()
	if err != nil {
		return err
	}
	defer mongo_pool.ReturnSession(session)

	log.Println("mongo did some thing!")
	return nil
}
