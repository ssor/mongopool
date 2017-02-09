package mongo_pool

import (
	"errors"
	"sync"
	"time"

	"fmt"

	"gopkg.in/mgo.v2"
)

//
// MongoSessionPool  负责管理 mongo 的连接数,防止太多的连接冲垮 mongo
// 同时负责检查连接的状态,断线之后可以自动重新连接
//
var (
	Err_no_session     = errors.New("new session left")
	Err_mongo_conn_err = fmt.Errorf("lost mongo db server")
)

type MongoSessionPool struct {
	mutex                 sync.Mutex
	conn                  *mgo.Session
	hosts                 string //mongo hosts
	max_session           int    // max session used in one node ,default 10
	current_session_count int
}

func NewMongoSessionPool(hosts string) *MongoSessionPool {

	pool := &MongoSessionPool{
		mutex:                 sync.Mutex{},
		hosts:                 hosts,
		max_session:           10,
		current_session_count: 0,
	}

	return pool
}

func (pool *MongoSessionPool) ReturnSession(session *mgo.Session) {
	session.Close()

	pool.mutex.Lock()
	pool.mutex.Unlock()

	pool.current_session_count--
}

func (pool *MongoSessionPool) GetSession() (*mgo.Session, error) {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	if pool.current_session_count > pool.max_session {
		return nil, Err_no_session
	}
	if pool.conn == nil {
		return nil, Err_mongo_conn_err
	}
	session := pool.conn.Copy()
	pool.current_session_count++
	return session, nil
}

func (pool *MongoSessionPool) Run() {

	pool.conn = initMongo(pool.hosts)
	if pool.conn == nil {
		panic("mongo connect failed")
	}

	ticker := time.NewTicker(30 * time.Second) //出发消息发送轮询事件
	go func() {
		for {
			<-ticker.C
			if pool.conn == nil {
				pool.conn = initMongo(pool.hosts)
			} else {
				if err := pool.conn.Ping(); err != nil {
					pool.conn = initMongo(pool.hosts)
				}
			}
		}
	}()

}

func initMongo(hosts string) *mgo.Session {

	mongoSession, err := mgo.DialWithTimeout(hosts, 3*time.Second)
	if err != nil {
		fmt.Println("connect mongo error: " + err.Error())
		return nil
	}

	return mongoSession
}
