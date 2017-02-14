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
	Err_no_session     = errors.New("no session left")
	Err_mongo_conn_err = fmt.Errorf("lost mongo db server")
)

type MongoSessionPool struct {
	mutex       sync.Mutex
	conn        *mgo.Session
	hosts       string //mongo hosts
	max_session int    // max session used in one node ,default 10
	sessions    []*mgo.Session
	// current_session_count int
}

func NewMongoSessionPool(hosts string, max_session_count int) *MongoSessionPool {
	if max_session_count <= 0 {
		max_session_count = 3
	}
	fmt.Println("[OK] mongo max_session set to ", max_session_count)

	pool := &MongoSessionPool{
		mutex:       sync.Mutex{},
		hosts:       hosts,
		max_session: max_session_count,
		sessions:    []*mgo.Session{},
		// current_session_count: 0,
	}

	return pool
}

func (pool *MongoSessionPool) ReturnSession(session *mgo.Session) {
	// session.Close()

	pool.mutex.Lock()

	defer pool.mutex.Unlock()

	pool.sessions = append(pool.sessions, session)
}

func (pool *MongoSessionPool) GetSession() (*mgo.Session, error) {
	// return pool.conn, nil
	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	// if pool.current_session_count > pool.max_session {
	// 	return nil, Err_no_session
	// }
	if pool.conn == nil {
		return nil, Err_mongo_conn_err
	}
	if len(pool.sessions) <= 0 {
		return nil, Err_no_session
	}
	// session := pool.conn.Copy()
	// pool.current_session_count++
	session := pool.sessions[0]
	pool.sessions = pool.sessions[1:]
	return session, nil
}

func (pool *MongoSessionPool) Run() {

	err := pool.initMongo()
	if err != nil {
		panic("mongo connect failed: " + err.Error())
	}
	ticker := time.NewTicker(30 * time.Second) //出发消息发送轮询事件
	go func() {
		for {
			<-ticker.C
			if pool.conn == nil {
				err = pool.initMongo()
				if err == nil {
					fmt.Println("[OK] reconnect to mongo success")
				} else {
					fmt.Println("[ERR] reconnect to mongo failed")
				}
			} else {
				if err := pool.conn.Ping(); err != nil {
					pool.conn = nil
					pool.sessions = []*mgo.Session{}
				}
			}
		}
	}()

}

func (pool *MongoSessionPool) initMongo() error {

	mongoSession, err := mgo.DialWithTimeout(pool.hosts, 3*time.Second)
	if err != nil {
		fmt.Println("connect mongo error: " + err.Error())
		return err
	}
	pool.conn = mongoSession
	pool.conn.SetPoolLimit(pool.max_session)
	for index := 0; index < pool.max_session; index++ {
		pool.sessions = append(pool.sessions, mongoSession.Copy())
	}

	fmt.Println("[OK] ", len(pool.sessions), "sessions cached")

	return nil
}
