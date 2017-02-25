package mongo_pool

import (
	"errors"
	"sync"
	"time"

	"fmt"

	"gopkg.in/mgo.v2"
	// "github.com/ssor/mgo"
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
	conneting   bool
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
	// mgo.SetDebug(true)
	// mgo.SetLogger(logger(func(depth int, s string) error {
	// 	fmt.Println(depth, " : ", s)
	// 	return nil
	// }))
	return pool
}

type logger func(depth int, s string) error

func (l logger) Output(calldepth int, s string) error {
	if l != nil {
		return l(calldepth, s)
	}
	return nil
}

func (pool *MongoSessionPool) ReturnSession(session *mgo.Session, err error) {

	pool.mutex.Lock()

	defer pool.mutex.Unlock()

	if err == nil {
		if session != nil {
			pool.sessions = append(pool.sessions, session)
		}
	} else {
		fmt.Println("[TIP] session err, need to reconn: ", err)
		for _, session := range pool.sessions {
			session.Close()
		}
		pool.sessions = pool.sessions[0:0]

		if pool.conneting == false {
			pool.conneting = true
			go pool.reconnect()
		}
	}

}

func (pool *MongoSessionPool) GetSession() (*mgo.Session, error) {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	if pool.conn == nil {
		return nil, Err_mongo_conn_err
	}
	if pool.sessions == nil || len(pool.sessions) <= 0 {
		return nil, Err_no_session
	}
	session := pool.sessions[0]
	pool.sessions = pool.sessions[1:]
	return session, nil
}

func (pool *MongoSessionPool) Run() {

	err := pool.initMongo()
	if err != nil {
		panic("mongo connect failed: " + err.Error())
	}
}

func (pool *MongoSessionPool) reconnect() {
	fmt.Println("[TIP] Trying to reconnect to mongo...")
	time.Sleep(5 * time.Second)
	pool.conn.Refresh()
	pool.generateSessionPool()

	pool.mutex.Lock()
	defer pool.mutex.Unlock()
	pool.conneting = false
}

func (pool *MongoSessionPool) initMongo() error {

	mongoSession, err := mgo.DialWithTimeout(pool.hosts, 3*time.Second)
	if err != nil {
		fmt.Println("connect mongo error: " + err.Error())
		return err
	}

	pool.mutex.Lock()
	defer pool.mutex.Unlock()
	pool.conn = mongoSession
	pool.conn.SetPoolLimit(pool.max_session)
	pool.generateSessionPool()

	fmt.Println("[OK] ", len(pool.sessions), "sessions cached")

	return nil
}

func (pool *MongoSessionPool) generateSessionPool() {
	if pool.sessions == nil {
		pool.sessions = []*mgo.Session{}
	}
	for index := 0; index < pool.max_session; index++ {
		pool.sessions = append(pool.sessions, pool.conn.Copy())
	}
}
