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
	ErrNoSession = errors.New("no session left")
	ErrNoServer  = fmt.Errorf("lost mongo db server")
)

type MongoSessionPool struct {
	mutex      sync.Mutex
	conn       *mgo.Session
	hosts      string //mongo hosts
	maxSession int    // max session used in one node ,default 10
	sessions   []*mgo.Session
	conneting  bool
}

// NewMongoSessionPool init a pool
func NewMongoSessionPool(hosts string, maxSessionCount int) *MongoSessionPool {
	if maxSessionCount <= 0 {
		maxSessionCount = 3
	}
	fmt.Println("[OK] mongo max_session set to ", maxSessionCount)

	pool := &MongoSessionPool{
		mutex:      sync.Mutex{},
		hosts:      hosts,
		maxSession: maxSessionCount,
		sessions:   []*mgo.Session{},
	}
	return pool
}

// ReturnSession will regain the session used away
func (pool *MongoSessionPool) ReturnSession(session *mgo.Session, err error) {

	pool.mutex.Lock()

	defer pool.mutex.Unlock()

	if err == nil {
		if session != nil {
			pool.sessions = append(pool.sessions, session)
		}
		return
	}

	if err == ErrNoServer { // in fact, this should not happend
		return
	}

	if err == ErrNoSession { // session must be nil
		return
	}

	// now if err may be mongo disconnect err or data err
	// if it's mongo disconnect err, we do below
	// if it's not, we do below although it's not the connection's err, we assume that the app will dispose this error later

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

// GetSession return a mongo session to be used
func (pool *MongoSessionPool) GetSession() (*mgo.Session, error) {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	if pool.conn == nil {
		return nil, ErrNoServer
	}
	if pool.sessions == nil || len(pool.sessions) <= 0 {
		return nil, ErrNoSession
	}
	session := pool.sessions[0]
	pool.sessions = pool.sessions[1:]
	return session, nil
}

// Run will start a mongo connection
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
	pool.conn.SetPoolLimit(pool.maxSession)
	pool.generateSessionPool()

	fmt.Println("[OK] ", len(pool.sessions), "sessions cached")

	return nil
}

func (pool *MongoSessionPool) generateSessionPool() {
	if pool.sessions == nil {
		pool.sessions = []*mgo.Session{}
	}
	for index := 0; index < pool.maxSession; index++ {
		pool.sessions = append(pool.sessions, pool.conn.Copy())
	}
}
