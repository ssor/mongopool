package mongo_pool

import (
	"errors"
	"sync"
	"time"

	"fmt"

	"io"

	"gopkg.in/mgo.v2"
)

//
// MongoSessionPool  负责管理 mongo 的连接数,防止太多的连接冲垮 mongo
// 同时负责检查连接的状态,断线之后可以自动重新连接
//
var (
	ErrNoSession = errors.New("no session left")
	ErrNoServer  = fmt.Errorf("lost mongo db server")
	ErrTimeout   = fmt.Errorf("timeout")

	DefaultTimeout = 5 * time.Second
)

type MongoSessionPool struct {
	mutex      sync.Mutex
	conn       *mgo.Session
	hosts      string //mongo hosts
	maxSession int    // max session used in one node ,default 10
	sessions   chan *mgo.Session
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
		sessions:   make(chan *mgo.Session, maxSessionCount),
	}
	return pool
}

// ReturnSession will regain the session used away
func (pool *MongoSessionPool) ReturnSession(session *mgo.Session, err error) {
	if err == nil {
		if session != nil {
			pool.sessions <- session
		}
		return
	}

	if err == ErrNoServer ||
		err == ErrNoSession || // session must be nil
		err == ErrTimeout { // in fact, this should not happend
		return
	}

	// now if err may be mongo disconnect err or data err
	// if it's mongo disconnect err, we do below
	// if it's not, we do below although it's not the connection's err, we assume that the app will dispose this error later

	if session != nil {
		pool.sessions <- session
	}

	if err == io.EOF {
		fmt.Println("[TIP] session err, need to reconn: ", err)
		if pool.conneting == false {
			pool.mutex.Lock()
			defer pool.mutex.Unlock()
			pool.conneting = true
			go pool.reconnect()
		}
	}
}

func (pool *MongoSessionPool) GetSessionTimeout(timeout time.Duration) (*mgo.Session, error) {
	if pool.conn == nil {
		return nil, ErrNoServer
	}
	if pool.sessions == nil {
		return nil, ErrNoSession
	}

	ticker := time.After(timeout)
	select {
	case <-ticker:
		return nil, ErrTimeout
	case session := <-pool.sessions:
		return session, nil
	}
}

// GetSession return a mongo session to be used
func (pool *MongoSessionPool) GetSession() (*mgo.Session, error) {
	return pool.GetSessionTimeout(DefaultTimeout)
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
	for index := 0; index < pool.maxSession; index++ {
		s := <-pool.sessions
		s.Close()
	}
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
		pool.sessions = make(chan *mgo.Session, pool.maxSession)
	}
	for index := 0; index < pool.maxSession; index++ {
		pool.sessions <- pool.conn.Copy()
	}
}
