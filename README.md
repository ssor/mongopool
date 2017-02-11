# mongopool
library to connect to mongodb

# features
1. support the count of max connections config
2. support redial mongo if mongo shutdown and setup again

# how to use

1. init a pool 
```
	Mongo_pool = mongo_pool.NewMongoSessionPool("127.0.0.1", 3)
	Mongo_pool.Run()
```

2. get a session and after using, return the session
```
	session, err := mongo_pool.GetSession()
	if err != nil {
		return err
	}
	defer mongo_pool.ReturnSession(session)
```