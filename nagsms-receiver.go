package main

import (
	"github.com/AgileBits/go-redis-queue/redisqueue"
	"github.com/BurntSushi/toml"
	"github.com/garyburd/redigo/redis"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"net/http"
	"os"
	"time"
)

var (
	conf tomlConfig
	// RedisPool creates new pool for redis
	RedisPool *redis.Pool
	ll        map[string]log.Level
)

const (
	letterBytes   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ-"
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

type oracleConf struct {
	OracleHost       string
	OraclePort       string
	OracleUser       string
	OraclePassword   string
	OracleSid        string
	OracleMaxOpenCon int
	OracleMaxIdleCon int
}

type redisConf struct {
	RedisHost         string
	RedisPort         string
	RedisPassword     string
	RedisQueue        string
	RedisMaxActiveCon int
	RedisMaxIdleCon   int
}

type loggingConf struct {
	LogPath  string
	LogLevel string
}

type appConf struct {
	AppListenAddr string
	AppListenPort string
	AppHandlerURI string
}

type tomlConfig struct {
	OracleConf  oracleConf  `toml:"oracle"`
	RedisConf   redisConf   `toml:"redis"`
	LoggingConf loggingConf `toml:"logging"`
	AppConf     appConf     `toml:"app"`
}

func loadConf(filename string) (tomlConfig, error) {
	if _, err := toml.DecodeFile(filename, &conf); err != nil {
		return conf, err
	}
	return conf, nil
}

func init() {
	var err error
	conf, err = loadConf("config.toml")
	if err != nil {
		log.Fatal(err)
	}
	ll = map[string]log.Level{
		"debug":   log.DebugLevel,
		"info":    log.InfoLevel,
		"warning": log.WarnLevel,
		"error":   log.ErrorLevel,
		"fatal":   log.FatalLevel,
	}
	filename := conf.LoggingConf.LogPath
	Formatter := new(log.JSONFormatter)
	log.SetFormatter(Formatter)
	log.SetLevel(ll[conf.LoggingConf.LogLevel])
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		log.SetOutput(os.Stdout)
		log.Error(err)
	} else {
		log.SetOutput(f)
	}
	RedisPool = redisCon(conf.RedisConf.RedisHost+":"+conf.RedisConf.RedisPort, conf.RedisConf.RedisPassword, conf.RedisConf.RedisMaxActiveCon, conf.RedisConf.RedisMaxIdleCon)
}

func randString(n int) string {
	var src = rand.NewSource(time.Now().UnixNano())
	b := make([]byte, n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}
	return string(b)
}

func redisCon(rhost, password string, maxActiveCon, maxIdleCon int) *redis.Pool {
	return &redis.Pool{
		MaxActive: maxActiveCon,
		MaxIdle:   maxIdleCon,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", rhost)
			if err != nil {
				log.Error(err)
			} else {
				log.Debug("Redis connection established")
			}
			if password != "" {
				if _, err := c.Do("AUTH", password); err != nil {
					log.Error(err)
					c.Close()
					return nil, err
				}
			} else {
				log.Debug("Redis password accepted")
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

func putQueue(rhost, qname, data string) {
	c := RedisPool.Get()
	defer c.Close()

	q := redisqueue.New(qname, c)
	rnd := randString(12)

	stringdata := rnd + " " + data
	_, err := q.Push(stringdata)
	if err != nil {
		log.Error("Cannot put data to queue: ", err)
	} else {
		log.Debug("Data was added: ", stringdata)
	}

	log.Debug("RND: ", rnd)
}

func handler(w http.ResponseWriter, r *http.Request) {
	tel, ok := r.URL.Query()["tel"]
	if !ok || len(tel) < 1 {
		log.Errorf("Url Param %s is missing", "tel")
		return
	}
	msg, ok := r.URL.Query()["msg"]
	if !ok || len(msg) < 1 {
		log.Errorf("Url Param %s is missing", "msg")
		return
	}
	data := tel[0] + " " + msg[0]
	go putQueue(conf.RedisConf.RedisHost+":"+conf.RedisConf.RedisPort, conf.RedisConf.RedisQueue, data)
}

func main() {
	http.HandleFunc(conf.AppConf.AppHandlerURI, handler)
	http.ListenAndServe(conf.AppConf.AppListenAddr+":"+conf.AppConf.AppListenPort, nil)
}
