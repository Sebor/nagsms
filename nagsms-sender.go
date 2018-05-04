package main

import (
	"database/sql"
	"github.com/AgileBits/go-redis-queue/redisqueue"
	"github.com/BurntSushi/toml"
	"github.com/garyburd/redigo/redis"
	log "github.com/sirupsen/logrus"
	_ "gopkg.in/rana/ora.v4"
	"os"
	"strings"
	"time"
)

var (
	conf tomlConfig
	// RedisPool creates new pool for redis
	RedisPool *redis.Pool
	ll        map[string]log.Level
	db        *sql.DB
	query     = `declare
	nResult Number;
	vTextSMS VARCHAR2(4000) := :1;
	vSystemID VARCHAR2(128) := 'NAGIOS';
	vType VARCHAR2(128) := 'NagiosInfo';
	vMsidn VARCHAR2(20) := :2;
	begin
	  nResult := MFM.TCS_NOTIFICATIONS.SEND_NOTIFICATION(pTextSMS => vTextSMS, pSystemID => vSystemID, pType => vType, pMsidn => vMsidn);
	end;`
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
	logfile := conf.LoggingConf.LogPath
	Formatter := new(log.JSONFormatter)
	log.SetFormatter(Formatter)
	log.SetLevel(ll[conf.LoggingConf.LogLevel])
	f, err := os.OpenFile(logfile, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		log.SetOutput(os.Stdout)
		log.Error(err)
	} else {
		log.SetOutput(f)
	}
	dsn := conf.OracleConf.OracleUser + "/" + conf.OracleConf.OraclePassword + "@" + conf.OracleConf.OracleHost + ":" + conf.OracleConf.OraclePort + "/" + conf.OracleConf.OracleSid
	db, _ = oracleCon(dsn, conf.OracleConf.OracleMaxOpenCon, conf.OracleConf.OracleMaxIdleCon)
	RedisPool = redisCon(conf.RedisConf.RedisHost+":"+conf.RedisConf.RedisPort, conf.RedisConf.RedisPassword, conf.RedisConf.RedisMaxActiveCon, conf.RedisConf.RedisMaxIdleCon)
}

func loadConf(filename string) (tomlConfig, error) {
	var conf tomlConfig
	if _, err := toml.DecodeFile(filename, &conf); err != nil {
		return conf, err
	}
	return conf, nil
}

func oracleCon(dsn string, maxOpenCon, maxIdleCon int) (*sql.DB, error) {
	db, err := sql.Open("ora", dsn)
	if err != nil {
		log.Error("Oracle connection error: ", err)
	}
	db.SetMaxOpenConns(maxOpenCon)
	db.SetMaxIdleConns(maxIdleCon)
	return db, err
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

func send2oracle(msg, tel string) {
	_, err := db.Exec(query, msg, tel)
	if err != nil {
		log.Error("Query execution error: ", err)
	} else {
		log.Debug("Query executed")
	}
}

func redisPop() (tel, msg string) {
	c := RedisPool.Get()
	defer c.Close()
	q := redisqueue.New(conf.RedisConf.RedisQueue, c)
	job, err := q.Pop()
	if err != nil {
		log.Error("Cannot pop data from queue: ", err)
		main()
	}
	if job != "" {
		data := strings.Split(job, " ")
		tel = data[1]
		msg = strings.Join(data[2:], " ")
	} else {
		time.Sleep(1 * time.Second)
	}
	return tel, msg
}
func sendSms() {
	for {
		if err := db.Ping(); err != nil {
			log.Fatal("Oracle connection error: ", err)
		} else {
			log.Debug("Oracle connection established")
		}
		// Revert order because of query declaration
		msg, tel := redisPop()
		send2oracle(tel, msg)
	}
}

func main() {
	sendSms()
}
