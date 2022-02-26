package cassConnection

import (
	"fmt"
	"time"

	"github.com/gocql/gocql"
)

func ConnectCassandra() *gocql.Session {
	var err error
	cluster := gocql.NewCluster("localhost")
	cluster.ProtoVersion = 4
	cluster.ConnectTimeout = time.Second * 5
	cluster.Timeout = time.Second * 5
	cluster.NumConns = 10
	cluster.ReconnectInterval = time.Second * 1
	cluster.SocketKeepalive = 0
	cluster.DisableInitialHostLookup = true
	cluster.IgnorePeerAddr = true
	cluster.Events.DisableNodeStatusEvents = true
	cluster.Events.DisableTopologyEvents = true
	cluster.Events.DisableSchemaEvents = true
	cluster.WriteCoalesceWaitTime = 0
	cluster.ReconnectionPolicy = &gocql.ConstantReconnectionPolicy{MaxRetries: 5000, Interval: 5 * time.Second}

	Session, err := cluster.CreateSession()
	if err != nil {
		fmt.Println(" CreateSession Error : ", err.Error())
		panic(err)
	}
	fmt.Println("cassandra init done")
	if err = Session.Query(`CREATE KEYSPACE IF NOT EXISTS example_database WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}`).Exec(); err != nil {
		fmt.Println(" CREATE KEYSPACE Error : ", err.Error())
	}

	if err = Session.Query(`CREATE TABLE IF NOT EXISTS example_database.data(key text, value smallint, tel int, age int, name text, PRIMARY KEY((key, value)))`).Exec(); err != nil {
		fmt.Println(" CREATE TABLE Error : ", err.Error())
	}
	return Session
}
