package main

import (
	"flag"
	"log"
	"ratelimiter/db"
)

var nodeAddr = flag.String("node_addr", "", "the address to listen for node communication")

func main() {
	flag.Parse()

	if len(*nodeAddr) == 0 {
		log.Fatalf("nodeAddr shouldn't be empty\n")
	}

	redisAddr := "localhost:6379"
	redisPWD := ""
	config := db.NewInitConfig(2, 20, *nodeAddr, 3, redisAddr, redisPWD)

	acc, err := db.NewAccessor(redisAddr, redisPWD)
	if err != nil {
		log.Fatalf("db.NewAccessor error: %v\n", err)
	}
	m, err := db.NewMaintainer(acc.GetClient(), config)
	if err != nil {
		log.Fatalf("db.NewMaintainer error: %v\n", err)
	}
	m.RunLoop()
	m.Wait()
}
