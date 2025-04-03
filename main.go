package main

import (
	"github.com/joho/godotenv"
	"log"
	"persist_worker/dynamodb"
	"persist_worker/persist"
)

func main() {
	log.Println("Starting persist Worker")
	err := godotenv.Load()
	if err != nil {
		log.Println("No .ENV file found, using default values")
	}

	dynamodb.InitDB()
	persist.StartRedisToDBSyncLoop()
}
