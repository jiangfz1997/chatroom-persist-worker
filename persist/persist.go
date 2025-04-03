package persist

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/redis/go-redis/v9"
	"log"
	"os"
	"persist_worker/dynamodb"
	"time"
)

var ctx = context.Background()

var Rdb = redis.NewClient(&redis.Options{
	Addr:     os.Getenv("REDIS_ADDR"), // e.g. "redis:6379"
	Password: "",                      // no password set
	DB:       0,
})

var persistTickerInterval = 10 * time.Second

func StartRedisToDBSyncLoop() {
	ticker := time.NewTicker(persistTickerInterval)
	log.Println("🌀 持久化任务启动，每", persistTickerInterval)
	for range ticker.C {
		syncAllRooms()
	}
}

func syncAllRooms() {
	roomIDs := getAllRoomIDs()
	for _, roomID := range roomIDs {
		syncRoomMessages(roomID)
	}
}

func syncRoomMessages(roomID string) {
	key := "room:" + roomID + ":to_persist"
	for i := 0; i < 100; i++ {
		msg, err := Rdb.LPop(ctx, key).Result()
		if errors.Is(err, redis.Nil) {
			break
		}
		if err != nil {
			log.Printf("❌ Redis LPOP 出错: %v", err)
			break
		}
		saveToDatabase(roomID, msg)
	}
}

func saveToDatabase(roomID string, rawMsg string) {
	var data struct {
		Sender string `json:"sender"`
		Text   string `json:"text"`
	}
	if err := json.Unmarshal([]byte(rawMsg), &data); err != nil {
		log.Println("⚠️ JSON 解析失败:", err)
		return
	}

	msg := dynamodb.NewMessage(roomID, data.Sender, data.Text)
	if err := dynamodb.SaveMessage(msg); err != nil {
		log.Printf("❌ DynamoDB 存储失败: %v", err)
	} else {
		log.Printf("✅ 成功写入 DynamoDB: [%s] %s", data.Sender, data.Text)
	}
}

func getAllRoomIDs() []string {
	roomIDs, err := Rdb.SMembers(ctx, "rooms:active").Result()
	if err != nil {
		log.Printf("❌ 无法获取活跃房间列表: %v", err)
		return []string{}
	}
	return roomIDs
}
