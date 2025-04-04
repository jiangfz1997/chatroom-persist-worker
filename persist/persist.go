package persist

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/redis/go-redis/v9"
	"os"
	"persist_worker/dynamodb"
	log "persist_worker/logger"
	"strconv"
	"time"
)

var ctx = context.Background()

var Rdb = redis.NewClient(&redis.Options{
	Addr:     os.Getenv("REDIS_ADDR"), // e.g. "redis:6379"
	Password: "",                      // no password set
	DB:       0,
})

var persistTickerInterval time.Duration

func init() {
	val := os.Getenv("PERSISTTICKER")
	if val == "" {
		persistTickerInterval = 30 * time.Second
	} else {
		n, err := strconv.Atoi(val)
		if err != nil {
			log.Log.Warnf("⚠️ 无法解析 PERSISTTICKER=%s，使用默认 30s", val)
			n = 30
		}
		persistTickerInterval = time.Duration(n) * time.Second
	}

	log.Log.Infof("🕒 持久化间隔设置为: %v", persistTickerInterval)
}

func StartRedisToDBSyncLoop() {

	ticker := time.NewTicker(persistTickerInterval)
	log.Log.Infof("🌀 持久化任务启动，每 %v s执行一次", persistTickerInterval)
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
			log.Log.Infof("✅ 房间 [%s] 消息队列已空", roomID)
			break
		}
		if err != nil {
			log.Log.Warnf("❌ Redis LPOP 出错: %v", err)
			break
		}
		saveToDatabase(roomID, msg)
	}
}

func saveToDatabase(roomID string, rawMsg string) {
	var data struct {
		Sender    string `json:"sender"`
		Text      string `json:"text"`
		RoomID    string `json:"roomID"`
		TimeStamp string `json:"sentAt"`
	}
	if err := json.Unmarshal([]byte(rawMsg), &data); err != nil {
		log.Log.Errorf("⚠️ JSON 解析失败:", err)
		return
	}

	msg := dynamodb.NewMessage(data.RoomID, data.Sender, data.TimeStamp, data.Text)
	if err := dynamodb.SaveMessage(msg); err != nil {
		log.Log.Errorf("❌ DynamoDB 存储失败: %v", err)
	} else {
		log.Log.Infof("✅ 成功写入 DynamoDB: [%s] %s", data.Sender, data.Text)
	}
}

func getAllRoomIDs() []string {
	roomIDs, err := Rdb.SMembers(ctx, "rooms:active").Result()
	if err != nil {
		log.Log.Errorf("❌ 无法获取活跃房间列表: %v", err)
		return []string{}
	}
	return roomIDs
}
