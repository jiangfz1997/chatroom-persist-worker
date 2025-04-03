package dynamodb

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"os"
	log "persist_worker/logger"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

var DB *ddb.Client

func InitDB() {
	endpoint := os.Getenv("DYNAMODB_ENDPOINT") // 本地模式會設這個
	region := os.Getenv("DYNAMODB_REGION")
	if region == "" {
		region = "us-west-2" // fallback
	}
	var cfg aws.Config
	var err error

	if endpoint != "" {
		log.Log.Info("🧪 连接本地 DynamoDB (local mode)")
		log.Log.Infof("🔌 使用 endpoint: %s", endpoint)

		// 设置本地模拟器的 endpoint
		customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, _ ...interface{}) (aws.Endpoint, error) {
			if service == ddb.ServiceID {
				return aws.Endpoint{
					URL:           endpoint,
					SigningRegion: region,
				}, nil
			}
			return aws.Endpoint{}, fmt.Errorf("unknown endpoint requested")
		})

		// 加载配置，添加本地用的 dummy 凭证
		cfg, err = config.LoadDefaultConfig(context.TODO(),
			config.WithRegion(region),
			config.WithEndpointResolverWithOptions(customResolver),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "dummy")),
		)
		log.Log.Infof("📦 DynamoDB Config Loaded | Region: %s", cfg.Region)

		if cfg.Retryer != nil {
			log.Log.Info("🔁 Retryer 已配置（重试机制启用）")
		}

		if cfg.Credentials != nil {
			creds, err := cfg.Credentials.Retrieve(context.TODO())
			if err != nil {
				log.Log.Warnf("⚠️ 获取凭证失败: %v", err)
			} else {
				log.Log.Infof("🔐 使用的凭证：AccessKey=%s (Provider=%s)", creds.AccessKeyID, creds.Source)
			}
		}
		if err != nil {
			log.Log.Fatal("❌ 加载本地 DynamoDB 配置失败:", err)
		}

	} else {
		log.Log.Info("🚀 连接 AWS DynamoDB（真实云服务）")
		// 加载默认配置，依赖环境变量或 IAM 角色
		cfg, err = config.LoadDefaultConfig(context.TODO(),
			config.WithRegion(region),
		)
		if err != nil {
			log.Log.Fatalf("❌ 加载 AWS 配置失败:", err)
		}
	}

	// 创建 DynamoDB 客户端
	DB = ddb.NewFromConfig(cfg)
	log.Log.Info("Connected to DynamoDB")

	resp, err := DB.ListTables(context.TODO(), &ddb.ListTablesInput{})
	if err != nil {
		log.Log.Errorf("⚠️ 无法列出表，连接可能有误: %v", err)
	} else {
		log.Log.Infof("📋 当前 DynamoDB 表: %v", resp.TableNames)
	}
}
