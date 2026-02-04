package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	PostgresURL string
	RedisAddr   string
	GRPCAddr    string
	HTTPAddr    string
	MetricsAddr string
	ServiceName string
	WorkerID    string
	PollTimeout time.Duration
}

func Load(serviceName string) Config {
	return Config{
		PostgresURL: getEnv("POSTGRES_URL", "postgres://scheduler:scheduler@postgres:5432/scheduler?sslmode=disable"),
		RedisAddr:   getEnv("REDIS_ADDR", "redis:6379"),
		GRPCAddr:    getEnv("GRPC_ADDR", ":9000"),
		HTTPAddr:    getEnv("HTTP_ADDR", ":8080"),
		MetricsAddr: getEnv("METRICS_ADDR", ":9090"),
		ServiceName: serviceName,
		WorkerID:    getEnv("WORKER_ID", "worker-1"),
		PollTimeout: getDuration("POLL_TIMEOUT", 5*time.Second),
	}
}

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func getDuration(key string, fallback time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if parsed, err := time.ParseDuration(value); err == nil {
			return parsed
		}
	}
	return fallback
}

func getInt(key string, fallback int) int {
	if value := os.Getenv(key); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil {
			return parsed
		}
	}
	return fallback
}
