package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os/exec"
	"strings"
	"time"

	"distributed-job-scheduler/pkg/config"
	"distributed-job-scheduler/pkg/logging"
	"distributed-job-scheduler/pkg/pb"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const queueKey = "task_queue"

func main() {
	cfg := config.Load("worker")
	logger, err := logging.New(cfg.ServiceName)
	if err != nil {
		panic(err)
	}
	defer logger.Sync()

	pb.RegisterJSONCodec()
	conn, err := grpc.Dial(cfg.GRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.ForceCodec(pb.JSONCodec{})))
	if err != nil {
		logger.Fatal("failed to connect to scheduler", zap.Error(err))
	}
	defer conn.Close()
	client := pb.NewSchedulerServiceClient(conn)

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		logger.Fatal("failed to connect to redis", zap.Error(err))
	}

	go serveMetrics(cfg.MetricsAddr, logger)

	logger.Info("worker started", zap.String("worker_id", cfg.WorkerID))
	for {
		result, err := rdb.BLPop(context.Background(), cfg.PollTimeout, queueKey).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue
			}
			logger.Error("failed to pop task", zap.Error(err))
			continue
		}
		if len(result) < 2 {
			continue
		}
		taskID := result[1]
		handleTask(client, cfg.WorkerID, taskID, logger)
	}
}

func serveMetrics(addr string, logger *zap.Logger) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	logger.Info("metrics listening", zap.String("addr", addr))
	if err := http.ListenAndServe(addr, mux); err != nil {
		logger.Fatal("metrics server error", zap.Error(err))
	}
}

func handleTask(client pb.SchedulerServiceClient, workerID, taskID string, logger *zap.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	task, err := client.GetTask(ctx, &pb.GetTaskRequest{WorkerID: workerID, TaskID: taskID})
	if err != nil {
		logger.Warn("failed to claim task", zap.String("task_id", taskID), zap.Error(err))
		return
	}

	logger.Info("executing task", zap.String("job_id", task.JobID), zap.String("node_id", task.NodeID))
	output, err := executeCommand(task.Command, time.Duration(task.TimeoutSeconds)*time.Second)
	status := err == nil
	if err != nil {
		logger.Error("task failed", zap.String("node_id", task.NodeID), zap.Error(err))
	}

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = client.ReportTaskResult(ctx, &pb.ReportTaskResultRequest{
		WorkerID: workerID,
		JobID:    task.JobID,
		NodeID:   task.NodeID,
		Success:  status,
		Output:   output,
		Attempt:  task.Attempt,
	})
	if err != nil {
		logger.Error("failed to report result", zap.Error(err))
		return
	}
	logger.Info("task reported", zap.String("node_id", task.NodeID), zap.Bool("success", status))
}

func executeCommand(command string, timeout time.Duration) (string, error) {
	cmd := exec.Command("/bin/sh", "-c", command)
	if timeout > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		cmd = exec.CommandContext(ctx, "/bin/sh", "-c", command)
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		return strings.TrimSpace(string(output)), fmt.Errorf("command failed: %w", err)
	}
	return strings.TrimSpace(string(output)), nil
}
