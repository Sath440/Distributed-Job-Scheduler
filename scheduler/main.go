package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"distributed-job-scheduler/pkg/config"
	"distributed-job-scheduler/pkg/logging"
	"distributed-job-scheduler/pkg/pb"
	"distributed-job-scheduler/pkg/store"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const queueKey = "task_queue"

type schedulerServer struct {
	store  *store.Store
	redis  *redis.Client
	logger *zap.Logger

	latencyHistogram prometheus.Histogram
	retryCounter     prometheus.Counter
	failureCounter   prometheus.Counter
}

func main() {
	cfg := config.Load("scheduler")
	logger, err := logging.New(cfg.ServiceName)
	if err != nil {
		panic(err)
	}
	defer logger.Sync()

	ctx := context.Background()
	st, err := store.New(ctx, cfg.PostgresURL)
	if err != nil {
		logger.Fatal("failed to connect to postgres", zap.Error(err))
	}
	defer st.Close()

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		logger.Fatal("failed to connect to redis", zap.Error(err))
	}

	pb.RegisterJSONCodec()
	metrics := initMetrics()

	srv := &schedulerServer{
		store:            st,
		redis:            rdb,
		logger:           logger,
		latencyHistogram: metrics.latency,
		retryCounter:     metrics.retries,
		failureCounter:   metrics.failures,
	}

	go srv.runMaintenance(ctx)
	go serveMetrics(cfg.MetricsAddr, logger)

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		logger.Fatal("failed to listen", zap.Error(err))
	}

	grpcServer := grpc.NewServer(grpc.ForceServerCodec(pb.JSONCodec{}))
	pb.RegisterSchedulerServiceServer(grpcServer, srv)
	logger.Info("scheduler listening", zap.String("addr", cfg.GRPCAddr))
	if err := grpcServer.Serve(lis); err != nil {
		logger.Fatal("grpc server error", zap.Error(err))
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

func (s *schedulerServer) SubmitJob(ctx context.Context, req *pb.SubmitJobRequest) (*pb.SubmitJobResponse, error) {
	if len(req.Job.Nodes) == 0 {
		return nil, errors.New("job requires at least one node")
	}
	jobID, err := s.store.CreateJob(ctx, req.Job.Name)
	if err != nil {
		return nil, err
	}

	nodeIDs := map[string]struct{}{}
	for _, node := range req.Job.Nodes {
		if node.ID == "" {
			return nil, errors.New("node id is required")
		}
		if _, exists := nodeIDs[node.ID]; exists {
			return nil, fmt.Errorf("duplicate node id: %s", node.ID)
		}
		nodeIDs[node.ID] = struct{}{}
		err = s.store.InsertNode(ctx, jobID, store.Node{
			NodeID:         node.ID,
			Name:           node.Name,
			Command:        node.Command,
			Retries:        node.Retries,
			TimeoutSeconds: node.TimeoutSeconds,
			IdempotencyKey: node.IdempotencyKey,
		})
		if err != nil {
			return nil, err
		}
	}

	for _, node := range req.Job.Nodes {
		for _, dep := range node.DependsOn {
			if _, exists := nodeIDs[dep]; !exists {
				return nil, fmt.Errorf("dependency %s not found for node %s", dep, node.ID)
			}
			if err := s.store.InsertDependency(ctx, jobID, node.ID, dep); err != nil {
				return nil, err
			}
		}
	}

	if err := s.enqueueReady(ctx, jobID); err != nil {
		return nil, err
	}

	s.logger.Info("job submitted", zap.String("job_id", jobID))
	return &pb.SubmitJobResponse{JobID: jobID}, nil
}

func (s *schedulerServer) GetTask(ctx context.Context, req *pb.GetTaskRequest) (*pb.GetTaskResponse, error) {
	parts := strings.SplitN(req.TaskID, ":", 2)
	if len(parts) != 2 {
		return nil, errors.New("invalid task id")
	}
	jobID := parts[0]
	nodeID := parts[1]

	node, started, err := s.store.TryStartNode(ctx, jobID, nodeID)
	if err != nil {
		return nil, err
	}
	if !started {
		return nil, errors.New("task not runnable")
	}

	return &pb.GetTaskResponse{
		JobID:          node.JobID,
		NodeID:         node.NodeID,
		Command:        node.Command,
		TimeoutSeconds: node.TimeoutSeconds,
		Attempt:        node.Attempt,
	}, nil
}

func (s *schedulerServer) ReportTaskResult(ctx context.Context, req *pb.ReportTaskResultRequest) (*pb.ReportTaskResultResponse, error) {
	node, err := s.store.FetchNode(ctx, req.JobID, req.NodeID)
	if err != nil {
		return nil, err
	}
	if node.Status == store.StatusSuccess || node.Status == store.StatusFailed {
		return &pb.ReportTaskResultResponse{Status: node.Status}, nil
	}

	completedAt := time.Now()
	if req.Success {
		if err := s.store.UpdateNodeResult(ctx, req.JobID, req.NodeID, store.StatusSuccess, req.Output, completedAt, nil); err != nil {
			return nil, err
		}
		startedAt := completedAt
		if node.StartedAt != nil {
			startedAt = *node.StartedAt
		}
		s.latencyHistogram.Observe(completedAt.Sub(startedAt).Seconds())
		if err := s.enqueueReady(ctx, req.JobID); err != nil {
			return nil, err
		}
	} else {
		s.failureCounter.Inc()
		if node.Attempt <= node.Retries {
			backoff := computeBackoff(node.Attempt)
			nextRun := time.Now().Add(backoff)
			s.retryCounter.Inc()
			if err := s.store.UpdateNodeResult(ctx, req.JobID, req.NodeID, store.StatusRetryWait, req.Output, completedAt, &nextRun); err != nil {
				return nil, err
			}
		} else {
			if err := s.store.UpdateNodeResult(ctx, req.JobID, req.NodeID, store.StatusFailed, req.Output, completedAt, nil); err != nil {
				return nil, err
			}
		}
	}

	if err := s.store.MarkJobIfComplete(ctx, req.JobID); err != nil {
		return nil, err
	}

	return &pb.ReportTaskResultResponse{Status: "recorded"}, nil
}

func (s *schedulerServer) enqueueReady(ctx context.Context, jobID string) error {
	ids, err := s.store.ReadyNodeIDs(ctx, jobID)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err := s.redis.RPush(ctx, queueKey, fmt.Sprintf("%s:%s", jobID, id)).Err(); err != nil {
			return err
		}
	}
	return nil
}

func (s *schedulerServer) runMaintenance(ctx context.Context) {
	retryTicker := time.NewTicker(5 * time.Second)
	stuckTicker := time.NewTicker(10 * time.Second)
	defer retryTicker.Stop()
	defer stuckTicker.Stop()

	for {
		select {
		case <-retryTicker.C:
			s.requeueDue(ctx)
		case <-stuckTicker.C:
			s.recoverStuck(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (s *schedulerServer) requeueDue(ctx context.Context) {
	nodes, err := s.store.ListRetryDueNodes(ctx)
	if err != nil {
		s.logger.Error("failed to list retry nodes", zap.Error(err))
		return
	}
	for _, node := range nodes {
		if err := s.redis.RPush(ctx, queueKey, fmt.Sprintf("%s:%s", node.JobID, node.NodeID)).Err(); err != nil {
			s.logger.Error("failed to requeue node", zap.Error(err))
		}
	}
}

func (s *schedulerServer) recoverStuck(ctx context.Context) {
	nodes, err := s.store.ListStuckRunningNodes(ctx)
	if err != nil {
		s.logger.Error("failed to list stuck nodes", zap.Error(err))
		return
	}
	for _, node := range nodes {
		if node.Attempt <= node.Retries {
			backoff := computeBackoff(node.Attempt)
			nextRun := time.Now().Add(backoff)
			if err := s.store.UpdateNodeResult(ctx, node.JobID, node.NodeID, store.StatusRetryWait, "stuck task recovered", time.Now(), &nextRun); err != nil {
				s.logger.Error("failed to mark retry", zap.Error(err))
			}
			continue
		}
		if err := s.store.UpdateNodeResult(ctx, node.JobID, node.NodeID, store.StatusFailed, "stuck task failed", time.Now(), nil); err != nil {
			s.logger.Error("failed to mark failed", zap.Error(err))
		}
	}
}

func computeBackoff(attempt int32) time.Duration {
	if attempt <= 0 {
		return 1 * time.Second
	}
	base := time.Duration(1<<minInt(int(attempt), 6)) * time.Second
	return base
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

type metricsBundle struct {
	latency  prometheus.Histogram
	retries  prometheus.Counter
	failures prometheus.Counter
}

func initMetrics() metricsBundle {
	latency := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "task_latency_seconds",
		Help:    "Task execution latency in seconds.",
		Buckets: prometheus.DefBuckets,
	})
	retries := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "task_retries_total",
		Help: "Total number of task retries.",
	})
	failures := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "task_failures_total",
		Help: "Total number of task failures.",
	})

	prometheus.MustRegister(latency, retries, failures)
	return metricsBundle{latency: latency, retries: retries, failures: failures}
}
