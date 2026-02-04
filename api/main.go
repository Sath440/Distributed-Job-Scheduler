package main

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"distributed-job-scheduler/pkg/config"
	"distributed-job-scheduler/pkg/logging"
	"distributed-job-scheduler/pkg/pb"
	"distributed-job-scheduler/pkg/store"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type apiServer struct {
	store  *store.Store
	client pb.SchedulerServiceClient
	logger *zap.Logger
}

func main() {
	cfg := config.Load("api")
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

	pb.RegisterJSONCodec()
	conn, err := grpc.Dial(cfg.GRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.ForceCodec(pb.JSONCodec{})))
	if err != nil {
		logger.Fatal("failed to connect to scheduler", zap.Error(err))
	}
	defer conn.Close()

	server := &apiServer{
		store:  st,
		client: pb.NewSchedulerServiceClient(conn),
		logger: logger,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/jobs", server.handleJobs)
	mux.HandleFunc("/v1/jobs/", server.handleJobStatus)
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })

	logger.Info("api listening", zap.String("addr", cfg.HTTPAddr))
	if err := http.ListenAndServe(cfg.HTTPAddr, mux); err != nil {
		logger.Fatal("api server error", zap.Error(err))
	}
}

func (a *apiServer) handleJobs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var req pb.SubmitJobRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	resp, err := a.client.SubmitJob(ctx, &req)
	if err != nil {
		a.logger.Error("failed to submit job", zap.Error(err))
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, resp)
}

func (a *apiServer) handleJobStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	jobID := strings.TrimPrefix(r.URL.Path, "/v1/jobs/")
	if jobID == "" {
		writeError(w, http.StatusBadRequest, "job id required")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	status, err := a.store.JobStatus(ctx, jobID)
	if err != nil {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	nodes, err := a.store.FetchJobNodes(ctx, jobID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	response := map[string]any{
		"job_id": jobID,
		"status": status,
		"nodes":  nodes,
	}
	writeJSON(w, http.StatusOK, response)
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
