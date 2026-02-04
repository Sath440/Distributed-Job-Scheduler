package pb

import (
	"context"
	"encoding/json"

	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
)

const (
	SchedulerServiceName = "scheduler.v1.SchedulerService"
)

// JSONCodec provides a lightweight codec for gRPC without generated protobufs.
type JSONCodec struct{}

func (JSONCodec) Name() string { return "json" }

func (JSONCodec) Marshal(v any) ([]byte, error) { return json.Marshal(v) }

func (JSONCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

func RegisterJSONCodec() {
	encoding.RegisterCodec(JSONCodec{})
}

type JobNode struct {
	ID             string   `json:"id"`
	Name           string   `json:"name"`
	Command        string   `json:"command"`
	Retries        int32    `json:"retries"`
	TimeoutSeconds int32    `json:"timeout_seconds"`
	IdempotencyKey string   `json:"idempotency_key"`
	DependsOn      []string `json:"depends_on"`
}

type JobDefinition struct {
	Name  string    `json:"name"`
	Nodes []JobNode `json:"nodes"`
}

type SubmitJobRequest struct {
	Job JobDefinition `json:"job"`
}

type SubmitJobResponse struct {
	JobID string `json:"job_id"`
}

type GetTaskRequest struct {
	WorkerID string `json:"worker_id"`
	TaskID   string `json:"task_id"`
}

type GetTaskResponse struct {
	JobID          string `json:"job_id"`
	NodeID         string `json:"node_id"`
	Command        string `json:"command"`
	TimeoutSeconds int32  `json:"timeout_seconds"`
	Attempt        int32  `json:"attempt"`
}

type ReportTaskResultRequest struct {
	WorkerID string `json:"worker_id"`
	JobID    string `json:"job_id"`
	NodeID   string `json:"node_id"`
	Success  bool   `json:"success"`
	Output   string `json:"output"`
	Attempt  int32  `json:"attempt"`
}

type ReportTaskResultResponse struct {
	Status string `json:"status"`
}

type SchedulerServiceServer interface {
	SubmitJob(context.Context, *SubmitJobRequest) (*SubmitJobResponse, error)
	GetTask(context.Context, *GetTaskRequest) (*GetTaskResponse, error)
	ReportTaskResult(context.Context, *ReportTaskResultRequest) (*ReportTaskResultResponse, error)
}

type SchedulerServiceClient interface {
	SubmitJob(ctx context.Context, in *SubmitJobRequest, opts ...grpc.CallOption) (*SubmitJobResponse, error)
	GetTask(ctx context.Context, in *GetTaskRequest, opts ...grpc.CallOption) (*GetTaskResponse, error)
	ReportTaskResult(ctx context.Context, in *ReportTaskResultRequest, opts ...grpc.CallOption) (*ReportTaskResultResponse, error)
}

type schedulerServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewSchedulerServiceClient(cc grpc.ClientConnInterface) SchedulerServiceClient {
	return &schedulerServiceClient{cc: cc}
}

func (c *schedulerServiceClient) SubmitJob(ctx context.Context, in *SubmitJobRequest, opts ...grpc.CallOption) (*SubmitJobResponse, error) {
	out := new(SubmitJobResponse)
	err := c.cc.Invoke(ctx, "/"+SchedulerServiceName+"/SubmitJob", in, out, opts...)
	return out, err
}

func (c *schedulerServiceClient) GetTask(ctx context.Context, in *GetTaskRequest, opts ...grpc.CallOption) (*GetTaskResponse, error) {
	out := new(GetTaskResponse)
	err := c.cc.Invoke(ctx, "/"+SchedulerServiceName+"/GetTask", in, out, opts...)
	return out, err
}

func (c *schedulerServiceClient) ReportTaskResult(ctx context.Context, in *ReportTaskResultRequest, opts ...grpc.CallOption) (*ReportTaskResultResponse, error) {
	out := new(ReportTaskResultResponse)
	err := c.cc.Invoke(ctx, "/"+SchedulerServiceName+"/ReportTaskResult", in, out, opts...)
	return out, err
}

func RegisterSchedulerServiceServer(s *grpc.Server, srv SchedulerServiceServer) {
	s.RegisterService(&grpc.ServiceDesc{
		ServiceName: SchedulerServiceName,
		HandlerType: (*SchedulerServiceServer)(nil),
		Methods: []grpc.MethodDesc{
			{
				MethodName: "SubmitJob",
				Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
					in := new(SubmitJobRequest)
					if err := dec(in); err != nil {
						return nil, err
					}
					if interceptor == nil {
						return srv.(SchedulerServiceServer).SubmitJob(ctx, in)
					}
					info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/" + SchedulerServiceName + "/SubmitJob"}
					handler := func(ctx context.Context, req any) (any, error) {
						return srv.(SchedulerServiceServer).SubmitJob(ctx, req.(*SubmitJobRequest))
					}
					return interceptor(ctx, in, info, handler)
				},
			},
			{
				MethodName: "GetTask",
				Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
					in := new(GetTaskRequest)
					if err := dec(in); err != nil {
						return nil, err
					}
					if interceptor == nil {
						return srv.(SchedulerServiceServer).GetTask(ctx, in)
					}
					info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/" + SchedulerServiceName + "/GetTask"}
					handler := func(ctx context.Context, req any) (any, error) {
						return srv.(SchedulerServiceServer).GetTask(ctx, req.(*GetTaskRequest))
					}
					return interceptor(ctx, in, info, handler)
				},
			},
			{
				MethodName: "ReportTaskResult",
				Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
					in := new(ReportTaskResultRequest)
					if err := dec(in); err != nil {
						return nil, err
					}
					if interceptor == nil {
						return srv.(SchedulerServiceServer).ReportTaskResult(ctx, in)
					}
					info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/" + SchedulerServiceName + "/ReportTaskResult"}
					handler := func(ctx context.Context, req any) (any, error) {
						return srv.(SchedulerServiceServer).ReportTaskResult(ctx, req.(*ReportTaskResultRequest))
					}
					return interceptor(ctx, in, info, handler)
				},
			},
		},
	}, srv)
}
