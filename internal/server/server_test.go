package server

import (
	"context"
	"flag"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opencensus.io/examples/exporter"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	api "github.com/kazukousen/go-distributed/api/v1"
	"github.com/kazukousen/go-distributed/internal/log"
)

var testDebug = flag.Bool("debug", false, "enable observability for debugging")

func TestMain(m *testing.M) {
	flag.Parse()

	if *testDebug {
		logger, err := zap.NewDevelopment()
		if err != nil {
			panic(err)
		}
		zap.ReplaceGlobals(logger)
	}

	os.Exit(m.Run())
}

func TestGRPCServer(t *testing.T) {
	for scenario, f := range map[string]func(t *testing.T, client api.LogClient, commitLog CommitLog){
		"produce/consume a message to/from the log succeeds": testGRPCServer_ProduceComsume,
		"produce/consume stream succeeds":                    testGrpcServer_ConsumePastBoundary,
		"consume past log boundary fails":                    testGrpcServer_ProduceConsumeStream,
	} {
		t.Run(scenario, func(t *testing.T) {

			client, commitLog, teardown := setupServerTest(t)
			defer teardown()

			f(t, client, commitLog)
		})
	}
}

func testGRPCServer_ProduceComsume(t *testing.T, client api.LogClient, commitLog CommitLog) {
	ctx := context.Background()

	want := &api.Record{
		Value: []byte("hello world"),
	}

	produce, err := client.Produce(ctx, &api.ProduceRequest{
		Record: want,
	})
	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: produce.Offset,
	})
	require.NoError(t, err)
	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, want.Offset, consume.Record.Offset)
}

func testGrpcServer_ConsumePastBoundary(t *testing.T, client api.LogClient, commitLog CommitLog) {
	ctx := context.Background()

	produce, err := client.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{
			Value: []byte("hello world"),
		},
	})
	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: produce.Offset + 1,
	})
	require.Nil(t, consume)

	got := status.Code(err)
	want := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	require.Equal(t, want, got)
}

func testGrpcServer_ProduceConsumeStream(t *testing.T, client api.LogClient, commitLog CommitLog) {
	ctx := context.Background()

	records := []*api.Record{
		{Value: []byte("first")},
		{Value: []byte("second")},
	}
	{
		stream, err := client.ProduceStream(ctx)
		require.NoError(t, err)

		for off, record := range records {
			err = stream.Send(&api.ProduceRequest{
				Record: record,
			})
			require.NoError(t, err)

			res, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, uint64(off), res.Offset)
		}
	}
	{
		stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
		require.NoError(t, err)

		for i, record := range records {
			res, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, res.Record, &api.Record{
				Value:  record.Value,
				Offset: uint64(i),
			})
		}
	}
}

func setupServerTest(t *testing.T) (client api.LogClient, commitLog CommitLog, teardown func()) {
	t.Helper()

	l, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	dir, err := os.MkdirTemp("", "server-test")
	require.NoError(t, err)

	cLog, err := log.NewLog(dir, 0, 0, 0)
	require.NoError(t, err)

	server, err := NewGRPCServer(cLog)
	require.NoError(t, err)

	go func() {
		server.Serve(l)
	}()

	cliOpts := []grpc.DialOption{grpc.WithInsecure()}
	cc, err := grpc.Dial(l.Addr().String(), cliOpts...)
	require.NoError(t, err)

	client = api.NewLogClient(cc)

	var telemetryExporter *exporter.LogExporter
	if *testDebug {
		metricsLogFile, err := os.CreateTemp("", "metrics-*.log")
		require.NoError(t, err)
		t.Logf("metrics log file: %s", metricsLogFile.Name())

		tracesLogFile, err := os.CreateTemp("", "traces-*.log")
		require.NoError(t, err)
		t.Logf("traces log file: %s", tracesLogFile.Name())

		telemetryExporter, err = exporter.NewLogExporter(exporter.Options{
			MetricsLogFile:    metricsLogFile.Name(),
			TracesLogFile:     tracesLogFile.Name(),
			ReportingInterval: time.Second,
		})
		require.NoError(t, err)
		err = telemetryExporter.Start()
		require.NoError(t, err)
	}

	return client, cLog, func() {
		server.Stop()
		cc.Close()
		l.Close()
		cLog.Remove()
		if telemetryExporter != nil {
			time.Sleep(1500 * time.Millisecond)
			telemetryExporter.Stop()
			telemetryExporter.Close()
		}
	}
}
