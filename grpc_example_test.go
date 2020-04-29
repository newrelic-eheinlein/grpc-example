package example

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	v1 "sample-go-apps/grpc-example/v1"
)

// Used to be able to connect to a local, in-memory server.
type DialerFunc func(string, time.Duration) (net.Conn, error)

func TestTraceObserverRoundTrip(t *testing.T) {
	for i := 0; i < 50; i++ {
		test(t)
	}
}

func test(t *testing.T) {
	s := newTestObsServer(t)
	defer s.Close()

	createAndRunClient(s.dialer, 5)
	s.expectServer.WaitForSpans(t, 5, 2*time.Second)

}

func createAndRunClient(dialer DialerFunc, count int) {
	conn, _ := grpc.Dial("bufnet",
		grpc.WithDialer(dialer),
		grpc.WithInsecure(),
	)
	defer func() {
		// This is the key sleep here
		time.Sleep(500 * time.Millisecond)
		_ = conn.Close()
	}()

	serviceClient := v1.NewIngestServiceClient(conn)
	spanClient, _ := serviceClient.RecordSpan(context.Background())
	defer func() {
		if err := spanClient.CloseSend(); err != nil {
			fmt.Println("error closing trace observer sender")
		}
	}()

	for i := 0; i < count; i++ {
		span := v1.Span{TraceId: "something"}
		if err := spanClient.Send(&span); err != nil {
			fmt.Println("error sending span")
		}
	}
}

type expectServer struct {
	spansReceivedChan chan struct{}
}

func (s *expectServer) RecordSpan(stream v1.IngestService_RecordSpanServer) error {
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			return nil
		} else if nil != err {
			return err
		}
		s.spansReceivedChan <- struct{}{}
	}
}

// A server and the information needed to connect to it
type testObsServer struct {
	*expectServer
	server *grpc.Server
	conn   *grpc.ClientConn
	dialer DialerFunc
}

func (ts *testObsServer) Close() {
	_ = ts.conn.Close()
	ts.server.Stop()
}

// Create an in-memory gRPC server
func newTestObsServer(t *testing.T) testObsServer {
	grpcServer := grpc.NewServer()
	s := &expectServer{
		spansReceivedChan: make(chan struct{}, 10),
	}
	v1.RegisterIngestServiceServer(grpcServer, s)
	lis := bufconn.Listen(1024 * 1024)

	go grpcServer.Serve(lis)

	bufDialer := func(string, time.Duration) (net.Conn, error) {
		return lis.Dial()
	}
	conn, err := grpc.Dial("bufnet",
		grpc.WithDialer(bufDialer),
		grpc.WithInsecure(),
		grpc.WithBlock(), // create the connection synchronously
	)
	if err != nil {
		t.Fatal("failure to create ClientConn", err)
	}
	return testObsServer{
		expectServer: s,
		server:       grpcServer,
		conn:         conn,
		dialer:       bufDialer,
	}
}

// Wait until the expected number of Spans have been received, or the timeout is reached
func (s *expectServer) WaitForSpans(t *testing.T, expected int, secTimeout time.Duration) {
	var rcvd int
	timeout := time.NewTicker(secTimeout)
	for {
		select {
		case <-s.spansReceivedChan:
			rcvd++
			if rcvd >= expected {
				return
			}
		case <-timeout.C:
			t.Errorf("Did not receive expected spans before timeout - expected %d but got %d", expected, rcvd)
			return
		}
	}
}
