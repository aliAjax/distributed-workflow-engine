package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type jsonCodec struct{}

func (jsonCodec) Name() string { return "json" }

func (jsonCodec) Marshal(v any) ([]byte, error) { return json.Marshal(v) }

func (jsonCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

func main() {
	var addr string
	var token string
	var method string
	flag.StringVar(&addr, "addr", "localhost:19090", "gRPC server address")
	flag.StringVar(&token, "token", "dev-secret-token", "bearer token")
	flag.StringVar(&method, "method", "Health", "method to invoke")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(jsonCodec{})),
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer conn.Close()

	request := map[string]any{}
	if method == "CreateWorkflow" {
		request = map[string]any{
			"name":        "grpc-workflow",
			"description": "created over gRPC",
			"definition": map[string]any{
				"name":    "grpc-workflow",
				"version": 1,
				"nodes": map[string]any{
					"start": map[string]any{"id": "start", "type": "echo", "name": "start", "command": "grpc"},
					"done":  map[string]any{"id": "done", "type": "log", "name": "done", "command": "ok"},
				},
				"edges": []any{map[string]any{"from": "start", "to": "done"}},
			},
		}
	}
	response := map[string]any{}
	fullMethod := fmt.Sprintf("/workflow.v1.Management/%s", method)
	if err := conn.Invoke(ctx, fullMethod, &request, &response); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	raw, _ := json.Marshal(response)
	fmt.Println(string(raw))
}
