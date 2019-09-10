// +build functional

package cri_containerd

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/containerd/typeurl"

	_ "github.com/Microsoft/hcsshim/test/functional/manifest"
	"github.com/containerd/containerd"
	eventtypes "github.com/containerd/containerd/api/events"
	eventsapi "github.com/containerd/containerd/api/services/events/v1"
	eventruntime "github.com/containerd/containerd/runtime"
	"github.com/gogo/protobuf/types"
	"google.golang.org/grpc"
	runtime "k8s.io/kubernetes/pkg/kubelet/apis/cri/runtime/v1alpha2"
)

const (
	daemonAddress                = "tcp://127.0.0.1:2376"
	connectTimeout               = time.Second * 10
	testNamespace                = "cri-containerd-test"
	wcowProcessRuntimeHandler    = "runhcs-wcow-process"
	wcowHypervisorRuntimeHandler = "runhcs-wcow-hypervisor"
	lcowRuntimeHandler           = "runhcs-lcow"
	imageWindowsRS5Nanoserver    = "mcr.microsoft.com/windows/nanoserver:1809"
	imageWindowsRS5Servercore    = "mcr.microsoft.com/windows/servercore:1809"
	imageLcowK8sPause            = "k8s.gcr.io/pause:3.1"
	imageLcowAlpine              = "docker.io/library/alpine:latest"
	imageLcowCosmos              = "cosmosarno/spark-master:2.4.1_2019-04-18_8e864ce"
)

func createGRPCConn(ctx context.Context) (*grpc.ClientConn, error) {
	conn, err := grpc.DialContext(ctx, daemonAddress, grpc.WithInsecure(), grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
		return net.DialTimeout("tcp", "127.0.0.1:2376", timeout)
	}))
	return conn, err
}

func newTestRuntimeClient(t *testing.T) runtime.RuntimeServiceClient {
	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()
	conn, err := createGRPCConn(ctx)
	if err != nil {
		t.Fatalf("failed to dial runtime client: %v", err)
	}
	return runtime.NewRuntimeServiceClient(conn)
}

func newTestEventService(t *testing.T) containerd.EventService {
	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()
	conn, err := createGRPCConn(ctx)
	if err != nil {
		t.Fatalf("Failed to create a client connection %v", err)
	}
	return containerd.NewEventServiceFromClient(eventsapi.NewEventsClient(conn))
}

func newTestImageClient(t *testing.T) runtime.ImageServiceClient {
	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()
	conn, err := createGRPCConn(ctx)
	if err != nil {
		t.Fatalf("failed to dial runtime client: %v", err)
	}
	return runtime.NewImageServiceClient(conn)
}

func getTargetRunTopics() (topicNames []string, filters []string) {
	topicNames = []string{
		eventruntime.TaskCreateEventTopic,
		eventruntime.TaskStartEventTopic,
		eventruntime.TaskExitEventTopic,
		eventruntime.TaskDeleteEventTopic,
	}

	filters = make([]string, len(topicNames))

	for i, name := range topicNames {
		filters[i] = fmt.Sprintf(`topic=="%v"`, name)
	}
	return topicNames, filters
}

func convertEvent(e *types.Any) (string, interface{}, error) {
	id := ""
	evt, err := typeurl.UnmarshalAny(e)
	if err != nil {
		return "", nil, err
	}

	switch event := evt.(type) {
	case *eventtypes.TaskCreate:
		id = event.ContainerID
	case *eventtypes.TaskStart:
		id = event.ContainerID
	case *eventtypes.TaskDelete:
		id = event.ContainerID
	case *eventtypes.TaskExit:
		id = event.ContainerID
	default:
		return "", nil, errors.New("test does not support this event")
	}
	return id, evt, nil
}

func pullRequiredImages(t *testing.T, images []string) {
	pullRequiredImagesWithLabels(t, images, map[string]string{
		"sandbox-platform": "windows/amd64", // Not required for Windows but makes the test safer depending on defaults in the config.
	})
}

func pullRequiredLcowImages(t *testing.T, images []string) {
	pullRequiredImagesWithLabels(t, images, map[string]string{
		"sandbox-platform": "linux/amd64",
	})
}

func pullRequiredImagesWithLabels(t *testing.T, images []string, labels map[string]string) {
	if len(images) < 1 {
		return
	}

	client := newTestImageClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sb := &runtime.PodSandboxConfig{
		Labels: labels,
	}
	for _, image := range images {
		_, err := client.PullImage(ctx, &runtime.PullImageRequest{
			Image: &runtime.ImageSpec{
				Image: image,
			},
			SandboxConfig: sb,
		})
		if err != nil {
			t.Fatalf("failed PullImage for image: %s, with error: %v", image, err)
		}
	}
}
