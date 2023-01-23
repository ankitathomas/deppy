package catalogsource

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/operator-framework/api/pkg/operators/v1alpha1"
	catalogsourceapi "github.com/operator-framework/operator-registry/pkg/api"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/operator-framework/deppy/pkg/deppy/input"
	"github.com/operator-framework/deppy/pkg/lib/grpc"
)

type RegistryClient interface {
	ListEntities(ctx context.Context, catsrc *v1alpha1.CatalogSource) ([]*input.Entity, error)
}

type registryGRPCClient struct {
	timeout time.Duration
	client  client.Client
}

func NewRegistryGRPCClient(grpcTimeout time.Duration, client client.Client) RegistryClient {
	if grpcTimeout == 0 {
		grpcTimeout = grpc.DefaultGRPCTimeout
	}
	return &registryGRPCClient{timeout: grpcTimeout, client: client}
}

func (r *registryGRPCClient) ListEntities(ctx context.Context, catalogSource *v1alpha1.CatalogSource) ([]*input.Entity, error) {
	// TODO: create GRPC connections separately
	conn, err := grpc.ConnectWithTimeout(ctx, addressFromCatalogSource(ctx, r.client, catalogSource), r.timeout)
	if conn != nil {
		defer conn.Close()
	}
	if err != nil {
		return nil, err
	}

	catsrcClient := catalogsourceapi.NewRegistryClient(conn)
	stream, err := catsrcClient.ListBundles(ctx, &catalogsourceapi.ListBundlesRequest{})

	if err != nil {
		return nil, fmt.Errorf("ListBundles failed: %v", err)
	}

	var entities []*input.Entity
	catalogPackages := map[string]*catalogsourceapi.Package{}
	catalogSourceID := fmt.Sprintf("%s/%s", catalogSource.Namespace, catalogSource.Name)
	for {
		bundle, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return entities, fmt.Errorf("failed to read bundle stream: %v", err)
		}

		packageKey := fmt.Sprintf("%s/%s", catalogSourceID, bundle.PackageName)
		pkg, ok := catalogPackages[packageKey]
		if !ok {
			pkg, err = catsrcClient.GetPackage(ctx, &catalogsourceapi.GetPackageRequest{Name: bundle.PackageName})
			if err != nil {
				return entities, fmt.Errorf("failed to get package %s: %v", bundle.PackageName, err)
			}
			catalogPackages[packageKey] = pkg
		}

		entity, err := entityFromBundle(catalogSourceID, pkg, bundle)
		if err != nil {
			return entities, fmt.Errorf("failed to parse entity %s: %v", entity.Identifier(), err)
		}
		entities = append(entities, entity)
	}
	return entities, nil
}

func addressFromCatalogSource(ctx context.Context, c client.Client, catalogSource *v1alpha1.CatalogSource) string {
	if catalogSource == nil {
		return ""
	}
	svc := v1.Service{}
	svcName := types.NamespacedName{
		Namespace: catalogSource.Status.RegistryServiceStatus.ServiceNamespace,
		Name:      catalogSource.Status.RegistryServiceStatus.ServiceName,
	}
	if err := c.Get(ctx, svcName,
		&svc); err != nil {
		// Cannot detect the backing service, don't refresh cache
		return ""
	}
	if svc.Spec.Type == v1.ServiceTypeClusterIP {
		return catalogSource.Address()
	}
	if len(svc.Spec.Selector[catalogSourceSelectorLabel]) == 0 {
		// Cannot verify if image is recent
		return ""
	}
	pods := v1.PodList{}
	if err := c.List(ctx, &pods,
		client.MatchingLabelsSelector{
			Selector: labels.SelectorFromValidatedSet(
				map[string]string{catalogSourceSelectorLabel: svc.Spec.Selector[catalogSourceSelectorLabel]},
			)}); err != nil {
		return ""
	}
	if len(pods.Items) < 1 {
		// service doesn't have pods backing it yet
		return ""
	}
	if len(pods.Items[0].Status.ContainerStatuses) < 1 {
		// pod not ready
		return ""
	}
	if len(pods.Items[0].Status.ContainerStatuses[0].ImageID) == 0 {
		// pod not ready
		return ""
	}
	return fmt.Sprintf("%s:%d", pods.Items[0].Status.HostIP, svc.Spec.Ports[0].NodePort)
}
