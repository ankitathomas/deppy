package main

import (
	"context"
	"github.com/operator-framework/deppy/internal/source/adapter/api"
	"github.com/operator-framework/deppy/internal/source/adapter/catalogsource"
	"github.com/operator-framework/operator-lifecycle-manager/pkg/api/client/clientset/versioned"
	"github.com/operator-framework/operator-lifecycle-manager/pkg/api/client/informers/externalversions"
	clientv1alpha1 "github.com/operator-framework/operator-lifecycle-manager/pkg/api/client/clientset/versioned/typed/operators/v1alpha1"
	"github.com/operator-framework/operator-registry/pkg/lib/graceful"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/flowcontrol"

	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"k8s.io/client-go/tools/clientcmd"
	"net"
	"os"
	"time"
)

func main() {
	cmd := &cobra.Command{
		Short: "adapter",
		Long:  `runs a deppy adapter that converts CatalogSource contents to deppy source entities`,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if debug, _ := cmd.Flags().GetBool("debug"); debug {
				logrus.SetLevel(logrus.DebugLevel)
			}
			return nil
		},
		RunE: runCmdFunc,
	}
	cmd.Flags().StringP("port", "p", "50052", "port number to serve on")
	cmd.Flags().StringP("namespace", "n", "default", "namespace for CatalogSource")
	cmd.Flags().StringP("source", "s", "", "name of CatalogSource")
	cmd.Flags().StringP("address", "a", ":50051", "Address of CatalogSource. Ignored if --namespace and --source and provided")
	cmd.Flags().StringP("kubeconfig", "c", os.Getenv("KUBECONFIG"), "absolute path to the kubeconfig file")

	if err := cmd.Execute(); err != nil {
		logrus.Errorf("Failed to run deppy source adapter: %v", err)
		os.Exit(1)
	}
}

func runCmdFunc(cmd *cobra.Command, _ []string) error {
	port, err := cmd.Flags().GetString("port")
	if err != nil {
		return err
	}

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return fmt.Errorf("failed to listen: %s", err)
	}

	logger := logrus.NewEntry(logrus.New())
	deppyCatsrcAdapter, err := newCatalogSourceAdapter(cmd.Flags(), logger)
	if err != nil {
		return fmt.Errorf("Invalid CatalogSourceAdapter configuration: %v", err)
	}

	grpcServer := grpc.NewServer()
	api.RegisterDeppySourceAdapterServer(grpcServer, deppyCatsrcAdapter)
	reflection.Register(grpcServer)

	return graceful.Shutdown(logger, func() error {
		logger.Info("Starting server on port ", port)
		return grpcServer.Serve(lis)
	}, func() {
		grpcServer.GracefulStop()
	})
}

func newCatalogSourceAdapter(flags *pflag.FlagSet, logger *logrus.Entry) (*catalogsource.CatalogSourceDeppyAdapter, error) {
	var opts = []catalogsource.CatalogSourceDeppyAdapterOptions{}
	ns, err := flags.GetString("namespace")
	if err != nil {
		return nil, err
	}
	name, err := flags.GetString("source")
	if err != nil {
		return nil, err
	}
	if ns != "" && name != "" {
		opts = append(opts, catalogsource.WithNamespacedSource(name, ns))

		kubeconfigPath, err := flags.GetString("kubeconfig")
		if err != nil {
			return nil, err
		}

		c, err := newCatsrcLister(kubeconfigPath)
		if err != nil {
			return nil, err
		}
		catsrcs, err := c.CatalogSources(ns).List(context.TODO(), v1.ListOptions{})
		logger.Infof("Lister: %v, %v", catsrcs, err)

clusterConfig, err := rest.InClusterConfig()
logger.Infof("ClusterConfig: %+v, %v", clusterConfig, err)

		config, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
		if err != nil {
			return nil, err
		}

		// Create a new client for OLM types (CRs)
		crClient, err := versioned.NewForConfig(config)
		if err != nil {
			return nil, err
		}

		// TODO: jitter
		catsrcLister := externalversions.NewSharedInformerFactoryWithOptions(crClient, 5*time.Minute).Operators().V1alpha1().CatalogSources().Lister()
		opts = append(opts, catalogsource.WithLister(catsrcLister))
	} else {
		address, err := flags.GetString("address")
		if err != nil {
			return nil, err
		}
		if address != "" {
			opts = append(opts, catalogsource.WithSourceAddress(name, address))
		}
	}

	return catalogsource.NewCatalogSourceDeppyAdapter(opts...)
}

func newCatsrcLister(kubeconfigPath string) (*clientv1alpha1.OperatorsV1alpha1Client, error){
	c, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		return nil, err
	}

	configShallowCopy := *c

	if configShallowCopy.UserAgent == "" {
		configShallowCopy.UserAgent = rest.DefaultKubernetesUserAgent()
	}

	// share the transport between all clients
	httpClient, err := rest.HTTPClientFor(&configShallowCopy)
	if err != nil {
		return nil, err
	}


	if configShallowCopy.RateLimiter == nil && configShallowCopy.QPS > 0 {
		if configShallowCopy.Burst <= 0 {
			return nil, fmt.Errorf("burst is required to be greater than 0 when RateLimiter is not set and QPS is set to greater than 0")
		}
		configShallowCopy.RateLimiter = flowcontrol.NewTokenBucketRateLimiter(configShallowCopy.QPS, configShallowCopy.Burst)
	}
	v1alpha1Client, err := clientv1alpha1.NewForConfigAndClient(&configShallowCopy, httpClient)
	if err != nil {
		return nil, err
	}
	return v1alpha1Client, nil
}