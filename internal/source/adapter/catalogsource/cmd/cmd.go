package main

import (
	"github.com/operator-framework/deppy/internal/source/adapter/api"
	"github.com/operator-framework/deppy/internal/source/adapter/catalogsource"
	"github.com/operator-framework/operator-lifecycle-manager/pkg/api/client/clientset/versioned"
	"github.com/operator-framework/operator-lifecycle-manager/pkg/api/client/informers/externalversions"
	"github.com/operator-framework/operator-registry/pkg/lib/graceful"

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

	deppyCatsrcAdapter, err := newCatalogSourceAdapter(cmd.Flags())
	if err != nil {
		return fmt.Errorf("Invalid CatalogSourceAdapter configuration: %v", err)
	}

	grpcServer := grpc.NewServer()
	logger := logrus.NewEntry(logrus.New())
	api.RegisterDeppySourceAdapterServer(grpcServer, deppyCatsrcAdapter)
	reflection.Register(grpcServer)

	return graceful.Shutdown(logger, func() error {
		logger.Info("Starting server on port ", port)
		return grpcServer.Serve(lis)
	}, func() {
		grpcServer.GracefulStop()
	})
}

func newCatalogSourceAdapter(flags *pflag.FlagSet) (*catalogsource.CatalogSourceDeppyAdapter, error) {
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
	}

	address, err := flags.GetString("address")
	if err != nil {
		return nil, err
	}
	if address != "" {
		opts = append(opts, catalogsource.WithSourceAddress(name, address))
	} else {
		kubeconfigPath, err := flags.GetString("kubeconfig")
		if err != nil {
			return nil, err
		}

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
	}
	return catalogsource.NewCatalogSourceDeppyAdapter(opts...)
}
