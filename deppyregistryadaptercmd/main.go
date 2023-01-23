package main

import (
	"context"
	"os"

	"github.com/operator-framework/api/pkg/operators/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/operator-framework/deppy/pkg/deppy/input/catalogsource"
)

func main() {
	logger := zap.New()
	cli, err := dynamic.NewForConfig(ctrl.GetConfigOrDie())
	if err != nil {
		logger.Error(err, "error creating restcli")
		os.Exit(1)
	}

	watch, err := cli.Resource(schema.GroupVersionResource{
		Group:    v1alpha1.GroupName,
		Version:  v1alpha1.GroupVersion,
		Resource: "catalogsources",
	}).Watch(context.TODO(), v1.ListOptions{})
	if err != nil {
		logger.Error(err, "error listing catsrc")
		os.Exit(1)
	}

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		logger.Error(err, "failed to add scheme")
		os.Exit(1)
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		logger.Error(err, "failed to add catalogSource to scheme")
		os.Exit(1)
	}

	c, err := client.New(ctrl.GetConfigOrDie(), client.Options{
		Scheme: scheme,
	})

	if err != nil {
		logger.Error(err, "failed to create controller-runtime client")
		os.Exit(1)
	}

	cacheCli := catalogsource.NewCachedRegistryQuerier(watch, c, catalogsource.NewRegistryGRPCClient(0, c), &logger)

	logger.Info("Starting cache")
	cacheCli.StartCache(context.TODO())
}
