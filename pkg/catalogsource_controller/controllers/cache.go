package controllers

import (
	"context"
	"fmt"
	"github.com/go-logr/logr"
	"github.com/operator-framework/api/pkg/operators/v1alpha1"
	"github.com/operator-framework/deppy/pkg/entitysource"
	"google.golang.org/grpc/connectivity"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type CatSrcEntityCache struct {
	sourceMap   map[string]sourceID
	entityCache cache.Indexer
	logr.Logger
	client.Client
}

type entity struct {
	entitysource.Entity
	source types.NamespacedName
}

type sourceID struct {
	imageID string
	podHash string
}

func NewEntityCache(client client.Client, log logr.Logger) *CatSrcEntityCache {
	store := cache.NewIndexer(func(obj interface{}) (string, error) {
		e, ok := obj.(entity)
		if !ok {
			return "", fmt.Errorf("invalid object type: expected Entity, got: %v")
		}
		return string(e.ID()), nil
	}, cache.Indexers{
		"olm.CatalogSource": func(obj interface{}) ([]string, error) {
			e, ok := obj.(entity)
			if !ok {
				return nil, fmt.Errorf("invalid object type: expected Entity, got: %v")
			}
			return []string{e.source.String()}, nil
		},
	})

	return &CatSrcEntityCache{
		entityCache: store,
		Client:      client,
		Logger:      log,
	}
}

func (c *CatSrcEntityCache) Update(obj interface{}) {
	c.Logger.Info(fmt.Sprintf("CatsrcEntityHandler: OnUpdate: %v", obj))
	defer c.Logger.Info(fmt.Sprintf("CatsrcEntityHandler: OnUpdate complete, Current cache: %v", c.entityCache.List()))
	// refresh cache
	catsrc, ok := obj.(v1alpha1.CatalogSource)
	if !ok {
		c.Logger.Info("enqueued object is not a catalogSource, ignoring")
		return
	}

	c.updateCatsrc(&catsrc)
}

func (c *CatSrcEntityCache) Delete(obj interface{}) {
	c.Logger.Info(fmt.Sprintf("CatsrcEntityHandler: OnDelete: %v", obj))
	defer c.Logger.Info(fmt.Sprintf("CatsrcEntityHandler: OnDelete complete, Current cache: %v", c.entityCache.List()))
	// purge cache
	catsrc, ok := obj.(v1alpha1.CatalogSource)
	if !ok {
		c.Logger.Info("enqueued object is not a catalogSource, ignoring")
		return
	}

	c.deleteCatsrc(&catsrc)

	delete(c.sourceMap, types.NamespacedName{Namespace: catsrc.Namespace, Name: catsrc.Name}.String())

	//	c.entityCache.Resync()
}

func isReady(catsrc *v1alpha1.CatalogSource) bool {
	return catsrc.Status.GRPCConnectionState.LastObservedState == connectivity.Ready.String()
}

func (c *CatSrcEntityCache) hasUpdates(catsrc *v1alpha1.CatalogSource) bool {
	if _, ok := c.sourceMap[types.NamespacedName{Namespace: catsrc.Namespace, Name: catsrc.Name}.String()]; !ok {
		return true
	}

	pods := &v1.PodList{}
	err := c.Client.List(context.TODO(), pods, &client.ListOptions{
		Namespace:     catsrc.Namespace,
		LabelSelector: labels.SelectorFromValidatedSet(labels.Set{"olm.catalogSource": catsrc.Name}),
	})
	if err != nil || pods == nil || len(pods.Items) < 1 {
		// Unable to identify if source was updated, assume yes
		c.Logger.Info("unable to verify catalogsource Pod state, assuming updates available")
		return true
	}

	oldID := c.sourceMap[types.NamespacedName{Namespace: catsrc.Namespace, Name: catsrc.Name}.String()]
	newID := pods.Items[0]
	if oldID.imageID != newID.Status.ContainerStatuses[0].ImageID {
		return true
	}
	if oldID.podHash != newID.Labels["olm.pod-spec-hash"] {
		return true
	}
	return false
}

func (c *CatSrcEntityCache) updateCatsrc(catsrc *v1alpha1.CatalogSource) {
	if !isReady(catsrc) {
		c.Logger.Info("catalogSource not ready")
		return
	}

	if !c.hasUpdates(catsrc) {
		c.Logger.Info("catalogSource has no updates")
		return
	}

	pods := &v1.PodList{}
	err := c.Client.List(context.TODO(), pods, &client.ListOptions{
		Namespace:     catsrc.Namespace,
		LabelSelector: labels.SelectorFromValidatedSet(labels.Set{"olm.catalogSource": catsrc.Name}),
	})
	if err != nil || pods == nil || len(pods.Items) < 1 {
		c.Logger.Error(err, "unable to find catalogSource pod")
		// cannot track the service state
		return
	}

	entities, err := listEntities(context.TODO(), catsrc)
	if err != nil {
		c.Logger.Error(err, "failed to listEntities")
		return
	}

	// remove existing entries
	c.deleteCatsrc(catsrc)

	for _, e := range entities {
		err := c.entityCache.Add(entity{
			Entity: *e,
			source: types.NamespacedName{Namespace: catsrc.Namespace, Name: catsrc.Name},
		})
		if err != nil {
			c.Logger.Error(err, fmt.Sprintf("failed to cache entities for catalogSource %s/%s", catsrc.Namespace, catsrc.Name))
		}
	}

	c.sourceMap[types.NamespacedName{Namespace: catsrc.Namespace, Name: catsrc.Name}.String()] = sourceID{
		imageID: pods.Items[0].Status.ContainerStatuses[0].ImageID,
		podHash: pods.Items[0].Labels["olm.pod-spec-hash"],
	}
}

func (c *CatSrcEntityCache) deleteCatsrc(catsrc *v1alpha1.CatalogSource) {
	toDelete, _ := c.entityCache.ByIndex("olm.CatalogSource", types.NamespacedName{Namespace: catsrc.Namespace, Name: catsrc.Name}.String())
	for _, k := range toDelete {
		c.entityCache.Delete(k)
	}
}
