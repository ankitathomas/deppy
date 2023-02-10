package catalogsource

import (
	"context"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/operator-framework/api/pkg/operators/v1alpha1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/operator-framework/deppy/pkg/deppy"
	"github.com/operator-framework/deppy/pkg/deppy/input"
)

// const catalogSourceSelectorLabel = "olm.catalogSource"
const defaultCatalogSourceSyncInterval = 5 * time.Minute

type CachedRegistryEntitySource struct {
	sync.RWMutex
	watch        watch.Interface
	client       client.Client
	rClient      RegistryClient
	logger       *logr.Logger
	cache        map[string]sourceCache
	done         chan struct{}
	queue        workqueue.RateLimitingInterface // for unmanaged catalogsources
	syncInterval time.Duration
}

type sourceCache struct {
	Items []*input.Entity
	//imageID string
}

type Option func(*CachedRegistryEntitySource)

func WithSyncInterval(d time.Duration) Option {
	return func(c *CachedRegistryEntitySource) {
		c.syncInterval = d
	}
}

func NewCachedRegistryQuerier(watch watch.Interface, client client.Client, rClient RegistryClient, logger *logr.Logger, options ...Option) *CachedRegistryEntitySource {
	if logger == nil {
		l := zap.New()
		logger = &l
	}
	c := &CachedRegistryEntitySource{
		watch:        watch,
		client:       client,
		rClient:      rClient,
		logger:       logger,
		done:         make(chan struct{}),
		cache:        map[string]sourceCache{},
		queue:        workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter()),
		syncInterval: defaultCatalogSourceSyncInterval,
	}
	for _, o := range options {
		o(c)
	}
	return c
}

func (r *CachedRegistryEntitySource) StartCache(ctx context.Context) {
	// TODO: constraints for limiting watched catalogSources
	// TODO: respect CatalogSource priorities
	if err := r.populate(ctx); err != nil {
		r.logger.Error(err, "error populating initial entity cache")
	} else {
		r.logger.Info("Populated initial cache")
	}
	// watching catalogSource for changes works only with OLM managed catalogSources.
	go r.ProcessQueue(ctx)
	for {
		select {
		case <-r.done:
			return
		case entry := <-r.watch.ResultChan():
			encodedObj, err := json.Marshal(entry.Object)
			if err != nil {
				r.logger.Error(err, "cannot reconcile non-catalogSource: marshalling failed", "event", entry.Type, "object", entry.Object)
				continue
			}
			catalogSource := v1alpha1.CatalogSource{}
			err = json.Unmarshal(encodedObj, &catalogSource)
			if err != nil {
				r.logger.Error(err, "cannot reconcile non-catalogSource: unmarshalling failed", "event", entry.Type, "object", entry.Object)
				continue
			}

			switch entry.Type {
			case watch.Deleted:
				func() {
					r.RWMutex.Lock()
					defer r.RWMutex.Unlock()
					catalogSourceKey := types.NamespacedName{Namespace: catalogSource.Namespace, Name: catalogSource.Name}
					delete(r.cache, catalogSourceKey.String())
					r.logger.Info("Completed cache delete", "catalogSource", catalogSourceKey)
				}()
			case watch.Added, watch.Modified:
				r.syncCatalogSource(ctx, catalogSource)
			}
		}
	}
}

func (r *CachedRegistryEntitySource) StopCache() {
	r.RWMutex.Lock()
	defer r.RWMutex.Unlock()
	r.queue.ShutDown()
	close(r.done)
}

func (r *CachedRegistryEntitySource) ProcessQueue(ctx context.Context) {
	for {
		item, _ := r.queue.Get() // block till there is a new item
		defer r.queue.Done(item)
		if _, ok := item.(types.NamespacedName); ok {
			var catalogSource v1alpha1.CatalogSource
			if err := r.client.Get(ctx, item.(types.NamespacedName), &catalogSource); err != nil {
				r.logger.Info("cannot find catalogSource, skipping cache update", "CatalogSource", item)
				return
			}
			r.syncCatalogSource(ctx, catalogSource)
		}
	}
}

// handle added or updated catalogSource
func (r *CachedRegistryEntitySource) syncCatalogSource(ctx context.Context, catalogSource v1alpha1.CatalogSource) {
	catalogSourceKey := types.NamespacedName{Namespace: catalogSource.Namespace, Name: catalogSource.Name}
	r.RWMutex.Lock()
	defer r.RWMutex.Unlock()
	//imageID, err := r.imageIDFromCatalogSource(ctx, &catalogSource)
	//if err != nil {
	//	r.logger.V(1).Info(fmt.Sprintf("failed to get latest imageID for catalogSource %s/%s, skipping cache update: %v", catalogSource.Name, catalogSource.Namespace, err))
	//	return
	//}
	//if len(imageID) == 0 {
	//	// there is no source/address backing this catalog, we can't list anything from it
	//	return
	//}
	//if oldEntry, ok := r.cache[catalogSourceKey.String()]; ok && imageID == oldEntry.imageID {
	//	if isManagedCatalogSource(catalogSource) {
	//		// image hasn't changed since last sync
	//		return
	//	}
	//}
	entities, err := r.rClient.ListEntities(ctx, &catalogSource)
	if err != nil {
		r.logger.Error(err, "failed to list entities for catalogSource entity cache update", "catalogSource", catalogSourceKey)
		if !isManagedCatalogSource(catalogSource) {
			r.queue.AddRateLimited(catalogSourceKey)
		}
		return
	}
	r.cache[catalogSourceKey.String()] = sourceCache{
		Items: entities,
		//imageID: imageID,
	}
	if !isManagedCatalogSource(catalogSource) {
		r.queue.Forget(catalogSourceKey)
		r.queue.AddAfter(catalogSourceKey, r.syncInterval)
	}
	r.logger.Info("Completed cache update", "catalogSource", catalogSourceKey)
}

func (r *CachedRegistryEntitySource) Get(ctx context.Context, id deppy.Identifier) *input.Entity {
	r.RWMutex.RLock()
	defer r.RWMutex.RUnlock()
	for _, entries := range r.cache {
		for _, entity := range entries.Items {
			if entity.Identifier() == id {
				return entity
			}
		}
	}
	return nil
}

func (r *CachedRegistryEntitySource) Filter(ctx context.Context, filter input.Predicate) (input.EntityList, error) {
	r.RWMutex.RLock()
	defer r.RWMutex.RUnlock()
	resultSet := input.EntityList{}
	for _, entries := range r.cache {
		for _, entity := range entries.Items {
			if filter(entity) {
				resultSet = append(resultSet, *entity)
			}
		}
	}
	return resultSet, nil
}

func (r *CachedRegistryEntitySource) GroupBy(ctx context.Context, fn input.GroupByFunction) (input.EntityListMap, error) {
	r.RWMutex.RLock()
	defer r.RWMutex.RUnlock()
	resultSet := input.EntityListMap{}
	for _, entries := range r.cache {
		for _, entity := range entries.Items {
			keys := fn(entity)
			for _, key := range keys {
				resultSet[key] = append(resultSet[key], *entity)
			}
		}
	}
	return resultSet, nil
}

func (r *CachedRegistryEntitySource) Iterate(ctx context.Context, fn input.IteratorFunction) error {
	r.RWMutex.RLock()
	defer r.RWMutex.RUnlock()
	for _, entries := range r.cache {
		for _, entity := range entries.Items {
			if err := fn(entity); err != nil {
				return err
			}
		}
	}
	return nil
}

// populate initializes the state of an empty cache from the catalogSources currently present on the cluster
func (r *CachedRegistryEntitySource) populate(ctx context.Context) error {
	r.RWMutex.Lock()
	defer r.RWMutex.Unlock()
	catalogSourceList := v1alpha1.CatalogSourceList{}
	if err := r.client.List(ctx, &catalogSourceList); err != nil {
		return err
	}
	var errs []error
	for _, catalogSource := range catalogSourceList.Items {
		//imageID, err := r.imageIDFromCatalogSource(ctx, &catalogSource)
		//if err != nil {
		//	errs = append(errs, err)
		//	continue
		//}
		//if len(imageID) == 0 {
		//	continue
		//}
		entities, err := r.rClient.ListEntities(ctx, &catalogSource)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		catalogSourceKey := types.NamespacedName{Namespace: catalogSource.Namespace, Name: catalogSource.Name}
		r.cache[catalogSourceKey.String()] = sourceCache{
			Items: entities,
			//imageID: imageID,
		}
	}
	if len(errs) > 0 {
		return errors.NewAggregate(errs)
	}
	return nil
}

//// imageIDFromCatalogSource returns the current image ref being run by a catalogSource
//func (r *CachedRegistryEntitySource) imageIDFromCatalogSource(ctx context.Context, catalogSource *v1alpha1.CatalogSource) (string, error) {
//	if catalogSource == nil {
//		return "", nil
//	}
//	if !isManagedCatalogSource(*catalogSource) {
//		// ignore image resolution for hardcoded catalogSources
//		return catalogSource.Spec.Address, nil
//	}
//
//	if catalogSource.Status.RegistryServiceStatus == nil {
//		return "", fmt.Errorf("catalogSource %s/%s not ready", catalogSource.Namespace, catalogSource.Name)
//	}
//
//	svc := corev1.Service{}
//	svcName := types.NamespacedName{
//		Namespace: catalogSource.Status.RegistryServiceStatus.ServiceNamespace,
//		Name:      catalogSource.Status.RegistryServiceStatus.ServiceName,
//	}
//	if err := r.client.Get(ctx, svcName,
//		&svc); err != nil {
//		// Cannot detect the backing service, don't refresh cache
//		return "", fmt.Errorf("failed to get service %s for catalogSource %s/%s: %v", svcName, catalogSource.Namespace, catalogSource.Name, err)
//	}
//	if len(svc.Spec.Selector[catalogSourceSelectorLabel]) == 0 {
//		// Cannot verify if image is recent
//		return "", fmt.Errorf("failed to get pod for service %s for catalogSource %s/%s: missing selector %s", svcName, catalogSource.Namespace, catalogSource.Name, catalogSourceSelectorLabel)
//	}
//	pods := corev1.PodList{}
//	if err := r.client.List(ctx, &pods,
//		client.MatchingLabelsSelector{
//			Selector: labels.SelectorFromValidatedSet(
//				map[string]string{catalogSourceSelectorLabel: svc.Spec.Selector[catalogSourceSelectorLabel]},
//			)}); err != nil {
//		return "", fmt.Errorf("failed to get pod for catalogSource %s/%s: %v", catalogSource.Namespace, catalogSource.Name, err)
//	}
//	if len(pods.Items) < 1 {
//		return "", fmt.Errorf("failed to get pod for catalogSource %s/%s: no pods matching selector %s", catalogSource.Namespace, catalogSource.Name, catalogSourceSelectorLabel)
//	}
//	if len(pods.Items[0].Status.ContainerStatuses) < 1 {
//		// pod not ready
//		return "", fmt.Errorf("failed to read imageID from catalogSource pod %s/%s: no containers ready on pod", pods.Items[0].Namespace, pods.Items[0].Name)
//	}
//	if len(pods.Items[0].Status.ContainerStatuses[0].ImageID) == 0 {
//		// pod not ready
//		return "", fmt.Errorf("failed to read imageID from catalogSource pod %s/%s: container state: %s", pods.Items[0].Namespace, pods.Items[0].Name, pods.Items[0].Status.ContainerStatuses[0].State)
//	}
//	return pods.Items[0].Status.ContainerStatuses[0].ImageID, nil
//}

// TODO: find better way to identify catalogSources unmanaged by olm
func isManagedCatalogSource(catalogSource v1alpha1.CatalogSource) bool {
	return len(catalogSource.Spec.Address) == 0
}
