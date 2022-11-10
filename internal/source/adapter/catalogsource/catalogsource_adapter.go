package catalogsource

import (
	"github.com/operator-framework/api/pkg/operators/v1alpha1"
	"github.com/operator-framework/deppy/internal/source/adapter/api"
	"github.com/operator-framework/operator-lifecycle-manager/pkg/api/client/clientset/versioned"
	"github.com/operator-framework/operator-lifecycle-manager/pkg/api/client/informers/externalversions"
	listers "github.com/operator-framework/operator-lifecycle-manager/pkg/api/client/listers/operators/v1alpha1"
	controllerruntime "sigs.k8s.io/controller-runtime"

	"fmt"
	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"time"
)

const defaultCatsrcTimeout = 5 * time.Minute

type CatalogSourceDeppyAdapter struct {
	api.UnimplementedDeppySourceAdapterServer
	catsrc        *v1alpha1.CatalogSource
	catsrcLister  listers.CatalogSourceLister
	catsrcTimeout time.Duration
	logger        *logrus.Entry
}

func NewCatalogSourceDeppyAdapter(opts ...CatalogSourceDeppyAdapterOptions) (*CatalogSourceDeppyAdapter, error) {
	c := &CatalogSourceDeppyAdapter{
		UnimplementedDeppySourceAdapterServer: api.UnimplementedDeppySourceAdapterServer{},
		catsrcTimeout:                         defaultCatsrcTimeout,
		logger:                                logrus.NewEntry(logrus.New()),
	}
	for _, o := range opts {
		o(c)
	}
	if c.logger == nil {
		c.logger = logrus.NewEntry(logrus.New())
	}
	if c.catsrc == nil {
		return nil, fmt.Errorf("CatalogSourceDeppyAdapter requires non-nil catsrc")
	}
	if c.catsrc.Name != "" && c.catsrc.Namespace != "" {
		if c.catsrcLister == nil {
			return nil, fmt.Errorf("CatalogSource %s/%s requires WithLister()", c.catsrc.Namespace, c.catsrc.Name)
		}
		cat, err := c.catsrcLister.CatalogSources(c.catsrc.Namespace).Get(c.catsrc.Name)
		if err != nil {
			c.logger.Errorf("could not list catsrc: %v", err)
		} else {
			c.logger.Infof("catsrc lister success: %+v", cat)
		}
		if cc, err2 := controllerruntime.GetConfig(); err2 == nil {
			crClient, err := versioned.NewForConfig(cc)
			if err != nil {
				c.logger.Infof("Error creating versioned config: %v", err)
			} else {
				catsrcLister := externalversions.NewSharedInformerFactoryWithOptions(crClient, 5*time.Minute).Operators().V1alpha1().CatalogSources().Lister()
				cat, err := catsrcLister.CatalogSources(c.catsrc.Namespace).Get(c.catsrc.Name)
				c.logger.Infof("testing v2 config: %v, %v", err, cat)
			}
		} else  {
			c.logger.Infof("Error creating controllerruntime config: %v", err2)
		}
	}
	return c, nil
}

type CatalogSourceDeppyAdapterOptions func(*CatalogSourceDeppyAdapter)

func WithLogger(l *logrus.Entry) CatalogSourceDeppyAdapterOptions {
	return func(c *CatalogSourceDeppyAdapter) {
		c.logger = l
	}
}

func WithLister(l listers.CatalogSourceLister) CatalogSourceDeppyAdapterOptions {
	return func(c *CatalogSourceDeppyAdapter) {
		c.catsrcLister = l
	}
}

func WithTimeout(d time.Duration) CatalogSourceDeppyAdapterOptions {
	return func(c *CatalogSourceDeppyAdapter) {
		c.catsrcTimeout = d
	}
}

func WithSourceAddress(name, a string) CatalogSourceDeppyAdapterOptions {
	return func(c *CatalogSourceDeppyAdapter) {
		c.catsrc = &v1alpha1.CatalogSource{
			ObjectMeta: metav1.ObjectMeta{
				Name: name,
			},
			Spec: v1alpha1.CatalogSourceSpec{
				Address: a,
			},
		}
	}
}

func WithNamespacedSource(name, namespace string) CatalogSourceDeppyAdapterOptions {
	return func(c *CatalogSourceDeppyAdapter) {
		c.catsrc = &v1alpha1.CatalogSource{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
		}
	}
}

func (s *CatalogSourceDeppyAdapter) ListEntities(req *api.ListEntitiesRequest, stream api.DeppySourceAdapter_ListEntitiesServer) error {
	s.logger.Infof("ListEntities: %v, catsrc %s/%s (%s)", time.Now(), s.catsrc.Namespace, s.catsrc.Name, s.catsrc.Spec.Address)
	// TODO: better way to identify local vs non-local catsrc
	// TODO: watch catsrc for changes
	if s.catsrc.Namespace != "" && s.catsrc.Name != "" {
		catsrc, err := s.catsrcLister.CatalogSources(s.catsrc.Namespace).Get(s.catsrc.Name)
		if err != nil {
			s.logger.Errorf("ListEntities: Failed to list: %v", err)
			return err
		}
		s.catsrc = catsrc
	}

	entities, err := s.listEntities(stream.Context())
	if err != nil {
		return err
	}
	for _, e := range entities {
		if err := stream.Send(&api.Entity{Id: &api.EntityID{
			Source:  e.Id.Source,
			Package: e.Id.Package,
			Version: e.Id.Version,
			Name:    e.Id.CSVName,
		}, Properties: e.Properties}); err != nil {
			return err
		}
	}

	return nil
}