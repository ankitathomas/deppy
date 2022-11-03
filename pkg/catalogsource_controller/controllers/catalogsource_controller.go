/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	"fmt"
	"github.com/operator-framework/api/pkg/operators/v1alpha1"
	v1 "k8s.io/api/apps/v1"
	errors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"time"
)

// CatalogSourceReconciler reconciles a CatalogSource object
type CatalogSourceReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	source *CatSrcEntityCache
}

//+kubebuilder:rbac:groups=operators.coreos.com,resources=catalogsources,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=operators.coreos.com,resources=catalogsources/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=operators.coreos.com,resources=catalogsources/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the CatalogSource object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.13.1/pkg/reconcile
func (r *CatalogSourceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.V(1).Info(fmt.Sprintf("CatalogSourceReconciler: reconciling %s/%s", req.Namespace, req.Name))

	obj := v1alpha1.CatalogSource{}
	err := r.Client.Get(ctx, req.NamespacedName, &obj)
	if err != nil {
		if errors.IsNotFound(err) {
			r.source.Delete(obj)
			return ctrl.Result{Requeue: false}, nil
		} else {
			logger.V(1).Error(err, fmt.Sprintf("failed to get catalogsource for reconciliation"))
			return ctrl.Result{RequeueAfter: time.Minute * 1}, nil
		}
	}
	r.source.Update(obj)
	return ctrl.Result{RequeueAfter: time.Minute * 5}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *CatalogSourceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		// Uncomment the following line adding a pointer to an instance of the controlled resource as an argument
		// For().
		For(&v1alpha1.CatalogSource{}).
		Owns(&v1.Deployment{}).
		Complete(r)
}

func NewCatalogSourceReconciler(mgr ctrl.Manager) *CatalogSourceReconciler {
	return &CatalogSourceReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		source: NewEntityCache(mgr.GetClient(), mgr.GetLogger()),
	}
}
