// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package world

import (
	volrep "github.com/csi-addons/kubernetes-csi-addons/api/replication.storage/v1alpha1"
	rmn "github.com/ramendr/ramen/api/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ocmv1 "open-cluster-management.io/api/cluster/v1"
	clrapiv1beta1 "open-cluster-management.io/api/cluster/v1beta1"
	ocmworkv1 "open-cluster-management.io/api/work/v1"
	viewv1beta1 "open-cluster-management.io/multicloud-operators-subscription/pkg/apis/view/v1beta1"
)

// NewScheme returns a scheme with every type the framework touches.
func NewScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(s))
	utilruntime.Must(rmn.AddToScheme(s))
	utilruntime.Must(ocmv1.AddToScheme(s))
	utilruntime.Must(clrapiv1beta1.AddToScheme(s))
	utilruntime.Must(ocmworkv1.AddToScheme(s))
	utilruntime.Must(viewv1beta1.AddToScheme(s))
	utilruntime.Must(volrep.AddToScheme(s))

	return s
}
