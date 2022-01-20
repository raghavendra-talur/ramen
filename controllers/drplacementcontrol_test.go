package controllers

import (
	"testing"

	ramen "github.com/ramendr/ramen/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestdRPolicySupportsMetro(t *testing.T) {
	var s3ProfileNameConnectSucc = "fakeS3Profile"
	clusters := [...]ramen.ManagedCluster{
		{Name: `cluster0`, S3ProfileName: s3ProfileNameConnectSucc, Region: "east"},
		{Name: `cluster1`, S3ProfileName: s3ProfileNameConnectSucc, Region: "west"},
		{Name: `cluster2`, S3ProfileName: s3ProfileNameConnectSucc, Region: "east"},
	}
	objectMetas := [...]metav1.ObjectMeta{
		{Name: `drpolicy0`},
		{Name: `drpolicy1`},
	}
	drpolicies := [...]ramen.DRPolicy{
		{
			ObjectMeta: objectMetas[0],
			Spec:       ramen.DRPolicySpec{DRClusterSet: clusters[0:2], SchedulingInterval: `00m`},
		},
		{
			ObjectMeta: objectMetas[1],
			Spec:       ramen.DRPolicySpec{DRClusterSet: clusters[1:3], SchedulingInterval: `9999999d`},
		},
	}

	for _, drpolicy := range drpolicies {
		dRPolicySupportsMetro(&drpolicy)
	}
}
