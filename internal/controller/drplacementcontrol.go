// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package controllers

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/go-logr/logr"
	clrapiv1beta1 "github.com/open-cluster-management-io/api/cluster/v1beta1"
	errorswrapper "github.com/pkg/errors"
	"golang.org/x/exp/slices"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rmn "github.com/ramendr/ramen/api/v1alpha1"
	rmnutil "github.com/ramendr/ramen/internal/controller/util"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ramendr/ramen/internal/controller/volsync"

	"github.com/google/uuid"
	ocmworkv1 "github.com/open-cluster-management/api/work/v1"
	viewv1beta1 "github.com/stolostron/multicloud-operators-foundation/pkg/apis/view/v1beta1"
	plrv1 "github.com/stolostron/multicloud-operators-placementrule/pkg/apis/apps/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	ctrlcontroller "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	argocdv1alpha1hack "github.com/ramendr/ramen/internal/controller/argocd"
)

const (
	// Annotations for MW and PlacementRule
	DRPCNameAnnotation      = "drplacementcontrol.ramendr.openshift.io/drpc-name"
	DRPCNamespaceAnnotation = "drplacementcontrol.ramendr.openshift.io/drpc-namespace"

	// Annotation that stores the UID of DRPC that created the resource on the managed cluster using a ManifestWork
	DRPCUIDAnnotation = "drplacementcontrol.ramendr.openshift.io/drpc-uid"

	// Annotation for the last cluster on which the application was running
	LastAppDeploymentCluster = "drplacementcontrol.ramendr.openshift.io/last-app-deployment-cluster"

	// Annotation for application namespace on the managed cluster
	DRPCAppNamespace = "drplacementcontrol.ramendr.openshift.io/app-namespace"
)

var (
	WaitForAppResourceRestoreToComplete error = errorswrapper.New("Waiting for App resources to be restored...")
	WaitForVolSyncDestRepToComplete     error = errorswrapper.New("Waiting for VolSync RD to complete...")
	WaitForSourceCluster                error = errorswrapper.New("Waiting for primary to provide Protected PVCs...")
	WaitForVolSyncManifestWorkCreation  error = errorswrapper.New("Waiting for VolSync ManifestWork to be created...")
	WaitForVolSyncRDInfoAvailibility    error = errorswrapper.New("Waiting for VolSync RDInfo...")
)

type DRType string

const (
	DRTypeSync  = DRType("sync")
	DRTypeAsync = DRType("async")
)

type DRPCInstance struct {
	reconciler           *DRPlacementControlReconciler
	ctx                  context.Context
	log                  logr.Logger
	instance             *rmn.DRPlacementControl
	savedInstanceStatus  rmn.DRPlacementControlStatus
	drPolicy             *rmn.DRPolicy
	drClusters           []rmn.DRCluster
	mcvRequestInProgress bool
	volSyncDisabled      bool
	userPlacement        client.Object
	vrgs                 map[string]*rmn.VolumeReplicationGroup
	vrgNamespace         string
	ramenConfig          *rmn.RamenConfig
	mwu                  rmnutil.MWUtil
	drType               DRType
}

func (d *DRPCInstance) startProcessing() bool {
	d.log.Info("Starting to process placement")

	requeue := true
	done, processingErr := d.processPlacement()

	if d.shouldUpdateStatus() || d.statusUpdateTimeElapsed() {
		if err := d.reconciler.updateDRPCStatus(d.ctx, d.instance, d.userPlacement, d.log); err != nil {
			errMsg := fmt.Sprintf("error from update DRPC status: %v", err)
			if processingErr != nil {
				errMsg += fmt.Sprintf(", error from process placement: %v", processingErr)
			}

			d.log.Info(errMsg)

			return requeue
		}
	}

	if processingErr != nil {
		d.log.Info("Process placement", "error", processingErr.Error())

		return requeue
	}

	requeue = !done
	d.log.Info("Completed processing placement", "requeue", requeue)

	return requeue
}

func (d *DRPCInstance) processPlacement() (bool, error) {
	d.log.Info("Process DRPC Placement", "DRAction", d.instance.Spec.Action)

	switch d.instance.Spec.Action {
	case rmn.ActionFailover:
		return d.RunFailover()
	case rmn.ActionRelocate:
		return d.RunRelocate()
	}

	// Not a failover or a relocation.  Must be an initial deployment.
	return d.RunInitialDeployment()
}

//nolint:funlen
func (d *DRPCInstance) RunInitialDeployment() (bool, error) {
	d.log.Info("Running initial deployment")

	const done = true

	homeCluster, homeClusterNamespace := d.getHomeClusterForInitialDeploy()

	if homeCluster == "" {
		err := fmt.Errorf("PreferredCluster not set. Placement (%v)", d.userPlacement)
		addOrUpdateCondition(&d.instance.Status.Conditions, rmn.ConditionAvailable, d.instance.Generation,
			d.getConditionStatusForTypeAvailable(), string(d.instance.Status.Phase), err.Error())
		// needStatusUpdate is not set. Still better to capture the event to report later
		rmnutil.ReportIfNotPresent(d.reconciler.eventRecorder, d.instance, corev1.EventTypeWarning,
			rmnutil.EventReasonDeployFail, err.Error())

		return !done, err
	}

	d.log.Info(fmt.Sprintf("Using homeCluster %s for initial deployment",
		homeCluster))

	// Check if we already deployed in the homeCluster or elsewhere
	deployed, clusterName := d.isDeployed(homeCluster)
	if deployed && clusterName != homeCluster {
		err := d.ensureVRGManifestWork(clusterName)
		if err != nil {
			return !done, err
		}

		// IF deployed on cluster that is not the preferred HomeCluster, then we are done
		return done, nil
	}

	// Ensure that initial deployment is complete
	if !deployed || !d.isUserPlRuleUpdated(homeCluster) {
		d.setStatusInitiating()

		_, err := d.startDeploying(homeCluster, homeClusterNamespace)
		if err != nil {
			addOrUpdateCondition(&d.instance.Status.Conditions, rmn.ConditionAvailable, d.instance.Generation,
				d.getConditionStatusForTypeAvailable(), string(d.instance.Status.Phase), err.Error())

			return !done, err
		}

		d.setConditionOnInitialDeploymentCompletion()

		return !done, nil
	}

	err := d.ensureVRGManifestWork(clusterName)
	if err != nil {
		return !done, err
	}

	// If we get here, the deployment is successful
	err = d.EnsureVolSyncReplicationSetup(homeCluster)
	if err != nil {
		return !done, err
	}

	// Update our 'well known' preferred placement
	d.updatePreferredDecision()
	d.setDRState(rmn.Deployed)

	d.setConditionOnInitialDeploymentCompletion()

	d.setProgression(rmn.ProgressionCompleted)

	d.setActionDuration()

	return done, nil
}

func (d *DRPCInstance) getHomeClusterForInitialDeploy() (string, string) {
	// Check if the user wants to use the preferredCluster
	homeCluster := ""
	homeClusterNamespace := ""

	if d.instance.Spec.PreferredCluster != "" {
		homeCluster = d.instance.Spec.PreferredCluster
		homeClusterNamespace = d.instance.Spec.PreferredCluster
	}

	// FIXME: The question is, should we care about dynamic home cluster selection. This feature has
	// always been available, but we never used it.  If not used, why have it, and keep carrying it?

	// if homeCluster == "" && d.drpcPlacementRule != nil && len(d.drpcPlacementRule.Status.Decisions) != 0 {
	// 	homeCluster = d.drpcPlacementRule.Status.Decisions[0].ClusterName
	// 	homeClusterNamespace = d.drpcPlacementRule.Status.Decisions[0].ClusterNamespace
	// }

	return homeCluster, homeClusterNamespace
}

// isDeployed check to see if the initial deployment is already complete to this
// homeCluster or elsewhere
func (d *DRPCInstance) isDeployed(homeCluster string) (bool, string) {
	if d.isVRGAlreadyDeployedOnTargetCluster(homeCluster) {
		d.log.Info(fmt.Sprintf("Already deployed on homeCluster %s. Last state: %s",
			homeCluster, d.getLastDRState()))

		return true, homeCluster
	}

	clusterName, found := d.isVRGAlreadyDeployedElsewhere(homeCluster)
	if found {
		errMsg := fmt.Sprintf("Failed to place deployment on cluster %s, as it is active on cluster %s",
			homeCluster, clusterName)
		d.log.Info(errMsg)

		// Update our 'well known' preferred placement
		d.updatePreferredDecision()

		return true, clusterName
	}

	return false, ""
}

func (d *DRPCInstance) isUserPlRuleUpdated(homeCluster string) bool {
	plRule := ConvertToPlacementRule(d.userPlacement)
	if plRule != nil {
		return len(plRule.Status.Decisions) > 0 &&
			plRule.Status.Decisions[0].ClusterName == homeCluster
	}

	// Othewise, it is a Placement object
	plcmt := ConvertToPlacement(d.userPlacement)
	if plcmt != nil {
		clusterDecision := d.reconciler.getClusterDecision(d.userPlacement)

		return clusterDecision.ClusterName == homeCluster
	}

	return false
}

// isVRGAlreadyDeployedOnTargetCluster will check whether a VRG exists in the targetCluster and
// whether it is in protected state, and primary.
func (d *DRPCInstance) isVRGAlreadyDeployedOnTargetCluster(targetCluster string) bool {
	d.log.Info(fmt.Sprintf("isAlreadyDeployedAndProtected? - %q", reflect.ValueOf(d.vrgs).MapKeys()))

	return d.getCachedVRG(targetCluster) != nil
}

func (d *DRPCInstance) getCachedVRG(clusterName string) *rmn.VolumeReplicationGroup {
	vrg, found := d.vrgs[clusterName]
	if !found {
		d.log.Info("VRG not found on cluster", "Name", clusterName)

		return nil
	}

	return vrg
}

func (d *DRPCInstance) isVRGAlreadyDeployedElsewhere(clusterToSkip string) (string, bool) {
	for clusterName, vrg := range d.vrgs {
		if clusterName == clusterToSkip {
			continue
		}

		// We are checking for the initial deployment. Only return the cluster if the VRG on it is primary.
		if isVRGPrimary(vrg) {
			return clusterName, true
		}
	}

	return "", false
}

func (d *DRPCInstance) startDeploying(homeCluster, homeClusterNamespace string) (bool, error) {
	const done = true

	// Make sure we record the state that we are deploying
	d.setDRState(rmn.Deploying)
	d.setProgression(rmn.ProgressionCreatingMW)
	// Create VRG first, to leverage user PlacementRule decision to skip placement and move to cleanup
	err := d.createVRGManifestWork(homeCluster, rmn.Primary)
	if err != nil {
		return false, err
	}

	// TODO: Why are we not waiting for ClusterDataReady here? Are there any corner cases?

	// We have a home cluster
	d.setProgression(rmn.ProgressionUpdatingPlRule)

	err = d.updateUserPlacementRule(homeCluster, homeClusterNamespace)
	if err != nil {
		rmnutil.ReportIfNotPresent(d.reconciler.eventRecorder, d.instance, corev1.EventTypeWarning,
			rmnutil.EventReasonDeployFail, err.Error())

		return !done, err
	}

	// All good, update the preferred decision and state
	d.instance.Status.PreferredDecision.ClusterName = d.instance.Spec.PreferredCluster
	d.instance.Status.PreferredDecision.ClusterNamespace = d.instance.Spec.PreferredCluster

	d.log.Info("Updated PreferredDecision", "PreferredDecision", d.instance.Status.PreferredDecision)

	d.setDRState(rmn.Deployed)

	return done, nil
}

// RunFailover:
// 0. Check if failoverCluster is a valid target as Secondary (or already is a Primary)
// 1. If already failed over or in the process (VRG on failoverCluster is Primary), ensure failover is complete and
// then ensure cleanup
// 2. Else, if failover is initiated (VRG ManifestWork is create as Primary), then try again till VRG manifests itself
// on the failover cluster
// 3. Else, initiate failover to the desired failoverCluster (switchToFailoverCluster)
func (d *DRPCInstance) RunFailover() (bool, error) {
	d.log.Info("Entering RunFailover", "state", d.getLastDRState())

	const done = true

	if d.instance.Spec.FailoverCluster == "" {
		msg := "missing value for spec.FailoverCluster"
		addOrUpdateCondition(&d.instance.Status.Conditions, rmn.ConditionAvailable, d.instance.Generation,
			d.getConditionStatusForTypeAvailable(), string(d.instance.Status.Phase), msg)

		return done, fmt.Errorf(msg)
	}

	failoverCluster := d.instance.Spec.FailoverCluster
	if !d.isValidFailoverTarget(failoverCluster) {
		err := fmt.Errorf("unable to start failover, spec.FailoverCluster (%s) is not a valid Secondary target",
			failoverCluster)
		addOrUpdateCondition(&d.instance.Status.Conditions, rmn.ConditionAvailable, d.instance.Generation,
			d.getConditionStatusForTypeAvailable(), string(d.instance.Status.Phase), err.Error())

		return !done, err
	}

	// IFF VRG exists and it is primary in the failoverCluster, then ensure failover is complete and
	// clean up and setup VolSync if needed.
	if d.vrgExistsAndPrimary(failoverCluster) {
		d.updatePreferredDecision()
		d.setDRState(rmn.FailedOver)
		addOrUpdateCondition(&d.instance.Status.Conditions, rmn.ConditionAvailable, d.instance.Generation,
			metav1.ConditionTrue, string(d.instance.Status.Phase), "Completed")

		// Make sure VolRep 'Data' and VolSync 'setup' conditions are ready
		ready := d.checkReadiness(failoverCluster)
		if !ready {
			d.log.Info("VRGCondition not ready to finish failover")
			d.setProgression(rmn.ProgressionWaitForReadiness)

			return !done, nil
		}

		return d.ensureActionCompleted(failoverCluster)
	} else if yes, err := d.mwExistsAndPlacementUpdated(failoverCluster); yes || err != nil {
		// We have to wait for the VRG to appear on the failoverCluster or
		// in case of an error, try again later
		return !done, err
	}

	d.setStatusInitiating()

	return d.switchToFailoverCluster()
}

// isValidFailoverTarget determines if the passed in cluster is a valid target to failover to. A valid failover target
// may already be Primary, if it is Secondary then it has to be protecting PVCs with VolSync.
// NOTE: Currently there is a gap where, right after DR protection when a Secondary VRG is not yet created for VolSync
// workloads, a failover if initiated will pass these checks. When we fix to retain VRG for VR as well, a more
// deterministic check for VRG as Secondary can be performed.
func (d *DRPCInstance) isValidFailoverTarget(cluster string) bool {
	annotations := make(map[string]string)
	annotations[DRPCNameAnnotation] = d.instance.GetName()
	annotations[DRPCNamespaceAnnotation] = d.instance.GetNamespace()

	vrg, err := d.reconciler.MCVGetter.GetVRGFromManagedCluster(d.instance.Name, d.vrgNamespace, cluster, annotations)
	if err != nil {
		if errors.IsNotFound(err) {
			d.log.Info(fmt.Sprintf("VRG not found on %q", cluster))

			// Valid target as there would be no VRG for VR and Sync cases
			return true
		}

		return false
	}

	if isVRGPrimary(vrg) {
		// VRG is Primary, valid target with possible failover in progress
		return true
	}

	// Valid target only if VRG is protecting PVCs with VS and its status is also Secondary
	if d.drType == DRTypeAsync && vrg.Status.State == rmn.SecondaryState &&
		!vrg.Spec.VolSync.Disabled && len(vrg.Spec.VolSync.RDSpec) != 0 {
		return true
	}

	return false
}

func (d *DRPCInstance) checkClusterFenced(cluster string, drClusters []rmn.DRCluster) (bool, error) {
	for i := range drClusters {
		if drClusters[i].Name != cluster {
			continue
		}

		drClusterFencedCondition := findCondition(drClusters[i].Status.Conditions, rmn.DRClusterConditionTypeFenced)
		if drClusterFencedCondition == nil {
			d.log.Info("drCluster fenced condition not available", "cluster", drClusters[i].Name)

			return false, nil
		}

		if drClusterFencedCondition.Status != metav1.ConditionTrue ||
			drClusterFencedCondition.ObservedGeneration != drClusters[i].Generation {
			d.log.Info("drCluster fenced condition is not true", "cluster", drClusters[i].Name)

			return false, nil
		}

		return true, nil
	}

	return false, fmt.Errorf("failed to get the fencing status for the cluster %s", cluster)
}

func (d *DRPCInstance) switchToFailoverCluster() (bool, error) {
	const done = true
	// Make sure we record the state that we are failing over
	d.setDRState(rmn.FailingOver)
	addOrUpdateCondition(&d.instance.Status.Conditions, rmn.ConditionAvailable, d.instance.Generation,
		d.getConditionStatusForTypeAvailable(), string(d.instance.Status.Phase), "Starting failover")
	addOrUpdateCondition(&d.instance.Status.Conditions, rmn.ConditionPeerReady, d.instance.Generation,
		metav1.ConditionFalse, rmn.ReasonNotStarted,
		fmt.Sprintf("Started failover to cluster %q", d.instance.Spec.FailoverCluster))
	d.setProgression(rmn.ProgressionCheckingFailoverPrequisites)

	curHomeCluster := d.getCurrentHomeClusterName(d.instance.Spec.FailoverCluster, d.drClusters)
	if curHomeCluster == "" {
		msg := "Invalid Failover request. Current home cluster does not exists"
		d.log.Info(msg)
		addOrUpdateCondition(&d.instance.Status.Conditions, rmn.ConditionAvailable, d.instance.Generation,
			d.getConditionStatusForTypeAvailable(), string(d.instance.Status.Phase), msg)

		err := fmt.Errorf("failover requested on invalid state %v", d.instance.Status)
		rmnutil.ReportIfNotPresent(d.reconciler.eventRecorder, d.instance, corev1.EventTypeWarning,
			rmnutil.EventReasonSwitchFailed, err.Error())

		return done, err
	}

	if met, err := d.checkFailoverPrerequisites(curHomeCluster); !met || err != nil {
		return !done, err
	}

	d.setProgression(rmn.ProgressionFailingOverToCluster)

	newHomeCluster := d.instance.Spec.FailoverCluster

	err := d.switchToCluster(newHomeCluster, "")
	if err != nil {
		addOrUpdateCondition(&d.instance.Status.Conditions, rmn.ConditionAvailable, d.instance.Generation,
			d.getConditionStatusForTypeAvailable(), string(d.instance.Status.Phase), err.Error())
		rmnutil.ReportIfNotPresent(d.reconciler.eventRecorder, d.instance, corev1.EventTypeWarning,
			rmnutil.EventReasonSwitchFailed, err.Error())

		return !done, err
	}

	d.updatePreferredDecision()
	d.setDRState(rmn.FailedOver)
	addOrUpdateCondition(&d.instance.Status.Conditions, rmn.ConditionAvailable, d.instance.Generation,
		d.getConditionStatusForTypeAvailable(), string(d.instance.Status.Phase), "Completed")
	d.log.Info("Failover completed", "state", d.getLastDRState())

	// The failover is complete, but we still need to clean up the failed primary.
	// hence, returning a NOT done
	return !done, nil
}

func (d *DRPCInstance) getCurrentHomeClusterName(toCluster string, drClusters []rmn.DRCluster) string {
	clusterDecision := d.reconciler.getClusterDecision(d.userPlacement)
	if clusterDecision.ClusterName != "" {
		return clusterDecision.ClusterName
	}

	if d.instance.Status.PreferredDecision.ClusterName != "" {
		return d.instance.Status.PreferredDecision.ClusterName
	}

	// otherwise, just return the peer cluster
	for i := range drClusters {
		if drClusters[i].Name != toCluster {
			return drClusters[i].Name
		}
	}

	// If all fails, then we have no curHomeCluster
	return ""
}

// checkFailoverPrerequisites checks for any failover prerequsites that need to be met on the
// failoverCluster before initiating a failover.
// Returns:
//   - bool: Indicating if prerequisites are met
//   - error: Any error in determining the prerequisite status
func (d *DRPCInstance) checkFailoverPrerequisites(curHomeCluster string) (bool, error) {
	var (
		met bool
		err error
	)

	if d.drType == DRTypeSync {
		met, err = d.checkMetroFailoverPrerequisites(curHomeCluster)
	} else {
		met = d.checkRegionalFailoverPrerequisites()
	}

	if err == nil && met {
		return true, nil
	}

	msg := "Waiting for spec.failoverCluster to meet failover prerequsites"

	if err != nil {
		msg = err.Error()

		rmnutil.ReportIfNotPresent(d.reconciler.eventRecorder, d.instance, corev1.EventTypeWarning,
			rmnutil.EventReasonSwitchFailed, err.Error())
	}

	addOrUpdateCondition(&d.instance.Status.Conditions, rmn.ConditionAvailable, d.instance.Generation,
		d.getConditionStatusForTypeAvailable(), string(d.instance.Status.Phase), msg)

	return met, err
}

// checkMetroFailoverPrerequisites checks for any MetroDR failover prerequsites that need to be met on the
// failoverCluster before initiating a failover from the curHomeCluster.
// Returns:
//   - bool: Indicating if prerequisites are met
//   - error: Any error in determining the prerequisite status
func (d *DRPCInstance) checkMetroFailoverPrerequisites(curHomeCluster string) (bool, error) {
	met := true

	d.setProgression(rmn.ProgressionWaitForFencing)

	fenced, err := d.checkClusterFenced(curHomeCluster, d.drClusters)
	if err != nil {
		return !met, err
	}

	if !fenced {
		return !met, fmt.Errorf("current home cluster %s is not fenced", curHomeCluster)
	}

	return met, nil
}

// checkRegionalFailoverPrerequisites checks for any RegionalDR failover prerequsites that need to be met on the
// failoverCluster before initiating a failover.
// Returns:
//   - bool: Indicating if prerequisites are met
func (d *DRPCInstance) checkRegionalFailoverPrerequisites() bool {
	d.setProgression(rmn.ProgressionWaitForStorageMaintenanceActivation)

	for _, drCluster := range d.drClusters {
		if drCluster.Name != d.instance.Spec.FailoverCluster {
			continue
		}

		// we want to work with failover cluster only, because the previous primary cluster might be unreachable
		if required, activationsRequired := requiresRegionalFailoverPrerequisites(
			d.ctx,
			d.reconciler.APIReader,
			[]string{drCluster.Spec.S3ProfileName},
			d.instance.GetName(), d.vrgNamespace,
			d.vrgs, d.instance.Spec.FailoverCluster,
			d.reconciler.ObjStoreGetter, d.log); required {
			return checkFailoverMaintenanceActivations(drCluster, activationsRequired, d.log)
		}

		break
	}

	return true
}

// requiresRegionalFailoverPrerequisites checks protected PVCs as reported by the last known Primary cluster
// to determine if this instance requires failover maintenance modes to be active prior to initiating
// a failover
func requiresRegionalFailoverPrerequisites(
	ctx context.Context,
	apiReader client.Reader,
	s3ProfileNames []string,
	drpcName string,
	vrgNamespace string,
	vrgs map[string]*rmn.VolumeReplicationGroup,
	failoverCluster string,
	objectStoreGetter ObjectStoreGetter,
	log logr.Logger,
) (
	bool,
	map[string]rmn.StorageIdentifiers,
) {
	activationsRequired := map[string]rmn.StorageIdentifiers{}

	vrg := getLastKnownPrimaryVRG(vrgs, failoverCluster)
	if vrg == nil {
		vrg = GetLastKnownVRGPrimaryFromS3(ctx, apiReader, s3ProfileNames, drpcName, vrgNamespace, objectStoreGetter, log)
		if vrg == nil {
			// TODO: Is this an error, should we ensure at least one VRG is found in the edge cases?
			// Potentially missing VRG and so stop failover? How to recover in that case?
			log.Info("Failed to find last known primary", "cluster", failoverCluster)

			return false, activationsRequired
		}
	}

	for _, protectedPVC := range vrg.Status.ProtectedPVCs {
		if len(protectedPVC.StorageIdentifiers.ReplicationID.Modes) == 0 {
			continue
		}

		if !hasMode(protectedPVC.StorageIdentifiers.ReplicationID.Modes, rmn.MModeFailover) {
			continue
		}

		// TODO: Assumption is that if there is a mMode then the ReplicationID is a must, err otherwise?
		key := protectedPVC.StorageIdentifiers.StorageProvisioner + protectedPVC.StorageIdentifiers.ReplicationID.ID
		if _, ok := activationsRequired[key]; !ok {
			activationsRequired[key] = protectedPVC.StorageIdentifiers
		}
	}

	return len(activationsRequired) != 0, activationsRequired
}

// getLastKnownPrimaryVRG gets the last known Primary VRG from the cluster that is not the current targetCluster
// This is done inspecting VRGs from the MCV reports, and in case not found, fetching it from the s3 store
func getLastKnownPrimaryVRG(
	vrgs map[string]*rmn.VolumeReplicationGroup,
	targetCluster string,
) *rmn.VolumeReplicationGroup {
	var vrgToInspect *rmn.VolumeReplicationGroup

	for drcluster, vrg := range vrgs {
		if drcluster == targetCluster {
			continue
		}

		if isVRGPrimary(vrg) {
			// TODO: Potentially when there are more than on primary VRGs find the best one?
			vrgToInspect = vrg

			break
		}
	}

	if vrgToInspect == nil {
		return nil
	}

	return vrgToInspect
}

func GetLastKnownVRGPrimaryFromS3(
	ctx context.Context,
	apiReader client.Reader,
	s3ProfileNames []string,
	sourceVrgName string,
	sourceVrgNamespace string,
	objectStoreGetter ObjectStoreGetter,
	log logr.Logger,
) *rmn.VolumeReplicationGroup {
	var latestVrg *rmn.VolumeReplicationGroup

	var latestUpdateTime time.Time

	for _, s3ProfileName := range s3ProfileNames {
		objectStorer, _, err := objectStoreGetter.ObjectStore(
			ctx, apiReader, s3ProfileName, "drpolicy validation", log)
		if err != nil {
			log.Info("Creating object store failed", "error", err)

			continue
		}

		sourcePathNamePrefix := s3PathNamePrefix(sourceVrgNamespace, sourceVrgName)

		vrg := &rmn.VolumeReplicationGroup{}
		if err := vrgObjectDownload(objectStorer, sourcePathNamePrefix, vrg); err != nil {
			log.Info(fmt.Sprintf("Failed to get VRG from s3 store - s3ProfileName %s. Err %v", s3ProfileName, err))

			continue
		}

		if !isVRGPrimary(vrg) {
			log.Info("Found a non-primary vrg on s3 store", "name", vrg.GetName(), "namespace", vrg.GetNamespace())

			continue
		}

		// Compare lastUpdateTime with the latestUpdateTime
		if latestVrg == nil || vrg.Status.LastUpdateTime.After(latestUpdateTime) {
			latestUpdateTime = vrg.Status.LastUpdateTime.Time
			latestVrg = vrg

			log.Info("Found a primary vrg on s3 store", "name",
				latestVrg.GetName(), "namespace", latestVrg.GetNamespace(), "s3Store", s3ProfileName)
		}
	}

	return latestVrg
}

// hasMode is a helper routine that checks if a list of modes has the passed in mode
func hasMode(modes []rmn.MMode, mode rmn.MMode) bool {
	for _, modeInList := range modes {
		if modeInList == mode {
			return true
		}
	}

	return false
}

// checkFailoverMaintenanceActivations checks if all required storage backend maintenance activations are met
func checkFailoverMaintenanceActivations(drCluster rmn.DRCluster,
	activationsRequired map[string]rmn.StorageIdentifiers,
	log logr.Logger,
) bool {
	for _, activationRequired := range activationsRequired {
		if !checkActivationForStorageIdentifier(
			drCluster.Status.MaintenanceModes,
			activationRequired,
			rmn.MModeConditionFailoverActivated,
			log,
		) {
			return false
		}
	}

	return true
}

// checkActivationForStorageIdentifier checks if provided storageIdentifier failover maintenance mode is
// in an activated state as reported in the passed in ClusterMaintenanceMode list
func checkActivationForStorageIdentifier(
	mModeStatus []rmn.ClusterMaintenanceMode,
	storageIdentifier rmn.StorageIdentifiers,
	activation rmn.MModeStatusConditionType,
	log logr.Logger,
) bool {
	for _, statusMMode := range mModeStatus {
		log.Info("Processing ClusterMaintenanceMode for match", "clustermode", statusMMode, "desiredmode", storageIdentifier)

		if statusMMode.StorageProvisioner != storageIdentifier.StorageProvisioner ||
			statusMMode.TargetID != storageIdentifier.ReplicationID.ID {
			continue
		}

		for _, condition := range statusMMode.Conditions {
			if condition.Type != string(activation) {
				continue
			}

			if condition.Status == metav1.ConditionTrue {
				return true
			}

			return false
		}

		return false
	}

	return false
}

// runRelocate checks if pre-conditions for relocation are met, and if so performs the relocation
// Pre-requisites for relocation are checked as follows:
//   - The exists at least one VRG across clusters (there is no state where we do not have a VRG as
//     primary or secondary once initial deployment is complete)
//   - Ensures that there is only one primary, before further state transitions
//   - If there are multiple primaries, wait for one of the primaries to transition
//     to a secondary. This can happen if MCV reports older VRG state as MW is being applied
//     to the cluster.
//   - Check if peers are ready
//   - If there are secondaries in flight, ensure they report secondary as the observed state
//     before moving forward
//   - preferredCluster should not report as Secondary, as it will never transition out of delete state
//     in the future, as there would be no primary. This can happen, if in between relocate the
//     preferred cluster was switched
//   - User needs to recover by changing the preferredCluster back to the initial intent
//   - Check if we already relocated to the preferredCluster, and ensure cleanup actions
//   - Check if current primary (that is not the preferred cluster), is ready to switch over
//   - Relocate!
//
//nolint:gocognit,cyclop,funlen
func (d *DRPCInstance) RunRelocate() (bool, error) {
	d.log.Info("Entering RunRelocate", "state", d.getLastDRState(), "progression", d.getProgression())

	const done = true

	preferredCluster := d.instance.Spec.PreferredCluster
	preferredClusterNamespace := d.instance.Spec.PreferredCluster

	// Before relocating to the preferredCluster, do a quick validation and select the current preferred cluster.
	curHomeCluster, err := d.validateAndSelectCurrentPrimary(preferredCluster)
	if err != nil {
		addOrUpdateCondition(&d.instance.Status.Conditions, rmn.ConditionAvailable, d.instance.Generation,
			d.getConditionStatusForTypeAvailable(), string(d.instance.Status.Phase), err.Error())

		return !done, err
	}

	// If already relocated to preferredCluster; ensure required setup is complete
	if curHomeCluster != "" && d.vrgExistsAndPrimary(preferredCluster) {
		d.setDRState(rmn.Relocating)
		d.updatePreferredDecision()

		ready := d.checkReadiness(preferredCluster)
		if !ready {
			d.log.Info("VRGCondition not ready to finish relocation")
			d.setProgression(rmn.ProgressionWaitForReadiness)

			return !done, nil
		}

		d.setDRState(rmn.Relocated)
		addOrUpdateCondition(&d.instance.Status.Conditions, rmn.ConditionAvailable, d.instance.Generation,
			metav1.ConditionTrue, string(d.instance.Status.Phase), "Completed")

		return d.ensureActionCompleted(preferredCluster)
	}

	d.setStatusInitiating()

	// Check if current primary (that is not the preferred cluster), is ready to switch over
	if curHomeCluster != "" && curHomeCluster != preferredCluster &&
		!d.readyToSwitchOver(curHomeCluster, preferredCluster) {
		errMsg := fmt.Sprintf("current cluster (%s) has not completed protection actions", curHomeCluster)
		addOrUpdateCondition(&d.instance.Status.Conditions, rmn.ConditionAvailable, d.instance.Generation,
			d.getConditionStatusForTypeAvailable(), string(d.instance.Status.Phase), errMsg)

		return !done, fmt.Errorf(errMsg)
	}

	if d.getLastDRState() != rmn.Relocating && !d.validatePeerReady() {
		return !done, fmt.Errorf("clean up secondaries is pending, peer is not ready")
	}

	if curHomeCluster != "" && curHomeCluster != preferredCluster {
		result, err := d.quiesceAndRunFinalSync(curHomeCluster)
		if err != nil {
			return !done, err
		}

		if !result {
			return !done, nil
		}
	}

	return d.relocate(preferredCluster, preferredClusterNamespace, rmn.Relocating)
}

func (d *DRPCInstance) ensureActionCompleted(srcCluster string) (bool, error) {
	const done = true

	err := d.ensureVRGManifestWork(srcCluster)
	if err != nil {
		return !done, err
	}

	err = d.ensurePlacement(srcCluster)
	if err != nil {
		return !done, err
	}

	d.setProgression(rmn.ProgressionCleaningUp)

	// Cleanup and setup VolSync if enabled
	err = d.ensureCleanupAndVolSyncReplicationSetup(srcCluster)
	if err != nil {
		return !done, err
	}

	d.setProgression(rmn.ProgressionCompleted)

	d.setActionDuration()

	return done, nil
}

func (d *DRPCInstance) ensureCleanupAndVolSyncReplicationSetup(srcCluster string) error {
	// If we have VolSync replication, this is the perfect time to reset the RDSpec
	// on the primary. This will cause the RD to be cleared on the primary
	err := d.ResetVolSyncRDOnPrimary(srcCluster)
	if err != nil {
		return err
	}

	// Check if the reset has already been applied. ResetVolSyncRDOnPrimary resets the VRG
	// in the MW, but the VRGs in the vrgs slice are fetched using MCV.
	vrg, ok := d.vrgs[srcCluster]
	if !ok || len(vrg.Spec.VolSync.RDSpec) != 0 {
		return fmt.Errorf(fmt.Sprintf("Waiting for RDSpec count on cluster %s to go to zero. VRG OK? %v",
			srcCluster, ok))
	}

	err = d.EnsureCleanup(srcCluster)
	if err != nil {
		return err
	}

	// After we ensured peers are clean, The VolSync ReplicationSource (RS) will automatically get
	// created, but for the ReplicationDestination, we need to explicitly tell the VRG to create it.
	err = d.EnsureVolSyncReplicationSetup(srcCluster)
	if err != nil {
		return err
	}

	return nil
}

func (d *DRPCInstance) quiesceAndRunFinalSync(homeCluster string) (bool, error) {
	const done = true

	result, err := d.prepareForFinalSync(homeCluster)
	if err != nil {
		return !done, err
	}

	if !result {
		d.setProgression(rmn.ProgressionPreparingFinalSync)

		return !done, nil
	}

	clusterDecision := d.reconciler.getClusterDecision(d.userPlacement)
	if clusterDecision.ClusterName != "" {
		d.setDRState(rmn.Relocating)
		addOrUpdateCondition(&d.instance.Status.Conditions, rmn.ConditionAvailable, d.instance.Generation,
			d.getConditionStatusForTypeAvailable(), string(d.instance.Status.Phase), "Starting quiescing for relocation")

		// clear current user PlacementRule's decision
		d.setProgression(rmn.ProgressionClearingPlacement)

		err := d.clearUserPlacementRuleStatus()
		if err != nil {
			return !done, err
		}
	}

	// Ensure final sync has been taken
	result, err = d.runFinalSync(homeCluster)
	if err != nil {
		return !done, err
	}

	if !result {
		d.setProgression(rmn.ProgressionRunningFinalSync)

		return !done, nil
	}

	d.setProgression(rmn.ProgressionFinalSyncComplete)

	return done, nil
}

func (d *DRPCInstance) prepareForFinalSync(homeCluster string) (bool, error) {
	d.log.Info(fmt.Sprintf("Preparing final sync on cluster %s", homeCluster))

	const done = true

	vrg, ok := d.vrgs[homeCluster]

	if !ok {
		d.log.Info(fmt.Sprintf("prepareForFinalSync: VRG not available on cluster %s", homeCluster))

		return !done, fmt.Errorf("VRG not found on Cluster %s", homeCluster)
	}

	if !vrg.Status.PrepareForFinalSyncComplete {
		err := d.updateVRGToPrepareForFinalSync(homeCluster)
		if err != nil {
			return !done, err
		}

		// updated VRG to run final sync. Give it time...
		d.log.Info(fmt.Sprintf("Giving enough time to prepare for final sync on cluster %s", homeCluster))

		return !done, nil
	}

	d.log.Info("Preparing for final sync completed", "cluster", homeCluster)

	return done, nil
}

func (d *DRPCInstance) runFinalSync(homeCluster string) (bool, error) {
	d.log.Info(fmt.Sprintf("Running final sync on cluster %s", homeCluster))

	const done = true

	vrg, ok := d.vrgs[homeCluster]

	if !ok {
		d.log.Info(fmt.Sprintf("runFinalSync: VRG not available on cluster %s", homeCluster))

		return !done, fmt.Errorf("VRG not found on Cluster %s", homeCluster)
	}

	if !vrg.Status.FinalSyncComplete {
		err := d.updateVRGToRunFinalSync(homeCluster)
		if err != nil {
			return !done, err
		}

		// updated VRG to run final sync. Give it time...
		d.log.Info(fmt.Sprintf("Giving it enough time to run final sync on cluster %s", homeCluster))

		return !done, nil
	}

	d.log.Info("Running final sync completed", "cluster", homeCluster)

	return done, nil
}

func (d *DRPCInstance) areMultipleVRGsPrimary() bool {
	numOfPrimaries := 0

	for _, vrg := range d.vrgs {
		if isVRGPrimary(vrg) {
			numOfPrimaries++
		}
	}

	return numOfPrimaries > 1
}

func (d *DRPCInstance) validatePeerReady() bool {
	condition := findCondition(d.instance.Status.Conditions, rmn.ConditionPeerReady)
	if condition == nil || condition.Status == metav1.ConditionTrue {
		return true
	}

	d.log.Info("validatePeerReady", "Condition", condition)

	return false
}

func (d *DRPCInstance) selectCurrentPrimaryAndSecondaries() (string, []string) {
	var secondaryVRGs []string

	primaryVRG := ""

	for cn, vrg := range d.vrgs {
		if isVRGPrimary(vrg) && primaryVRG == "" {
			primaryVRG = cn
		}

		if isVRGSecondary(vrg) {
			secondaryVRGs = append(secondaryVRGs, cn)
		}
	}

	return primaryVRG, secondaryVRGs
}

func (d *DRPCInstance) validateAndSelectCurrentPrimary(preferredCluster string) (string, error) {
	// Relocation requires preferredCluster to be configured
	if preferredCluster == "" {
		return "", fmt.Errorf("preferred cluster not valid")
	}

	// No VRGs found, invalid state, possibly deployment was not started
	if len(d.vrgs) == 0 {
		return "", fmt.Errorf("no VRGs exists. Can't relocate")
	}

	// Check for at most a single cluster in primary state
	if d.areMultipleVRGsPrimary() {
		return "", fmt.Errorf("multiple primaries in transition detected")
	}
	// Pre-relocate cleanup
	homeCluster, _ := d.selectCurrentPrimaryAndSecondaries()

	return homeCluster, nil
}

// readyToSwitchOver checks App resources are ready and the cluster data has been protected.
// ClusterDataProtected condition indicates if the related cluster data for an App (Managed
// by this DRPC instance) has been protected (uploaded to the S3 store(s)) or not.
func (d *DRPCInstance) readyToSwitchOver(homeCluster string, preferredCluster string) bool {
	d.log.Info(fmt.Sprintf("Checking if VRG Data is available on cluster %s", homeCluster))

	if d.drType == DRTypeSync {
		// check fencing status in the preferredCluster
		fenced, err := d.checkClusterFenced(preferredCluster, d.drClusters)
		if err != nil {
			d.log.Info(fmt.Sprintf("Checking if Cluster %s is Fenced failed %v",
				preferredCluster, err.Error()))

			return false
		}

		if fenced {
			d.log.Info(fmt.Sprintf("Cluster %s is Fenced", preferredCluster))

			return false
		}
	}
	// Allow switch over when PV data is ready and the cluster data is protected
	return d.isVRGConditionMet(homeCluster, VRGConditionTypeDataReady) &&
		d.isVRGConditionMet(homeCluster, VRGConditionTypeClusterDataProtected)
}

func (d *DRPCInstance) checkReadiness(homeCluster string) bool {
	vrg := d.vrgs[homeCluster]
	if vrg == nil {
		return false
	}

	return d.isVRGConditionMet(homeCluster, VRGConditionTypeDataReady) &&
		d.isVRGConditionMet(homeCluster, VRGConditionTypeClusterDataReady) &&
		vrg.Status.State == rmn.PrimaryState
}

func (d *DRPCInstance) isVRGConditionMet(cluster string, conditionType string) bool {
	const ready = true

	d.log.Info(fmt.Sprintf("Checking if VRG is %s on cluster %s", conditionType, cluster))

	vrg := d.vrgs[cluster]

	if vrg == nil {
		d.log.Info(fmt.Sprintf("isVRGConditionMet: VRG not available on cluster %s", cluster))

		return !ready
	}

	condition := findCondition(vrg.Status.Conditions, conditionType)
	if condition == nil {
		d.log.Info(fmt.Sprintf("VRG %s condition not available on cluster %s", conditionType, cluster))

		return !ready
	}

	d.log.Info(fmt.Sprintf("VRG status condition: %s is %s", conditionType, condition.Status))

	return condition.Status == metav1.ConditionTrue &&
		condition.ObservedGeneration == vrg.Generation
}

func (d *DRPCInstance) relocate(preferredCluster, preferredClusterNamespace string, drState rmn.DRState) (bool, error) {
	const done = true

	d.setDRState(drState)
	addOrUpdateCondition(&d.instance.Status.Conditions, rmn.ConditionAvailable, d.instance.Generation,
		d.getConditionStatusForTypeAvailable(), string(d.instance.Status.Phase), "Starting relocation")
	addOrUpdateCondition(&d.instance.Status.Conditions, rmn.ConditionPeerReady, d.instance.Generation,
		metav1.ConditionFalse, rmn.ReasonNotStarted,
		fmt.Sprintf("Relocation in progress to cluster %q", preferredCluster))

	// Setting up relocation ensures that all VRGs in all managed cluster are secondaries
	err := d.setupRelocation(preferredCluster)
	if err != nil {
		addOrUpdateCondition(&d.instance.Status.Conditions, rmn.ConditionAvailable, d.instance.Generation,
			d.getConditionStatusForTypeAvailable(), string(d.instance.Status.Phase), err.Error())

		return !done, err
	}

	err = d.switchToCluster(preferredCluster, preferredClusterNamespace)
	if err != nil {
		addOrUpdateCondition(&d.instance.Status.Conditions, rmn.ConditionAvailable, d.instance.Generation,
			d.getConditionStatusForTypeAvailable(), string(d.instance.Status.Phase), err.Error())

		return !done, err
	}

	d.updatePreferredDecision()
	d.setDRState(rmn.Relocated)
	addOrUpdateCondition(&d.instance.Status.Conditions, rmn.ConditionAvailable, d.instance.Generation,
		d.getConditionStatusForTypeAvailable(), string(d.instance.Status.Phase), "Completed")

	d.log.Info("Relocation completed", "State", d.getLastDRState())

	// The relocation is complete, but we still need to clean up the previous
	// primary, hence, returning a NOT done
	return !done, nil
}

func (d *DRPCInstance) setupRelocation(preferredCluster string) error {
	d.log.Info(fmt.Sprintf("setupRelocation to preferredCluster %s", preferredCluster))

	// During relocation, the preferredCluster does not contain a VRG or the VRG is already
	// secondary. We need to skip checking if the VRG for it is secondary to avoid messing up with the
	// order of execution (it could be refactored better to avoid this complexity). IOW, if we first update
	// VRG in all clusters to secondaries, and then we call switchToCluster, and If switchToCluster does not
	// complete in one shot, then coming back to this loop will reset the preferredCluster to secondary again.
	clusterToSkip := preferredCluster
	if !d.ensureVRGIsSecondaryEverywhere(clusterToSkip) {
		d.setProgression(rmn.ProgressionEnsuringVolumesAreSecondary)
		// During relocation, both clusters should be up and both must be secondaries before we proceed.
		if !d.moveVRGToSecondaryEverywhere() {
			return fmt.Errorf("failed to move VRG to secondary everywhere")
		}

		if !d.ensureVRGIsSecondaryEverywhere("") {
			return fmt.Errorf("waiting for VRGs to move to secondaries everywhere")
		}
	}

	if !d.ensureDataProtected(clusterToSkip) {
		return fmt.Errorf("waiting for data protection")
	}

	return nil
}

// switchToCluster is a series of steps for switching to the targetCluster as Primary,
// - It moves VRG to Primary on the targetCluster and ensures that VRG reports required readiness
// - Once VRG is ready, it updates the placement to trigger workload roll out to the targetCluster
// NOTE:
// Currently this function never gets to invoke updateUserPlacementRule as, if a VRG is found to be ready in
// checkReadiness, then the same VRG would have been found as Primary in RunFailover or RunRelocate, which would
// hence start processing the switching to cluster in those functions rather than here.
// As a result only when a VRG is not found as Primary (IOW nil from MCV), would checkReadiness be called and that
// would report false, till the VRG is found as above.
// TODO: This hence can be corrected to remove the call to updateUserPlacementRule and further lines of code
func (d *DRPCInstance) switchToCluster(targetCluster, targetClusterNamespace string) error {
	d.log.Info("switchToCluster", "cluster", targetCluster)

	createdOrUpdated, err := d.createVRGManifestWorkAsPrimary(targetCluster)
	if err != nil {
		return err
	}

	if createdOrUpdated {
		d.setProgression(rmn.ProgressionWaitingForResourceRestore)

		// We just created MWs. Give it time until the App resources have been restored
		return fmt.Errorf("%w)", WaitForAppResourceRestoreToComplete)
	}

	if !d.checkReadiness(targetCluster) {
		d.setProgression(rmn.ProgressionWaitingForResourceRestore)

		return fmt.Errorf("%w)", WaitForAppResourceRestoreToComplete)
	}

	err = d.updateUserPlacementRule(targetCluster, targetClusterNamespace)
	if err != nil {
		return err
	}

	d.setProgression(rmn.ProgressionUpdatedPlacement)

	return nil
}

func (d *DRPCInstance) createVRGManifestWorkAsPrimary(targetCluster string) (bool, error) {
	d.log.Info("create or update VRG if it does not exists or is not primary", "cluster", targetCluster)

	vrg, err := d.getVRGFromManifestWork(targetCluster)
	if err != nil {
		if !errors.IsNotFound(err) {
			return false, err
		}
	}

	if vrg != nil {
		if vrg.Spec.ReplicationState == rmn.Primary {
			d.log.Info("VRG MW already Primary on this cluster", "name", vrg.Name, "cluster", targetCluster)

			return false, nil
		}

		_, err := d.updateVRGState(targetCluster, rmn.Primary)
		if err != nil {
			d.log.Info(fmt.Sprintf("Failed to update VRG to primary on cluster %s. Err (%v)", targetCluster, err))

			return false, err
		}

		return true, nil
	}

	err = d.createVRGManifestWork(targetCluster, rmn.Primary)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (d *DRPCInstance) getVRGFromManifestWork(clusterName string) (*rmn.VolumeReplicationGroup, error) {
	mw, err := d.mwu.FindManifestWorkByType(rmnutil.MWTypeVRG, clusterName)
	if err != nil {
		return nil, fmt.Errorf("%w", err)
	}

	vrg, err := rmnutil.ExtractVRGFromManifestWork(mw)
	if err != nil {
		return nil, err
	}

	return vrg, nil
}

func (d *DRPCInstance) vrgExistsAndPrimary(targetCluster string) bool {
	vrg, ok := d.vrgs[targetCluster]
	if !ok || !isVRGPrimary(vrg) {
		return false
	}

	if rmnutil.ResourceIsDeleted(vrg) {
		return false
	}

	d.log.Info(fmt.Sprintf("Already %q to cluster %s", d.getLastDRState(), targetCluster))

	return true
}

func (d *DRPCInstance) mwExistsAndPlacementUpdated(targetCluster string) (bool, error) {
	_, err := d.mwu.FindManifestWorkByType(rmnutil.MWTypeVRG, targetCluster)
	if err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}

		return false, err
	}

	clusterDecision := d.reconciler.getClusterDecision(d.userPlacement)
	if clusterDecision.ClusterName == "" ||
		clusterDecision.ClusterName != targetCluster {
		return false, nil
	}

	return true, nil
}

func (d *DRPCInstance) moveVRGToSecondaryEverywhere() bool {
	d.log.Info("Move VRG to secondary everywhere")

	failedCount := 0

	for _, clusterName := range rmnutil.DRPolicyClusterNames(d.drPolicy) {
		_, err := d.updateVRGState(clusterName, rmn.Secondary)
		if err != nil {
			if errors.IsNotFound(err) {
				continue
			}

			d.log.Info(fmt.Sprintf("Failed to update VRG to secondary on cluster %s. Error %s",
				clusterName, err.Error()))

			failedCount++
		}
	}

	if failedCount != 0 {
		d.log.Info("Failed to update VRG to secondary", "FailedCount", failedCount)

		return false
	}

	return true
}

func (d *DRPCInstance) cleanupSecondaries(skipCluster string) (bool, error) {
	for _, clusterName := range rmnutil.DRPolicyClusterNames(d.drPolicy) {
		if skipCluster == clusterName {
			continue
		}

		// If VRG hasn't been deleted, then make sure that the MW for it is deleted and
		// return and wait, but first make sure that the cluster is accessible
		if err := checkAccessToVRGOnCluster(d.reconciler.MCVGetter, d.instance.GetName(), d.instance.GetNamespace(),
			d.vrgNamespace, clusterName); err != nil {
			return false, err
		}

		mwDeleted, err := d.ensureVRGManifestWorkOnClusterDeleted(clusterName)
		if err != nil {
			return false, err
		}

		if !mwDeleted {
			return false, nil
		}

		d.log.Info("MW has been deleted. Check the VRG")

		if !d.ensureVRGDeleted(clusterName) {
			d.log.Info("VRG has not been deleted yet", "cluster", clusterName)

			return false, nil
		}

		err = d.reconciler.MCVGetter.DeleteVRGManagedClusterView(d.instance.Name, d.vrgNamespace, clusterName,
			rmnutil.MWTypeVRG)
		// MW is deleted, VRG is deleted, so we no longer need MCV for the VRG
		if err != nil {
			d.log.Info("Deletion of VRG MCV failed")

			return false, fmt.Errorf("deletion of VRG MCV failed %w", err)
		}

		err = d.reconciler.MCVGetter.DeleteNamespaceManagedClusterView(d.instance.Name, d.vrgNamespace, clusterName,
			rmnutil.MWTypeNS)
		// MCV for Namespace is no longer needed
		if err != nil {
			d.log.Info("Deletion of Namespace MCV failed")

			return false, fmt.Errorf("deletion of namespace MCV failed %w", err)
		}
	}

	return true, nil
}

func checkAccessToVRGOnCluster(mcvGetter rmnutil.ManagedClusterViewGetter,
	name, drpcNamespace, vrgNamespace, clusterName string,
) error {
	annotations := make(map[string]string)

	annotations[DRPCNameAnnotation] = name
	annotations[DRPCNamespaceAnnotation] = drpcNamespace

	_, err := mcvGetter.GetVRGFromManagedCluster(name,
		vrgNamespace, clusterName, annotations)
	if err != nil {
		if !errors.IsNotFound(err) {
			return err
		}
	}

	return nil
}

func (d *DRPCInstance) updateUserPlacementRule(homeCluster, reason string) error {
	d.log.Info(fmt.Sprintf("Updating user Placement %s homeCluster %s",
		d.userPlacement.GetName(), homeCluster))

	added := rmnutil.AddAnnotation(d.instance, LastAppDeploymentCluster, homeCluster)
	if added {
		if err := d.reconciler.Update(d.ctx, d.instance); err != nil {
			return err
		}
	}

	newPD := &clrapiv1beta1.ClusterDecision{
		ClusterName: homeCluster,
		Reason:      reason,
	}

	return d.reconciler.updateUserPlacementStatusDecision(d.ctx, d.userPlacement, newPD)
}

func (d *DRPCInstance) clearUserPlacementRuleStatus() error {
	d.log.Info("Clearing user Placement", "name", d.userPlacement.GetName())

	return d.reconciler.updateUserPlacementStatusDecision(d.ctx, d.userPlacement, nil)
}

func (d *DRPCInstance) updatePreferredDecision() {
	if d.instance.Spec.PreferredCluster != "" &&
		reflect.DeepEqual(d.instance.Status.PreferredDecision, rmn.PlacementDecision{}) {
		d.instance.Status.PreferredDecision = rmn.PlacementDecision{
			ClusterName:      d.instance.Spec.PreferredCluster,
			ClusterNamespace: d.instance.Spec.PreferredCluster,
		}
	}
}

func (d *DRPCInstance) createVRGManifestWork(homeCluster string, repState rmn.ReplicationState) error {
	// TODO: check if VRG MW here as a less expensive way to validate if Namespace exists
	err := d.ensureNamespaceManifestWork(homeCluster)
	if err != nil {
		return fmt.Errorf("createVRGManifestWork couldn't ensure namespace '%s' on cluster %s exists",
			d.vrgNamespace, homeCluster)
	}

	// create VRG ManifestWork
	d.log.Info("Creating VRG ManifestWork",
		"Last State:", d.getLastDRState(), "cluster", homeCluster)

	vrg := d.generateVRG(homeCluster, repState)
	vrg.Spec.VolSync.Disabled = d.volSyncDisabled

	annotations := make(map[string]string)

	annotations[DRPCNameAnnotation] = d.instance.Name
	annotations[DRPCNamespaceAnnotation] = d.instance.Namespace

	if err := d.mwu.CreateOrUpdateVRGManifestWork(
		d.instance.Name, d.vrgNamespace,
		homeCluster, vrg, annotations); err != nil {
		d.log.Error(err, "failed to create or update VolumeReplicationGroup manifest")

		return fmt.Errorf("failed to create or update VolumeReplicationGroup manifest in namespace %s (%w)", homeCluster, err)
	}

	return nil
}

// ensureVRGManifestWork ensures that the VRG ManifestWork exists and matches the current VRG state.
// TODO: This may be safe only when the VRG is primary - check if callers use this correctly.
func (d *DRPCInstance) ensureVRGManifestWork(homeCluster string) error {
	d.log.Info("Ensure VRG ManifestWork",
		"Last State:", d.getLastDRState(), "cluster", homeCluster)

	cachedVrg := d.vrgs[homeCluster]
	if cachedVrg == nil {
		return fmt.Errorf("failed to get vrg from cluster %s", homeCluster)
	}

	return d.createVRGManifestWork(homeCluster, cachedVrg.Spec.ReplicationState)
}

func (d *DRPCInstance) ensurePlacement(homeCluster string) error {
	clusterDecision := d.reconciler.getClusterDecision(d.userPlacement)
	if clusterDecision.ClusterName == "" ||
		homeCluster != clusterDecision.ClusterName {
		d.updatePreferredDecision()

		return d.updateUserPlacementRule(homeCluster, homeCluster)
	}

	return nil
}

func vrgAction(drpcAction rmn.DRAction) rmn.VRGAction {
	switch drpcAction {
	case rmn.ActionFailover:
		return rmn.VRGActionFailover
	case rmn.ActionRelocate:
		return rmn.VRGActionRelocate
	default:
		return ""
	}
}

func (d *DRPCInstance) setVRGAction(vrg *rmn.VolumeReplicationGroup) {
	action := vrgAction(d.instance.Spec.Action)
	if action == "" {
		return
	}

	vrg.Spec.Action = action
}

func (d *DRPCInstance) generateVRG(dstCluster string, repState rmn.ReplicationState) rmn.VolumeReplicationGroup {
	vrg := rmn.VolumeReplicationGroup{
		TypeMeta: metav1.TypeMeta{Kind: "VolumeReplicationGroup", APIVersion: "ramendr.openshift.io/v1alpha1"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      d.instance.Name,
			Namespace: d.vrgNamespace,
			Annotations: map[string]string{
				DestinationClusterAnnotationKey: dstCluster,
				DoNotDeletePVCAnnotation:        d.instance.GetAnnotations()[DoNotDeletePVCAnnotation],
				DRPCUIDAnnotation:               string(d.instance.UID),
				rmnutil.IsCGEnabledAnnotation:   d.instance.GetAnnotations()[rmnutil.IsCGEnabledAnnotation],
			},
		},
		Spec: rmn.VolumeReplicationGroupSpec{
			PVCSelector:          d.instance.Spec.PVCSelector,
			ProtectedNamespaces:  d.instance.Spec.ProtectedNamespaces,
			ReplicationState:     repState,
			S3Profiles:           AvailableS3Profiles(d.drClusters),
			KubeObjectProtection: d.instance.Spec.KubeObjectProtection,
		},
	}

	d.setVRGAction(&vrg)
	vrg.Spec.Async = d.generateVRGSpecAsync()
	vrg.Spec.Sync = d.generateVRGSpecSync()

	return vrg
}

func (d *DRPCInstance) generateVRGSpecAsync() *rmn.VRGAsyncSpec {
	if dRPolicySupportsRegional(d.drPolicy, d.drClusters) {
		return &rmn.VRGAsyncSpec{
			ReplicationClassSelector:         d.drPolicy.Spec.ReplicationClassSelector,
			VolumeSnapshotClassSelector:      d.drPolicy.Spec.VolumeSnapshotClassSelector,
			VolumeGroupSnapshotClassSelector: d.drPolicy.Spec.VolumeGroupSnapshotClassSelector,
			SchedulingInterval:               d.drPolicy.Spec.SchedulingInterval,
		}
	}

	return nil
}

func (d *DRPCInstance) generateVRGSpecSync() *rmn.VRGSyncSpec {
	if d.drType == DRTypeSync {
		return &rmn.VRGSyncSpec{}
	}

	return nil
}

func dRPolicySupportsRegional(drpolicy *rmn.DRPolicy, drClusters []rmn.DRCluster) bool {
	return rmnutil.DrpolicyRegionNamesAsASet(drpolicy, drClusters).Len() > 1
}

func dRPolicySupportsMetro(drpolicy *rmn.DRPolicy, drclusters []rmn.DRCluster) (
	supportsMetro bool,
	metroMap map[rmn.Region][]string,
) {
	allRegionsMap := make(map[rmn.Region][]string)
	metroMap = make(map[rmn.Region][]string)

	for _, managedCluster := range rmnutil.DRPolicyClusterNames(drpolicy) {
		for _, v := range drclusters {
			if v.Name == managedCluster {
				allRegionsMap[v.Spec.Region] = append(
					allRegionsMap[v.Spec.Region],
					managedCluster)
			}
		}
	}

	for k, v := range allRegionsMap {
		if len(v) > 1 {
			supportsMetro = true
			metroMap[k] = v
		}
	}

	return supportsMetro, metroMap
}

func (d *DRPCInstance) ensureNamespaceManifestWork(homeCluster string) error {
	// Ensure the MW for the namespace exists
	mw, err := d.mwu.FindManifestWorkByType(rmnutil.MWTypeNS, homeCluster)
	if err != nil {
		if !errors.IsNotFound(err) {
			return fmt.Errorf("failed to get NS MW (%w)", err)
		}

		annotations := make(map[string]string)

		annotations[DRPCNameAnnotation] = d.instance.Name
		annotations[DRPCNamespaceAnnotation] = d.instance.Namespace

		err := d.mwu.CreateOrUpdateNamespaceManifest(d.instance.Name, d.vrgNamespace, homeCluster, annotations)
		if err != nil {
			return fmt.Errorf("failed to create namespace '%s' on cluster %s: %w", d.vrgNamespace, homeCluster, err)
		}

		d.log.Info(fmt.Sprintf("Created Namespace '%s' on cluster %s", d.vrgNamespace, homeCluster))

		return nil // created namespace
	}

	// Ensure the OCM backup label does not exists, otherwise, remove it.
	labels := mw.GetLabels()
	if labels == nil {
		return nil
	}

	if _, ok := labels[rmnutil.OCMBackupLabelKey]; ok {
		delete(mw.Labels, rmnutil.OCMBackupLabelKey)

		return d.reconciler.Update(d.ctx, mw)
	}

	return nil
}

func isVRGPrimary(vrg *rmn.VolumeReplicationGroup) bool {
	return (vrg.Spec.ReplicationState == rmn.Primary)
}

func isVRGSecondary(vrg *rmn.VolumeReplicationGroup) bool {
	return (vrg.Spec.ReplicationState == rmn.Secondary)
}

func (d *DRPCInstance) EnsureCleanup(clusterToSkip string) error {
	d.log.Info("ensuring cleanup on secondaries")

	condition := findCondition(d.instance.Status.Conditions, rmn.ConditionPeerReady)

	if condition == nil {
		msg := "Starting cleanup check"
		d.log.Info(msg)
		addOrUpdateCondition(&d.instance.Status.Conditions, rmn.ConditionPeerReady, d.instance.Generation,
			metav1.ConditionFalse, rmn.ReasonProgressing, msg)

		condition = findCondition(d.instance.Status.Conditions, rmn.ConditionPeerReady)
	}

	if condition.Reason == rmn.ReasonSuccess &&
		condition.Status == metav1.ConditionTrue &&
		condition.ObservedGeneration == d.instance.Generation {
		d.log.Info("Condition values tallied, cleanup is considered complete")

		return nil
	}

	d.log.Info(fmt.Sprintf("PeerReady Condition is %s, msg: %s", condition.Status, condition.Message))

	// IFF we have VolSync PVCs, then no need to clean up
	homeCluster := clusterToSkip

	repReq, err := d.IsVolSyncReplicationRequired(homeCluster)
	if err != nil {
		return fmt.Errorf("failed to check if VolSync replication is required (%w)", err)
	}

	if repReq {
		return d.cleanupForVolSync(clusterToSkip)
	}

	clean, err := d.cleanupSecondaries(clusterToSkip)
	if err != nil {
		addOrUpdateCondition(&d.instance.Status.Conditions, rmn.ConditionPeerReady, d.instance.Generation,
			metav1.ConditionFalse, rmn.ReasonCleaning, err.Error())

		return err
	}

	if !clean {
		msg := "cleaning secondaries"
		addOrUpdateCondition(&d.instance.Status.Conditions, rmn.ConditionPeerReady, d.instance.Generation,
			metav1.ConditionFalse, rmn.ReasonCleaning, msg)

		return fmt.Errorf("waiting to clean secondaries")
	}

	addOrUpdateCondition(&d.instance.Status.Conditions, rmn.ConditionPeerReady, d.instance.Generation,
		metav1.ConditionTrue, rmn.ReasonSuccess, "Cleaned")

	return nil
}

//nolint:gocognit
func (d *DRPCInstance) cleanupForVolSync(clusterToSkip string) error {
	d.log.Info("VolSync needs both VRGs. No need to clean up secondary")
	d.log.Info("Ensure secondary on peer")

	peersReady := true

	for _, clusterName := range rmnutil.DRPolicyClusterNames(d.drPolicy) {
		if clusterToSkip == clusterName {
			continue
		}

		justUpdated, err := d.updateVRGState(clusterName, rmn.Secondary)
		if err != nil {
			d.log.Info(fmt.Sprintf("Failed to update VRG state for cluster %s. Err (%v)", clusterName, err))

			peersReady = false

			// Recreate the VRG ManifestWork for the secondary. This typically happens during Hub Recovery.
			if errors.IsNotFound(err) {
				err := d.createVolSyncDestManifestWork(clusterToSkip)
				if err != nil {
					return err
				}
			}

			break
		}

		// IFF just updated, no need to use MCV to check if the state has been
		// applied. Wait for the next round of reconcile. Otherwise, check if
		// the change to secondary has been reflected.
		if justUpdated || !d.ensureVRGIsSecondaryOnCluster(clusterName) {
			peersReady = false

			break
		}
	}

	if !peersReady {
		return fmt.Errorf("still waiting for peer to be ready")
	}

	addOrUpdateCondition(&d.instance.Status.Conditions, rmn.ConditionPeerReady, d.instance.Generation,
		metav1.ConditionTrue, rmn.ReasonSuccess, "Ready")

	return nil
}

func (d *DRPCInstance) ensureVRGManifestWorkOnClusterDeleted(clusterName string) (bool, error) {
	d.log.Info("Ensuring MW for the VRG is deleted", "cluster", clusterName)

	const done = true

	mw, err := d.mwu.FindManifestWorkByType(rmnutil.MWTypeVRG, clusterName)
	if err != nil {
		if errors.IsNotFound(err) {
			return done, nil
		}

		return !done, fmt.Errorf("failed to retrieve ManifestWork (%w)", err)
	}

	if rmnutil.ResourceIsDeleted(mw) {
		d.log.Info("Waiting for VRG MW to be fully deleted", "cluster", clusterName)
		// As long as the Manifestwork still exist, then we are not done
		return !done, nil
	}

	// If .spec.ReplicateSpec has not already been updated to secondary, then update it.
	// If we do update it to secondary, then we have to wait for the MW to be applied
	updated, err := d.updateVRGState(clusterName, rmn.Secondary)
	if err != nil || updated {
		return !done, err
	}

	if d.ensureVRGIsSecondaryOnCluster(clusterName) {
		err := d.mwu.DeleteManifestWorksForCluster(clusterName)
		if err != nil {
			return !done, fmt.Errorf("%w", err)
		}
	}

	d.log.Info("Request not complete yet", "cluster", clusterName)

	if d.instance.Spec.ProtectedNamespaces != nil && len(*d.instance.Spec.ProtectedNamespaces) > 0 {
		d.setProgression(rmn.ProgressionWaitOnUserToCleanUp)
	}

	// IF we get here, either the VRG has not transitioned to secondary (yet) or delete didn't succeed. In either cases,
	// we need to make sure that the VRG object is deleted. IOW, we still have to wait
	return !done, nil
}

// ensureVRGIsSecondaryEverywhere iterates through all the clusters in the DRCluster set,
// and for each cluster, it checks whether the VRG (if exists) is secondary. It will skip
// a cluster if provided. It returns true if all clusters report secondary for the VRG,
// otherwise, it returns false
func (d *DRPCInstance) ensureVRGIsSecondaryEverywhere(clusterToSkip string) bool {
	for _, clusterName := range rmnutil.DRPolicyClusterNames(d.drPolicy) {
		if clusterToSkip == clusterName {
			continue
		}

		if d.instance.Spec.ProtectedNamespaces != nil && len(*d.instance.Spec.ProtectedNamespaces) > 0 {
			d.setProgression(rmn.ProgressionWaitOnUserToCleanUp)
		}

		if !d.ensureVRGIsSecondaryOnCluster(clusterName) {
			d.log.Info("Still waiting for VRG to transition to secondary", "cluster", clusterName)

			return false
		}
	}

	return true
}

// ensureVRGIsSecondaryOnCluster returns true when VRG is secondary or it does not exists on the cluster
func (d *DRPCInstance) ensureVRGIsSecondaryOnCluster(clusterName string) bool {
	d.log.Info(fmt.Sprintf("Ensure VRG %s is secondary on cluster %s", d.instance.Name, clusterName))

	d.mcvRequestInProgress = false

	annotations := make(map[string]string)

	annotations[DRPCNameAnnotation] = d.instance.Name
	annotations[DRPCNamespaceAnnotation] = d.instance.Namespace

	vrg, err := d.reconciler.MCVGetter.GetVRGFromManagedCluster(d.instance.Name,
		d.vrgNamespace, clusterName, annotations)
	if err != nil {
		if errors.IsNotFound(err) {
			return true // ensured
		}

		d.log.Info("Failed to get VRG", "errorValue", err)

		d.mcvRequestInProgress = true

		return false
	}

	if vrg.Status.State != rmn.SecondaryState || vrg.Status.ObservedGeneration != vrg.Generation {
		d.log.Info(fmt.Sprintf("VRG on %s has not transitioned to secondary yet. Spec-State/Status-State %s/%s",
			clusterName, vrg.Spec.ReplicationState, vrg.Status.State))

		return false
	}

	return true
}

// Check for DataProtected condition to be true everywhere except the
// preferredCluster where the app is being relocated to.
// This is because, preferredCluster wont have a VRG in a secondary state when
// relocate is started at first. preferredCluster will get VRG as primary when DRPC is
// about to move the workload to the preferredCluser. And before doing that, DataProtected
// has to be ensured. This can only be done at the other cluster which has been moved to
// secondary by now.
func (d *DRPCInstance) ensureDataProtected(targetCluster string) bool {
	for _, clusterName := range rmnutil.DRPolicyClusterNames(d.drPolicy) {
		if targetCluster == clusterName {
			continue
		}

		if !d.ensureDataProtectedOnCluster(clusterName) {
			d.log.Info("Still waiting for data sync to complete", "cluster", clusterName)

			return false
		}
	}

	return true
}

func (d *DRPCInstance) ensureDataProtectedOnCluster(clusterName string) bool {
	// this check is done only for relocation. Since this function can be called during
	// failover as well, trying to ensure that data is completely synced in the new
	// cluster where the app is going to be placed might not be successful. Only for
	// relocate this check is made.
	d.log.Info(fmt.Sprintf("Ensure VRG %s as secondary has the data protected on  %s",
		d.instance.Name, clusterName))

	d.mcvRequestInProgress = false

	annotations := make(map[string]string)

	annotations[DRPCNameAnnotation] = d.instance.Name
	annotations[DRPCNamespaceAnnotation] = d.instance.Namespace

	vrg, err := d.reconciler.MCVGetter.GetVRGFromManagedCluster(d.instance.Name,
		d.vrgNamespace, clusterName, annotations)
	if err != nil {
		if errors.IsNotFound(err) {
			// expectation is that VRG should be present. Otherwise, this function
			// would not have been called. Return false
			d.log.Info("VRG not found", "errorValue", err)

			return false
		}

		d.log.Info("Failed to get VRG", "errorValue", err)

		d.mcvRequestInProgress = true

		return false
	}

	dataProtectedCondition := findCondition(vrg.Status.Conditions, VRGConditionTypeDataProtected)
	if dataProtectedCondition == nil {
		d.log.Info(fmt.Sprintf("VRG DataProtected condition not available for cluster %s (%v)",
			clusterName, vrg))

		return false
	}

	if dataProtectedCondition.Status != metav1.ConditionTrue ||
		dataProtectedCondition.ObservedGeneration != vrg.Generation {
		d.log.Info(fmt.Sprintf("VRG data protection is not complete for cluster %s for %v",
			clusterName, vrg))

		return false
	}

	return true
}

func (d *DRPCInstance) ensureVRGDeleted(clusterName string) bool {
	d.mcvRequestInProgress = false

	annotations := make(map[string]string)

	annotations[DRPCNameAnnotation] = d.instance.Name
	annotations[DRPCNamespaceAnnotation] = d.instance.Namespace

	vrg, err := d.reconciler.MCVGetter.GetVRGFromManagedCluster(d.instance.Name,
		d.vrgNamespace, clusterName, annotations)
	if err != nil {
		// Only NotFound error is accepted
		if errors.IsNotFound(err) {
			return true // ensured
		}

		d.log.Info("Failed to get VRG", "error", err)

		d.mcvRequestInProgress = true
		// Retry again
		return false
	}

	d.log.Info(fmt.Sprintf("VRG not deleted(%s)", vrg.Name))

	return false
}

func (d *DRPCInstance) updateVRGState(clusterName string, state rmn.ReplicationState) (bool, error) {
	d.log.Info(fmt.Sprintf("Updating VRG ReplicationState to %s for cluster %s", state, clusterName))

	vrg, err := d.getVRGFromManifestWork(clusterName)
	if err != nil {
		return false, fmt.Errorf("failed to update VRG state. ClusterName %s (%w)",
			clusterName, err)
	}

	if vrg.Spec.ReplicationState == state {
		d.log.Info(fmt.Sprintf("VRG.Spec.ReplicationState %s already set to %s on this cluster %s",
			vrg.Name, state, clusterName))

		return false, nil
	}

	vrg.Spec.ReplicationState = state
	if state == rmn.Secondary {
		// Turn off the final sync flags
		vrg.Spec.PrepareForFinalSync = false
		vrg.Spec.RunFinalSync = false
	}

	d.setVRGAction(vrg)

	err = d.updateManifestWork(clusterName, vrg)
	if err != nil {
		return false, err
	}

	d.log.Info(fmt.Sprintf("Updated VRG %s running in cluster %s to secondary", vrg.Name, clusterName))

	return true, nil
}

func (d *DRPCInstance) updateVRGToPrepareForFinalSync(clusterName string) error {
	d.log.Info(fmt.Sprintf("Updating VRG Spec to prepare for final sync on cluster %s", clusterName))

	vrg, err := d.getVRGFromManifestWork(clusterName)
	if err != nil {
		return fmt.Errorf("failed to update VRG state. ClusterName %s (%w)",
			clusterName, err)
	}

	if vrg.Spec.PrepareForFinalSync {
		d.log.Info(fmt.Sprintf("VRG %s on cluster %s already has the prepare for final sync flag set",
			vrg.Name, clusterName))

		return nil
	}

	vrg.Spec.PrepareForFinalSync = true
	vrg.Spec.RunFinalSync = false

	err = d.updateManifestWork(clusterName, vrg)
	if err != nil {
		return err
	}

	d.log.Info(fmt.Sprintf("Updated VRG %s running in cluster %s to prepare for the final sync",
		vrg.Name, clusterName))

	return nil
}

func (d *DRPCInstance) updateVRGToRunFinalSync(clusterName string) error {
	d.log.Info(fmt.Sprintf("Updating VRG Spec to run final sync on cluster %s", clusterName))

	vrg, err := d.getVRGFromManifestWork(clusterName)
	if err != nil {
		return fmt.Errorf("failed to update VRG state. ClusterName %s (%w)",
			clusterName, err)
	}

	if vrg.Spec.RunFinalSync {
		d.log.Info(fmt.Sprintf("VRG %s on cluster %s already has the final sync flag set",
			vrg.Name, clusterName))

		return nil
	}

	vrg.Spec.RunFinalSync = true
	vrg.Spec.PrepareForFinalSync = false

	err = d.updateManifestWork(clusterName, vrg)
	if err != nil {
		return err
	}

	d.log.Info(fmt.Sprintf("Updated VRG %s running in cluster %s to run the final sync",
		vrg.Name, clusterName))

	return nil
}

func (d *DRPCInstance) updateManifestWork(clusterName string, vrg *rmn.VolumeReplicationGroup) error {
	mw, err := d.mwu.FindManifestWorkByType(rmnutil.MWTypeVRG, clusterName)
	if err != nil {
		return fmt.Errorf("%w", err)
	}

	vrgClientManifest, err := d.mwu.GenerateManifest(vrg)
	if err != nil {
		d.log.Error(err, "failed to generate manifest")

		return fmt.Errorf("failed to generate VRG manifest (%w)", err)
	}

	mw.Spec.Workload.Manifests[0] = *vrgClientManifest

	return d.reconciler.Update(d.ctx, mw)
}

func (d *DRPCInstance) setDRState(nextState rmn.DRState) {
	if d.instance.Status.Phase != nextState {
		d.log.Info(fmt.Sprintf("Phase: Current '%s'. Next '%s'",
			d.instance.Status.Phase, nextState))

		d.instance.Status.Phase = nextState
		d.instance.Status.ObservedGeneration = d.instance.Generation
		d.reportEvent(nextState)
	}
}

func updateDRPCProgression(
	drpc *rmn.DRPlacementControl, nextProgression rmn.ProgressionStatus, log logr.Logger,
) bool {
	if drpc.Status.Progression != nextProgression {
		log.Info(fmt.Sprintf("Progression: Current '%s'. Next '%s'",
			drpc.Status.Progression, nextProgression))

		drpc.Status.Progression = nextProgression

		return true
	}

	return false
}

/*
DRPC Status.Progression has several distinct progressions depending on the action being performed. The following
comment is to help identify which progressions belong to which actions for reference purposes.

deployProgressions are used to indicate progression during initial deployment processing

	deployProgressions := {
		ProgressionCreatingMW,
		ProgressionUpdatingPlRule,
		ProgressionEnsuringVolSyncSetup,
		ProgressionSettingupVolsyncDest,
		ProgressionCompleted,
	}

failoverProgressions are used to indicate progression during failover action processing
- preFailoverProgressions indicates Progressions that are noted before creating VRG on the failoverCluster
- postFailoverProgressions indicates Progressions that are noted post creating VRG on the failoverCluster

	preFailoverProgressions := {
		ProgressionCheckingFailoverPrequisites,
		ProgressionWaitForFencing,
		ProgressionWaitForStorageMaintenanceActivation,
	}

	postFailoverProgressions := {
		ProgressionFailingOverToCluster,
		ProgressionWaitingForResourceRestore,
		ProgressionEnsuringVolSyncSetup,
		ProgressionSettingupVolsyncDest,
		ProgressionWaitForReadiness,
		ProgressionUpdatedPlacement,
		ProgressionCompleted,
		ProgressionCleaningUp,
		ProgressionWaitOnUserToCleanUp,
	}

relocateProgressions are used to indicate progression during relocate action processing
- preSwitch indicates Progressions that are noted before creating VRG on the preferredCluster
- postSwitch indicates Progressions that are noted post creating VRG on the preferredCluster

	preRelocateProgressions := []rmn.ProgressionStatus{
		rmn.ProgressionPreparingFinalSync,
		rmn.ProgressionClearingPlacement,
		rmn.ProgressionRunningFinalSync,
		rmn.ProgressionFinalSyncComplete,
		rmn.ProgressionEnsuringVolumesAreSecondary,
		rmn.ProgressionWaitOnUserToCleanUp,
	}

	postRelocateProgressions := {
		ProgressionCompleted,
		ProgressionCleaningUp,
		ProgressionWaitingForResourceRestore,
		ProgressionWaitForReadiness,
		ProgressionUpdatedPlacement,
		ProgressionEnsuringVolSyncSetup,
		ProgressionSettingupVolsyncDest,
	}

specialProgressions are used to indicate special cases irrespective of action or initial deployment

	specialProgressions := {
		ProgressionDeleting,
		ProgressionActionPaused,
	}
*/
func (d *DRPCInstance) setProgression(nextProgression rmn.ProgressionStatus) {
	updateDRPCProgression(d.instance, nextProgression, d.log)
}

func IsPreRelocateProgression(status rmn.ProgressionStatus) bool {
	preRelocateProgressions := []rmn.ProgressionStatus{
		rmn.ProgressionPreparingFinalSync,
		rmn.ProgressionClearingPlacement,
		rmn.ProgressionRunningFinalSync,
		rmn.ProgressionFinalSyncComplete,
		rmn.ProgressionEnsuringVolumesAreSecondary,
	}

	return slices.Contains(preRelocateProgressions, status)
}

//nolint:cyclop
func (d *DRPCInstance) shouldUpdateStatus() bool {
	for _, condition := range d.instance.Status.Conditions {
		if condition.ObservedGeneration != d.instance.Generation {
			return true
		}
	}

	if !reflect.DeepEqual(d.savedInstanceStatus, d.instance.Status) {
		return true
	}

	homeCluster := ""

	clusterDecision := d.reconciler.getClusterDecision(d.userPlacement)
	if clusterDecision != nil && clusterDecision.ClusterName != "" {
		homeCluster = clusterDecision.ClusterName
	}

	if homeCluster == "" {
		return false
	}

	vrg := d.vrgs[homeCluster]
	if vrg == nil {
		return false
	}

	if !vrg.Status.LastGroupSyncTime.Equal(d.instance.Status.LastGroupSyncTime) {
		return true
	}

	if vrg.Status.LastGroupSyncDuration != d.instance.Status.LastGroupSyncDuration {
		return true
	}

	if vrg.Status.LastGroupSyncBytes != d.instance.Status.LastGroupSyncBytes {
		return true
	}

	if vrg.Status.KubeObjectProtection.CaptureToRecoverFrom != nil {
		vrgKubeObjectProtectionTime := vrg.Status.KubeObjectProtection.CaptureToRecoverFrom.EndTime
		if !vrgKubeObjectProtectionTime.Equal(d.instance.Status.LastKubeObjectProtectionTime) {
			return true
		}
	}

	return !reflect.DeepEqual(d.instance.Status.ResourceConditions.Conditions, vrg.Status.Conditions)
}

//nolint:exhaustive
func (d *DRPCInstance) reportEvent(nextState rmn.DRState) {
	eventReason := "unknown state"
	eventType := corev1.EventTypeWarning
	msg := "next state not known"

	switch nextState {
	case rmn.Deploying:
		eventReason = rmnutil.EventReasonDeploying
		eventType = corev1.EventTypeNormal
		msg = "Deploying the application and VRG"
	case rmn.Deployed:
		eventReason = rmnutil.EventReasonDeploySuccess
		eventType = corev1.EventTypeNormal
		msg = "Successfully deployed the application and VRG"
	case rmn.FailingOver:
		eventReason = rmnutil.EventReasonFailingOver
		eventType = corev1.EventTypeWarning
		msg = "Failing over the application and VRG"
	case rmn.FailedOver:
		eventReason = rmnutil.EventReasonFailoverSuccess
		eventType = corev1.EventTypeNormal
		msg = "Successfully failedover the application and VRG"
	case rmn.Relocating:
		eventReason = rmnutil.EventReasonRelocating
		eventType = corev1.EventTypeNormal
		msg = "Relocating the application and VRG"
	case rmn.Relocated:
		eventReason = rmnutil.EventReasonRelocationSuccess
		eventType = corev1.EventTypeNormal
		msg = "Successfully relocated the application and VRG"
	}

	rmnutil.ReportIfNotPresent(d.reconciler.eventRecorder, d.instance, eventType,
		eventReason, msg)
}

func (d *DRPCInstance) getConditionStatusForTypeAvailable() metav1.ConditionStatus {
	if d.isInFinalPhase() {
		return metav1.ConditionTrue
	}

	if d.isInProgressingPhase() {
		return metav1.ConditionFalse
	}

	return metav1.ConditionUnknown
}

//nolint:exhaustive
func (d *DRPCInstance) isInFinalPhase() bool {
	switch d.instance.Status.Phase {
	case rmn.Deployed:
		fallthrough
	case rmn.FailedOver:
		fallthrough
	case rmn.Relocated:
		return true
	default:
		return false
	}
}

//nolint:exhaustive
func (d *DRPCInstance) isInProgressingPhase() bool {
	switch d.instance.Status.Phase {
	case rmn.Initiating:
		fallthrough
	case rmn.Deploying:
		fallthrough
	case rmn.FailingOver:
		fallthrough
	case rmn.Relocating:
		return true
	default:
		return false
	}
}

func (d *DRPCInstance) getLastDRState() rmn.DRState {
	return d.instance.Status.Phase
}

func (d *DRPCInstance) getProgression() rmn.ProgressionStatus {
	return d.instance.Status.Progression
}

//nolint:exhaustive
func (d *DRPCInstance) getRequeueDuration() time.Duration {
	d.log.Info("Getting requeue duration", "last known DR state", d.getLastDRState())

	const (
		failoverRequeueDelay   = time.Minute * 5
		relocationRequeueDelay = time.Second * 2
	)

	duration := time.Second // second

	switch d.getLastDRState() {
	case rmn.FailingOver:
		duration = failoverRequeueDelay
	case rmn.Relocating:
		duration = relocationRequeueDelay
	}

	return duration
}

func (d *DRPCInstance) setConditionOnInitialDeploymentCompletion() {
	addOrUpdateCondition(&d.instance.Status.Conditions, rmn.ConditionAvailable, d.instance.Generation,
		d.getConditionStatusForTypeAvailable(), string(d.instance.Status.Phase), "Initial deployment completed")

	addOrUpdateCondition(&d.instance.Status.Conditions, rmn.ConditionPeerReady, d.instance.Generation,
		metav1.ConditionTrue, rmn.ReasonSuccess, "Ready")
}

func (d *DRPCInstance) setStatusInitiating() {
	if !(d.instance.Status.Phase == "" ||
		d.instance.Status.Phase == rmn.WaitForUser ||
		d.instance.Status.Phase == rmn.Deployed ||
		d.instance.Status.Phase == rmn.FailedOver ||
		d.instance.Status.Phase == rmn.Relocated) {
		return
	}

	d.setDRState(rmn.Initiating)
	d.setProgression("")

	d.instance.Status.ActionStartTime = &metav1.Time{Time: time.Now()}
	d.instance.Status.ActionDuration = nil
}

func (d *DRPCInstance) setActionDuration() {
	if !(d.instance.Status.ActionDuration == nil && d.instance.Status.ActionStartTime != nil) {
		return
	}

	duration := time.Since(d.instance.Status.ActionStartTime.Time)
	d.instance.Status.ActionDuration = &metav1.Duration{Duration: duration}

	d.log.Info(fmt.Sprintf("%s transition completed. Started at: %v and it took: %v",
		fmt.Sprintf("%v", d.instance.Status.Phase), d.instance.Status.ActionStartTime, duration))
}

func (d *DRPCInstance) EnsureVolSyncReplicationSetup(homeCluster string) error {
	d.log.Info(fmt.Sprintf("Ensure VolSync replication has been setup for cluster %s", homeCluster))

	if d.volSyncDisabled {
		d.log.Info("VolSync is disabled")

		return nil
	}

	vsRepNeeded, err := d.IsVolSyncReplicationRequired(homeCluster)
	if err != nil {
		return err
	}

	if !vsRepNeeded {
		d.log.Info("No PVCs found that require VolSync replication")

		return nil
	}

	err = d.ensureVolSyncReplicationCommon(homeCluster)
	if err != nil {
		return err
	}

	return d.ensureVolSyncReplicationDestination(homeCluster)
}

func (d *DRPCInstance) ensureVolSyncReplicationCommon(srcCluster string) error {
	// Make sure we have Source and Destination VRGs - Source should already have been created at this point
	d.setProgression(rmn.ProgressionEnsuringVolSyncSetup)

	vrgMWCount := d.mwu.GetVRGManifestWorkCount(rmnutil.DRPolicyClusterNames(d.drPolicy))

	const maxNumberOfVRGs = 2
	if len(d.vrgs) != maxNumberOfVRGs || vrgMWCount != maxNumberOfVRGs {
		// Create the destination VRG
		err := d.createVolSyncDestManifestWork(srcCluster)
		if err != nil {
			return err
		}

		return WaitForVolSyncManifestWorkCreation
	}

	if _, found := d.vrgs[srcCluster]; !found {
		return fmt.Errorf("failed to find source VolSync VRG in cluster %s. VRGs %v", srcCluster, d.vrgs)
	}

	// Now we should have a source and destination VRG created
	// Since we will use VolSync - create/ensure & propagate a shared psk rsynctls secret to both the src and dst clusters
	pskSecretNameHub := fmt.Sprintf("%s-vs-secret-hub", d.instance.GetName())

	// Ensure/Create the secret on the hub
	pskSecretHub, err := volsync.ReconcileVolSyncReplicationSecret(d.ctx, d.reconciler.Client, d.instance,
		pskSecretNameHub, d.instance.GetNamespace(), d.log)
	if err != nil {
		d.log.Error(err, "Unable to create psk secret on hub for VolSync")

		return fmt.Errorf("%w", err)
	}

	// Propagate the secret to all clusters
	// Note that VRG spec will not contain the psk secret name, we're going to name based on the VRG name itself
	pskSecretNameCluster := volsync.GetVolSyncPSKSecretNameFromVRGName(d.instance.GetName()) // VRG name == DRPC name

	clustersToPropagateSecret := []string{}
	for clusterName := range d.vrgs {
		clustersToPropagateSecret = append(clustersToPropagateSecret, clusterName)
	}

	err = volsync.PropagateSecretToClusters(d.ctx, d.reconciler.Client, pskSecretHub,
		d.instance, clustersToPropagateSecret, pskSecretNameCluster, d.vrgNamespace, d.log)
	if err != nil {
		d.log.Error(err, "Error propagating secret to clusters", "clustersToPropagateSecret", clustersToPropagateSecret)

		return fmt.Errorf("%w", err)
	}

	return nil
}

func (d *DRPCInstance) ensureVolSyncReplicationDestination(srcCluster string) error {
	d.setProgression(rmn.ProgressionSettingupVolsyncDest)

	srcVRG, found := d.vrgs[srcCluster]
	if !found {
		return fmt.Errorf("failed to find source VolSync VRG in cluster %s. VRGs %v", srcCluster, d.vrgs)
	}

	d.log.Info("Ensuring VolSync replication destination")

	if len(srcVRG.Status.ProtectedPVCs) == 0 {
		d.log.Info("ProtectedPVCs on pirmary cluster is empty")

		return WaitForSourceCluster
	}

	for dstCluster, dstVRG := range d.vrgs {
		if dstCluster == srcCluster {
			continue
		}

		if dstVRG == nil {
			return fmt.Errorf("invalid VolSync VRG entry")
		}

		volSyncPVCCount := d.getVolSyncPVCCount(srcCluster)
		if len(dstVRG.Spec.VolSync.RDSpec) != volSyncPVCCount || d.containsMismatchVolSyncPVCs(srcVRG, dstVRG) {
			err := d.updateDestinationVRG(dstCluster, srcVRG, dstVRG)
			if err != nil {
				return fmt.Errorf("failed to update dst VRG on cluster %s - %w", dstCluster, err)
			}
		}

		d.log.Info(fmt.Sprintf("Ensured VolSync replication destination for cluster %s", dstCluster))
		// TODO: Should we handle more than one dstVRG? For now, just settle for one.
		break
	}

	return nil
}

// containsMismatchVolSyncPVCs returns true if a VolSync protected pvc in the source VRG is not
// found in the destination VRG RDSpecs.  Since we never delete protected PVCS from the source VRG,
// we don't check for other case - a protected PVC in destination not found in the source.
func (d *DRPCInstance) containsMismatchVolSyncPVCs(srcVRG *rmn.VolumeReplicationGroup,
	dstVRG *rmn.VolumeReplicationGroup,
) bool {
	for _, protectedPVC := range srcVRG.Status.ProtectedPVCs {
		if !protectedPVC.ProtectedByVolSync {
			continue
		}

		for _, rdSpec := range dstVRG.Spec.VolSync.RDSpec {
			if protectedPVC.Name == rdSpec.ProtectedPVC.Name &&
				protectedPVC.Namespace == rdSpec.ProtectedPVC.Namespace {
				return false
			}
		}

		// VolSync PVC not found in destination.
		return true
	}

	return false
}

func (d *DRPCInstance) updateDestinationVRG(clusterName string, srcVRG *rmn.VolumeReplicationGroup,
	dstVRG *rmn.VolumeReplicationGroup,
) error {
	// clear RDSpec
	dstVRG.Spec.VolSync.RDSpec = nil

	for _, protectedPVC := range srcVRG.Status.ProtectedPVCs {
		if !protectedPVC.ProtectedByVolSync {
			continue
		}

		rdSpec := rmn.VolSyncReplicationDestinationSpec{
			ProtectedPVC: protectedPVC,
		}

		dstVRG.Spec.VolSync.RDSpec = append(dstVRG.Spec.VolSync.RDSpec, rdSpec)
	}

	return d.updateVRGSpec(clusterName, dstVRG)
}

func (d *DRPCInstance) IsVolSyncReplicationRequired(homeCluster string) (bool, error) {
	if d.volSyncDisabled {
		d.log.Info("VolSync is disabled")

		return false, nil
	}

	const required = true

	d.log.Info("Checking if there are PVCs for VolSync replication...", "cluster", homeCluster)

	vrg := d.vrgs[homeCluster]

	if vrg == nil {
		d.log.Info(fmt.Sprintf("isVolSyncReplicationRequired: VRG not available on cluster %s - VRGs %v",
			homeCluster, d.vrgs))

		return false, fmt.Errorf("failed to find VRG on homeCluster %s", homeCluster)
	}

	if len(vrg.Status.ProtectedPVCs) == 0 {
		return false, WaitForSourceCluster
	}

	for _, protectedPVC := range vrg.Status.ProtectedPVCs {
		if protectedPVC.ProtectedByVolSync {
			return required, nil
		}
	}

	return !required, nil
}

func (d *DRPCInstance) getVolSyncPVCCount(homeCluster string) int {
	pvcCount := 0
	vrg := d.vrgs[homeCluster]

	if vrg == nil {
		d.log.Info(fmt.Sprintf("getVolSyncPVCCount: VRG not available on cluster %s", homeCluster))

		return pvcCount
	}

	for _, protectedPVC := range vrg.Status.ProtectedPVCs {
		if protectedPVC.ProtectedByVolSync {
			pvcCount++
		}
	}

	return pvcCount
}

func (d *DRPCInstance) updateVRGSpec(clusterName string, tgtVRG *rmn.VolumeReplicationGroup) error {
	mw, err := d.mwu.FindManifestWorkByType(rmnutil.MWTypeVRG, clusterName)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil
		}

		d.log.Error(err, "failed to update VRG")

		return fmt.Errorf("failed to update VRG MW, in namespace %s (%w)",
			clusterName, err)
	}

	d.log.Info(fmt.Sprintf("Updating VRG ownedby MW %s for cluster %s", mw.Name, clusterName))

	vrg, err := rmnutil.ExtractVRGFromManifestWork(mw)
	if err != nil {
		d.log.Error(err, "failed to update VRG state")

		return err
	}

	if vrg.Spec.ReplicationState != rmn.Secondary {
		d.log.Info(fmt.Sprintf("VRG %s is not secondary on this cluster %s", vrg.Name, mw.Namespace))

		return fmt.Errorf("failed to update MW due to wrong VRG state (%v) for the request",
			vrg.Spec.ReplicationState)
	}

	vrg.Spec.VolSync.RDSpec = tgtVRG.Spec.VolSync.RDSpec

	vrgClientManifest, err := d.mwu.GenerateManifest(vrg)
	if err != nil {
		d.log.Error(err, "failed to generate manifest")

		return fmt.Errorf("failed to generate VRG manifest (%w)", err)
	}

	mw.Spec.Workload.Manifests[0] = *vrgClientManifest

	err = d.reconciler.Update(d.ctx, mw)
	if err != nil {
		return fmt.Errorf("failed to update MW (%w)", err)
	}

	d.log.Info(fmt.Sprintf("Updated VRG running in cluster %s. VRG (%s)", clusterName, vrg.Name))

	return nil
}

// createVolSyncDestManifestWork creates volsync Secondaries skipping the cluster referenced in clusterToSkip.
// Typically, clusterToSkip is passed in as the cluster where volsync is the Primary.
func (d *DRPCInstance) createVolSyncDestManifestWork(clusterToSkip string) error {
	// create VRG ManifestWork
	d.log.Info("Creating VRG ManifestWork for destination clusters",
		"Last State:", d.getLastDRState(), "homeCluster", clusterToSkip)

	// Create or update ManifestWork for all the peers
	for _, dstCluster := range rmnutil.DRPolicyClusterNames(d.drPolicy) {
		if dstCluster == clusterToSkip {
			// skip source cluster
			continue
		}

		err := d.ensureNamespaceManifestWork(dstCluster)
		if err != nil {
			return fmt.Errorf("creating ManifestWork couldn't ensure namespace '%s' on cluster %s exists",
				d.instance.Namespace, dstCluster)
		}

		annotations := make(map[string]string)

		annotations[DRPCNameAnnotation] = d.instance.Name
		annotations[DRPCNamespaceAnnotation] = d.instance.Namespace

		vrg := d.generateVRG(dstCluster, rmn.Secondary)
		if err := d.mwu.CreateOrUpdateVRGManifestWork(
			d.instance.Name, d.vrgNamespace,
			dstCluster, vrg, annotations); err != nil {
			d.log.Error(err, "failed to create or update VolumeReplicationGroup manifest")

			return fmt.Errorf("failed to create or update VolumeReplicationGroup manifest in namespace %s (%w)", dstCluster, err)
		}

		// For now, assume only a pair of clusters in the DRClusterSet
		break
	}

	return nil
}

func (d *DRPCInstance) ResetVolSyncRDOnPrimary(clusterName string) error {
	if d.volSyncDisabled {
		d.log.Info("VolSync is disabled")

		return nil
	}

	mw, err := d.mwu.FindManifestWorkByType(rmnutil.MWTypeVRG, clusterName)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil
		}

		d.log.Error(err, "failed to update VRG state")

		return fmt.Errorf("failed to update VRG state for MW, in namespace %s (%w)",
			clusterName, err)
	}

	d.log.Info(fmt.Sprintf("Resetting RD VRG ownedby MW %s for cluster %s", mw.Name, clusterName))

	vrg, err := rmnutil.ExtractVRGFromManifestWork(mw)
	if err != nil {
		d.log.Error(err, "failed to extract VRG state")

		return err
	}

	if vrg.Spec.ReplicationState != rmn.Primary {
		d.log.Info(fmt.Sprintf("VRG %s not primary on this cluster %s", vrg.Name, mw.Namespace))

		return fmt.Errorf(fmt.Sprintf("VRG %s not primary on this cluster %s", vrg.Name, mw.Namespace))
	}

	if len(vrg.Spec.VolSync.RDSpec) == 0 {
		d.log.Info(fmt.Sprintf("RDSpec for %s has already been cleared on this cluster %s", vrg.Name, mw.Namespace))

		return nil
	}

	vrg.Spec.VolSync.RDSpec = nil

	vrgClientManifest, err := d.mwu.GenerateManifest(vrg)
	if err != nil {
		d.log.Error(err, "failed to generate manifest")

		return fmt.Errorf("failed to generate VRG manifest (%w)", err)
	}

	mw.Spec.Workload.Manifests[0] = *vrgClientManifest

	err = d.reconciler.Update(d.ctx, mw)
	if err != nil {
		return fmt.Errorf("failed to update MW (%w)", err)
	}

	d.log.Info(fmt.Sprintf("Updated VRG running in cluster %s to secondary. VRG (%v)", clusterName, vrg))

	return nil
}

const (
	// DRPC CR finalizer
	DRPCFinalizer string = "drpc.ramendr.openshift.io/finalizer"

	// Ramen scheduler
	RamenScheduler string = "ramen"

	ClonedPlacementRuleNameFormat string = "drpc-plrule-%s-%s"

	// StatusCheckDelay is used to frequencly update the DRPC status when the reconciler is idle.
	// This is needed in order to sync up the DRPC status and the VRG status.
	StatusCheckDelay = time.Minute * 10

	// PlacementDecisionName format, prefix is the Placement name, and suffix is a PlacementDecision index
	PlacementDecisionName = "%s-decision-%d"

	// Maximum retries to create PlacementDecisionName with an increasing index in case of conflicts
	// with existing PlacementDecision resources
	MaxPlacementDecisionConflictCount = 5

	DestinationClusterAnnotationKey = "drplacementcontrol.ramendr.openshift.io/destination-cluster"

	DoNotDeletePVCAnnotation    = "drplacementcontrol.ramendr.openshift.io/do-not-delete-pvc"
	DoNotDeletePVCAnnotationVal = "true"
)

var InitialWaitTimeForDRPCPlacementRule = errorswrapper.New("Waiting for DRPC Placement to produces placement decision")

// ProgressCallback of function type
type ProgressCallback func(string, string)

// DRPlacementControlReconciler reconciles a DRPlacementControl object
type DRPlacementControlReconciler struct {
	client.Client
	APIReader           client.Reader
	Log                 logr.Logger
	MCVGetter           rmnutil.ManagedClusterViewGetter
	Scheme              *runtime.Scheme
	Callback            ProgressCallback
	eventRecorder       *rmnutil.EventReporter
	savedInstanceStatus rmn.DRPlacementControlStatus
	ObjStoreGetter      ObjectStoreGetter
	RateLimiter         *workqueue.RateLimiter
}

func ManifestWorkPredicateFunc() predicate.Funcs {
	mwPredicate := predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return false
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return false
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return false
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			log := ctrl.Log.WithName("Predicate").WithName("ManifestWork")

			oldMW, ok := e.ObjectOld.DeepCopyObject().(*ocmworkv1.ManifestWork)
			if !ok {
				log.Info("Failed to deep copy older ManifestWork")

				return false
			}
			newMW, ok := e.ObjectNew.DeepCopyObject().(*ocmworkv1.ManifestWork)
			if !ok {
				log.Info("Failed to deep copy newer ManifestWork")

				return false
			}

			log.Info(fmt.Sprintf("Update event for MW %s/%s", oldMW.Name, oldMW.Namespace))

			return !reflect.DeepEqual(oldMW.Status, newMW.Status)
		},
	}

	return mwPredicate
}

func filterMW(mw *ocmworkv1.ManifestWork) []ctrl.Request {
	if mw.Annotations[DRPCNameAnnotation] == "" ||
		mw.Annotations[DRPCNamespaceAnnotation] == "" {
		return []ctrl.Request{}
	}

	return []ctrl.Request{
		reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      mw.Annotations[DRPCNameAnnotation],
				Namespace: mw.Annotations[DRPCNamespaceAnnotation],
			},
		},
	}
}

func ManagedClusterViewPredicateFunc() predicate.Funcs {
	log := ctrl.Log.WithName("Predicate").WithName("MCV")
	mcvPredicate := predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return false
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return false
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return false
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldMCV, ok := e.ObjectOld.DeepCopyObject().(*viewv1beta1.ManagedClusterView)
			if !ok {
				log.Info("Failed to deep copy older MCV")

				return false
			}
			newMCV, ok := e.ObjectNew.DeepCopyObject().(*viewv1beta1.ManagedClusterView)
			if !ok {
				log.Info("Failed to deep copy newer MCV")

				return false
			}

			log.Info(fmt.Sprintf("Update event for MCV %s/%s", oldMCV.Name, oldMCV.Namespace))

			return !reflect.DeepEqual(oldMCV.Status, newMCV.Status)
		},
	}

	return mcvPredicate
}

func filterMCV(mcv *viewv1beta1.ManagedClusterView) []ctrl.Request {
	if mcv.Annotations[DRPCNameAnnotation] == "" ||
		mcv.Annotations[DRPCNamespaceAnnotation] == "" {
		return []ctrl.Request{}
	}

	return []ctrl.Request{
		reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      mcv.Annotations[DRPCNameAnnotation],
				Namespace: mcv.Annotations[DRPCNamespaceAnnotation],
			},
		},
	}
}

func PlacementRulePredicateFunc() predicate.Funcs {
	log := ctrl.Log.WithName("DRPCPredicate").WithName("UserPlRule")
	usrPlRulePredicate := predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return false
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			return false
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return false
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			log.Info("Delete event")

			return true
		},
	}

	return usrPlRulePredicate
}

func filterUsrPlRule(usrPlRule *plrv1.PlacementRule) []ctrl.Request {
	if usrPlRule.Annotations[DRPCNameAnnotation] == "" ||
		usrPlRule.Annotations[DRPCNamespaceAnnotation] == "" {
		return []ctrl.Request{}
	}

	return []ctrl.Request{
		reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      usrPlRule.Annotations[DRPCNameAnnotation],
				Namespace: usrPlRule.Annotations[DRPCNamespaceAnnotation],
			},
		},
	}
}

func PlacementPredicateFunc() predicate.Funcs {
	log := ctrl.Log.WithName("DRPCPredicate").WithName("UserPlmnt")
	usrPlmntPredicate := predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return false
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			return false
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return false
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			log.Info("Delete event")

			return true
		},
	}

	return usrPlmntPredicate
}

func filterUsrPlmnt(usrPlmnt *clrapiv1beta1.Placement) []ctrl.Request {
	if usrPlmnt.Annotations[DRPCNameAnnotation] == "" ||
		usrPlmnt.Annotations[DRPCNamespaceAnnotation] == "" {
		return []ctrl.Request{}
	}

	return []ctrl.Request{
		reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      usrPlmnt.Annotations[DRPCNameAnnotation],
				Namespace: usrPlmnt.Annotations[DRPCNamespaceAnnotation],
			},
		},
	}
}

func DRClusterPredicateFunc() predicate.Funcs {
	log := ctrl.Log.WithName("DRPCPredicate").WithName("DRCluster")
	drClusterPredicate := predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return false
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return false
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return false
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			log.Info("Update event")

			return DRClusterUpdateOfInterest(e.ObjectOld.(*rmn.DRCluster), e.ObjectNew.(*rmn.DRCluster))
		},
	}

	return drClusterPredicate
}

// DRClusterUpdateOfInterest checks if the new DRCluster resource as compared to the older version
// requires any attention, it checks for the following updates:
//   - If any maintenance mode is reported as activated
//   - If drcluster was marked for deletion
//
// TODO: Needs some logs for easier troubleshooting
func DRClusterUpdateOfInterest(oldDRCluster, newDRCluster *rmn.DRCluster) bool {
	for _, mModeNew := range newDRCluster.Status.MaintenanceModes {
		// Check if new conditions have failover activated, if not this maintenance mode is NOT of interest
		conditionNew := getFailoverActivatedCondition(mModeNew)
		if conditionNew == nil ||
			conditionNew.Status == metav1.ConditionFalse ||
			conditionNew.Status == metav1.ConditionUnknown {
			continue
		}

		// Check if failover maintenance mode was already activated as part of an older update to DRCluster, if NOT
		// this change is of interest
		if activated := checkFailoverActivation(oldDRCluster, mModeNew.StorageProvisioner, mModeNew.TargetID); !activated {
			return true
		}
	}

	// Exhausted all failover activation checks, the only interesting update is deleting a drcluster.
	return rmnutil.ResourceIsDeleted(newDRCluster)
}

// checkFailoverActivation checks if provided provisioner and storage instance is activated as per the
// passed in DRCluster resource status. It currently only checks for:
//   - Failover activation condition
func checkFailoverActivation(drcluster *rmn.DRCluster, provisioner string, targetID string) bool {
	for _, mMode := range drcluster.Status.MaintenanceModes {
		if !(mMode.StorageProvisioner == provisioner && mMode.TargetID == targetID) {
			continue
		}

		condition := getFailoverActivatedCondition(mMode)
		if condition == nil ||
			condition.Status == metav1.ConditionFalse ||
			condition.Status == metav1.ConditionUnknown {
			return false
		}

		return true
	}

	return false
}

// getFailoverActivatedCondition is a helper routine that returns the FailoverActivated condition
// from a given ClusterMaintenanceMode if found, or nil otherwise
func getFailoverActivatedCondition(mMode rmn.ClusterMaintenanceMode) *metav1.Condition {
	for _, condition := range mMode.Conditions {
		if condition.Type != string(rmn.MModeConditionFailoverActivated) {
			continue
		}

		return &condition
	}

	return nil
}

// FilterDRCluster filters for DRPC resources that should be reconciled due to a DRCluster watch event
func (r *DRPlacementControlReconciler) FilterDRCluster(drcluster *rmn.DRCluster) []ctrl.Request {
	log := ctrl.Log.WithName("DRPCFilter").WithName("DRCluster").WithValues("cluster", drcluster)

	var drpcCollections []DRPCAndPolicy

	var err error

	if rmnutil.ResourceIsDeleted(drcluster) {
		drpcCollections, err = DRPCsUsingDRCluster(r.Client, log, drcluster)
	} else {
		drpcCollections, err = DRPCsFailingOverToCluster(r.Client, log, drcluster.GetName())
	}

	if err != nil {
		log.Info("Failed to process filter")

		return nil
	}

	requests := make([]reconcile.Request, 0)
	for idx := range drpcCollections {
		requests = append(requests,
			reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      drpcCollections[idx].drpc.GetName(),
					Namespace: drpcCollections[idx].drpc.GetNamespace(),
				},
			})
	}

	return requests
}

type DRPCAndPolicy struct {
	drpc     *rmn.DRPlacementControl
	drPolicy *rmn.DRPolicy
}

// DRPCsUsingDRCluster finds DRPC resources using the DRcluster.
func DRPCsUsingDRCluster(k8sclient client.Client, log logr.Logger, drcluster *rmn.DRCluster) ([]DRPCAndPolicy, error) {
	drpolicies := &rmn.DRPolicyList{}
	if err := k8sclient.List(context.TODO(), drpolicies); err != nil {
		log.Error(err, "Failed to list DRPolicies", "drcluster", drcluster.GetName())

		return nil, err
	}

	found := []DRPCAndPolicy{}

	for i := range drpolicies.Items {
		drpolicy := &drpolicies.Items[i]

		if rmnutil.DrpolicyContainsDrcluster(drpolicy, drcluster.GetName()) {
			log.Info("Found DRPolicy referencing DRCluster", "drpolicy", drpolicy.GetName())

			drpcs, err := DRPCsUsingDRPolicy(k8sclient, log, drpolicy)
			if err != nil {
				return nil, err
			}

			for _, drpc := range drpcs {
				found = append(found, DRPCAndPolicy{drpc: drpc, drPolicy: drpolicy})
			}
		}
	}

	return found, nil
}

// DRPCsUsingDRPolicy finds DRPC resources that reference the DRPolicy.
func DRPCsUsingDRPolicy(
	k8sclient client.Client,
	log logr.Logger,
	drpolicy *rmn.DRPolicy,
) ([]*rmn.DRPlacementControl, error) {
	drpcs := &rmn.DRPlacementControlList{}
	if err := k8sclient.List(context.TODO(), drpcs); err != nil {
		log.Error(err, "Failed to list DRPCs", "drpolicy", drpolicy.GetName())

		return nil, err
	}

	found := []*rmn.DRPlacementControl{}

	for i := range drpcs.Items {
		drpc := &drpcs.Items[i]

		if drpc.Spec.DRPolicyRef.Name != drpolicy.GetName() {
			continue
		}

		log.Info("Found DRPC referencing drpolicy",
			"name", drpc.GetName(),
			"namespace", drpc.GetNamespace(),
			"drpolicy", drpolicy.GetName())

		found = append(found, drpc)
	}

	return found, nil
}

// DRPCsFailingOverToCluster lists DRPC resources that are failing over to the passed in drcluster
//
//nolint:gocognit
func DRPCsFailingOverToCluster(k8sclient client.Client, log logr.Logger, drcluster string) ([]DRPCAndPolicy, error) {
	drpolicies := &rmn.DRPolicyList{}
	if err := k8sclient.List(context.TODO(), drpolicies); err != nil {
		// TODO: If we get errors, do we still get an event later and/or for all changes from where we
		// processed the last DRCluster update?
		log.Error(err, "Failed to list DRPolicies")

		return nil, err
	}

	drpcCollections := make([]DRPCAndPolicy, 0)

	for drpolicyIdx := range drpolicies.Items {
		drpolicy := &drpolicies.Items[drpolicyIdx]

		if rmnutil.DrpolicyContainsDrcluster(drpolicy, drcluster) {
			drClusters, err := GetDRClusters(context.TODO(), k8sclient, drpolicy)
			if err != nil || len(drClusters) <= 1 {
				log.Error(err, "Failed to get DRClusters")

				return nil, err
			}

			// Skip if policy is of type metro, fake the from and to cluster
			if metro, _ := dRPolicySupportsMetro(drpolicy, drClusters); metro {
				log.Info("Sync DRPolicy detected, skipping!")

				break
			}

			log.Info("Processing DRPolicy referencing DRCluster", "drpolicy", drpolicy.GetName())

			drpcs, err := DRPCsFailingOverToClusterForPolicy(k8sclient, log, drpolicy, drcluster)
			if err != nil {
				return nil, err
			}

			for idx := range drpcs {
				dprcCollection := DRPCAndPolicy{
					drpc:     drpcs[idx],
					drPolicy: drpolicy,
				}

				drpcCollections = append(drpcCollections, dprcCollection)
			}
		}
	}

	return drpcCollections, nil
}

// DRPCsFailingOverToClusterForPolicy filters DRPC resources that reference the DRPolicy and are failing over
// to the target cluster passed in
//
//nolint:gocognit
func DRPCsFailingOverToClusterForPolicy(
	k8sclient client.Client,
	log logr.Logger,
	drpolicy *rmn.DRPolicy,
	drcluster string,
) ([]*rmn.DRPlacementControl, error) {
	drpcs := &rmn.DRPlacementControlList{}
	if err := k8sclient.List(context.TODO(), drpcs); err != nil {
		log.Error(err, "Failed to list DRPCs", "drpolicy", drpolicy.GetName())

		return nil, err
	}

	filteredDRPCs := make([]*rmn.DRPlacementControl, 0)

	for idx := range drpcs.Items {
		drpc := &drpcs.Items[idx]

		if drpc.Spec.DRPolicyRef.Name != drpolicy.GetName() {
			continue
		}

		if rmnutil.ResourceIsDeleted(drpc) {
			continue
		}

		if !(drpc.Spec.Action == rmn.ActionFailover && drpc.Spec.FailoverCluster == drcluster) {
			continue
		}

		if condition := meta.FindStatusCondition(drpc.Status.Conditions, rmn.ConditionAvailable); condition != nil &&
			condition.Status == metav1.ConditionTrue &&
			condition.ObservedGeneration == drpc.Generation {
			continue
		}

		log.Info("DRPC detected as failing over to cluster",
			"name", drpc.GetName(),
			"namespace", drpc.GetNamespace(),
			"drpolicy", drpolicy.GetName())

		filteredDRPCs = append(filteredDRPCs, drpc)
	}

	return filteredDRPCs, nil
}

// SetupWithManager sets up the controller with the Manager.
//
//nolint:funlen
func (r *DRPlacementControlReconciler) SetupWithManager(mgr ctrl.Manager) error {
	mwPred := ManifestWorkPredicateFunc()

	mwMapFun := handler.EnqueueRequestsFromMapFunc(handler.MapFunc(
		func(ctx context.Context, obj client.Object) []reconcile.Request {
			mw, ok := obj.(*ocmworkv1.ManifestWork)
			if !ok {
				return []reconcile.Request{}
			}

			ctrl.Log.Info(fmt.Sprintf("DRPC: Filtering ManifestWork (%s/%s)", mw.Name, mw.Namespace))

			return filterMW(mw)
		}))

	mcvPred := ManagedClusterViewPredicateFunc()

	mcvMapFun := handler.EnqueueRequestsFromMapFunc(handler.MapFunc(
		func(ctx context.Context, obj client.Object) []reconcile.Request {
			mcv, ok := obj.(*viewv1beta1.ManagedClusterView)
			if !ok {
				return []reconcile.Request{}
			}

			ctrl.Log.Info(fmt.Sprintf("DRPC: Filtering MCV (%s/%s)", mcv.Name, mcv.Namespace))

			return filterMCV(mcv)
		}))

	usrPlRulePred := PlacementRulePredicateFunc()

	usrPlRuleMapFun := handler.EnqueueRequestsFromMapFunc(handler.MapFunc(
		func(ctx context.Context, obj client.Object) []reconcile.Request {
			usrPlRule, ok := obj.(*plrv1.PlacementRule)
			if !ok {
				return []reconcile.Request{}
			}

			ctrl.Log.Info(fmt.Sprintf("DRPC: Filtering User PlacementRule (%s/%s)", usrPlRule.Name, usrPlRule.Namespace))

			return filterUsrPlRule(usrPlRule)
		}))

	usrPlmntPred := PlacementPredicateFunc()

	usrPlmntMapFun := handler.EnqueueRequestsFromMapFunc(handler.MapFunc(
		func(ctx context.Context, obj client.Object) []reconcile.Request {
			usrPlmnt, ok := obj.(*clrapiv1beta1.Placement)
			if !ok {
				return []reconcile.Request{}
			}

			ctrl.Log.Info(fmt.Sprintf("DRPC: Filtering User Placement (%s/%s)", usrPlmnt.Name, usrPlmnt.Namespace))

			return filterUsrPlmnt(usrPlmnt)
		}))

	drClusterPred := DRClusterPredicateFunc()

	drClusterMapFun := handler.EnqueueRequestsFromMapFunc(handler.MapFunc(
		func(ctx context.Context, obj client.Object) []reconcile.Request {
			drCluster, ok := obj.(*rmn.DRCluster)
			if !ok {
				return []reconcile.Request{}
			}

			ctrl.Log.Info(fmt.Sprintf("DRPC Map: Filtering DRCluster (%s)", drCluster.Name))

			return r.FilterDRCluster(drCluster)
		}))

	r.eventRecorder = rmnutil.NewEventReporter(mgr.GetEventRecorderFor("controller_DRPlacementControl"))

	options := ctrlcontroller.Options{
		MaxConcurrentReconciles: getMaxConcurrentReconciles(ctrl.Log),
	}
	if r.RateLimiter != nil {
		options.RateLimiter = *r.RateLimiter
	}

	return ctrl.NewControllerManagedBy(mgr).
		WithOptions(options).
		For(&rmn.DRPlacementControl{}).
		Watches(&ocmworkv1.ManifestWork{}, mwMapFun, builder.WithPredicates(mwPred)).
		Watches(&viewv1beta1.ManagedClusterView{}, mcvMapFun, builder.WithPredicates(mcvPred)).
		Watches(&plrv1.PlacementRule{}, usrPlRuleMapFun, builder.WithPredicates(usrPlRulePred)).
		Watches(&clrapiv1beta1.Placement{}, usrPlmntMapFun, builder.WithPredicates(usrPlmntPred)).
		Watches(&rmn.DRCluster{}, drClusterMapFun, builder.WithPredicates(drClusterPred)).
		Complete(r)
}

//nolint:lll
// +kubebuilder:rbac:groups=ramendr.openshift.io,resources=drplacementcontrols,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ramendr.openshift.io,resources=drplacementcontrols/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ramendr.openshift.io,resources=drplacementcontrols/finalizers,verbs=update
// +kubebuilder:rbac:groups=ramendr.openshift.io,resources=drpolicies,verbs=get;list;watch
// +kubebuilder:rbac:groups=ramendr.openshift.io,resources=drclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=apps.open-cluster-management.io,resources=placementrules,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps.open-cluster-management.io,resources=placementrules/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps.open-cluster-management.io,resources=placementrules/finalizers,verbs=get;create;update;patch;delete
// +kubebuilder:rbac:groups=cluster.open-cluster-management.io,resources=managedclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=work.open-cluster-management.io,resources=manifestworks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=view.open-cluster-management.io,resources=managedclusterviews,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=policy.open-cluster-management.io,resources=policies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=policy.open-cluster-management.io,resources=placementbindings,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=events,verbs=get;create;patch;update
// +kubebuilder:rbac:groups=cluster.open-cluster-management.io,resources=placementdecisions,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cluster.open-cluster-management.io,resources=placementdecisions/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cluster.open-cluster-management.io,resources=placements,verbs=get;list;watch;update
// +kubebuilder:rbac:groups=cluster.open-cluster-management.io,resources=placements/finalizers,verbs=update
// +kubebuilder:rbac:groups=argoproj.io,resources=applicationsets,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the DRPlacementControl object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.7.0/pkg/reconcile
//
//nolint:funlen,gocognit,gocyclo,cyclop
func (r *DRPlacementControlReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := r.Log.WithValues("DRPC", req.NamespacedName, "rid", uuid.New())

	logger.Info("Entering reconcile loop")
	defer logger.Info("Exiting reconcile loop")

	drpc := &rmn.DRPlacementControl{}

	err := r.APIReader.Get(ctx, req.NamespacedName, drpc)
	if err != nil {
		if errors.IsNotFound(err) {
			logger.Info(fmt.Sprintf("DRPC object not found %v", req.NamespacedName))
			// Request object not found, could have been deleted after reconcile request.
			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, errorswrapper.Wrap(err, "failed to get DRPC object")
	}

	// Save a copy of the instance status to be used for the VRG status update comparison
	drpc.Status.DeepCopyInto(&r.savedInstanceStatus)

	ensureDRPCConditionsInited(&drpc.Status.Conditions, drpc.Generation, "Initialization")

	_, ramenConfig, err := ConfigMapGet(ctx, r.APIReader)
	if err != nil {
		err = fmt.Errorf("failed to get the ramen configMap: %w", err)
		r.recordFailure(ctx, drpc, nil, "Error", err.Error(), logger)

		return ctrl.Result{}, err
	}

	var placementObj client.Object

	placementObj, err = getPlacementOrPlacementRule(ctx, r.Client, drpc, logger)
	if err != nil && !(errors.IsNotFound(err) && rmnutil.ResourceIsDeleted(drpc)) {
		r.recordFailure(ctx, drpc, placementObj, "Error", err.Error(), logger)

		return ctrl.Result{}, err
	}

	if isBeingDeleted(drpc, placementObj) {
		// DPRC depends on User PlacementRule/Placement. If DRPC or/and the User PlacementRule is deleted,
		// then the DRPC should be deleted as well. The least we should do here is to clean up DPRC.
		err := r.processDeletion(ctx, drpc, placementObj, logger)
		if err != nil {
			logger.Info(fmt.Sprintf("Error in deleting DRPC: (%v)", err))

			statusErr := r.setDeletionStatusAndUpdate(ctx, drpc)
			if statusErr != nil {
				err = fmt.Errorf("drpc deletion failed: %w and status update failed: %w", err, statusErr)
			}

			return ctrl.Result{}, err
		}

		return ctrl.Result{}, nil
	}

	err = ensureDRPCValidNamespace(drpc, ramenConfig)
	if err != nil {
		r.recordFailure(ctx, drpc, placementObj, "Error", err.Error(), logger)

		return ctrl.Result{}, err
	}

	err = r.ensureNoConflictingDRPCs(ctx, drpc, ramenConfig, logger)
	if err != nil {
		r.recordFailure(ctx, drpc, placementObj, "Error", err.Error(), logger)

		return ctrl.Result{}, err
	}

	drPolicy, err := r.getAndEnsureValidDRPolicy(ctx, drpc, logger)
	if err != nil {
		r.recordFailure(ctx, drpc, placementObj, "Error", err.Error(), logger)

		return ctrl.Result{}, err
	}

	// Updates labels, finalizers and set the placement as the owner of the DRPC
	updated, err := r.updateAndSetOwner(ctx, drpc, placementObj, logger)
	if err != nil {
		return ctrl.Result{}, err
	}

	if updated {
		// Reload before proceeding
		return ctrl.Result{Requeue: true}, nil
	}

	// Rebuild DRPC state if needed
	requeue, err := r.ensureDRPCStatusConsistency(ctx, drpc, drPolicy, placementObj, logger)
	if err != nil {
		return ctrl.Result{}, err
	}

	if requeue {
		return ctrl.Result{Requeue: true}, r.updateDRPCStatus(ctx, drpc, placementObj, logger)
	}

	d, err := r.createDRPCInstance(ctx, drPolicy, drpc, placementObj, ramenConfig, logger)
	if err != nil && !errorswrapper.Is(err, InitialWaitTimeForDRPCPlacementRule) {
		err2 := r.updateDRPCStatus(ctx, drpc, placementObj, logger)

		return ctrl.Result{}, fmt.Errorf("failed to create DRPC instance (%w) and (%v)", err, err2)
	}

	if errorswrapper.Is(err, InitialWaitTimeForDRPCPlacementRule) {
		const initialWaitTime = 5

		r.recordFailure(ctx, drpc, placementObj, "Waiting",
			fmt.Sprintf("%v - wait time: %v", InitialWaitTimeForDRPCPlacementRule, initialWaitTime), logger)

		return ctrl.Result{RequeueAfter: time.Second * initialWaitTime}, nil
	}

	return r.reconcileDRPCInstance(d, logger)
}

func (r *DRPlacementControlReconciler) setDeletionStatusAndUpdate(
	ctx context.Context, drpc *rmn.DRPlacementControl,
) error {
	updated := updateDRPCProgression(drpc, rmn.ProgressionDeleting, r.Log)
	drpc.Status.Phase = rmn.Deleting
	drpc.Status.ObservedGeneration = drpc.Generation

	if updated {
		if err := r.Status().Update(ctx, drpc); err != nil {
			return fmt.Errorf("failed to update DRPC status: (%w)", err)
		}
	}

	return nil
}

func (r *DRPlacementControlReconciler) recordFailure(ctx context.Context, drpc *rmn.DRPlacementControl,
	placementObj client.Object, reason, msg string, log logr.Logger,
) {
	needsUpdate := addOrUpdateCondition(&drpc.Status.Conditions, rmn.ConditionAvailable,
		drpc.Generation, metav1.ConditionFalse, reason, msg)
	if needsUpdate {
		err := r.updateDRPCStatus(ctx, drpc, placementObj, log)
		if err != nil {
			log.Info(fmt.Sprintf("Failed to update DRPC status (%v)", err))
		}
	}
}

func (r *DRPlacementControlReconciler) setLastSyncTimeMetric(syncMetrics *SyncTimeMetrics,
	t *metav1.Time, log logr.Logger,
) {
	if syncMetrics == nil {
		return
	}

	log.Info(fmt.Sprintf("Setting metric: (%s)", LastSyncTimestampSeconds))

	if t == nil {
		syncMetrics.LastSyncTime.Set(0)

		return
	}

	syncMetrics.LastSyncTime.Set(float64(t.ProtoTime().Seconds))
}

func (r *DRPlacementControlReconciler) setLastSyncDurationMetric(syncDurationMetrics *SyncDurationMetrics,
	t *metav1.Duration, log logr.Logger,
) {
	if syncDurationMetrics == nil {
		return
	}

	log.Info(fmt.Sprintf("setting metric: (%s)", LastSyncDurationSeconds))

	if t == nil {
		syncDurationMetrics.LastSyncDuration.Set(0)

		return
	}

	syncDurationMetrics.LastSyncDuration.Set(t.Seconds())
}

func (r *DRPlacementControlReconciler) setLastSyncBytesMetric(syncDataBytesMetrics *SyncDataBytesMetrics,
	b *int64, log logr.Logger,
) {
	if syncDataBytesMetrics == nil {
		return
	}

	log.Info(fmt.Sprintf("setting metric: (%s)", LastSyncDataBytes))

	if b == nil {
		syncDataBytesMetrics.LastSyncDataBytes.Set(0)

		return
	}

	syncDataBytesMetrics.LastSyncDataBytes.Set(float64(*b))
}

// setWorkloadProtectionMetric sets the workload protection info metric, where 0 indicates not protected and
// 1 indicates protected
func (r *DRPlacementControlReconciler) setWorkloadProtectionMetric(workloadProtectionMetrics *WorkloadProtectionMetrics,
	conditions []metav1.Condition, log logr.Logger,
) {
	if workloadProtectionMetrics == nil {
		return
	}

	log.Info(fmt.Sprintf("setting metric: (%s)", WorkloadProtectionStatus))

	protected := 0

	for idx := range conditions {
		if conditions[idx].Type == rmn.ConditionProtected && conditions[idx].Status == metav1.ConditionTrue {
			protected = 1

			break
		}
	}

	workloadProtectionMetrics.WorkloadProtectionStatus.Set(float64(protected))
}

//nolint:funlen
func (r *DRPlacementControlReconciler) createDRPCInstance(
	ctx context.Context,
	drPolicy *rmn.DRPolicy,
	drpc *rmn.DRPlacementControl,
	placementObj client.Object,
	ramenConfig *rmn.RamenConfig,
	log logr.Logger,
) (*DRPCInstance, error) {
	log.Info("Creating DRPC instance")

	drClusters, err := GetDRClusters(ctx, r.Client, drPolicy)
	if err != nil {
		return nil, err
	}

	// We only create DRPC PlacementRule if the preferred cluster is not configured
	err = r.getDRPCPlacementRule(ctx, drpc, placementObj, drPolicy, log)
	if err != nil {
		return nil, err
	}

	vrgNamespace, err := selectVRGNamespace(r.Client, r.Log, drpc, placementObj)
	if err != nil {
		return nil, err
	}

	vrgs, _, _, err := getVRGsFromManagedClusters(r.MCVGetter, drpc, drClusters, vrgNamespace, log)
	if err != nil {
		return nil, err
	}

	d := &DRPCInstance{
		reconciler:      r,
		ctx:             ctx,
		log:             log,
		instance:        drpc,
		userPlacement:   placementObj,
		drPolicy:        drPolicy,
		drClusters:      drClusters,
		vrgs:            vrgs,
		vrgNamespace:    vrgNamespace,
		volSyncDisabled: ramenConfig.VolSync.Disabled,
		ramenConfig:     ramenConfig,
		mwu: rmnutil.MWUtil{
			Client:          r.Client,
			APIReader:       r.APIReader,
			Ctx:             ctx,
			Log:             log,
			InstName:        drpc.Name,
			TargetNamespace: vrgNamespace,
		},
	}

	d.drType = DRTypeAsync

	isMetro, _ := dRPolicySupportsMetro(drPolicy, drClusters)
	if isMetro {
		d.volSyncDisabled = true
		d.drType = DRTypeSync

		log.Info("volsync is set to disabled")
	}

	if !d.volSyncDisabled && drpcInAdminNamespace(drpc, ramenConfig) {
		d.volSyncDisabled = !ramenConfig.MultiNamespace.VolsyncSupported
	}

	// Save the instance status
	d.instance.Status.DeepCopyInto(&d.savedInstanceStatus)

	return d, nil
}

func (r *DRPlacementControlReconciler) createSyncMetricsInstance(
	drPolicy *rmn.DRPolicy, drpc *rmn.DRPlacementControl,
) *SyncMetrics {
	syncTimeMetricLabels := SyncTimeMetricLabels(drPolicy, drpc)
	syncTimeMetrics := NewSyncTimeMetric(syncTimeMetricLabels)

	syncDurationMetricLabels := SyncDurationMetricLabels(drPolicy, drpc)
	syncDurationMetrics := NewSyncDurationMetric(syncDurationMetricLabels)

	syncDataBytesLabels := SyncDataBytesMetricLabels(drPolicy, drpc)
	syncDataMetrics := NewSyncDataBytesMetric(syncDataBytesLabels)

	return &SyncMetrics{
		SyncTimeMetrics:      syncTimeMetrics,
		SyncDurationMetrics:  syncDurationMetrics,
		SyncDataBytesMetrics: syncDataMetrics,
	}
}

func (r *DRPlacementControlReconciler) createWorkloadProtectionMetricsInstance(
	drpc *rmn.DRPlacementControl,
) *WorkloadProtectionMetrics {
	workloadProtectionLabels := WorkloadProtectionStatusLabels(drpc)
	workloadProtectionMetrics := NewWorkloadProtectionStatusMetric(workloadProtectionLabels)

	return &WorkloadProtectionMetrics{
		WorkloadProtectionStatus: workloadProtectionMetrics.WorkloadProtectionStatus,
	}
}

// isBeingDeleted returns true if either DRPC, user placement, or both are being deleted
func isBeingDeleted(drpc *rmn.DRPlacementControl, usrPl client.Object) bool {
	return rmnutil.ResourceIsDeleted(drpc) ||
		(usrPl != nil && rmnutil.ResourceIsDeleted(usrPl))
}

func (r *DRPlacementControlReconciler) reconcileDRPCInstance(d *DRPCInstance, log logr.Logger) (ctrl.Result, error) {
	// Last status update time BEFORE we start processing
	var beforeProcessing metav1.Time
	if d.instance.Status.LastUpdateTime != nil {
		beforeProcessing = *d.instance.Status.LastUpdateTime
	}

	if !ensureVRGsManagedByDRPC(d.log, d.mwu, d.vrgs, d.instance, d.vrgNamespace) {
		log.Info("Requeing... VRG adoption in progress")

		return ctrl.Result{Requeue: true}, nil
	}

	requeue := d.startProcessing()
	log.Info("Finished processing", "Requeue?", requeue)

	if !requeue {
		log.Info("Done reconciling", "state", d.getLastDRState())
		r.Callback(d.instance.Name, string(d.getLastDRState()))
	}

	if d.mcvRequestInProgress && d.getLastDRState() != "" {
		duration := d.getRequeueDuration()
		log.Info(fmt.Sprintf("Requeing after %v", duration))

		return reconcile.Result{RequeueAfter: duration}, nil
	}

	if requeue {
		log.Info("Requeing...")

		return ctrl.Result{Requeue: true}, nil
	}

	// Last status update time AFTER processing
	var afterProcessing metav1.Time
	if d.instance.Status.LastUpdateTime != nil {
		afterProcessing = *d.instance.Status.LastUpdateTime
	}

	requeueTimeDuration := r.getStatusCheckDelay(beforeProcessing, afterProcessing)
	log.Info("Requeue time", "duration", requeueTimeDuration)

	return ctrl.Result{RequeueAfter: requeueTimeDuration}, nil
}

func (r *DRPlacementControlReconciler) getAndEnsureValidDRPolicy(ctx context.Context,
	drpc *rmn.DRPlacementControl, log logr.Logger,
) (*rmn.DRPolicy, error) {
	drPolicy, err := GetDRPolicy(ctx, r.Client, drpc, log)
	if err != nil {
		return nil, fmt.Errorf("failed to get DRPolicy %w", err)
	}

	if rmnutil.ResourceIsDeleted(drPolicy) {
		// If drpolicy is deleted then return
		// error to fail drpc reconciliation
		return nil, fmt.Errorf("drPolicy '%s' referred by the DRPC is deleted, DRPC reconciliation would fail",
			drpc.Spec.DRPolicyRef.Name)
	}

	if err := rmnutil.DrpolicyValidated(drPolicy); err != nil {
		return nil, fmt.Errorf("DRPolicy not valid %w", err)
	}

	return drPolicy, nil
}

func GetDRPolicy(ctx context.Context, client client.Client,
	drpc *rmn.DRPlacementControl, log logr.Logger,
) (*rmn.DRPolicy, error) {
	drPolicy := &rmn.DRPolicy{}
	name := drpc.Spec.DRPolicyRef.Name
	namespace := drpc.Spec.DRPolicyRef.Namespace

	err := client.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, drPolicy)
	if err != nil {
		log.Error(err, "failed to get DRPolicy")

		return nil, fmt.Errorf("%w", err)
	}

	return drPolicy, nil
}

func GetDRClusters(ctx context.Context, client client.Client, drPolicy *rmn.DRPolicy) ([]rmn.DRCluster, error) {
	drClusters := []rmn.DRCluster{}

	for _, managedCluster := range rmnutil.DRPolicyClusterNames(drPolicy) {
		drCluster := &rmn.DRCluster{}

		err := client.Get(ctx, types.NamespacedName{Name: managedCluster}, drCluster)
		if err != nil {
			return nil, fmt.Errorf("failed to get DRCluster (%s) %w", managedCluster, err)
		}

		// TODO: What if the DRCluster is deleted? If new DRPC fail reconciliation
		drClusters = append(drClusters, *drCluster)
	}

	return drClusters, nil
}

// updateObjectMetadata updates drpc labels, annotations and finalizer, and also updates placementObj finalizer
func (r DRPlacementControlReconciler) updateObjectMetadata(ctx context.Context,
	drpc *rmn.DRPlacementControl, placementObj client.Object, log logr.Logger,
) error {
	update := false

	update = rmnutil.AddLabel(drpc, rmnutil.OCMBackupLabelKey, rmnutil.OCMBackupLabelValue)
	update = rmnutil.AddFinalizer(drpc, DRPCFinalizer) || update

	vrgNamespace, err := selectVRGNamespace(r.Client, r.Log, drpc, placementObj)
	if err != nil {
		return err
	}

	update = rmnutil.AddAnnotation(drpc, DRPCAppNamespace, vrgNamespace) || update

	if update {
		if err := r.Update(ctx, drpc); err != nil {
			log.Error(err, "Failed to add annotations, labels, or finalizer to drpc")

			return fmt.Errorf("%w", err)
		}
	}

	// add finalizer to User PlacementRule/Placement
	finalizerAdded := rmnutil.AddFinalizer(placementObj, DRPCFinalizer)
	if finalizerAdded {
		if err := r.Update(ctx, placementObj); err != nil {
			log.Error(err, "Failed to add finalizer to user placement rule")

			return fmt.Errorf("%w", err)
		}
	}

	return nil
}

func (r *DRPlacementControlReconciler) processDeletion(ctx context.Context,
	drpc *rmn.DRPlacementControl, placementObj client.Object, log logr.Logger,
) error {
	log.Info("Processing DRPC deletion")

	if !controllerutil.ContainsFinalizer(drpc, DRPCFinalizer) {
		return nil
	}

	// Run finalization logic for dprc.
	// If the finalization logic fails, don't remove the finalizer so
	// that we can retry during the next reconciliation.
	if err := r.finalizeDRPC(ctx, drpc, placementObj, log); err != nil {
		return err
	}

	if placementObj != nil && controllerutil.ContainsFinalizer(placementObj, DRPCFinalizer) {
		if err := r.finalizePlacement(ctx, placementObj); err != nil {
			return err
		}
	}

	updateDRPCProgression(drpc, rmn.ProgressionDeleted, r.Log)
	// Remove DRPCFinalizer from DRPC.
	controllerutil.RemoveFinalizer(drpc, DRPCFinalizer)

	if err := r.Update(ctx, drpc); err != nil {
		return fmt.Errorf("failed to update drpc %w", err)
	}

	r.Callback(drpc.Name, "deleted")

	return nil
}

//nolint:funlen,cyclop
func (r *DRPlacementControlReconciler) finalizeDRPC(ctx context.Context, drpc *rmn.DRPlacementControl,
	placementObj client.Object, log logr.Logger,
) error {
	log.Info("Finalizing DRPC")

	clonedPlRuleName := fmt.Sprintf(ClonedPlacementRuleNameFormat, drpc.Name, drpc.Namespace)
	// delete cloned placementrule, if one created.
	if drpc.Spec.PreferredCluster == "" {
		err := r.deleteClonedPlacementRule(ctx, clonedPlRuleName, drpc.Namespace, log)
		if err != nil {
			return err
		}
	}

	// Cleanup volsync secret-related resources (policy/plrule/binding)
	err := volsync.CleanupSecretPropagation(ctx, r.Client, drpc, r.Log)
	if err != nil {
		return fmt.Errorf("failed to clean up volsync secret-related resources (%w)", err)
	}

	vrgNamespace, err := selectVRGNamespace(r.Client, r.Log, drpc, placementObj)
	if err != nil {
		return err
	}

	mwu := rmnutil.MWUtil{
		Client:          r.Client,
		APIReader:       r.APIReader,
		Ctx:             ctx,
		Log:             r.Log,
		InstName:        drpc.Name,
		TargetNamespace: vrgNamespace,
	}

	drPolicy, err := GetDRPolicy(ctx, r.Client, drpc, log)
	if err != nil {
		return fmt.Errorf("failed to get DRPolicy while finalizing DRPC (%w)", err)
	}

	drClusters, err := GetDRClusters(ctx, r.Client, drPolicy)
	if err != nil {
		return fmt.Errorf("failed to get drclusters. Error (%w)", err)
	}

	// Verify VRGs have been deleted
	vrgs, _, _, err := getVRGsFromManagedClusters(r.MCVGetter, drpc, drClusters, vrgNamespace, log)
	if err != nil {
		return fmt.Errorf("failed to retrieve VRGs. We'll retry later. Error (%w)", err)
	}

	if !ensureVRGsManagedByDRPC(r.Log, mwu, vrgs, drpc, vrgNamespace) {
		return fmt.Errorf("VRG adoption in progress")
	}

	// delete manifestworks (VRGs)
	for _, drClusterName := range rmnutil.DRPolicyClusterNames(drPolicy) {
		err := mwu.DeleteManifestWorksForCluster(drClusterName)
		if err != nil {
			return fmt.Errorf("%w", err)
		}
	}

	if len(vrgs) != 0 {
		return fmt.Errorf("waiting for VRGs count to go to zero")
	}

	// delete MCVs used in the previous call
	if err := r.deleteAllManagedClusterViews(drpc, rmnutil.DRPolicyClusterNames(drPolicy)); err != nil {
		return fmt.Errorf("error in deleting MCV (%w)", err)
	}

	// delete metrics if matching labels are found
	syncTimeMetricLabels := SyncTimeMetricLabels(drPolicy, drpc)
	DeleteSyncTimeMetric(syncTimeMetricLabels)

	syncDurationMetricLabels := SyncDurationMetricLabels(drPolicy, drpc)
	DeleteSyncDurationMetric(syncDurationMetricLabels)

	syncDataBytesMetricLabels := SyncDataBytesMetricLabels(drPolicy, drpc)
	DeleteSyncDataBytesMetric(syncDataBytesMetricLabels)

	workloadProtectionLabels := WorkloadProtectionStatusLabels(drpc)
	DeleteWorkloadProtectionStatusMetric(workloadProtectionLabels)

	return nil
}

func (r *DRPlacementControlReconciler) deleteAllManagedClusterViews(
	drpc *rmn.DRPlacementControl, clusterNames []string,
) error {
	// Only after the VRGs have been deleted, we delete the MCVs for the VRGs and the NS
	for _, drClusterName := range clusterNames {
		err := r.MCVGetter.DeleteVRGManagedClusterView(drpc.Name, drpc.Namespace, drClusterName, rmnutil.MWTypeVRG)
		// Delete MCV for the VRG
		if err != nil {
			return fmt.Errorf("failed to delete VRG MCV %w", err)
		}

		err = r.MCVGetter.DeleteNamespaceManagedClusterView(drpc.Name, drpc.Namespace, drClusterName, rmnutil.MWTypeNS)
		// Delete MCV for Namespace
		if err != nil {
			return fmt.Errorf("failed to delete namespace MCV %w", err)
		}
	}

	return nil
}

func (r *DRPlacementControlReconciler) getDRPCPlacementRule(ctx context.Context,
	drpc *rmn.DRPlacementControl, placementObj client.Object,
	drPolicy *rmn.DRPolicy, log logr.Logger,
) error {
	var drpcPlRule *plrv1.PlacementRule
	// create the cloned placementrule if and only if the Spec.PreferredCluster is not provided
	if drpc.Spec.PreferredCluster == "" {
		var err error

		plRule := ConvertToPlacementRule(placementObj)
		if plRule == nil {
			return fmt.Errorf("invalid user PlacementRule")
		}

		drpcPlRule, err = r.getOrClonePlacementRule(ctx, drpc, drPolicy, plRule, log)
		if err != nil {
			log.Error(err, "failed to get DRPC PlacementRule")

			return err
		}

		// Make sure that we give time to the DRPC PlacementRule to run and produces decisions
		if drpcPlRule != nil && len(drpcPlRule.Status.Decisions) == 0 {
			return fmt.Errorf("%w", InitialWaitTimeForDRPCPlacementRule)
		}
	} else {
		log.Info("Preferred cluster is configured. Dynamic selection is disabled",
			"PreferredCluster", drpc.Spec.PreferredCluster)
	}

	return nil
}

func (r *DRPlacementControlReconciler) finalizePlacement(
	ctx context.Context,
	placementObj client.Object,
) error {
	controllerutil.RemoveFinalizer(placementObj, DRPCFinalizer)

	err := r.Update(ctx, placementObj)
	if err != nil {
		return fmt.Errorf("failed to update User PlacementRule/Placement %w", err)
	}

	return nil
}

func (r *DRPlacementControlReconciler) updateAndSetOwner(
	ctx context.Context,
	drpc *rmn.DRPlacementControl,
	usrPlacement client.Object,
	log logr.Logger,
) (bool, error) {
	if err := r.annotateObject(ctx, drpc, usrPlacement, log); err != nil {
		return false, err
	}

	if err := r.updateObjectMetadata(ctx, drpc, usrPlacement, log); err != nil {
		return false, err
	}

	return r.setDRPCOwner(ctx, drpc, usrPlacement, log)
}

func getPlacementOrPlacementRule(
	ctx context.Context,
	k8sclient client.Client,
	drpc *rmn.DRPlacementControl,
	log logr.Logger,
) (client.Object, error) {
	log.Info("Getting user placement object", "placementRef", drpc.Spec.PlacementRef)

	var usrPlacement client.Object

	var err error

	usrPlacement, err = getPlacementRule(ctx, k8sclient, drpc, log)
	if err != nil {
		if errors.IsNotFound(err) {
			// PacementRule not found. Check Placement instead
			usrPlacement, err = getPlacement(ctx, k8sclient, drpc, log)
		}

		if err != nil {
			return nil, err
		}
	} else {
		// Assert that there is no Placement object in the same namespace and with the same name as the PlacementRule
		_, err = getPlacement(ctx, k8sclient, drpc, log)
		if err == nil {
			return nil, fmt.Errorf(
				"can't proceed. PlacementRule and Placement CR with the same name exist on the same namespace")
		}
	}

	return usrPlacement, nil
}

func getPlacementRule(ctx context.Context, k8sclient client.Client,
	drpc *rmn.DRPlacementControl, log logr.Logger,
) (*plrv1.PlacementRule, error) {
	log.Info("Trying user PlacementRule", "usrPR", drpc.Spec.PlacementRef.Name+"/"+drpc.Spec.PlacementRef.Namespace)

	plRuleNamespace := drpc.Spec.PlacementRef.Namespace
	if plRuleNamespace == "" {
		plRuleNamespace = drpc.Namespace
	}

	if plRuleNamespace != drpc.Namespace {
		return nil, fmt.Errorf("referenced PlacementRule namespace (%s)"+
			" differs from DRPlacementControl resource namespace (%s)",
			drpc.Spec.PlacementRef.Namespace, drpc.Namespace)
	}

	usrPlRule := &plrv1.PlacementRule{}

	err := k8sclient.Get(ctx,
		types.NamespacedName{Name: drpc.Spec.PlacementRef.Name, Namespace: plRuleNamespace}, usrPlRule)
	if err != nil {
		log.Info(fmt.Sprintf("Get PlacementRule returned: %v", err))

		return nil, err
	}

	// If either DRPC or PlacementRule are being deleted, disregard everything else.
	if !isBeingDeleted(drpc, usrPlRule) {
		scName := usrPlRule.Spec.SchedulerName
		if scName != RamenScheduler {
			return nil, fmt.Errorf("placementRule %s does not have the ramen scheduler. Scheduler used %s",
				usrPlRule.Name, scName)
		}

		if usrPlRule.Spec.ClusterReplicas == nil || *usrPlRule.Spec.ClusterReplicas != 1 {
			log.Info("User PlacementRule replica count is not set to 1, reconciliation will only" +
				" schedule it to a single cluster")
		}
	}

	return usrPlRule, nil
}

func getPlacement(ctx context.Context, k8sclient client.Client,
	drpc *rmn.DRPlacementControl, log logr.Logger,
) (*clrapiv1beta1.Placement, error) {
	log.Info("Trying user Placement", "usrP", drpc.Spec.PlacementRef.Name+"/"+drpc.Spec.PlacementRef.Namespace)

	plmntNamespace := drpc.Spec.PlacementRef.Namespace
	if plmntNamespace == "" {
		plmntNamespace = drpc.Namespace
	}

	if plmntNamespace != drpc.Namespace {
		return nil, fmt.Errorf("referenced Placement namespace (%s)"+
			" differs from DRPlacementControl resource namespace (%s)",
			drpc.Spec.PlacementRef.Namespace, drpc.Namespace)
	}

	usrPlmnt := &clrapiv1beta1.Placement{}

	err := k8sclient.Get(ctx,
		types.NamespacedName{Name: drpc.Spec.PlacementRef.Name, Namespace: plmntNamespace}, usrPlmnt)
	if err != nil {
		log.Info(fmt.Sprintf("Get Placement returned: %v", err))

		return nil, err
	}

	// If either DRPC or Placement are being deleted, disregard everything else.
	if !isBeingDeleted(drpc, usrPlmnt) {
		if value, ok := usrPlmnt.GetAnnotations()[clrapiv1beta1.PlacementDisableAnnotation]; !ok || value == "false" {
			return nil, fmt.Errorf("placement %s must be disabled in order for Ramen to be the scheduler",
				usrPlmnt.Name)
		}

		if usrPlmnt.Spec.NumberOfClusters == nil || *usrPlmnt.Spec.NumberOfClusters != 1 {
			log.Info("User Placement number of clusters is not set to 1, reconciliation will only" +
				" schedule it to a single cluster")
		}
	}

	return usrPlmnt, nil
}

func (r *DRPlacementControlReconciler) annotateObject(ctx context.Context,
	drpc *rmn.DRPlacementControl, obj client.Object, log logr.Logger,
) error {
	if rmnutil.ResourceIsDeleted(obj) {
		return nil
	}

	if obj.GetAnnotations() == nil {
		obj.SetAnnotations(map[string]string{})
	}

	ownerName := obj.GetAnnotations()[DRPCNameAnnotation]
	ownerNamespace := obj.GetAnnotations()[DRPCNamespaceAnnotation]

	if ownerName == "" {
		obj.GetAnnotations()[DRPCNameAnnotation] = drpc.Name
		obj.GetAnnotations()[DRPCNamespaceAnnotation] = drpc.Namespace

		err := r.Update(ctx, obj)
		if err != nil {
			log.Error(err, "Failed to update Object annotation", "objName", obj.GetName())

			return fmt.Errorf("failed to update Object %s annotation '%s/%s' (%w)",
				obj.GetName(), DRPCNameAnnotation, drpc.Name, err)
		}

		return nil
	}

	if ownerName != drpc.Name || ownerNamespace != drpc.Namespace {
		log.Info("Object not owned by this DRPC", "objName", obj.GetName())

		return fmt.Errorf("object %s not owned by this DRPC '%s/%s'",
			obj.GetName(), drpc.Name, drpc.Namespace)
	}

	return nil
}

func (r *DRPlacementControlReconciler) setDRPCOwner(
	ctx context.Context, drpc *rmn.DRPlacementControl, owner client.Object, log logr.Logger,
) (bool, error) {
	const updated = true

	for _, ownerReference := range drpc.GetOwnerReferences() {
		if ownerReference.Name == owner.GetName() {
			return !updated, nil // ownerreference already set
		}
	}

	err := ctrl.SetControllerReference(owner, drpc, r.Client.Scheme())
	if err != nil {
		return !updated, fmt.Errorf("failed to set DRPC owner %w", err)
	}

	err = r.Update(ctx, drpc)
	if err != nil {
		return !updated, fmt.Errorf("failed to update drpc %s (%w)", drpc.GetName(), err)
	}

	log.Info(fmt.Sprintf("Object %s owns DRPC %s", owner.GetName(), drpc.GetName()))

	return updated, nil
}

func (r *DRPlacementControlReconciler) getOrClonePlacementRule(ctx context.Context,
	drpc *rmn.DRPlacementControl, drPolicy *rmn.DRPolicy,
	userPlRule *plrv1.PlacementRule, log logr.Logger,
) (*plrv1.PlacementRule, error) {
	log.Info("Getting PlacementRule or cloning it", "placement", drpc.Spec.PlacementRef)

	clonedPlRuleName := fmt.Sprintf(ClonedPlacementRuleNameFormat, drpc.Name, drpc.Namespace)

	clonedPlRule, err := r.getClonedPlacementRule(ctx, clonedPlRuleName, drpc.Namespace, log)
	if err != nil {
		if errors.IsNotFound(err) {
			clonedPlRule, err = r.clonePlacementRule(ctx, drPolicy, userPlRule, clonedPlRuleName, log)
			if err != nil {
				return nil, fmt.Errorf("failed to create cloned placementrule error: %w", err)
			}
		} else {
			log.Error(err, "Failed to get drpc placementRule", "name", clonedPlRuleName)

			return nil, err
		}
	}

	return clonedPlRule, nil
}

func (r *DRPlacementControlReconciler) getClonedPlacementRule(ctx context.Context,
	clonedPlRuleName, namespace string, log logr.Logger,
) (*plrv1.PlacementRule, error) {
	log.Info("Getting cloned PlacementRule", "name", clonedPlRuleName)

	clonedPlRule := &plrv1.PlacementRule{}

	err := r.Client.Get(ctx, types.NamespacedName{Name: clonedPlRuleName, Namespace: namespace}, clonedPlRule)
	if err != nil {
		return nil, fmt.Errorf("failed to get placementrule error: %w", err)
	}

	return clonedPlRule, nil
}

func (r *DRPlacementControlReconciler) clonePlacementRule(ctx context.Context,
	drPolicy *rmn.DRPolicy, userPlRule *plrv1.PlacementRule,
	clonedPlRuleName string, log logr.Logger,
) (*plrv1.PlacementRule, error) {
	log.Info("Creating a clone placementRule from", "name", userPlRule.Name)

	clonedPlRule := &plrv1.PlacementRule{}

	userPlRule.DeepCopyInto(clonedPlRule)

	clonedPlRule.Name = clonedPlRuleName
	clonedPlRule.ResourceVersion = ""
	clonedPlRule.Spec.SchedulerName = ""

	err := r.addClusterPeersToPlacementRule(drPolicy, clonedPlRule, log)
	if err != nil {
		log.Error(err, "Failed to add cluster peers to cloned placementRule", "name", clonedPlRuleName)

		return nil, err
	}

	err = r.Create(ctx, clonedPlRule)
	if err != nil {
		log.Error(err, "failed to clone placement rule", "name", clonedPlRule.Name)

		return nil, errorswrapper.Wrap(err, "failed to create PlacementRule")
	}

	return clonedPlRule, nil
}

func getVRGsFromManagedClusters(
	mcvGetter rmnutil.ManagedClusterViewGetter,
	drpc *rmn.DRPlacementControl,
	drClusters []rmn.DRCluster,
	vrgNamespace string,
	log logr.Logger,
) (map[string]*rmn.VolumeReplicationGroup, int, string, error) {
	vrgs := map[string]*rmn.VolumeReplicationGroup{}

	annotations := make(map[string]string)

	annotations[DRPCNameAnnotation] = drpc.Name
	annotations[DRPCNamespaceAnnotation] = drpc.Namespace

	var clustersQueriedSuccessfully int

	var failedCluster string

	for i := range drClusters {
		drCluster := &drClusters[i]

		vrg, err := mcvGetter.GetVRGFromManagedCluster(drpc.Name, vrgNamespace, drCluster.Name, annotations)
		if err != nil {
			// Only NotFound error is accepted
			if errors.IsNotFound(err) {
				log.Info(fmt.Sprintf("VRG not found on %q", drCluster.Name))
				clustersQueriedSuccessfully++

				continue
			}

			failedCluster = drCluster.Name

			log.Info(fmt.Sprintf("failed to retrieve VRG from %s. err (%v).", drCluster.Name, err))

			continue
		}

		clustersQueriedSuccessfully++

		if rmnutil.ResourceIsDeleted(drCluster) {
			log.Info("Skipping VRG on deleted drcluster", "drcluster", drCluster.Name, "vrg", vrg.Name)

			continue
		}

		vrgs[drCluster.Name] = vrg

		log.Info("VRG location", "VRG on", drCluster.Name)
	}

	// We are done if we successfully queried all drClusters
	if clustersQueriedSuccessfully == len(drClusters) {
		return vrgs, clustersQueriedSuccessfully, "", nil
	}

	if clustersQueriedSuccessfully == 0 {
		return vrgs, 0, "", fmt.Errorf("failed to retrieve VRGs from clusters")
	}

	return vrgs, clustersQueriedSuccessfully, failedCluster, nil
}

func (r *DRPlacementControlReconciler) deleteClonedPlacementRule(ctx context.Context,
	name, namespace string, log logr.Logger,
) error {
	plRule, err := r.getClonedPlacementRule(ctx, name, namespace, log)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil
		}

		return err
	}

	err = r.Client.Delete(ctx, plRule)
	if err != nil {
		return fmt.Errorf("failed to delete cloned plRule %w", err)
	}

	return nil
}

func (r *DRPlacementControlReconciler) addClusterPeersToPlacementRule(
	drPolicy *rmn.DRPolicy, plRule *plrv1.PlacementRule, log logr.Logger,
) error {
	if len(rmnutil.DRPolicyClusterNames(drPolicy)) == 0 {
		return fmt.Errorf("DRPolicy %s is missing DR clusters", drPolicy.Name)
	}

	for _, v := range rmnutil.DRPolicyClusterNames(drPolicy) {
		plRule.Spec.Clusters = append(plRule.Spec.Clusters, plrv1.GenericClusterReference{Name: v})
	}

	log.Info(fmt.Sprintf("Added clusters %v to placementRule from DRPolicy %s", plRule.Spec.Clusters, drPolicy.Name))

	return nil
}

// statusUpdateTimeElapsed returns whether it is time to update DRPC status or not
// DRPC status is updated at least once every StatusCheckDelay in order to refresh
// the VRG status.
func (d *DRPCInstance) statusUpdateTimeElapsed() bool {
	if d.instance.Status.LastUpdateTime == nil {
		return false
	}

	return d.instance.Status.LastUpdateTime.Add(StatusCheckDelay).Before(time.Now())
}

// getStatusCheckDelay returns the reconciliation requeue time duration when no requeue
// has been requested. We want the reconciliation to run at least once every StatusCheckDelay
// in order to refresh DRPC status with VRG status. The reconciliation will be called at any time.
// If it is called before the StatusCheckDelay has elapsed, and the DRPC status was not updated,
// then we must return the remaining time rather than the full StatusCheckDelay to prevent
// starving the status update, which is scheduled for at least once every StatusCheckDelay.
//
// Example: Assume at 10:00am was the last time when the reconciler ran and updated the status.
// The StatusCheckDelay is hard coded to 10 minutes.  If nothing is happening in the system that
// requires the reconciler to run, then the next run would be at 10:10am. If however, for any reason
// the reconciler is called, let's say, at 10:08am, and no update to the DRPC status was needed,
// then the requeue time duration should be 2 minutes and NOT the full StatusCheckDelay. That is:
// 10:00am + StatusCheckDelay - 10:08am = 2mins
func (r *DRPlacementControlReconciler) getStatusCheckDelay(
	beforeProcessing metav1.Time, afterProcessing metav1.Time,
) time.Duration {
	if beforeProcessing != afterProcessing {
		// DRPC's VRG status update processing time has changed during this
		// iteration of the reconcile loop.  Hence, the next attempt to update
		// the status should be after a delay of a standard polling interval
		// duration.
		return StatusCheckDelay
	}

	// DRPC's VRG status update processing time has NOT changed during this
	// iteration of the reconcile loop.  Hence, the next attempt to update the
	// status should be after the remaining duration of this polling interval has
	// elapsed: (beforeProcessing + StatusCheckDelay - time.Now())
	return time.Until(beforeProcessing.Add(StatusCheckDelay))
}

// updateDRPCStatus updates the DRPC sub-resource status with,
// - the current instance DRPC status as updated during reconcile
// - any updated VRG status as needs to be reflected in DRPC
// It also updates latest metrics for the current instance of DRPC.
//
//nolint:cyclop
func (r *DRPlacementControlReconciler) updateDRPCStatus(
	ctx context.Context, drpc *rmn.DRPlacementControl, userPlacement client.Object, log logr.Logger,
) error {
	log.Info("Updating DRPC status")

	r.updateResourceCondition(ctx, drpc, userPlacement)

	// set metrics if DRPC is not being deleted and if finalizer exists
	if !isBeingDeleted(drpc, userPlacement) && controllerutil.ContainsFinalizer(drpc, DRPCFinalizer) {
		if err := r.setDRPCMetrics(ctx, drpc, log); err != nil {
			// log the error but do not return the error
			log.Info("Failed to set drpc metrics", "errMSg", err)
		}
	}

	// TODO: This is too generic, why are all conditions reported for the current generation?
	// Each condition should choose for itself, no?
	for i, condition := range drpc.Status.Conditions {
		if condition.ObservedGeneration != drpc.Generation {
			drpc.Status.Conditions[i].ObservedGeneration = drpc.Generation
		}
	}

	if reflect.DeepEqual(r.savedInstanceStatus, drpc.Status) {
		log.Info("No need to update DRPC Status")

		return nil
	}

	now := metav1.Now()
	drpc.Status.LastUpdateTime = &now

	if err := r.Status().Update(ctx, drpc); err != nil {
		return errorswrapper.Wrap(err, "failed to update DRPC status")
	}

	log.Info("Updated DRPC Status")

	return nil
}

// updateResourceCondition updates DRPC status sub-resource with updated status from VRG if one exists,
// - The status update is NOT intended for a VRG that should be cleaned up on a peer cluster
// It also updates DRPC ConditionProtected based on current state of VRG.
//
//nolint:funlen
func (r *DRPlacementControlReconciler) updateResourceCondition(
	ctx context.Context, drpc *rmn.DRPlacementControl, userPlacement client.Object,
) {
	vrgNamespace, err := selectVRGNamespace(r.Client, r.Log, drpc, userPlacement)
	if err != nil {
		r.Log.Info("Failed to select VRG namespace", "error", err)

		return
	}

	clusterName := r.clusterForVRGStatus(drpc, userPlacement, r.Log)
	if clusterName == "" {
		r.Log.Info("Unable to determine managed cluster from which to inspect VRG, " +
			"skipping processing ResourceConditions")

		return
	}

	annotations := make(map[string]string)
	annotations[DRPCNameAnnotation] = drpc.Name
	annotations[DRPCNamespaceAnnotation] = drpc.Namespace

	vrg, err := r.MCVGetter.GetVRGFromManagedCluster(drpc.Name, vrgNamespace,
		clusterName, annotations)
	if err != nil {
		r.Log.Info("Failed to get VRG from managed cluster. Trying s3 store...", "errMsg", err.Error())

		// The VRG from the s3 store might be stale, however, the worst case should be at most around 1 minute.
		vrg = GetLastKnownVRGPrimaryFromS3(ctx, r.APIReader,
			GetAvailableS3Profiles(ctx, r.Client, drpc, r.Log),
			drpc.GetName(), vrgNamespace, r.ObjStoreGetter, r.Log)
		if vrg == nil {
			r.Log.Info("Failed to get VRG from s3 store")

			drpc.Status.ResourceConditions = rmn.VRGConditions{}

			updateProtectedConditionUnknown(drpc, clusterName)

			return
		}

		if vrg.ResourceVersion < drpc.Status.ResourceConditions.ResourceMeta.ResourceVersion {
			r.Log.Info("VRG resourceVersion is lower than the previously recorded VRG's resourceVersion in DRPC")
			// if the VRG resourceVersion is less, then leave the DRPC ResourceCondtions.ResourceMeta.ResourceVersion as is.
			return
		}
	}

	drpc.Status.ResourceConditions.ResourceMeta.Kind = vrg.Kind
	drpc.Status.ResourceConditions.ResourceMeta.Name = vrg.Name
	drpc.Status.ResourceConditions.ResourceMeta.Namespace = vrg.Namespace
	drpc.Status.ResourceConditions.ResourceMeta.Generation = vrg.Generation
	drpc.Status.ResourceConditions.ResourceMeta.ResourceVersion = vrg.ResourceVersion
	drpc.Status.ResourceConditions.Conditions = vrg.Status.Conditions

	protectedPVCs := []string{}
	for _, protectedPVC := range vrg.Status.ProtectedPVCs {
		protectedPVCs = append(protectedPVCs, protectedPVC.Name)
	}

	drpc.Status.ResourceConditions.ResourceMeta.ProtectedPVCs = protectedPVCs

	if vrg.Status.LastGroupSyncTime != nil || drpc.Spec.Action != rmn.ActionRelocate {
		drpc.Status.LastGroupSyncTime = vrg.Status.LastGroupSyncTime
		drpc.Status.LastGroupSyncDuration = vrg.Status.LastGroupSyncDuration
		drpc.Status.LastGroupSyncBytes = vrg.Status.LastGroupSyncBytes
	}

	if vrg.Status.KubeObjectProtection.CaptureToRecoverFrom != nil {
		drpc.Status.LastKubeObjectProtectionTime = &vrg.Status.KubeObjectProtection.CaptureToRecoverFrom.EndTime
	}

	updateDRPCProtectedCondition(drpc, vrg, clusterName)
}

// clusterForVRGStatus determines which cluster's VRG should be inspected for status updates to DRPC
func (r *DRPlacementControlReconciler) clusterForVRGStatus(
	drpc *rmn.DRPlacementControl, userPlacement client.Object, log logr.Logger,
) string {
	clusterName := ""

	clusterDecision := r.getClusterDecision(userPlacement)
	if clusterDecision != nil && clusterDecision.ClusterName != "" {
		clusterName = clusterDecision.ClusterName
	}

	switch drpc.Spec.Action {
	case rmn.ActionFailover:
		// Failover can rely on inspecting VRG from clusterDecision as it is never made nil, hence till
		// placementDecision is changed to failoverCluster, we can inspect VRG from the existing cluster
		return clusterName
	case rmn.ActionRelocate:
		if drpc.Status.ObservedGeneration != drpc.Generation {
			log.Info("DPRC observedGeneration mismatches current generation, using ClusterDecision instead",
				"Cluster", clusterName)

			return clusterName
		}

		// We will inspect VRG from the non-preferredCluster until it reports Secondary, and then switch to the
		// preferredCluster. This is done using Status.Progression for the DRPC
		if IsPreRelocateProgression(drpc.Status.Progression) {
			if value, ok := drpc.GetAnnotations()[LastAppDeploymentCluster]; ok && value != "" {
				log.Info("Using cluster from LastAppDeploymentCluster annotation", "Cluster", value)

				return value
			}

			log.Info("DPRC missing LastAppDeploymentCluster annotation, using ClusterDecision instead",
				"Cluster", clusterName)

			return clusterName
		}

		log.Info("Using DRPC preferredCluster, Relocate progression detected as switching to preferred cluster")

		return drpc.Spec.PreferredCluster
	}

	// In cases of initial deployment use VRG from the preferredCluster
	log.Info("Using DRPC preferredCluster, initial deploy detected")

	return drpc.Spec.PreferredCluster
}

func (r *DRPlacementControlReconciler) setDRPCMetrics(ctx context.Context,
	drpc *rmn.DRPlacementControl, log logr.Logger,
) error {
	log.Info("setting WorkloadProtectionMetrics")

	workloadProtectionMetrics := r.createWorkloadProtectionMetricsInstance(drpc)
	r.setWorkloadProtectionMetric(workloadProtectionMetrics, drpc.Status.Conditions, log)

	drPolicy, err := GetDRPolicy(ctx, r.Client, drpc, log)
	if err != nil {
		return fmt.Errorf("failed to get DRPolicy %w", err)
	}

	drClusters, err := GetDRClusters(ctx, r.Client, drPolicy)
	if err != nil {
		return err
	}

	// do not set sync metrics if metro-dr
	isMetro, _ := dRPolicySupportsMetro(drPolicy, drClusters)
	if isMetro {
		return nil
	}

	log.Info("setting SyncMetrics")

	syncMetrics := r.createSyncMetricsInstance(drPolicy, drpc)

	if syncMetrics != nil {
		r.setLastSyncTimeMetric(&syncMetrics.SyncTimeMetrics, drpc.Status.LastGroupSyncTime, log)
		r.setLastSyncDurationMetric(&syncMetrics.SyncDurationMetrics, drpc.Status.LastGroupSyncDuration, log)
		r.setLastSyncBytesMetric(&syncMetrics.SyncDataBytesMetrics, drpc.Status.LastGroupSyncBytes, log)
	}

	return nil
}

func ConvertToPlacementRule(placementObj interface{}) *plrv1.PlacementRule {
	var pr *plrv1.PlacementRule

	if obj, ok := placementObj.(*plrv1.PlacementRule); ok {
		pr = obj
	}

	return pr
}

func ConvertToPlacement(placementObj interface{}) *clrapiv1beta1.Placement {
	var p *clrapiv1beta1.Placement

	if obj, ok := placementObj.(*clrapiv1beta1.Placement); ok {
		p = obj
	}

	return p
}

func (r *DRPlacementControlReconciler) getClusterDecision(placementObj interface{},
) *clrapiv1beta1.ClusterDecision {
	switch obj := placementObj.(type) {
	case *plrv1.PlacementRule:
		return r.getClusterDecisionFromPlacementRule(obj)
	case *clrapiv1beta1.Placement:
		return r.getClusterDecisionFromPlacement(obj)
	default:
		return &clrapiv1beta1.ClusterDecision{}
	}
}

func (r *DRPlacementControlReconciler) getClusterDecisionFromPlacementRule(plRule *plrv1.PlacementRule,
) *clrapiv1beta1.ClusterDecision {
	var clusterName string
	if len(plRule.Status.Decisions) > 0 {
		clusterName = plRule.Status.Decisions[0].ClusterName
	}

	return &clrapiv1beta1.ClusterDecision{
		ClusterName: clusterName,
		Reason:      "PlacementRule decision",
	}
}

// getPlacementDecisionFromPlacement returns a PlacementDecision for the passed in Placement if found, and nil otherwise
// - The PlacementDecision is determined by listing all PlacementDecisions in the Placement namespace filtered on the
// Placement label as set by OCM
// - Function also ensures there is only one decision for a Placement, as the needed by the Ramen orchestrators, and
// if not returns an error
func (r *DRPlacementControlReconciler) getPlacementDecisionFromPlacement(placement *clrapiv1beta1.Placement,
) (*clrapiv1beta1.PlacementDecision, error) {
	matchLabels := map[string]string{
		clrapiv1beta1.PlacementLabel: placement.GetName(),
	}

	listOptions := []client.ListOption{
		client.InNamespace(placement.GetNamespace()),
		client.MatchingLabels(matchLabels),
	}

	plDecisions := &clrapiv1beta1.PlacementDecisionList{}
	if err := r.List(context.TODO(), plDecisions, listOptions...); err != nil {
		return nil, fmt.Errorf("failed to list PlacementDecisions (placement: %s)",
			placement.GetNamespace()+"/"+placement.GetName())
	}

	if len(plDecisions.Items) == 0 {
		return nil, nil
	}

	if len(plDecisions.Items) > 1 {
		return nil, fmt.Errorf("multiple PlacementDecisions found for Placement (count: %d, placement: %s)",
			len(plDecisions.Items), placement.GetNamespace()+"/"+placement.GetName())
	}

	plDecision := plDecisions.Items[0]
	r.Log.Info("Found ClusterDecision", "ClsDedicision", plDecision.Status.Decisions)

	if len(plDecision.Status.Decisions) > 1 {
		return nil, fmt.Errorf("multiple placements found in PlacementDecision"+
			" (count: %d, Placement: %s, PlacementDecision: %s)",
			len(plDecision.Status.Decisions),
			placement.GetNamespace()+"/"+placement.GetName(),
			plDecision.GetName()+"/"+plDecision.GetNamespace())
	}

	return &plDecision, nil
}

// getClusterDecisionFromPlacement returns the cluster decision for a given Placement if found
func (r *DRPlacementControlReconciler) getClusterDecisionFromPlacement(placement *clrapiv1beta1.Placement,
) *clrapiv1beta1.ClusterDecision {
	plDecision, err := r.getPlacementDecisionFromPlacement(placement)
	if err != nil {
		// TODO: err ignored by this caller
		r.Log.Info("failed to get placement decision", "error", err)

		return &clrapiv1beta1.ClusterDecision{}
	}

	if plDecision == nil || len(plDecision.Status.Decisions) == 0 {
		return &clrapiv1beta1.ClusterDecision{}
	}

	return &plDecision.Status.Decisions[0]
}

func (r *DRPlacementControlReconciler) updateUserPlacementStatusDecision(ctx context.Context,
	userPlacement interface{}, newCD *clrapiv1beta1.ClusterDecision,
) error {
	switch obj := userPlacement.(type) {
	case *plrv1.PlacementRule:
		return r.createOrUpdatePlacementRuleDecision(ctx, obj, newCD)
	case *clrapiv1beta1.Placement:
		return r.createOrUpdatePlacementDecision(ctx, obj, newCD)
	default:
		return fmt.Errorf("failed to find Placement or PlacementRule")
	}
}

func (r *DRPlacementControlReconciler) createOrUpdatePlacementRuleDecision(ctx context.Context,
	plRule *plrv1.PlacementRule, newCD *clrapiv1beta1.ClusterDecision,
) error {
	newStatus := plrv1.PlacementRuleStatus{}

	if newCD != nil {
		newStatus = plrv1.PlacementRuleStatus{
			Decisions: []plrv1.PlacementDecision{
				{
					ClusterName:      newCD.ClusterName,
					ClusterNamespace: newCD.ClusterName,
				},
			},
		}
	}

	if !reflect.DeepEqual(newStatus, plRule.Status) {
		plRule.Status = newStatus
		if err := r.Status().Update(ctx, plRule); err != nil {
			r.Log.Error(err, "failed to update user PlacementRule")

			return fmt.Errorf("failed to update userPlRule %s (%w)", plRule.GetName(), err)
		}

		r.Log.Info("Updated user PlacementRule status", "Decisions", plRule.Status.Decisions)
	}

	return nil
}

// createOrUpdatePlacementDecision updates the PlacementDecision status for the given Placement with the passed
// in new decision. If an existing PlacementDecision is not found, ad new Placement decision is created.
func (r *DRPlacementControlReconciler) createOrUpdatePlacementDecision(ctx context.Context,
	placement *clrapiv1beta1.Placement, newCD *clrapiv1beta1.ClusterDecision,
) error {
	plDecision, err := r.getPlacementDecisionFromPlacement(placement)
	if err != nil {
		return err
	}

	if plDecision == nil {
		if plDecision, err = r.createPlacementDecision(ctx, placement); err != nil {
			return err
		}
	}

	plDecision.Status = clrapiv1beta1.PlacementDecisionStatus{
		Decisions: []clrapiv1beta1.ClusterDecision{},
	}

	if newCD != nil {
		plDecision.Status = clrapiv1beta1.PlacementDecisionStatus{
			Decisions: []clrapiv1beta1.ClusterDecision{
				{
					ClusterName: newCD.ClusterName,
					Reason:      newCD.Reason,
				},
			},
		}
	}

	if err := r.Status().Update(ctx, plDecision); err != nil {
		return fmt.Errorf("failed to update placementDecision status (%w)", err)
	}

	r.Log.Info("Created/Updated PlacementDecision", "PlacementDecision", plDecision.Status.Decisions)

	return nil
}

// createPlacementDecision creates a new PlacementDecision for the given Placement. The PlacementDecision is
// named in a predetermined format, and is searchable using the Placement name label against the PlacementDecision.
// On conflicts with existing PlacementDecisions, the function retries, with limits, with different names to generate
// a new PlacementDecision.
func (r *DRPlacementControlReconciler) createPlacementDecision(ctx context.Context,
	placement *clrapiv1beta1.Placement,
) (*clrapiv1beta1.PlacementDecision, error) {
	index := 1

	plDecision := &clrapiv1beta1.PlacementDecision{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf(PlacementDecisionName, placement.GetName(), index),
			Namespace: placement.Namespace,
		},
	}

	// Set the Placement object to be the owner.  When it is deleted, the PlacementDecision is deleted
	err := ctrl.SetControllerReference(placement, plDecision, r.Client.Scheme())
	if err != nil {
		return nil, fmt.Errorf("failed to set controller reference %w", err)
	}

	plDecision.ObjectMeta.Labels = map[string]string{
		clrapiv1beta1.PlacementLabel:    placement.GetName(),
		"velero.io/exclude-from-backup": "true",
	}

	owner := metav1.NewControllerRef(placement, clrapiv1beta1.GroupVersion.WithKind("Placement"))
	plDecision.ObjectMeta.OwnerReferences = []metav1.OwnerReference{*owner}

	for index <= MaxPlacementDecisionConflictCount {
		if err = r.Create(ctx, plDecision); err == nil {
			return plDecision, nil
		}

		if !errors.IsAlreadyExists(err) {
			return nil, err
		}

		index++

		plDecision.ObjectMeta.Name = fmt.Sprintf(PlacementDecisionName, placement.GetName(), index)
	}

	return nil, fmt.Errorf("multiple PlacementDecision conflicts found, unable to create a new"+
		" PlacementDecision for Placement %s", placement.GetNamespace()+"/"+placement.GetName())
}

func getApplicationDestinationNamespace(
	client client.Client,
	log logr.Logger,
	placement client.Object,
) (string, error) {
	appSetList := argocdv1alpha1hack.ApplicationSetList{}
	if err := client.List(context.TODO(), &appSetList); err != nil {
		// If ApplicationSet CRD is not found in the API server,
		// default to Subscription behavior, and return the placement namespace as the target VRG namespace
		if meta.IsNoMatchError(err) {
			return placement.GetNamespace(), nil
		}

		return "", fmt.Errorf("ApplicationSet list: %w", err)
	}

	log.Info("Retrieved ApplicationSets", "count", len(appSetList.Items))
	//
	// TODO: change the following loop to use an index field on AppSet instead for faster lookup
	//
	for i := range appSetList.Items {
		appSet := &appSetList.Items[i]
		if len(appSet.Spec.Generators) > 0 &&
			appSet.Spec.Generators[0].ClusterDecisionResource != nil {
			pn := appSet.Spec.Generators[0].ClusterDecisionResource.LabelSelector.MatchLabels[clrapiv1beta1.PlacementLabel]
			if pn == placement.GetName() {
				log.Info("Found ApplicationSet for Placement", "name", appSet.Name, "placement", placement.GetName())
				// Retrieving the Destination.Namespace from Application.Spec requires iterating through all Applications
				// and checking their ownerReferences, which can be time-consuming. Alternatively, we can get the same
				// information from the ApplicationSet spec template section as it is done here.
				return appSet.Spec.Template.Spec.Destination.Namespace, nil
			}
		}
	}

	log.Info(fmt.Sprintf("Placement %s does not belong to any ApplicationSet. Defaulting the dest namespace to %s",
		placement.GetName(), placement.GetNamespace()))

	// Didn't find any ApplicationSet using this Placement. Assuming it is for Subscription.
	// Returning its own namespace as the default namespace
	return placement.GetNamespace(), nil
}

func selectVRGNamespace(
	client client.Client,
	log logr.Logger,
	drpc *rmn.DRPlacementControl,
	placementObj client.Object,
) (string, error) {
	if drpc.GetAnnotations() != nil && drpc.GetAnnotations()[DRPCAppNamespace] != "" {
		return drpc.GetAnnotations()[DRPCAppNamespace], nil
	}

	switch placementObj.(type) {
	case *clrapiv1beta1.Placement:
		vrgNamespace, err := getApplicationDestinationNamespace(client, log, placementObj)
		if err != nil {
			return "", err
		}

		return vrgNamespace, nil
	default:
		return drpc.Namespace, nil
	}
}

func addOrUpdateCondition(conditions *[]metav1.Condition, conditionType string,
	observedGeneration int64, status metav1.ConditionStatus, reason, msg string,
) bool {
	newCondition := metav1.Condition{
		Type:               conditionType,
		Status:             status,
		ObservedGeneration: observedGeneration,
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
		Message:            msg,
	}

	existingCondition := findCondition(*conditions, conditionType)
	if existingCondition == nil ||
		existingCondition.Status != newCondition.Status ||
		existingCondition.ObservedGeneration != newCondition.ObservedGeneration ||
		existingCondition.Reason != newCondition.Reason ||
		existingCondition.Message != newCondition.Message {
		setStatusCondition(conditions, newCondition)

		return true
	}

	return false
}

// Initial creation of the DRPC status condition. This will also preserve the ordering of conditions in the array
func ensureDRPCConditionsInited(conditions *[]metav1.Condition, observedGeneration int64, message string) {
	const DRPCTotalConditions = 3
	if len(*conditions) == DRPCTotalConditions {
		return
	}

	time := metav1.NewTime(time.Now())

	setStatusConditionIfNotFound(conditions, metav1.Condition{
		Type:               rmn.ConditionAvailable,
		Reason:             string(rmn.Initiating),
		ObservedGeneration: observedGeneration,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: time,
		Message:            message,
	})
	setStatusConditionIfNotFound(conditions, metav1.Condition{
		Type:               rmn.ConditionPeerReady,
		Reason:             string(rmn.Initiating),
		ObservedGeneration: observedGeneration,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: time,
		Message:            message,
	})
	setStatusConditionIfNotFound(conditions, metav1.Condition{
		Type:               rmn.ConditionProtected,
		Reason:             string(rmn.ReasonProtectedUnknown),
		ObservedGeneration: observedGeneration,
		Status:             metav1.ConditionFalse,
		LastTransitionTime: time,
		Message:            message,
	})
}

func GetAvailableS3Profiles(ctx context.Context, client client.Client,
	drpc *rmn.DRPlacementControl, log logr.Logger,
) []string {
	drPolicy, err := GetDRPolicy(ctx, client, drpc, log)
	if err != nil {
		log.Info("Failed to get DRPolicy", "err", err)

		return []string{}
	}

	drClusters, err := GetDRClusters(ctx, client, drPolicy)
	if err != nil {
		log.Info("Failed to get DRClusters", "err", err)

		return []string{}
	}

	return AvailableS3Profiles(drClusters)
}

func AvailableS3Profiles(drClusters []rmn.DRCluster) []string {
	profiles := sets.New[string]()

	for i := range drClusters {
		drCluster := &drClusters[i]
		if rmnutil.ResourceIsDeleted(drCluster) {
			continue
		}

		profiles.Insert(drCluster.Spec.S3ProfileName)
	}

	return sets.List(profiles)
}

type Progress int

const (
	Continue      = 1
	AllowFailover = 2
	Stop          = 3
)

func (r *DRPlacementControlReconciler) ensureDRPCStatusConsistency(
	ctx context.Context,
	drpc *rmn.DRPlacementControl,
	drPolicy *rmn.DRPolicy,
	placementObj client.Object,
	log logr.Logger,
) (bool, error) {
	requeue := true

	log.Info("Ensure DRPC Status Consistency")

	// This will always be false the first time the DRPC resource is first created OR after hub recovery
	if drpc.Status.Phase != "" && drpc.Status.Phase != rmn.WaitForUser {
		return !requeue, nil
	}

	dstCluster := drpc.Spec.PreferredCluster
	if drpc.Spec.Action == rmn.ActionFailover {
		dstCluster = drpc.Spec.FailoverCluster
	}

	progress, msg, err := r.determineDRPCState(ctx, drpc, drPolicy, placementObj, dstCluster, log)

	log.Info(msg)

	if err != nil {
		return requeue, err
	}

	switch progress {
	case Continue:
		return !requeue, nil
	case AllowFailover:
		drpc.Status.Phase = rmn.WaitForUser
		drpc.Status.ObservedGeneration = drpc.Generation
		updateDRPCProgression(drpc, rmn.ProgressionActionPaused, log)
		addOrUpdateCondition(&drpc.Status.Conditions, rmn.ConditionAvailable,
			drpc.Generation, metav1.ConditionTrue, rmn.ReasonSuccess, msg)
		addOrUpdateCondition(&drpc.Status.Conditions, rmn.ConditionPeerReady, drpc.Generation,
			metav1.ConditionTrue, rmn.ReasonSuccess, "Failover allowed")

		return requeue, nil
	default:
		msg := fmt.Sprintf("Operation Paused - User Intervention Required. %s", msg)

		log.Info(msg)
		updateDRPCProgression(drpc, rmn.ProgressionActionPaused, log)
		addOrUpdateCondition(&drpc.Status.Conditions, rmn.ConditionAvailable,
			drpc.Generation, metav1.ConditionFalse, rmn.ReasonPaused, msg)
		addOrUpdateCondition(&drpc.Status.Conditions, rmn.ConditionPeerReady, drpc.Generation,
			metav1.ConditionFalse, rmn.ReasonPaused, "User Intervention Required")

		return requeue, nil
	}
}

// determineDRPCState runs the following algorithm
// 1. Stop Condition for Both Failed Queries:
//    If attempts to query 2 clusters result in failure for both, the process is halted.

// 2. Initial Deployment without VRGs:
//    If 2 clusters are successfully queried, and no VRGs are found, proceed with the
//    initial deployment.

// 3. Handling Failures with S3 Store Check:
//    - If 2 clusters are queried, 1 fails, and 0 VRGs are found, perform the following checks:
//       - If the VRG is found in the S3 store, ensure that the DRPC action matches the VRG action.
//       If not, stop until the action is corrected, allowing failover if necessary (set PeerReady).
//       - If the VRG is not found in the S3 store and the failed cluster is not the destination
//       cluster, continue with the initial deployment.

// 4. Verification and Failover for VRGs on Failover Cluster:
//    If 2 clusters are queried, 1 fails, and 1 VRG is found on the failover cluster, check
//    the action:
//       - If the actions don't match, stop until corrected by the user.
//       - If they match, also stop but allow failover if the VRG in-hand is a secondary.
//       Otherwise, continue.

// 5. Handling VRGs on Destination Cluster:
//    If 2 clusters are queried successfully and 1 or more VRGs are found, and one of the
//    VRGs is on the destination cluster, perform the following checks:
//       - Continue with the action only if the DRPC and the found VRG action match.
//       - Stop until someone investigates if there is a mismatch, but allow failover to
//       take place (set PeerReady).

//  6. Otherwise, default to allowing Failover:
//     If none of the above conditions apply, allow failover (set PeerReady) but stop until
//     someone makes the necessary change.
//
//nolint:funlen,nestif,gocognit,gocyclo,cyclop
func (r *DRPlacementControlReconciler) determineDRPCState(
	ctx context.Context,
	drpc *rmn.DRPlacementControl,
	drPolicy *rmn.DRPolicy,
	placementObj client.Object,
	dstCluster string,
	log logr.Logger,
) (Progress, string, error) {
	log.Info("Rebuild DRPC state")

	vrgNamespace, err := selectVRGNamespace(r.Client, log, drpc, placementObj)
	if err != nil {
		log.Info("Failed to select VRG namespace")

		return Stop, "", err
	}

	drClusters, err := GetDRClusters(ctx, r.Client, drPolicy)
	if err != nil {
		return Stop, "", err
	}

	vrgs, successfullyQueriedClusterCount, failedCluster, err := getVRGsFromManagedClusters(
		r.MCVGetter, drpc, drClusters, vrgNamespace, log)
	if err != nil {
		log.Info("Failed to get a list of VRGs")

		return Stop, "", err
	}

	mwu := rmnutil.MWUtil{
		Client:          r.Client,
		APIReader:       r.APIReader,
		Ctx:             ctx,
		Log:             log,
		InstName:        drpc.Name,
		TargetNamespace: vrgNamespace,
	}

	if !ensureVRGsManagedByDRPC(log, mwu, vrgs, drpc, vrgNamespace) {
		msg := "VRG adoption in progress"

		return Stop, msg, nil
	}

	// IF 2 clusters queried, and both queries failed, then STOP
	if successfullyQueriedClusterCount == 0 {
		msg := "Stop - Number of clusters queried is 0"

		return Stop, msg, nil
	}

	// IF 2 clusters queried successfully and no VRGs, then continue with initial deployment
	if successfullyQueriedClusterCount == 2 && len(vrgs) == 0 {
		log.Info("Queried 2 clusters successfully")

		return Continue, "", nil
	}

	if drpc.Status.Phase == rmn.WaitForUser &&
		drpc.Spec.Action == rmn.ActionFailover &&
		drpc.Spec.FailoverCluster != failedCluster {
		log.Info("Continue. The action is failover and the failoverCluster is accessible")

		return Continue, "", nil
	}

	// IF queried 2 clusters queried, 1 failed and 0 VRG found, then check s3 store.
	// IF the VRG found in the s3 store, ensure that the DRPC action and the VRG action match. IF not, stop until
	// the action is corrected, but allow failover to take place if needed (set PeerReady)
	// If the VRG is not found in the s3 store and the failedCluster is not the destination cluster, then continue
	// with initial deploy
	if successfullyQueriedClusterCount == 1 && len(vrgs) == 0 {
		vrg := GetLastKnownVRGPrimaryFromS3(ctx, r.APIReader,
			AvailableS3Profiles(drClusters), drpc.GetName(), vrgNamespace, r.ObjStoreGetter, log)
		if vrg == nil {
			// IF the failed cluster is not the dest cluster, then this could be an initial deploy
			if failedCluster != dstCluster {
				return Continue, "", nil
			}

			msg := fmt.Sprintf("Unable to query all clusters and failed to get VRG from s3 store. Failed to query %s",
				failedCluster)

			return Stop, msg, nil
		}

		log.Info("Got VRG From s3", "VRG Spec", vrg.Spec, "VRG Annotations", vrg.GetAnnotations())

		if drpc.Spec.Action != rmn.DRAction(vrg.Spec.Action) {
			msg := fmt.Sprintf("Failover is allowed - Two different actions - drpcAction is '%s' and vrgAction from s3 is '%s'",
				drpc.Spec.Action, vrg.Spec.Action)

			return AllowFailover, msg, nil
		}

		if dstCluster == vrg.GetAnnotations()[DestinationClusterAnnotationKey] &&
			dstCluster != failedCluster {
			log.Info(fmt.Sprintf("VRG from s3. Same dstCluster %s/%s. Proceeding...",
				dstCluster, vrg.GetAnnotations()[DestinationClusterAnnotationKey]))

			return Continue, "", nil
		}

		msg := fmt.Sprintf("Failover is allowed - drpcAction:'%s'. vrgAction:'%s'. DRPCDstClstr:'%s'. vrgDstClstr:'%s'.",
			drpc.Spec.Action, vrg.Spec.Action, dstCluster, vrg.GetAnnotations()[DestinationClusterAnnotationKey])

		return AllowFailover, msg, nil
	}

	// IF 2 clusters queried, 1 failed and 1 VRG found on the failover cluster, then check the action, if they don't
	// match, stop until corrected by the user. If they do match, then also stop but allow failover if the VRG in-hand
	// is a secondary. Othewise, continue...
	if successfullyQueriedClusterCount == 1 && len(vrgs) == 1 {
		var clusterName string

		var vrg *rmn.VolumeReplicationGroup
		for k, v := range vrgs {
			clusterName, vrg = k, v

			break
		}

		// Post-HubRecovery, if the retrieved VRG from the surviving cluster is secondary, it wrongly halts
		// reconciliation for the workload. Only proceed if the retrieved VRG is primary.
		if vrg.Spec.ReplicationState == rmn.Primary &&
			drpc.Spec.Action != rmn.DRAction(vrg.Spec.Action) &&
			dstCluster == clusterName {
			msg := fmt.Sprintf("Stop - Two different actions for the same cluster - drpcAction:'%s'. vrgAction:'%s'",
				drpc.Spec.Action, vrg.Spec.Action)

			return Stop, msg, nil
		}

		if dstCluster != clusterName && vrg.Spec.ReplicationState == rmn.Secondary {
			log.Info(fmt.Sprintf("Failover is allowed. Action/dstCluster/ReplicationState %s/%s/%s",
				drpc.Spec.Action, dstCluster, vrg.Spec.ReplicationState))

			msg := "Failover is allowed - Primary is assumed to be on the failed cluster"

			return AllowFailover, msg, nil
		}

		log.Info("Same action, dstCluster, and ReplicationState is primary. Continuing")

		return Continue, "", nil
	}

	// Finally, IF 2 clusters queried successfully and 1 or more VRGs found, and if one of the VRGs is on the dstCluster,
	// then continue with action if and only if DRPC and the found VRG action match. otherwise, stop until someone
	// investigates but allow failover to take place (set PeerReady)
	if successfullyQueriedClusterCount == 2 && len(vrgs) >= 1 {
		var clusterName string

		var vrg *rmn.VolumeReplicationGroup

		for k, v := range vrgs {
			clusterName, vrg = k, v
			if vrg.Spec.ReplicationState == rmn.Primary {
				break
			}
		}

		// This can happen if a hub is recovered in the middle of a Relocate
		if vrg.Spec.ReplicationState == rmn.Secondary && len(vrgs) == 2 {
			msg := "Stop - Both VRGs have the same secondary state"

			return Stop, msg, nil
		}

		if drpc.Spec.Action == rmn.DRAction(vrg.Spec.Action) && dstCluster == clusterName {
			log.Info(fmt.Sprintf("Same Action and dest cluster %s/%s", drpc.Spec.Action, dstCluster))

			return Continue, "", nil
		}

		msg := fmt.Sprintf("Failover is allowed - VRGs count:'%d'. drpcAction:'%s'."+
			" vrgAction:'%s'. DstCluster:'%s'. vrgOnCluste '%s'",
			len(vrgs), drpc.Spec.Action, vrg.Spec.Action, dstCluster, clusterName)

		return AllowFailover, msg, nil
	}

	// IF none of the above, then allow failover (set PeerReady), but stop until someone makes the change
	msg := "Failover is allowed - User intervention is required"

	return AllowFailover, msg, nil
}

// ensureVRGsManagedByDRPC ensures that VRGs reported by ManagedClusterView are managed by the current instance of
// DRPC. This is done using the DRPC UID annotation on the viewed VRG matching the current DRPC UID and if not
// creating or updating the exisiting ManifestWork for the VRG.
// Returns a bool indicating true if VRGs are managed by the current DRPC resource
func ensureVRGsManagedByDRPC(
	log logr.Logger,
	mwu rmnutil.MWUtil,
	vrgs map[string]*rmn.VolumeReplicationGroup,
	drpc *rmn.DRPlacementControl,
	vrgNamespace string,
) bool {
	ensured := true

	for cluster, viewVRG := range vrgs {
		if rmnutil.ResourceIsDeleted(viewVRG) {
			log.Info("VRG reported by view undergoing deletion, during adoption",
				"cluster", cluster, "namespace", viewVRG.Namespace, "name", viewVRG.Name)

			continue
		}

		if viewVRG.GetAnnotations() != nil {
			if v, ok := viewVRG.Annotations[DRPCUIDAnnotation]; ok && v == string(drpc.UID) {
				continue
			}
		}

		adopted := adoptVRG(log, mwu, viewVRG, cluster, drpc, vrgNamespace)

		ensured = ensured && adopted
	}

	return ensured
}

// adoptVRG creates or updates the VRG ManifestWork to ensure that the current DRPC is managing the VRG resource
// Returns a bool indicating if adoption was completed (which is mostly false except when VRG MW is deleted)
func adoptVRG(
	log logr.Logger,
	mwu rmnutil.MWUtil,
	viewVRG *rmn.VolumeReplicationGroup,
	cluster string,
	drpc *rmn.DRPlacementControl,
	vrgNamespace string,
) bool {
	adopted := true

	mw, err := mwu.FindManifestWorkByType(rmnutil.MWTypeVRG, cluster)
	if err != nil {
		if !errors.IsNotFound(err) {
			log.Info("error fetching VRG ManifestWork during adoption", "error", err, "cluster", cluster)

			return !adopted
		}

		adoptOrphanVRG(log, mwu, viewVRG, cluster, drpc, vrgNamespace)

		return !adopted
	}

	if rmnutil.ResourceIsDeleted(mw) {
		log.Info("VRG ManifestWork found deleted during adoption", "cluster", cluster)

		return adopted
	}

	vrg, err := rmnutil.ExtractVRGFromManifestWork(mw)
	if err != nil {
		log.Info("error extracting VRG from ManifestWork during adoption", "error", err, "cluster", cluster)

		return !adopted
	}

	// NOTE: upgrade use case, to add DRPC UID for existing VRG MW
	adoptExistingVRGManifestWork(log, mwu, vrg, cluster, drpc, vrgNamespace)

	return !adopted
}

// adoptExistingVRGManifestWork updates an existing VRG ManifestWork as managed by the current DRPC resource
func adoptExistingVRGManifestWork(
	log logr.Logger,
	mwu rmnutil.MWUtil,
	vrg *rmn.VolumeReplicationGroup,
	cluster string,
	drpc *rmn.DRPlacementControl,
	vrgNamespace string,
) {
	log.Info("adopting existing VRG ManifestWork", "cluster", cluster, "namespace", vrg.Namespace, "name", vrg.Name)

	if vrg.GetAnnotations() == nil {
		vrg.Annotations = make(map[string]string)
	}

	if v, ok := vrg.Annotations[DRPCUIDAnnotation]; ok && v == string(drpc.UID) {
		// Annotation may already be set but not reflected on the resource view yet
		log.Info("detected VRGs DRPC UID annotation as existing",
			"cluster", cluster, "namespace", vrg.Namespace, "name", vrg.Name)

		return
	}

	vrg.Annotations[DRPCUIDAnnotation] = string(drpc.UID)

	annotations := make(map[string]string)
	annotations[DRPCNameAnnotation] = drpc.Name
	annotations[DRPCNamespaceAnnotation] = drpc.Namespace

	err := mwu.CreateOrUpdateVRGManifestWork(drpc.Name, vrgNamespace, cluster, *vrg, annotations)
	if err != nil {
		log.Info("error updating VRG via ManifestWork during adoption", "error", err, "cluster", cluster)
	}
}

// adoptOpphanVRG creates a missing ManifestWork for a VRG found via a ManagedClusterView
func adoptOrphanVRG(
	log logr.Logger,
	mwu rmnutil.MWUtil,
	viewVRG *rmn.VolumeReplicationGroup,
	cluster string,
	drpc *rmn.DRPlacementControl,
	vrgNamespace string,
) {
	log.Info("adopting orphaned VRG ManifestWork",
		"cluster", cluster, "namespace", viewVRG.Namespace, "name", viewVRG.Name)

	annotations := make(map[string]string)
	annotations[DRPCNameAnnotation] = drpc.Name
	annotations[DRPCNamespaceAnnotation] = drpc.Namespace

	// Adopt the namespace as well
	err := mwu.CreateOrUpdateNamespaceManifest(drpc.Name, vrgNamespace, cluster, annotations)
	if err != nil {
		log.Info("error creating namespace via ManifestWork during adoption", "error", err, "cluster", cluster)

		return
	}

	vrg := constructVRGFromView(viewVRG)
	if vrg.GetAnnotations() == nil {
		vrg.Annotations = make(map[string]string)
	}

	vrg.Annotations[DRPCUIDAnnotation] = string(drpc.UID)

	if err := mwu.CreateOrUpdateVRGManifestWork(
		drpc.Name, vrgNamespace,
		cluster, *vrg, annotations); err != nil {
		log.Info("error creating VRG via ManifestWork during adoption", "error", err, "cluster", cluster)
	}
}

// constructVRGFromView selectively constructs a VRG from a view, using its spec and only those annotations that
// would be set by the hub on the ManifestWork
func constructVRGFromView(viewVRG *rmn.VolumeReplicationGroup) *rmn.VolumeReplicationGroup {
	vrg := &rmn.VolumeReplicationGroup{
		TypeMeta: metav1.TypeMeta{Kind: "VolumeReplicationGroup", APIVersion: "ramendr.openshift.io/v1alpha1"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      viewVRG.Name,
			Namespace: viewVRG.Namespace,
		},
	}

	viewVRG.Spec.DeepCopyInto(&vrg.Spec)

	for k, v := range viewVRG.GetAnnotations() {
		switch k {
		case DestinationClusterAnnotationKey:
			fallthrough
		case DoNotDeletePVCAnnotation:
			fallthrough
		case rmnutil.IsCGEnabledAnnotation:
			fallthrough
		case DRPCUIDAnnotation:
			rmnutil.AddAnnotation(vrg, k, v)
		default:
		}
	}

	return vrg
}

func ensureDRPCValidNamespace(drpc *rmn.DRPlacementControl, ramenConfig *rmn.RamenConfig) error {
	if drpcInAdminNamespace(drpc, ramenConfig) {
		if !ramenConfig.MultiNamespace.FeatureEnabled {
			return fmt.Errorf("drpc cannot be in admin namespace when multinamespace feature is disabled")
		}

		if drpc.Spec.ProtectedNamespaces == nil || len(*drpc.Spec.ProtectedNamespaces) == 0 {
			return fmt.Errorf("drpc in admin namespace must have protected namespaces")
		}

		adminNamespace := drpcAdminNamespaceName(*ramenConfig)
		if slices.Contains(*drpc.Spec.ProtectedNamespaces, adminNamespace) {
			return fmt.Errorf("admin namespace cannot be a protected namespace, admin namespace: %s", adminNamespace)
		}

		return nil
	}

	if drpc.Spec.ProtectedNamespaces != nil && len(*drpc.Spec.ProtectedNamespaces) > 0 {
		adminNamespace := drpcAdminNamespaceName(*ramenConfig)

		return fmt.Errorf("drpc in non-admin namespace(%v) cannot have protected namespaces, admin-namespaces: %v",
			drpc.Namespace, adminNamespace)
	}

	return nil
}

func drpcsProtectCommonNamespace(drpcProtectedNs []string, otherDRPCProtectedNs []string) bool {
	for _, ns := range drpcProtectedNs {
		if slices.Contains(otherDRPCProtectedNs, ns) {
			return true
		}
	}

	return false
}

func (r *DRPlacementControlReconciler) getProtectedNamespaces(drpc *rmn.DRPlacementControl,
	log logr.Logger,
) ([]string, error) {
	if drpc.Spec.ProtectedNamespaces != nil && len(*drpc.Spec.ProtectedNamespaces) > 0 {
		return *drpc.Spec.ProtectedNamespaces, nil
	}

	placementObj, err := getPlacementOrPlacementRule(context.TODO(), r.Client, drpc, log)
	if err != nil {
		return []string{}, err
	}

	vrgNamespace, err := selectVRGNamespace(r.Client, log, drpc, placementObj)
	if err != nil {
		return []string{}, err
	}

	return []string{vrgNamespace}, nil
}

func (r *DRPlacementControlReconciler) ensureNoConflictingDRPCs(ctx context.Context,
	drpc *rmn.DRPlacementControl, ramenConfig *rmn.RamenConfig, log logr.Logger,
) error {
	drpcList := &rmn.DRPlacementControlList{}
	if err := r.Client.List(ctx, drpcList); err != nil {
		return fmt.Errorf("failed to list DRPlacementControls (%w)", err)
	}

	for i := range drpcList.Items {
		otherDRPC := &drpcList.Items[i]

		// Skip the drpc itself
		if otherDRPC.Name == drpc.Name && otherDRPC.Namespace == drpc.Namespace {
			continue
		}

		if err := r.twoDRPCsConflict(ctx, drpc, otherDRPC, ramenConfig, log); err != nil {
			return err
		}
	}

	return nil
}

func (r *DRPlacementControlReconciler) twoDRPCsConflict(ctx context.Context,
	drpc *rmn.DRPlacementControl, otherDRPC *rmn.DRPlacementControl, ramenConfig *rmn.RamenConfig, log logr.Logger,
) error {
	drpcIsInAdminNamespace := drpcInAdminNamespace(drpc, ramenConfig)
	otherDRPCIsInAdminNamespace := drpcInAdminNamespace(otherDRPC, ramenConfig)

	// we don't check for conflicts between drpcs in non-admin namespace
	if !drpcIsInAdminNamespace && !otherDRPCIsInAdminNamespace {
		return nil
	}

	// If the drpcs don't have common clusters, they definitely don't conflict
	common, err := r.drpcHaveCommonClusters(ctx, drpc, otherDRPC, log)
	if err != nil {
		return fmt.Errorf("failed to check if drpcs have common clusters (%w)", err)
	}

	if !common {
		return nil
	}

	drpcProtectedNamespaces, err := r.getProtectedNamespaces(drpc, log)
	if err != nil {
		return fmt.Errorf("failed to get protected namespaces for drpc: %v, %w", drpc.Name, err)
	}

	otherDRPCProtectedNamespaces, err := r.getProtectedNamespaces(otherDRPC, log)
	if err != nil {
		return fmt.Errorf("failed to get protected namespaces for drpc: %v, %w", otherDRPC.Name, err)
	}

	conflict := drpcsProtectCommonNamespace(drpcProtectedNamespaces, otherDRPCProtectedNamespaces)
	if conflict {
		return fmt.Errorf("drpc: %s and drpc: %s protect the same namespace",
			drpc.Name, otherDRPC.Name)
	}

	return nil
}

func drpcInAdminNamespace(drpc *rmn.DRPlacementControl, ramenConfig *rmn.RamenConfig) bool {
	adminNamespace := drpcAdminNamespaceName(*ramenConfig)

	return adminNamespace == drpc.Namespace
}

func (r *DRPlacementControlReconciler) drpcHaveCommonClusters(ctx context.Context,
	drpc, otherDRPC *rmn.DRPlacementControl, log logr.Logger,
) (bool, error) {
	drpolicy, err := GetDRPolicy(ctx, r.Client, drpc, log)
	if err != nil {
		return false, fmt.Errorf("failed to get DRPolicy %w", err)
	}

	otherDrpolicy, err := GetDRPolicy(ctx, r.Client, otherDRPC, log)
	if err != nil {
		return false, fmt.Errorf("failed to get DRPolicy %w", err)
	}

	drpolicyClusters := rmnutil.DRPolicyClusterNamesAsASet(drpolicy)
	otherDrpolicyClusters := rmnutil.DRPolicyClusterNamesAsASet(otherDrpolicy)

	return drpolicyClusters.Intersection(otherDrpolicyClusters).Len() > 0, nil
}
