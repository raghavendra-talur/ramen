#!/bin/bash

export MANAGEDCLUSTER1="${MANAGEDCLUSTER1:-cluster1}"
export MANAGEDCLUSTER2="${MANAGEDCLUSTER2:-managedCluster2}"
export HUBCLUSTER="${HUBCLUSTER:-hub}"
export CEPHCLUSTER="${CEPHCLUSTER:-cephcluster}"
export METRONET="${METRONET:-default}"
export MANAGEDCLUSTER_ROOK_NAMESPACE="${MANAGEDCLUSTER_ROOK_NAMESPACE:-rook-ceph}"
basedir="$(dirname "$(realpath "$0")")"
scriptdir="$basedir/ramen-1"

export PATH=${HOME}/.local/bin:${PATH}

function check_for_command() {
	if ! command -v "$1" >/dev/null
	then
		echo "command $1 not found"
		exit 1
	fi
}

function externalCluster_setFSID() {
        declare rook_tools_pod
        rook_tools_pod=$(kubectl --context "$1" -n "$2" get pods -o name | grep tools)
        ROOK_EXTERNAL_FSID=$(kubectl --context "$1" exec -n "$2" "${rook_tools_pod}" -- ceph fsid)
        export ROOK_EXTERNAL_FSID
}

function externalCluster_setNamespace() {
        if kubectl --context "$1" get ns -o name | grep -q "$2"
        then
                echo "namespace exists, continuing"
                NAMESPACE="$2"
                export NAMESPACE
        else
                kubectl --context "$1" create ns "$2"
                NAMESPACE="$2"
                export NAMESPACE
        fi
}

function externalCluster_setMonData() {
        declare rook_tools_pod mon_dump
        rook_tools_pod=$(kubectl --context "$1" -n "$2" get pods -o name | grep tools)
        mon_dump=$(kubectl --context "$1" exec -n "$2" "${rook_tools_pod}" -- ceph mon dump -f json 2>/dev/null)
        ROOK_EXTERNAL_CEPH_MON_DATA=$(echo "${mon_dump}" | jq --raw-output .mons[0].name)=$(echo "${mon_dump}" |jq --raw-output .mons[0].public_addrs.addrvec[0].addr)
        export ROOK_EXTERNAL_CEPH_MON_DATA
}

function externalCluster_setAdminSecret() {
        declare rook_tools_pod
        rook_tools_pod=$(kubectl --context "$1" -n "$2" get pods -o name | grep tools)
        ROOK_EXTERNAL_ADMIN_SECRET=$(kubectl --context "$1" exec -n "$2" "${rook_tools_pod}" -- ceph auth get-key client.admin)
        export ROOK_EXTERNAL_ADMIN_SECRET
}

function validate() {
	check_for_command jq
	check_for_command minikube
	check_for_command kubectl
}

validate

function connect_external_storage_cluster() {
        KUBECLUSTER=$1
    	kubectl config use-context "${KUBECLUSTER}"
	curl https://raw.githubusercontent.com/rook/rook/master/deploy/examples/import-external-cluster.sh | bash
        kubectl --context "${KUBECLUSTER}" -n "${MANAGEDCLUSTER_ROOK_NAMESPACE}" get secret rook-ceph-mon -o yaml | sed '/ceph-username/d' | sed '/ceph-secret/d' | kubectl --context "${KUBECLUSTER}" -n "${MANAGEDCLUSTER_ROOK_NAMESPACE}" apply -f -
	echo "======Deploying common.yaml========"
        kubectl --context "${KUBECLUSTER}" create -f https://raw.githubusercontent.com/rook/rook/master/deploy/examples/common.yaml
	echo "======Deploying rook crds=======" && sleep 100
        kubectl --context "${KUBECLUSTER}" create -f https://raw.githubusercontent.com/rook/rook/master/deploy/examples/crds.yaml
	echo "======Deploying rook operator=======" && sleep 100
        kubectl --context "${KUBECLUSTER}" create -f https://raw.githubusercontent.com/rook/rook/master/deploy/examples/operator.yaml
	echo "======Deploying cluster=======" && sleep 100
	kubectl --context "${KUBECLUSTER}" create -f "${scriptdir}/hack/dev-rook-cluster-external.yaml"
	echo "======Deploying toolbox=======" && sleep 100
        kubectl --context "${KUBECLUSTER}" create -f https://raw.githubusercontent.com/rook/rook/master/deploy/examples/toolbox.yaml
	
        # exit 0
	# echo "======Deploying pool=======" && sleep 100
	# kubectl --context ${KUBECLUSTER} create -f "${scriptdir}/hack/dev-rook-rbdpool.yaml"
	# echo "======Deploying rbd storageclass=======" && sleep 100

        kubectl --context "${KUBECLUSTER}" create -f "${scriptdir}/hack/dev-rook-sc.yaml"
        echo "======Patching default storageClass to rbd"
        kubectl --context "${KUBECLUSTER}" patch storageclass rook-ceph-block -p '{"metadata": {"annotations":{"storageclass.kubernetes.io/is-default-class":"true"}}}'
        kubectl --context "${KUBECLUSTER}" patch storageclass standard -p '{"metadata": {"annotations":{"storageclass.kubernetes.io/is-default-class":"false"}}}'
        kubectl --context "${KUBECLUSTER}" get storageclass
}

if [[ $1 == "external" ]]
then
	externalCluster_setFSID "${CEPHCLUSTER}" "${MANAGEDCLUSTER_ROOK_NAMESPACE}"
 	echo "$ROOK_EXTERNAL_FSID"
	externalCluster_setMonData "${CEPHCLUSTER}" "${MANAGEDCLUSTER_ROOK_NAMESPACE}"
 	echo "${ROOK_EXTERNAL_CEPH_MON_DATA}"
	externalCluster_setAdminSecret "${CEPHCLUSTER}" "${MANAGEDCLUSTER_ROOK_NAMESPACE}"
	echo "${ROOK_EXTERNAL_ADMIN_SECRET}"
	externalCluster_setNamespace "${MANAGEDCLUSTER1}" "${MANAGEDCLUSTER_ROOK_NAMESPACE}"
	echo "${NAMESPACE}"
	connect_external_storage_cluster "$MANAGEDCLUSTER1"
	sleep 22
	externalCluster_setNamespace "${HUBCLUSTER}" "${MANAGEDCLUSTER_ROOK_NAMESPACE}"
	echo "${NAMESPACE}"
	connect_external_storage_cluster "$HUBCLUSTER"

   exit 0
fi



if [[ $1 == "cleanup" ]]
then
        echo cleanup
        minikube delete --profile "${MANAGEDCLUSTER1}"
        minikube delete --profile "${MANAGEDCLUSTER2}"
        minikube delete --profile "${HUBCLUSTER}"
        minikube delete --profile "${CEPHCLUSTER}"
        #cd ceph-cluster
        #vagrant destroy --force
        #cd -
        exit 0
fi


if [[ $1 == "setup" ]]
then
        echo "hub cluster:" && echo "$HUBCLUSTER"
        echo "mangedcluster1:" && echo "$MANAGEDCLUSTER1"	
	echo "storage cluster:" && echo "$CEPHCLUSTER"
	echo "network:" && echo "$METRONET"
	
	metroNetIP=$(virsh net-dumpxml "$(virsh net-list --name | grep "${METRONET}")" | grep "ip address" | cut -d"=" -f2 | cut -d" " -f1 | tr -d "'")
        minikube start --profile "${HUBCLUSTER}" --network="${METRONET}" --insecure-registry="${metroNetIP}/24" --nodes=1 --extra-disks=1 --addons=registry
	echo "sleeping before proceeding" && sleep 22
	minikube start --profile "${MANAGEDCLUSTER1}" --network="${METRONET}" --insecure-registry="${metroNetIP}/24" --nodes=1 --extra-disks=1 --addons=registry
	# echo "sleeping before proceeding" && sleep 22
	# minikube start --profile ${MANAGEDCLUSTER2} --network=${METRONET} --insecure-registry="${metroNetIP}/24" --nodes=1 --extra-disks=1 --addons=registry
	# echo "sleeping before proceeding" && sleep 22
	minikube start --profile "${CEPHCLUSTER}"     --network="${METRONET}" --insecure-registry="${metroNetIP}/24" --nodes=1 --extra-disks=1 --addons=registry
	# echo "sleeping before proceeding" && sleep 22
	git clone https://github.com/open-cluster-management-io/registration-operator.git
	cd registration-operator || exit
	echo "====== deploying hub ========="
	kubectl config use-context "${HUBCLUSTER}"
	make deploy-hub
	# make deploy-spoke
	make deploy-spoke-operator
        make bootstrap-secret
        kustomize build deploy/klusterlet/config/samples | sed "s/clusterName: cluster1/clusterName: ${HUBCLUSTER}/g" | kubectl apply -f -
	sleep 44
        echo "====== deploying spoke managed cluster ========"
        kubectl config use-context "${MANAGEDCLUSTER1}"
        make deploy-spoke
        # make deploy-spoke-operator
        # make bootstrap-secret
        # kustomize build deploy/klusterlet/config/samples | sed "s/clusterName: cluster1/clusterName: ${MANAGEDCLUSTER1}/g" | kubectl apply -f -
        # kubectl config use-context ${MANAGEDCLUSTER2}
        # make deploy-spoke-operator
        # make bootstrap-secret
        # kustomize build deploy/klusterlet/config/samples | sed "s/clusterName: cluster1/clusterName: ${MANAGEDCLUSTER2}/g" | kubectl apply -f -
        cd - || exit
        sleep 44 #Wait for the CSR to reach the hub
        CSR=$(kubectl --context "${HUBCLUSTER}" get csr --no-headers -o name | grep "${HUBCLUSTER}")
        echo "$CSR"
        kubectl --context "${HUBCLUSTER}" certificate approve "$CSR"
        kubectl --context "${HUBCLUSTER}" patch  managedcluster "${HUBCLUSTER}" -p='{"spec":{"hubAcceptsClient":true}}' --type=merge

        sleep 44 #Wait for the CSR to reach the hub
        CSR=$(kubectl --context "${HUBCLUSTER}" get csr --no-headers -o name | grep "${MANAGEDCLUSTER1}")
        echo "$CSR"
	kubectl --context "${HUBCLUSTER}" certificate approve "$CSR"
	kubectl --context "${HUBCLUSTER}" patch  managedcluster "${MANAGEDCLUSTER1}" -p='{"spec":{"hubAcceptsClient":true}}' --type=merge

	# sleep 44 #Wait for the CSR to reach the hub
	# CSR=$(kubectl --context ${HUBCLUSTER} get csr --no-headers -o name | grep ${MANAGEDCLUSTER2})
 #      # echo $CSR
	# kubectl --context ${HUBCLUSTER} certificate approve $CSR
	# kubectl --context ${HUBCLUSTER} patch  managedcluster ${MANAGEDCLUSTER2} -p='{"spec":{"hubAcceptsClient":true}}' --type=merge

	# Deploy storage
	echo "======Deploying common.yaml========"
        kubectl --context "${CEPHCLUSTER}" create -f https://raw.githubusercontent.com/rook/rook/master/deploy/examples/common.yaml
	echo "======Deploying rook crds=======" && sleep 100
        kubectl --context "${CEPHCLUSTER}" create -f https://raw.githubusercontent.com/rook/rook/master/deploy/examples/crds.yaml
	echo "======Deploying rook operator=======" && sleep 100
        kubectl --context "${CEPHCLUSTER}" create -f https://raw.githubusercontent.com/rook/rook/master/deploy/examples/operator.yaml
	echo "======Deploying cluster=======" && sleep 100
	#kubectl --context ${CEPHCLUSTER} create -f https://raw.githubusercontent.com/rook/rook/master/deploy/examples/cluster.yaml
	kubectl --context "${CEPHCLUSTER}" create -f "${scriptdir}/hack/dev-rook-cluster.yaml"
	echo "======Deploying toolbox=======" && sleep 100
        kubectl --context "${CEPHCLUSTER}" create -f https://raw.githubusercontent.com/rook/rook/master/deploy/examples/toolbox.yaml
	echo "======Deploying pool=======" && sleep 100
        kubectl --context "${CEPHCLUSTER}" create -f "${scriptdir}/hack/dev-rook-rbdpool.yaml"
	# echo "======Deploying filesystem=======" && sleep 100
 	# kubectl --context ${CEPHCLUSTER} create -f https://raw.githubusercontent.com/rook/rook/master/deploy/examples/filesystem.yaml
	echo "======Deploying rbd storageclass=======" && sleep 100
        kubectl --context "${CEPHCLUSTER}" create -f "${scriptdir}/hack/dev-rook-sc.yaml"
	# echo "======Deploying fs storageclass=======" && sleep 100
        # kubectl --context ${CEPHCLUSTER} create -f https://raw.githubusercontent.com/rook/rook/master/deploy/examples/csi/cephfs/storageclass.yaml

	echo "======Patching default storageClass to rbd"
        kubectl --context "${CEPHCLUSTER}" patch storageclass rook-ceph-block -p '{"metadata": {"annotations":{"storageclass.kubernetes.io/is-default-class":"true"}}}'
        kubectl --context "${CEPHCLUSTER}" patch storageclass rook-cephfs -p '{"metadata": {"annotations":{"storageclass.kubernetes.io/is-default-class":"false"}}}'
        kubectl --context "${CEPHCLUSTER}" patch storageclass standard -p '{"metadata": {"annotations":{"storageclass.kubernetes.io/is-default-class":"false"}}}'
        kubectl --context "${CEPHCLUSTER}" get storageclass 
        # install ramen
        # on hub
        # on managedclusters
        exit 0
fi
