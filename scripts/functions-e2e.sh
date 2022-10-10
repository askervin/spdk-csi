#!/bin/bash

# This file contains functions for running and debugging e2e tests.
#
# Usage:
#
# source ./functions-e2e.sh         # load e2e-* helpers
#
# Example workflow:
#
# e2e-k8s-start
# e2e-spdk-build                    # build spdk image with SMA
# e2e-spdk-start                    # start spdk target and SMA server
# e2e-spdkcsi-build                 # build spdkcsi image
# e2e-test                          # launch go test -test.v ./e2e
# e2e-controller-logs               # see spdkcsi controller logs
# e2e-node-logs                     # see spdkcsi node logs
# e2e-controller-debug              # debug spdkcsi controller
# e2e-node-debug                    # debug spdkcsi node
# e2e-spdkcsi-stop                  # clear test pods and namespaces
# e2e-spdkcsi-build                 # rebuild spdkcsi, next "e2e-test"
#
# Example workflow 2: debug node plugin, trace sma server
#
# e2e-k8s-start
# e2e-spdk-build
# e2e-spdk-start
# e2e-sma-trace
# e2e-spdkcsi-build
# e2e-spdkcsi-start
# e2e-node-debug                    # set breakpoint in delve,
#                                   # NodeStageVolume()
#                                   # NodePublishVolume()
# kubectl create -f deploy/kubernetes/testpod.yaml
# dlv> prompt appears in e2e-debug-node
# kubectl delete -f deploy/kubernetes/testpod.yaml
# e2e-delete-deployments

SPDKCSI_DIR="$(dirname "${BASH_SOURCE}")/.."

DIR="${SPDKCSI_DIR}/scripts/ci"
# shellcheck source=scripts/ci/env
source "${DIR}/env"
# shellcheck source=scripts/ci/common.sh
source "${DIR}/common.sh"

SPDK_VERSION="${SPDK_VERSION:-master}" # spdkdev will be built from this git tag
SPDK_CONTAINER="spdkdev-e2e"
SPDK_IMAGE="spdkdev:latest"
SMA_CLIENT=/root/spdk/scripts/sma-client.py
SMA_SERVER=/root/spdk/scripts/sma.py
SMA_ADDRESS="${SMA_ADDRESS:-localhost}"
SMA_PORT="${SMA_PORT:-5114}"

if ! command -v kubectl >&/dev/null; then
    if [ -x /var/lib/minikube/binaries/${KUBE_VERSION}/kubectl ]; then
        export PATH=/var/lib/minikube/binaries/${KUBE_VERSION}:$PATH
        echo "added kubectl ${KUBE_VERSION} to PATH."
    else
        echo "warning: kubectl ${KUBE_VERSION} not found."
    fi
fi

for cmd in jq nc docker; do
    if ! command -v "$cmd" >&/dev/null; then
        echo "warning: command '$cmd' not found"
    fi
done

function e2e-sma-call() {
    # Usage:   e2e-sma-call METHOD [ARG=VALUE...]
    # Example: e2e-sma-call GetQosCapabilities 'device_type="my-device-type"'
    local jsonmsg="" sep="" key value key_value
    if [ -z "$1" ]; then
        echo "e2e-sma-call: missing METHOD"
        return 1
    fi
    jsonmsg+="{"
    jsonmsg+="\"method\": \"$1\","
    jsonmsg+="\"params\":{"
    shift
    for key_value in "$@"; do
        key="${key_value/=*/}"
        value="${key_value/*=/}"
        jsonmsg+="$sep\"$key\": $value"
        sep=","
    done
    jsonmsg+="}"
    jsonmsg+="}"
    echo "$jsonmsg" | jq
    echo "$jsonmsg" | docker exec -i "${SPDK_CONTAINER}" "${SMA_CLIENT}" --address "${SMA_ADDRESS}" --port "${SMA_PORT}"
}

function e2e-sma-trace() {
    local sma_server_pid
    sma_server_pid="$(pgrep -f '/root/spdk/scripts/sma.py --address')"
    [ -z "$sma_server_pid" ] && {
        echo "sma server not running. try: e2e-spdk-start"
        return 1
    }
    command -v strace >&/dev/null || {
        echo "strace not found."
        return 1
    }
    ( set -x
      strace -e trace=network -f -s 2048 -p "$sma_server_pid"
    )
}

function e2e-k8s-start() {
    ${SPDKCSI_DIR}/scripts/minikube.sh up
}

function e2e-k8s-stop() {
    minikube delete
}

function e2e-spdk-running() {
    docker inspect "${SPDK_CONTAINER}" >&/dev/null
}

function e2e-spdk-stop() {
    e2e-spdk-running || {
        echo "${SPDK_CONTAINER} not running"
        return 0
    }
    docker rm -f "${SPDK_CONTAINER}" || {
        echo "stopping ${SPDK_CONTAINER} failed (exit status $?)"
        return 0
    }
    echo "stopped"
}

function e2e-spdk-build() {
    docker build -t "${SPDK_IMAGE}" -f "${SPDKCSI_DIR}/deploy/spdk/Dockerfile" "${docker_proxy_opt[@]}" --build-arg TAG="${SPDK_VERSION}" "${SPDKCSI_DIR}/deploy/spdk" && echo "${SPDK_IMAGE} from version ${SPDK_VERSION} build successfully"
}

function e2e-spdk-start() {
    e2e-spdk-running && {
        echo "spdk already running. Restart with:"
        echo "e2e-spdk-stop; e2e-spdk-start"
        return 0
    }
    echo "======== start spdk target ========"
    # allocate 1024*2M hugepage
    sudo sh -c 'echo 1024 > /proc/sys/vm/nr_hugepages'
    # start spdk target
    sudo docker run -id --name "${SPDK_CONTAINER}" --privileged --net host -v /dev/hugepages:/dev/hugepages -v /dev/shm:/dev/shm ${SPDKIMAGE} /root/spdk/build/bin/spdk_tgt
    sleep 5s
    # wait for spdk target ready
    sudo docker exec -i "${SPDK_CONTAINER}" timeout 5s /root/spdk/scripts/rpc.py framework_wait_init
    # create 1G malloc bdev
    sudo docker exec -i "${SPDK_CONTAINER}" /root/spdk/scripts/rpc.py bdev_malloc_create -b Malloc0 1024 4096
    # create lvstore
    sudo docker exec -i "${SPDK_CONTAINER}" /root/spdk/scripts/rpc.py bdev_lvol_create_lvstore Malloc0 lvs0
    # start jsonrpc http proxy
    sudo docker exec -id "${SPDK_CONTAINER}" /root/spdk/scripts/rpc_http_proxy.py ${JSONRPC_IP} ${JSONRPC_PORT} ${JSONRPC_USER} ${JSONRPC_PASS}
    echo "======== start sma server at ${SMA_ADDRESS}:${SMA_PORT} ========"
    sudo docker exec -id "${SPDK_CONTAINER}" "${SMA_SERVER}" --address "${SMA_ADDRESS}" --port "${SMA_PORT}"
    sleep 1
    while ! nc -z "${SMA_ADDRESS}" "${SMA_PORT}"; do
        sleep 1
        echo -n "."
    done
    echo ok
}

function e2e-node-logs() {
    kubectl logs $(kubectl get pods | awk '/spdkcsi-node-/{print $1}') spdkcsi-node
}

function e2e-controller-logs() {
    kubectl logs spdkcsi-controller-0 spdkcsi-controller
}

function e2e-delete-deployments() {
    (
        kubectl delete daemonsets --all --now &
        kubectl delete statefulsets --all --now &
        kubectl delete configmaps --all --now &
        kubectl delete namespaces $(kubectl get namespaces | awk '/spdkcsi-/{print $1}') &
        wait
    )
}

function e2e-node-debug() {
    local pid_of_spkdcsi_node
    pid_of_spdkcsi_node="$(pgrep -f '/usr/local/bin/spdkcsi.*--node$')"
    [ -n "$pid_of_spdkcsi_node" ] || {
        echo "cannot find pid of: spdkcsi --node"
        return 1
    }
    e2e-debug-pid "$pid_of_spdkcsi_node"
}

function e2e-controller-debug() {
    local pid_of_spkdcsi_controller
    pid_of_spdkcsi_controller="$(pgrep -f '/usr/local/bin/spdkcsi.*--controller$')"
    [ -n "$pid_of_spdkcsi_controller" ] || {
        echo "cannot find pid of: spdkcsi --controller"
        return 1
    }
    e2e-debug-pid "$pid_of_spdkcsi_controller"
}

function e2e-debug-pid() {
    local pid="$1"
    local dlv="$(command -v dlv 2>/dev/null)"
    [ -n "$dlv" ] || {
        echo 'dlv not found. Tip: go install github.com/go-delve/delve@latest; PATH=$PATH:$HOME/go/bin'
        return 1
    }
    echo "Attaching $dlv to $pid"
    $dlv attach "$pid"
}

function e2e-spdkcsi-build() {
    e2e-delete-deployments
    ( cd "$SPDKCSI_DIR"; make DEBUG=1 image )
}

function e2e-spdkcsi-start() {
    (
        set -e -x
        cd "${SPDKCSI_DIR}/deploy/kubernetes"
        source ./deploy.sh
    )
}

function e2e-spdkcsi-stop() {
    (
        set -e -x
        cd "${SPDKCSI_DIR}/deploy/kubernetes"
        source ./deploy.sh teardown
    )
}

function e2e-test() {
    (
        set -e -x
        e2e-spdk-running || e2e-spdk-start
        cd "${SPDKCSI_DIR}"
        go test -test.v ./e2e
    )
}
