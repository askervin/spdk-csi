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
# e2e-controller-logs [-f]          # see spdkcsi controller logs
# e2e-node-logs [-f]                # see spdkcsi node logs
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

SPDKCSI_DIR="$(dirname "${BASH_SOURCE[@]}")/.."

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
K8S_WORKER_SSH_PORT=${K8S_WORKER_SSH_PORT:-10000}
K8S_WORKER_QMP_PORT=${K8S_WORKER_QMP_PORT:-9090}

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
    "${SPDKCSI_DIR}"/scripts/minikube.sh up
}

function e2e-k8s-stop() {
    minikube delete
}

function e2e-vm-k8s-start() {
    local workerdir=/tmp/e2e-k8s-worker
    local fedora_qcow2=${workerdir}/fedora-cloud-base.qcow2
    local cloudisodir=${workerdir}/cloud-init-iso-root
    local cloud_iso=${workerdir}/seed.iso
    local qemu=${QEMU:-qemu-system-x86_64}
    local required_cmd
    local k8s_node_count

    mkdir -p "$(dirname ${fedora_qcow2})"
    mkdir -p "${cloudisodir}"

    for required_cmd in curl mkpasswd cloud-localds jq "${qemu}"; do
        command -v "${required_cmd}" >/dev/null || {
            echo "missing: ${required_cmd}"
            return 1
        }
    done

    if [[ $(< "${workerdir}/qemu.pid" ) -gt 0 ]] && [[ -d /proc/$(< "${workerdir}/qemu.pid") ]]; then
        echo "worker vm (pid: $(<"${workerdir}/qemu.pid")) is running, not starting new"
        return 1
    fi

    # Download the cloud image if not already present
    [ -f "${fedora_qcow2}.clean" ] || ( curl -Lk https://fedora.mirrorservice.org/fedora/linux/releases/36/Cloud/x86_64/images/Fedora-Cloud-Base-36-1.5.x86_64.qcow2 > "${fedora_qcow2}.clean" )
    cp "${fedora_qcow2}.clean" "${fedora_qcow2}"
    qemu-img resize "${fedora_qcow2}" +30G

    # Prepare cloud-init
    (
        cd "${cloudisodir}"
        echo "instance-id: e2e-k8s-worker-start" > meta-data
        echo "local-hostname: e2eworker" >> meta-data
        cat > user-data << EOF
#cloud-config
disable_root: False
chpasswd: { expire: False }
ssh_pwauth: True
users:
- name: root
  lock_passwd: False
  ssh_authorized_keys:
  - $(< ~/.ssh/id_rsa.pub)
- name: fedora
  lock_passwd: False
  passwd: "$(echo fedora | mkpasswd -s)"
  ssh_authorized_keys:
  - $(< ~/.ssh/id_rsa.pub)
- name: debug
  shell: /bin/bash
  lock_passwd: False
  passwd: "$(echo debug | mkpasswd -s)"
  ssh_authorized_keys:
  - $(< ~/.ssh/id_rsa.pub)
  sudo: ALL=(ALL) NOPASSWD:ALL
- name: sys_sgci
  shell: /bin/bash
  lock_passwd: False
  ssh_authorized_keys:
  - $(< ~/.ssh/id_rsa.pub)
  sudo: ALL=(ALL) NOPASSWD:ALL
chpasswd:
  expire: False
  users:
  - name: root
    password: "$(echo root | mkpasswd -s)"
EOF
        cloud-localds "${cloud_iso}" user-data meta-data
    )
    [ -f "${cloud_iso}" ] || {
        echo "failed to create cloud-init image ${cloud_iso}"
        return 1
    }

    rm -f ${workerdir}/qemu-serial*; mkfifo ${workerdir}/qemu-serial.{in,out}
    ( set -x
      ${qemu} -m 2048 -machine accel=kvm -cpu host \
              -object memory-backend-file,id=mem,size=2048M,mem-path=/dev/hugepages,share=on,prealloc=yes,host-nodes=0,policy=bind \
              -smp 8,sockets=2,cores=2,threads=2 \
              -numa node,memdev=mem \
              -drive file="${fedora_qcow2}",if=virtio,format=qcow2 \
              -drive file="${cloud_iso}",if=virtio \
              -nographic \
              -netdev user,id=mynet0,hostfwd=tcp::${K8S_WORKER_SSH_PORT}-:22 \
              -device virtio-net-pci,netdev=mynet0 \
              -chardev pty,id=charserial0 \
              -device isa-serial,chardev=charserial0,id=serial0 \
              -qmp tcp:localhost:${K8S_WORKER_QMP_PORT},server,nowait -device pci-bridge,chassis_nr=1,id=pci.spdk.0 -device pci-bridge,chassis_nr=2,id=pci.spdk.1 \
              -device pci-bridge,chassis_nr=3,id=pci.spdk.2 -device pci-bridge,chassis_nr=4,id=pci.spdk.3 \
              -serial pipe:${workerdir}/qemu-serial >"${workerdir}/qemu-stdout" 2>"${workerdir}/qemu-stderr" &
      echo $! >"${workerdir}/qemu.pid"
    )
    sleep 1
    # Keep reading the serial console output, otherwise Qemu is blocked by a full pipe
    cat <"${workerdir}/qemu-serial.out" >"${workerdir}/qemu-serial.out.log" &

    # Now the virtual machine is booting up. Wait for ssh to start working
    SCP="scp -P ${K8S_WORKER_SSH_PORT}"
    K8WSSH="ssh -p ${K8S_WORKER_SSH_PORT} root@localhost"
    WORKER="root@localhost"

    ssh-keygen -R "[localhost]:${K8S_WORKER_SSH_PORT}"
    sleep 5
    echo -n "waiting for successful $K8WSSH"
    while :; do
        ssh -p ${K8S_WORKER_SSH_PORT} -o StrictHostKeyChecking=accept-new -o ConnectTimeout=1 root@localhost 'echo ssh login to vm works as $(whoami)' 2>/dev/null && break
        sleep 1
        if ! ([[ $(< "${workerdir}/qemu.pid" ) -gt 0 ]] && [[ -d /proc/$(< "${workerdir}/qemu.pid") ]]); then
            cat "${workerdir}/qemu-stdout" "${workerdir}/qemu-stderr"
            echo 'error: e2e-k8s-worker vm has died.'
            return 1
        fi
        echo -n "."
    done

    local kubeadm_bin
    local kubelet_bin
    local kubectl_bin
    local crictl_bin
    kubeadm_bin=$(command -v kubeadm)
    kubelet_bin=$(command -v kubelet)
    kubectl_bin=$(command -v kubectl)
    crictl_bin=$(command -v crictl)

    # Configure proxies
    if [ -n "$http_proxy" ]; then
        for vars in "filep='' linep='' file='/etc/environment'" \
                        "filep='' linep='export ' file='/etc/profile.d/proxy.sh'" \
                        "filep='[Service]' linep='Environment=' file='/etc/systemd/system/containerd.service.d/proxy.conf'"
        do
            ( eval $vars
              ext_no_proxy=192.168.0.0/16,172.16.0.0/12,10.0.0.0/8,.internal
              cat <<EOF |
${filep}
${linep}http_proxy=$http_proxy
${linep}https_proxy=$https_proxy
${linep}ftp_proxy=$ftp_proxy
${linep}no_proxy=$no_proxy,$ext_no_proxy
${linep}HTTP_PROXY=$http_proxy
${linep}HTTPS_PROXY=$https_proxy
${linep}FTP_PROXY=$ftp_proxy
${linep}NO_PROXY=$no_proxy,$ext_no_proxy
EOF
              $K8WSSH "mkdir -p $(dirname "$file"); cat > $file"
            )
        done
    fi

    # Create kubelet.service
    cat <<EOF |
[Unit]
Description=kubelet: The Kubernetes Node Agent
Documentation=https://kubernetes.io/docs/
Wants=network-online.target
After=network-online.target

[Service]
ExecStart=/usr/local/bin/kubelet --bootstrap-kubeconfig=/etc/kubernetes/bootstrap-kubelet.conf --kubeconfig=/etc/kubernetes/kubelet.conf --config=/var/lib/kubelet/config.yaml --container-runtime-endpoint=unix:///var/run/containerd/containerd.sock --pod-infra-container-image=k8s.gcr.io/pause:3.4.1
Restart=always
StartLimitInterval=0
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF
    $K8WSSH "mkdir -p /etc/systemd/system; cat > /etc/systemd/system/kubelet.service"

    # Add certs, containernetworking plugins and necessary binaries
    ( cd /
      tar czf - \
          /etc/cni \
          /opt/cni \
          /etc/kubelet-resolv.conf \
          | $K8WSSH "tar -C / -xzvf -"
      for host_bin in ${kubeadm_bin} ${kubelet_bin} ${kubectl_bin} ${crictl_bin}; do
          $SCP "${host_bin}" $WORKER:/usr/local/bin
      done
    )

    # Install packages, load modules, disable swap...
    $K8WSSH "set -e -x
            modprobe bridge 2>/dev/null && echo bridge >> /etc/modules-load.d/k8s.conf || :
            modprobe nf-tables-bridge 2>/dev/null && echo nf-tables-bridge >> /etc/modules-load.d/k8s.conf || :
            modprobe br_netfilter 2>/dev/null && echo br_netfilter >> /etc/modules-load.d/k8s.conf || :
            echo 1 > /proc/sys/net/ipv4/ip_forward
            dnf -y install dnf-plugins-core
            dnf config-manager --add-repo https://download.docker.com/linux/fedora/docker-ce.repo
            dnf install -y iproute-tc docker-ce docker-ce-cli containerd.io docker-compose-plugin conntrack iptables
            ln -s /opt/cni/bin /usr/libexec/cni
            systemctl enable containerd
            systemctl enable docker
            systemctl start containerd
            systemctl start docker
            systemctl enable kubelet
            grep -q PATH= /etc/environment || {
                export PATH=\$PATH:/usr/local/bin
                echo PATH=\$PATH >> /etc/environment
            }
            ( systemctl | iconv -f utf-8 -t ascii -c | awk '/swap/{print \$1}' | xargs -n 1 systemctl stop ) || :
            ( systemctl | iconv -f utf-8 -t ascii -c | awk '/swap/{print \$1}' | xargs -n 1 systemctl disable ) || :
            dnf remove -y zram-generator-defaults || :
            [ -f /etc/kubernetes/kubelet.conf ] && {
                yes | kubeadm reset
            }
            mkdir -p /etc/kubernetes/manifests
          "

    # Copy spdkcsi image so that spdkcsi node pod can use the already present image.
    # (Will be imported to containerd image repository later when containerd is installed.)
    declare -A ImageDict
    ImageDict=( ['registry.k8s.io/kube-apiserver:v1.25.0']="kube-apiserver"
                ['registry.k8s.io/kube-controller-manager:v1.25.0']="kube-controller-manager"
                ['registry.k8s.io/kube-scheduler:v1.25.0']="kube-scheduler" 
                ['registry.k8s.io/kube-proxy:v1.25.0']="kube-proxy"
                ['registry.k8s.io/pause:3.8']="registry-pause"
                ['registry.k8s.io/etcd:3.5.4-0']="etcd"
                ['registry.k8s.io/coredns/coredns:v1.9.3']="coredns" 
                ['k8s.gcr.io/pause:3.6']="k8s-pause"
                ['k8s.gcr.io/sig-storage/csi-attacher:v3.0.0']="csi-attacher"
                ['k8s.gcr.io/sig-storage/csi-node-driver-registrar:v2.0.1']="csi-node-driver-registrar"
                ['k8s.gcr.io/sig-storage/csi-provisioner:v2.0.2']="csi-provisioner"
                ['k8s.gcr.io/sig-storage/csi-snapshotter:v3.0.3']="csi-snapshotter"
                ['k8s.gcr.io/sig-storage/snapshot-controller:v3.0.3']="snapshot-controller"
                ['spdkcsi/spdkcsi:canary']="spdkcsi"
                ['gcr.io/k8s-minikube/storage-provisioner:v5']="storage-provisioner"
                ['fedora:33']="fedora"
                ['alpine:3.8']="alpine" )
    $K8WSSH mkdir -p /home/tmp
    for image in "${!ImageDict[@]}"
    do
        echo "$image => ${ImageDict[$image]}"
        ( set -x
            docker image save $image | $K8WSSH "cat > /home/tmp/${ImageDict[$image]}.image"
            $K8WSSH ctr -n k8s.io images import /home/tmp/${ImageDict[$image]}.image
        )
    done    
    
    $K8WSSH "set -e -x
            swapoff -a
            rm /etc/containerd/config.toml
            systemctl restart containerd
            kubeadm init
            "
}

function e2e-k8s-worker-stop() {
    if [[ $(< "${workerdir}/qemu.pid" ) -gt 0 ]] && [[ -d /proc/$(< "${workerdir}/qemu.pid") ]]; then
        kubectl delete node e2eworker
        kill "$(< "${workerdir}/qemu.pid" )"
        return 0
    fi
    echo "e2eworker not running"
    return 1
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
    docker_proxy_opt=("--build-arg" "http_proxy=$HTTP_PROXY" "--build-arg" "https_proxy=$HTTPS_PROXY")
    docker build -t "${SPDK_IMAGE}" -f "${SPDKCSI_DIR}/deploy/spdk/Dockerfile" "${docker_proxy_opt[@]}" --build-arg TAG="${SPDK_VERSION}" "${SPDKCSI_DIR}/deploy/spdk" && echo "${SPDK_IMAGE} from version ${SPDK_VERSION} build successfully"
}

function e2e-spdk-start() {
    e2e-spdk-running && {
        echo "spdk already running. Restart with:"
        echo "e2e-spdk-stop; e2e-spdk-start"
        return 0
    }
    echo "======== start spdk target ========"
    # allocate 2048*2M hugepage for both spdk and qemu hugepages
    sudo sh -c 'echo 2048 > /proc/sys/vm/nr_hugepages'
    # add host directories to the spdk container that will enable lsmod/modprobe vfio
    local docker_volumes="-v /sbin:/usr/local/sbin -v /lib/modules:/lib/modules"
    [ -d /bin/kmod ] && docker_volumes+=" -v /bin/kmod:/bin/kmod"
    # start spdk target
    sudo docker run -id --name "${SPDK_CONTAINER}" --privileged --net host -v /dev/hugepages:/dev/hugepages -v /var/tmp:/var/tmp -v /dev/shm:/dev/shm ${docker_volumes} ${SPDKIMAGE} 
    sudo docker exec -i "${SPDK_CONTAINER}" sh -c "HUGEMEM=4096 /root/spdk/scripts/setup.sh; /root/spdk/build/bin/spdk_tgt &"
    sleep 5s
    # wait for spdk target ready
    sudo docker exec -i "${SPDK_CONTAINER}" timeout 5s /root/spdk/scripts/rpc.py framework_wait_init
    # Create tcp transport
    sudo docker exec -i "${SPDK_CONTAINER}" /root/spdk/scripts/rpc.py nvmf_create_transport -t tcp
    sudo docker exec -i "${SPDK_CONTAINER}" /root/spdk/scripts/rpc.py nvmf_get_transports --trtype tcp
    # create 1G malloc bdev
    sudo docker exec -i "${SPDK_CONTAINER}" /root/spdk/scripts/rpc.py bdev_malloc_create -b Malloc0 1024 4096
    # create lvstore
    sudo docker exec -i "${SPDK_CONTAINER}" /root/spdk/scripts/rpc.py bdev_lvol_create_lvstore Malloc0 lvs0
    # start jsonrpc http proxy
    sudo docker exec -id "${SPDK_CONTAINER}" /root/spdk/scripts/rpc_http_proxy.py ${JSONRPC_IP} ${JSONRPC_PORT} ${JSONRPC_USER} ${JSONRPC_PASS}
    echo "======== start sma server at ${SMA_ADDRESS}:${SMA_PORT} ========"
    # start sma server
    sudo docker exec -d "${SPDK_CONTAINER}" sh -c "${SMA_SERVER} --address 127.0.0.1 --port ${SMA_PORT} --config /root/sma.yaml >& /var/log/$(basename ${SMA_SERVER}).log"

    sleep 1
    while ! nc -z "${SMA_ADDRESS}" "${SMA_PORT}"; do
        sleep 1
        echo -n "."
    done
    echo ok
}

function e2e-node-logs() {
    kubectl logs "$(kubectl get pods -o wide | awk "/spdkcsi-node-.*$(hostname)/{print \$1}")" spdkcsi-node "$@"
}

function e2e-worker-node-logs() {
    local crictl="crictl -r unix:/run/containerd/containerd.sock"
    $K8WSSH "container_id=\$($crictl ps --name spdkcsi-node | awk '/spdkcsi-node/{print \$1}');
        [ -z \$container_id ] && { echo spdkcsi-node container not found in worker; return 1; };
        $crictl logs \$container_id
        "
}

function e2e-controller-logs() {
    kubectl logs spdkcsi-controller-0 spdkcsi-controller "$@"
}

function e2e-delete-deployments() {
    (
        kubectl delete daemonsets --all --now &
        kubectl delete statefulsets --all --now &
        kubectl delete configmaps --all --now &
        kubectl delete namespaces "$(kubectl get namespaces | awk '/spdkcsi-/{print $1}')" &
        wait
    )
}

function e2e-node-debug() {
    pid_of_spdkcsi_node="$(pgrep -f '/usr/local/bin/spdkcsi.*--node$')"
    [ -n "$pid_of_spdkcsi_node" ] || {
        echo "cannot find pid of: spdkcsi --node"
        return 1
    }
    e2e-debug-pid "$pid_of_spdkcsi_node"
}

function e2e-controller-debug() {
    pid_of_spdkcsi_controller="$(pgrep -f '/usr/local/bin/spdkcsi.*--controller$')"
    [ -n "$pid_of_spdkcsi_controller" ] || {
        echo "cannot find pid of: spdkcsi --controller"
        return 1
    }
    e2e-debug-pid "$pid_of_spdkcsi_controller"
}

function e2e-debug-pid() {
    local pid
    pid="$1"
    local dlv
    dlv="$(command -v dlv 2>/dev/null)"
    [ -n "$dlv" ] || {
        echo "dlv not found. Tip: go install github.com/go-delve/delve/cmd/dlv@latest; PATH=$PATH:$HOME/go/bin"
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
        /bin/bash deploy.sh
    )
}

function e2e-spdkcsi-stop() {
    (
        set -e -x
        cd "${SPDKCSI_DIR}/deploy/kubernetes"
        /bin/bash deploy.sh teardown
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
