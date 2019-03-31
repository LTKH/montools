FROM centos:7

ENV APP_NAME="etcd" \
    IMAGE_VERSION="v3.3.12" \
    ETCD_ADVERTISE_CLIENT_URLS="http://127.0.0.1:2379" \
    ETCD_DATA_DIR="/opt/etcd/data/" \
    ETCD_LISTEN_CLIENT_URLS="http://0.0.0.0:2379"

RUN yum install wget -y
RUN wget -nc -P /tmp/etcd https://github.com/etcd-io/etcd/releases/download/v3.3.12/etcd-v3.3.12-linux-amd64.tar.gz &&\
    tar -zxf /tmp/etcd/etcd-v3.3.12-linux-amd64.tar.gz -C /tmp/etcd &&\
    cp /tmp/etcd/etcd-v3.3.12-linux-amd64/etcd /usr/bin &&\
    cp /tmp/etcd/etcd-v3.3.12-linux-amd64/etcdctl /usr/bin &&\
    rm -rf /tmp/etcd

RUN chmod g+rwX /usr/bin/etcd /usr/bin/etcdctl

EXPOSE 2379 2380
