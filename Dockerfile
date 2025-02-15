FROM golang:1.17 AS builder
ARG app_name

COPY . /src/
WORKDIR /src/
RUN go build -o /bin/${app_name} app/${app_name}/*.go

FROM alpine:3.15.0
ARG app_name
ARG http_listen_addr

ADD https://github.com/sgerrand/alpine-pkg-glibc/releases/download/2.34-r0/glibc-2.34-r0.apk /tmp
RUN apk update && \
    apk add --no-cache bash curl && \
    apk add --allow-untrusted /tmp/*.apk && rm -f /tmp/*.apk

EXPOSE 8010

COPY --from=builder /bin/${app_name} /bin/mtproxy

ENTRYPOINT ["/bin/mtproxy"]
CMD ["-listen.client-address={http_listen_addr}"]
