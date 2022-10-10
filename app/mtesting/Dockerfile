FROM golang:1.17 AS builder

COPY . /src/
WORKDIR /src/
RUN go build -o /bin/mtstress main.go

FROM alpine:3.15.0

ADD https://github.com/sgerrand/alpine-pkg-glibc/releases/download/2.34-r0/glibc-2.34-r0.apk /tmp
RUN apk update && \
    apk add --no-cache bash curl && \
    apk add --allow-untrusted /tmp/*.apk && rm -f /tmp/*.apk

COPY --from=builder /bin/mtstress /bin/mtstress

ENTRYPOINT ["/bin/mtstress"]
CMD ["-remoteWrite.url=http://vminsert.default.svc.cluster.local:8480/insert/0/influx/write"]
