FROM golang:1.17 AS builder
ARG app_name

COPY . /src/
WORKDIR /src/
RUN go build -o /bin/${app_name} app/${app_name}/main.go

FROM alpine:3.15.0
ARG app_name
ENV APP_NAME=${app_name}

COPY --from=builder /bin/${app_name} /bin/${app_name}

ENTRYPOINT ["/bin/${APP_NAME}"]
CMD ["-web.listen-address=:8065"]
