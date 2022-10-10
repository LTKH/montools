ENV APP_NAME=mtesting
FROM golang:1.17 AS builder

COPY . /src/
WORKDIR /src/
RUN echo "test - ${APP_NAME}"
RUN go build -o /bin/${APP_NAME} app/${APP_NAME}/main.go

FROM alpine:3.15.0

COPY --from=builder /bin/${APP_NAME} /bin/${APP_NAME}

ENTRYPOINT ["/bin/${APP_NAME}"]
CMD [""]
