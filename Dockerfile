FROM golang:1.17 AS builder

COPY . /src/
WORKDIR /src/
RUN go build -mod=vendor -o /bin/$app_name app/$app_name/main.go

FROM alpine:3.15.0

COPY --from=builder /bin/$app_name /bin/$app_name

ENTRYPOINT ["/bin/$app_name"]
CMD [""]
