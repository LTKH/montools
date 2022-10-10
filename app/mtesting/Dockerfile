FROM golang:1.17 AS builder

COPY . /src/
WORKDIR /src/
RUN go build -o /bin/mtesting main.go

FROM alpine:3.15.0

COPY --from=builder /bin/mtesting /bin/mtesting

ENTRYPOINT ["/bin/mtesting"]
CMD [""]
