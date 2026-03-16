FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY . .
RUN CGO_ENABLED=0 go build -o prom-mock .

FROM gcr.io/distroless/static:nonroot
COPY --from=builder /app/prom-mock /prom-mock
ENTRYPOINT ["/prom-mock"]
