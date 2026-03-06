FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY . .
RUN CGO_ENABLED=0 go build -o igw-mock .

FROM gcr.io/distroless/static:nonroot
COPY --from=builder /app/igw-mock /igw-mock
ENTRYPOINT ["/igw-mock"]
