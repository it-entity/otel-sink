FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /dummy-tracers .

FROM alpine:3.20
COPY --from=builder /dummy-tracers /dummy-tracers
EXPOSE 4318 6060
ENTRYPOINT ["/dummy-tracers"]
