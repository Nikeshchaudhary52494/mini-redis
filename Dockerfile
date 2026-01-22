# Build stage
FROM golang:1.23-alpine AS builder

WORKDIR /app

# Copy go mod and sum files
COPY go.mod ./

# Download all dependencies. Dependencies will be cached if the go.mod and go.sum files are not changed
RUN go mod download

# Copy the source from the current directory to the Working Directory inside the container
COPY . .

# Build the Go app
RUN go build -o mini-redis cmd/server/main.go

# Run stage
FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/mini-redis .

# Expose the default port (can be overridden)
EXPOSE 6379

# Use a shell script or direct command to allow environment variable expansion
# We default to port 6379 inside the container
CMD ["sh", "-c", "./mini-redis -port 6379 -peers \"$PEERS\""]
