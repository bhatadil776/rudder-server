# Build stage
FROM golang:1.20-alpine3.18 AS builder

RUN apk add --update make tzdata ca-certificates

# Set work directory
WORKDIR /app

# Copy go.mod and go.sum files to the workspace
COPY go.mod go.sum ./

# Download all dependencies
RUN go mod download

# Copy the source from the current directory to the working directory inside the container
COPY . .

# Build the Go app
RUN CGO_ENABLED=0 GOOS=linux go build -o main .

# Final stage
FROM scratch

# Set work directory
WORKDIR /root/

# Copy binary from build stage
COPY --from=builder /app/main .

# Command to run when starting the container
CMD ["./main"]
