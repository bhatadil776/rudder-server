# Build stage
# Using bullseye because alpine does not have git which is required to avoid proxy.golang.org timeouts
FROM golang:1.20.4-bullseye AS builder

RUN apt update && \
    apt install make tzdata ca-certificates git

# Set work directory
WORKDIR /app

# Copy go.mod and go.sum files to the workspace
COPY go.mod go.sum ./

# Download all dependencies
# Using GOPROXY=direct to avoid proxy.golang.org timeouts
#RUN GOPROXY=direct go mod download
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
