# Using multistage builds: one stage builds our service and one runs it.
# This makes our Dockerfile easy to read and maintain while keeping our
# build efficient and the image small.


# Stage 1 - build. Use the go image so we have the Go compiler, dependencies,
# and various system libraries available to us. However, these take up disk
# space and are not needed after the binary is compiled.
FROM golang:1.19-alpine AS build
WORKDIR /go/src/proglog
COPY . .
# NOTE: You must statically compile the binaries for them to run in the scratch
# image because it doesn't contain the system libraries needed to run
# dynamically linked binaries. Thats why we disable CGO, the compiler
# links it dynamically by default.
RUN CGO_ENABLED=0 go build -o /go/bin/proglog ./cmd/proglog
RUN GRPC_HEALTH_PROBE_VERSION=v0.3.2 && \
    wget -qO/bin/grpc_health_probe \
    https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/\${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64 && \
    chmod +x /bin/grpc_health_probe

# Stage 2 - run. Use the scratch image (smallest docker image) and copy
# the binary into this image. This is what we end up deploying.
FROM scratch
COPY --from=build /go/bin/proglog /bin/proglog
COPY --from=build /go/bin/grpc_health_probe /bin/grpc_health_probe
ENTRYPOINT ["/bin/proglog"]

