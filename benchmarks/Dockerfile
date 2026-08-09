# Runs the load generator on the compose network, so a benchmark measures
# FlexStore rather than the host's Docker port forwarder.
FROM golang:1.25 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/flexbench ./benchmarks/flexbench

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/flexbench /flexbench
USER nonroot:nonroot
WORKDIR /results
ENTRYPOINT ["/flexbench"]
