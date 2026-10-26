FROM golang:1.22-alpine AS build
WORKDIR /src
COPY . .
# go.sum is generated on first build if you haven't run `go mod tidy` locally.
RUN [ -f go.sum ] || go mod tidy
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ \
      ./cmd/raftkv-server ./cmd/raftkv-cli ./cmd/raftkv-bench

FROM alpine:3.20
COPY --from=build /out/ /usr/local/bin/
COPY deploy/ /etc/raftkv/
VOLUME /data
EXPOSE 7001
ENTRYPOINT ["raftkv-server"]
CMD ["-config", "/etc/raftkv/cluster.json", "-data", "/data"]
