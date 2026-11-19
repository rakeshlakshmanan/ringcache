# Build every binary under cmd/ and ship them in a distroless image.
#
# Build from the repository root so the whole module is in context:
#   docker build -f deploy/Dockerfile -t ringcache:dev .

FROM golang:1.27 AS build

WORKDIR /src

# Dependencies are their own layer, so editing source doesn't re-download
# the module cache.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# The commit hash is stamped into the binary so a running node can say
# which build it is. It defaults to "dev" when the build doesn't pass one.
ARG COMMIT=dev

# CGO off gives a static binary, which is what the distroless static
# image can run. Symbol and DWARF tables are stripped to keep it small.
RUN CGO_ENABLED=0 GOOS=linux go build \
      -trimpath \
      -ldflags "-s -w -X main.version=${COMMIT}" \
      -o /out/ ./cmd/...

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/ /usr/local/bin/

EXPOSE 7000 9100

# The image has no shell, so the health check runs the binary again in
# probe mode. It is passed the same flags the node runs with, so it
# reads the right metrics port.
HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=3 \
  CMD ["/usr/local/bin/node", "--healthcheck"]

USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/node"]
