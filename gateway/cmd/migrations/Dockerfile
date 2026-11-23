# Build stage
FROM golang:1.25-alpine AS build
WORKDIR /src

# go.work + go.mod dos dois módulos primeiro (cache de deps).
COPY go.work ./
COPY shared/go.mod shared/go.sum ./shared/
COPY gateway/go.mod gateway/go.sum ./gateway/
RUN cd shared && GOWORK=off go mod download
RUN cd gateway && GOWORK=off go mod download

# Sources.
COPY shared/ ./shared/
COPY gateway/ ./gateway/

# Build estático (CGO desabilitado).
WORKDIR /src/gateway
RUN CGO_ENABLED=0 GOWORK=off go build -o /out/migrations ./cmd/migrations

# Runtime stage
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/migrations /migrations
USER nonroot:nonroot
ENTRYPOINT ["/migrations"]
