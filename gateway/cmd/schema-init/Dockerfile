FROM golang:1.25-alpine AS build
WORKDIR /src

COPY go.work ./
COPY shared/go.mod shared/go.sum ./shared/
COPY gateway/go.mod gateway/go.sum ./gateway/
RUN cd shared && GOWORK=off go mod download
RUN cd gateway && GOWORK=off go mod download

COPY shared/ ./shared/
COPY gateway/ ./gateway/
WORKDIR /src/gateway
RUN CGO_ENABLED=0 GOWORK=off go build -o /out/schema-init ./cmd/schema-init

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/schema-init /schema-init
USER nonroot:nonroot
ENTRYPOINT ["/schema-init"]
