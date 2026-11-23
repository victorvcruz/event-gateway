ARG MODULE
FROM golang:1.25-alpine AS build
ARG MODULE
ARG COMMAND
ARG GO_BUILD_TAGS=""
WORKDIR /src
COPY go.work ./
COPY shared ./shared
COPY gateway ./gateway
COPY order-service ./order-service
COPY payment-service ./payment-service
COPY inventory-service ./inventory-service
COPY fulfillment-service ./fulfillment-service
COPY notification-service ./notification-service
RUN cd "/src/${MODULE}" && GOWORK=off go mod download && CGO_ENABLED=0 GOWORK=off go build -tags="${GO_BUILD_TAGS}" -o /out/app "./cmd/${COMMAND}"
RUN cd /src/shared && CGO_ENABLED=0 GOWORK=off go build -o /out/healthcheck ./cmd/healthcheck

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
COPY --from=build /out/healthcheck /healthcheck
USER nonroot:nonroot
ENTRYPOINT ["/app"]
