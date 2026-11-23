FROM golang:1.25-alpine AS build
ARG GO_BUILD_TAGS=""
WORKDIR /src
COPY go.work ./
COPY shared/go.mod shared/go.sum ./shared/
COPY gateway/go.mod gateway/go.sum ./gateway/
RUN cd shared && GOWORK=off go mod download
RUN cd gateway && GOWORK=off go mod download
COPY shared/ ./shared/
COPY gateway/ ./gateway/
WORKDIR /src/gateway
RUN CGO_ENABLED=0 GOWORK=off go build -tags="${GO_BUILD_TAGS}" -o /out/server ./cmd/server
RUN cd /src/shared && CGO_ENABLED=0 GOWORK=off go build -o /out/healthcheck ./cmd/healthcheck

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/server /server
COPY --from=build /out/healthcheck /healthcheck
USER nonroot:nonroot
EXPOSE 8080 9090
ENTRYPOINT ["/server"]
