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
RUN CGO_ENABLED=0 GOWORK=off go build -o /out/experiment-control ./cmd/experiment-control
RUN cd /src/shared && CGO_ENABLED=0 GOWORK=off go build -o /out/healthcheck ./cmd/healthcheck

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/experiment-control /experiment-control
COPY --from=build /out/healthcheck /healthcheck
USER nonroot:nonroot
EXPOSE 8086
ENTRYPOINT ["/experiment-control"]
