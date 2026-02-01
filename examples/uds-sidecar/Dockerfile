FROM golang:1.21-alpine AS build
WORKDIR /src
COPY . .
RUN go build -o /out/uds-sidecar ./main.go

FROM alpine:3.19
WORKDIR /app
COPY --from=build /out/uds-sidecar /usr/local/bin/uds-sidecar
ENTRYPOINT ["uds-sidecar"]
