FROM golang:1.21

WORKDIR /app
ENV CGO_ENABLED=0 GOCACHE=/tmp/go-build

COPY go.mod ./
RUN go mod download

COPY . .

RUN go test ./...

CMD ["go", "test", "./..."]
