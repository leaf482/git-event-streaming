FROM golang:1.24-alpine AS build

WORKDIR /src

COPY go.mod go.sum* ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/ingestor ./cmd/ingestor \
    && CGO_ENABLED=0 GOOS=linux go build -o /out/consumer ./cmd/consumer

FROM alpine:3.20

RUN apk add --no-cache ca-certificates wget \
    && addgroup -S app \
    && adduser -S app -G app

WORKDIR /app
COPY --from=build /out/ingestor /app/ingestor
COPY --from=build /out/consumer /app/consumer

USER app

EXPOSE 8080
ENTRYPOINT ["/app/ingestor"]
