FROM golang:1.25-alpine AS build

WORKDIR /src

COPY go.mod go.sum* ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/ingestor ./cmd/ingestor \
    && CGO_ENABLED=0 GOOS=linux go build -o /out/consumer ./cmd/consumer \
    && CGO_ENABLED=0 GOOS=linux go build -o /out/replay ./cmd/replay \
    && CGO_ENABLED=0 GOOS=linux go build -o /out/benchgen ./cmd/benchgen

FROM alpine:3.20

RUN apk add --no-cache ca-certificates wget \
    && addgroup -S app \
    && adduser -S app -G app

WORKDIR /app
COPY --from=build /out/ingestor /app/ingestor
COPY --from=build /out/consumer /app/consumer
COPY --from=build /out/replay /app/replay
COPY --from=build /out/benchgen /app/benchgen

USER app

EXPOSE 8080
ENTRYPOINT ["/app/ingestor"]
