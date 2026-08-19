FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/engine ./cmd/engine

FROM alpine:3.20
RUN addgroup -S workflow && adduser -S workflow -G workflow
WORKDIR /app
COPY --from=build /out/engine /usr/local/bin/engine
COPY migrations ./migrations
COPY configs/config.yaml ./configs/config.yaml
USER workflow
EXPOSE 8080 9090
ENTRYPOINT ["engine", "-config", "configs/config.yaml"]
