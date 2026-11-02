# WaCalls relay for Fly.io (single-process mode). Pure Go, no cgo.
FROM golang:1.26-alpine AS build
ENV GOTOOLCHAIN=auto CGO_ENABLED=0
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -trimpath -ldflags="-s -w" -o /out/wacalls ./cmd/server

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata
COPY --from=build /out/wacalls /usr/local/bin/wacalls
ENV WACALLS_MODE=single
EXPOSE 8080
# No -db flag: sessions live in Supabase (DATABASE_URL). Without it the service would fall back to a
# local SQLite file that is wiped on every deploy, so the deploy workflow refuses to run without it.
CMD ["wacalls", "-addr", ":8080"]
