FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# The SQLite driver is pure Go, so the binary needs no C toolchain.
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /tack ./cmd/tack

FROM alpine:3.22
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 tack && mkdir /data && chown tack /data
USER tack
COPY --from=build /tack /usr/local/bin/tack
ENV TACK_DATABASE_URL=sqlite:///data/tack.db
VOLUME /data
EXPOSE 2490
ENTRYPOINT ["tack"]
