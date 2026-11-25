# syntax=docker/dockerfile:1
FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/raftkv /out/raftctl /usr/local/bin/
USER nonroot:nonroot
VOLUME /data
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/raftkv"]
CMD ["--data-dir=/data"]
