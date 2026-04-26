FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/tinybus ./cmd/tinybus

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/tinybus /tinybus
USER nonroot:nonroot
# DATABASE_URL must be supplied at runtime. No HEALTHCHECK: distroless ships
# no shell. Use the platform's process-up probe instead.
ENTRYPOINT ["/tinybus"]
