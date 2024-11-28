FROM --platform=$BUILDPLATFORM golang:1.21-alpine AS builder

RUN apk add --no-cache git
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGETARCH
ARG TARGETOS

RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o mgodatagen

FROM --platform=$TARGETPLATFORM alpine:3.18

RUN apk add --no-cache ca-certificates
COPY --from=builder /build/mgodatagen /mgodatagen

ENTRYPOINT ["mgodatagen"]
CMD ["--help"]