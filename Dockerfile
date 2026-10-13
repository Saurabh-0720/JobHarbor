FROM golang:1.22-alpine AS build
WORKDIR /src
COPY . .
RUN go mod tidy && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /jobharbor ./cmd/jobharbor

FROM alpine:3.20
RUN adduser -D app
COPY --from=build /jobharbor /usr/local/bin/jobharbor
USER app
EXPOSE 8080
HEALTHCHECK CMD wget -qO- http://localhost:8080/healthz || exit 1
ENTRYPOINT ["jobharbor"]
