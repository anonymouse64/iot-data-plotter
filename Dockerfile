FROM golang:1.12-alpine as build-env

ENV GO111MODULE=on
ENV CGO_ENABLED=0
WORKDIR /root/git/iot-data-plotter
RUN apk update && apk add gcc git
COPY go.mod .
COPY go.sum .
RUN go mod download
COPY . .
RUN go build ./cmd/server

FROM alpine

COPY --from=build-env /root/git/iot-data-plotter/server /usr/bin/iot-data-plotter-server
COPY --from=build-env /root/git/iot-data-plotter/static /static
COPY --from=build-env /root/git/iot-data-plotter/templates /templates
COPY --from=build-env /root/git/iot-data-plotter/configs/default-config.toml /var/lib/iot-data-plotter/default-config.toml

# expect the config file to be mounted inside /config at config.toml
VOLUME ["/config"]

EXPOSE 3000

ENTRYPOINT ["/usr/bin/iot-data-plotter-server"]
CMD ["-c", "/config/config.toml", "start"]
