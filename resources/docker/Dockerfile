FROM golang:1.18 AS build

RUN useradd -u 10001 dimo

WORKDIR /go/src/github.com/DIMO-INC/trip-processor/
COPY . /go/src/github.com/DIMO-INC/trip-processor/

ENV CGO_ENABLED=0
ENV GOOS=linux
ENV GOFLAGS=-mod=vendor

RUN ls
RUN go mod tidy
RUN go mod vendor
RUN make install

FROM busybox AS package

LABEL maintainer="DIMO <hello@dimo.zone>"

WORKDIR /

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /etc/passwd /etc/passwd
COPY --from=build /go/src/github.com/DIMO-INC/trip-processor/target/bin/trip-processor .

USER dimo

EXPOSE 8080
EXPOSE 8888

CMD /trip-processor
