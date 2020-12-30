FROM golang:1.15

WORKDIR /app

RUN git clone --depth=1 https://github.com/evookelj/inmap.git

WORKDIR /app/inmap

RUN go install ./...

ENV INMAP_ROOT_DIR /app/inmap/
