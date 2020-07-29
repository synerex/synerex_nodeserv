FROM golang:1.13.0-buster

WORKDIR /go/src/github.com/synerex_nodeserv

# install tools
RUN apt-get update
RUN apt-get -y install zip unzip

# install protocol buffers
RUN curl -OL https://github.com/google/protobuf/releases/download/v3.9.1/protoc-3.9.1-linux-x86_64.zip
RUN unzip protoc-3.9.1-linux-x86_64.zip -d protoc3
RUN mv protoc3/bin/* /usr/local/bin/
RUN mv protoc3/include/* /usr/local/include/
RUN go get -u github.com/golang/protobuf/protoc-gen-go

# install grpc for golang lib
RUN go get -u google.golang.org/grpc

# copy code
COPY . .
RUN sed -i 's/\r//' ./entrypoint.sh
RUN chmod +x ./entrypoint.sh

# build
RUN go build

# expose port
EXPOSE 9990

ENTRYPOINT ["./entrypoint.sh"]
