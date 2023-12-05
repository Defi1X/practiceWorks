FROM golang:1.19-alpine

WORKDIR /app

COPY . .

EXPOSE 6565

RUN go env -w GO111MODULE=off

RUN go build -o main .

CMD ["./main"]
