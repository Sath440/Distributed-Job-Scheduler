FROM golang:1.25-alpine AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /bin/worker ./worker

FROM alpine:3.20
RUN adduser -D app
USER app
COPY --from=build /bin/worker /bin/worker
EXPOSE 9090
ENTRYPOINT ["/bin/worker"]
