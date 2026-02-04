FROM golang:1.25-alpine AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /bin/api ./api

FROM alpine:3.20
RUN adduser -D app
USER app
COPY --from=build /bin/api /bin/api
EXPOSE 8080 9090
ENTRYPOINT ["/bin/api"]
