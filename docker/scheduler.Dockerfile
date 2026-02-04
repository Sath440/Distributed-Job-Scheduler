FROM golang:1.25-alpine AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /bin/scheduler ./scheduler

FROM alpine:3.20
RUN adduser -D app
USER app
COPY --from=build /bin/scheduler /bin/scheduler
EXPOSE 9000 9090
ENTRYPOINT ["/bin/scheduler"]
