From golang:1.11 as builder
WORKDIR /go/src/app
COPY . .

RUN CGO_ENABLED=0 GOOS=linux GO111MODULE=on go build -a -installsuffix cgo -o app .


From alpine:3.9
RUN apk add --no-cache ca-certificates && update-ca-certificates
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
ENV SSL_CERT_DIR=/etc/ssl/certs
COPY --from=builder /go/src/app/app /app
CMD ["/app"]
EXPOSE 9042
