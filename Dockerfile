FROM golang:alpine AS prepare

RUN apk update && apk add --no-cache ca-certificates && update-ca-certificates

############################
# STEP 2 build a small image
############################
FROM scratch

COPY --from=prepare /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY ./artifacts /go/bin/

ENTRYPOINT ["/go/bin/goethe"]
