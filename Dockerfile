FROM alpine

LABEL maintainer="Toven Tang<ttw130@gmail.com>"

WORKDIR /app
COPY ./build/ .

CMD ["./debeclient", "-f", "etc/config.yaml"]