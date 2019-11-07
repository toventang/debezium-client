FROM alpine

LABEL maintainer="Toven Tang<ttw130@gmail.com>"

COPY ./build/debeclient /usr/local/bin/debeclient
COPY ./docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

RUN chmod 755 /usr/local/bin/docker-entrypoint.sh

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]