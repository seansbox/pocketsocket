FROM alpine
ARG VERSION=0.4.0
RUN apk add --no-cache ca-certificates unzip \
 && wget -qO ps.zip https://github.com/seansbox/pocketsocket/releases/download/v${VERSION}/pocketsocket_${VERSION}_linux_amd64.zip \
 && unzip -q ps.zip -d /pb && rm ps.zip
WORKDIR /pb
COPY pb_hooks pb_hooks
COPY pb_migrations pb_migrations
COPY pb_public pb_public
CMD ["./pocketsocket", "serve", "--http", "0.0.0.0:8090"]
