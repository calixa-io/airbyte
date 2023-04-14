FROM bitnami/kubectl AS builder
FROM node:alpine

COPY --from=builder /opt/bitnami/kubectl/bin /opt/bitnami/kubectl/bin
#COPY --from=node /usr/local/bin /usr/local/bin
#COPY --from=node /usr/local/lib /usr/local/lib
#COPY --from=node /opt /opt
RUN apk add bash
ENV PATH="/opt/bitnami/kubectl/bin:$PATH"
#ENTRYPOINT["echo", "Testing..."]
USER 1000
