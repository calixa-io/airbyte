FROM bitnami/kubectl AS builder
FROM node:alpine

COPY --from=builder /opt/bitnami/kubectl/bin /opt/bitnami/kubectl/bin
RUN apk add bash
ENV PATH="/opt/bitnami/kubectl/bin:$PATH"
USER 1000
