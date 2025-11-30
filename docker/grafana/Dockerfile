FROM grafana/grafana:latest

USER root

RUN apk update && apk add --no-cache unzip wget bash

COPY install-trino-plugin.sh /install-trino-plugin.sh
RUN chmod +x /install-trino-plugin.sh

RUN /install-trino-plugin.sh

ENV GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS=trino-datasource

USER grafana
