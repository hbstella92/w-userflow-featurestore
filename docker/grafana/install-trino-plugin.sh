#!/bin/sh
set -e

PLUGIN_DIR="/var/lib/grafana/plugins/trino-datasource"
RELEASE_URL="https://github.com/trinodb/grafana-trino/releases/download/v1.0.12/trino-datasource-1.0.12.zip"

echo "[INFO] Installing Trino Grafana plugin..."

mkdir -p "$PLUGIN_DIR"

echo "[INFO] Downloading plugin zip: $RELEASE_URL"
wget -q "$RELEASE_URL" -O /tmp/trino-datasource.zip
unzip -oq /tmp/trino-datasource.zip -d "$PLUGIN_DIR"
rm /tmp/trino-datasource.zip

echo "[INFO] Trino plugin installed at $PLUGIN_DIR"
