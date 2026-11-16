FROM python:3.13-slim

# App-local context; the maintained runtime is pinned, never duplicated here.
ADD https://codeload.github.com/kaikkone4/casambi-mqtt/tar.gz/e5e3fd661740479489c7ed797f5707a031feaa5f /tmp/bridge.tar.gz
RUN printf '%s  %s\n' d63f26c655465eda33e57f02b297abcf65f91b814897a9504aff625057196724 /tmp/bridge.tar.gz | sha256sum -c - \
    && mkdir -p /tmp/bridge /app/custom_components/casambi_mqtt /data \
    && tar -xzf /tmp/bridge.tar.gz --strip-components=1 -C /tmp/bridge \
    && cp /tmp/bridge/server.py /tmp/bridge/switch_decoder.py /tmp/bridge/requirements-server.txt /tmp/bridge/LICENSE /app/ \
    && cp -r /tmp/bridge/custom_components/casambi_mqtt/entities /app/custom_components/casambi_mqtt/ \
    && cp /tmp/bridge/custom_components/casambi_mqtt/health_protocol.py /tmp/bridge/custom_components/casambi_mqtt/const.py /app/custom_components/casambi_mqtt/ \
    && cp -r /tmp/bridge/THIRD_PARTY_LICENSES /app/ \
    && pip install --no-cache-dir -r /app/requirements-server.txt \
    && rm -rf /tmp/bridge /tmp/bridge.tar.gz

LABEL io.casambi.bridge.revision="e5e3fd661740479489c7ed797f5707a031feaa5f"
WORKDIR /app
COPY run.py bridge_supervisor.py /app/
ENTRYPOINT ["python", "/app/bridge_supervisor.py"]
