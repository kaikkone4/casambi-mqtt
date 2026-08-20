FROM python:3.13

WORKDIR /app

COPY requirements-server.txt .

RUN pip install --no-cache-dir -r requirements-server.txt

COPY custom_components/casambi_mqtt/entities/ custom_components/casambi_mqtt/entities/
# Apache-2.0 requires the licence and notices to travel with the artifact
# that carries the adapted casambi-bt switch decoder.
COPY THIRD_PARTY_LICENSES/ THIRD_PARTY_LICENSES/
COPY switch_decoder.py .
COPY server.py .

CMD ["python", "server.py"]
