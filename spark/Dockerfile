FROM apache/spark:3.5.8

USER root

WORKDIR /app

COPY spark/requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY . /app

ENV PYTHONPATH=/app

CMD ["sleep", "infinity"]