FROM python:3.8-slim

# Install Java
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean

# Set Java environment
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY jobs ./jobs

CMD ["python", "jobs/spark_streaming.py"]