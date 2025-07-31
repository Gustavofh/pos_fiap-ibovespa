FROM apache/airflow:2.10.5-python3.11

USER root

RUN apt-get update && \
    apt-get install -y wget unzip curl gnupg chromium-driver chromium && \
    ln -s /usr/bin/chromium /usr/bin/google-chrome && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

COPY src /opt/airflow/src

COPY entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
