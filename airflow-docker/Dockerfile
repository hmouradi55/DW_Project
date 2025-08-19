FROM apache/airflow:2.7.0-python3.10

USER root
# -------------------------------------------------------------
# 1. Headless Chrome + driver + minimal libs for Selenium
# -------------------------------------------------------------
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        chromium            \
        chromium-driver     \
        fonts-liberation    \
        libnss3 libatk1.0-0 libatk-bridge2.0-0 libgtk-3-0 \
        libx11-xcb1 libxcomposite1 libxdamage1 libxrandr2 libxfixes3 \
        libasound2 libxss1 libxtst6 libcairo2 libpangocairo-1.0-0 && \
    rm -rf /var/lib/apt/lists/*
ENV CHROME_BIN=/usr/bin/chromium
ENV PATH="$PATH:/usr/bin"          
# chromedriver is /usr/bin/chromedriver

USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
USER airflow
