FROM apache/airflow:3.1.8-python3.11

USER root

# Keep browser binaries in a shared location and expose only the project source
# and DAG folder on PYTHONPATH.
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright
ENV PYTHONPATH=/opt/airflow/project/src:/opt/airflow/dags

# Install the shared libraries Chromium needs when launched by Playwright.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libglib2.0-0 \
        libnss3 \
        libnspr4 \
        libdbus-1-3 \
        libatk1.0-0 \
        libatk-bridge2.0-0 \
        libcups2 \
        libdrm2 \
        libxkbcommon0 \
        libatspi2.0-0 \
        libxcomposite1 \
        libxdamage1 \
        libxfixes3 \
        libxrandr2 \
        libgbm1 \
        libasound2 \
        libpango-1.0-0 \
        libcairo2 \
        libx11-6 \
        libxcb1 \
        libxext6 \
        libxshmfence1 \
        libxrender1 \
        libfontconfig1 \
        libfreetype6 \
        ca-certificates \
        fonts-liberation \
    && rm -rf /var/lib/apt/lists/*

# Copy only what is needed to install the project package into the image.
COPY pyproject.toml README.md /tmp/project/
COPY src /tmp/project/src

RUN mkdir -p /ms-playwright \
    && chown -R airflow:0 /ms-playwright /tmp/project

USER airflow

# Install the ETL/runtime Python packages with Airflow constraints, then install
# the project package itself without re-resolving dependencies.
RUN pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      --constraint "${HOME}/constraints.txt" \
      "psycopg[binary]>=3.1,<4.0" \
      "PyYAML>=6.0" \
      "python-dotenv>=1.0,<2.0" \
      "beautifulsoup4>=4.12,<5.0" \
      "playwright>=1.52,<2.0" \
      "requests>=2.32,<3.0" \
    && pip install --no-cache-dir --no-deps /tmp/project \
    && pip install --no-cache-dir --no-deps "chardet==5.2.0" \
    && pip check \
    && python -m playwright install chromium \
    && rm -rf /tmp/project
