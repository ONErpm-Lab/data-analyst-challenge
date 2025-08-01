# Base image with PySpark
FROM apache/spark-py:v3.3.2

USER root

# Set working directory
WORKDIR /app

# Copy PySpark script
COPY pipeline/ /app/src

COPY pipeline/jars /opt/spark/jars

COPY dbt/ app/dbt

# Install dependencies (if needed)
COPY requirements.txt .

RUN python3 -m pip install --upgrade pip
RUN pip install uv
RUN uv pip install -r requirements.txt --system


# Command to run the script
#CMD ["spark-submit", "--jars", "/opt/spark/jars/postgresql-jdbc.jar", "/app/src/main.py"]

