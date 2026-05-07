# Dockerfile - Ultra-simple version using pre-built Jupyter Spark image
FROM jupyter/pyspark-notebook:latest

# Set working directory
WORKDIR /workspace

# Install additional tools
RUN pip install --no-cache-dir \
    great-expectations==0.17.12 \
    apache-airflow==2.7.3 \
    sqlalchemy==2.0.23 || true

# Expose ports
EXPOSE 8888 4040 8080

# Default: start Jupyter Lab
CMD ["start-notebook.sh", "--NotebookApp.token=''", "--NotebookApp.password=''"]
