FROM gcr.io/dataflow-templates-base/python311-template-launcher-base:latest

WORKDIR /pipeline
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

# Flex template launcher expects this
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="main.py"