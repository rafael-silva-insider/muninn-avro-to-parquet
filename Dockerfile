FROM gcr.io/dataflow-templates-base/python311-template-launcher-base:latest

WORKDIR /template

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Código do pipeline
COPY main.py .

# Variáveis exigidas pelo launcher do Flex Template
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/main.py"
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="/template/requirements.txt"

# Entrypoint do launcher
ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"]
