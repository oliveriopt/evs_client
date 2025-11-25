FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

# Install required OS packages
RUN apt-get update && apt-get install -y \
    unixodbc-dev \
    gcc \
    g++ \
    curl \
    gnupg2 \
    && rm -rf /var/lib/apt/lists/*

# Install Microsoft ODBC Driver 17 for SQL Server
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql17 \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy Beam script
COPY main.py .

# Set the environment variable for the Dataflow Flex Template launcher
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/main.py"
