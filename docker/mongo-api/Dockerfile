FROM python:3.9-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

# Add MongoDB GPG key
RUN wget -qO - https://www.mongodb.org/static/pgp/server-7.0.asc | gpg --dearmor > /usr/share/keyrings/mongodb-archive-keyring.gpg

# THE FIX: Added [trusted=yes] to bypass the 2026 SHA-1 security rejection
RUN echo "deb [arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-archive-keyring.gpg trusted=yes] https://repo.mongodb.org/apt/debian bullseye/mongodb-org/7.0 main" | tee /etc/apt/sources.list.d/mongodb-org-7.0.list

# Install mongosh and Flask
RUN apt-get update && apt-get install -y \
    mongodb-mongosh \
    && pip install --no-cache-dir flask \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY app.py .

CMD ["python", "-u", "app.py"]
