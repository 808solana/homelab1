FROM python:3.11-slim

# gcc + libc headers so bcrypt's compiled extension builds on slim images.
RUN apt-get update && apt-get install -y --no-install-recommends gcc libc6-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

# SQLite DB lives on a mounted volume so it survives restarts / redeployments.
RUN mkdir -p /app/data
VOLUME ["/app/data"]

EXPOSE 4000
# gevent worker + --timeout 0 so long SSE streams are never killed by gunicorn.
CMD ["gunicorn", "-k", "gevent", "-w", "4", "--timeout", "0", "-b", "0.0.0.0:4000", "proxy:app"]
