FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /srv/vfb-status

# Layer cache for deps
COPY requirements.txt .
RUN pip install -r requirements.txt

# App + config
COPY app ./app
COPY config ./config

EXPOSE 8000

# Drop privileges
RUN useradd --system --uid 10001 --gid 0 --home-dir /srv/vfb-status vfb \
 && chown -R vfb:0 /srv/vfb-status
USER 10001

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD python -c "import urllib.request,sys; \
sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/healthz', timeout=3).status==200 else 1)"

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
