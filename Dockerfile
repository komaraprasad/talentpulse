# TalentPulse server image (AWS ECS / EKS or any Docker host).
# Candidate data lives in PostgreSQL (DATABASE_URL); the container itself keeps nothing.
FROM python:3.12.10-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUTF8=1 \
    PIP_NO_CACHE_DIR=1 \
    TZ=Asia/Kolkata \
    TALENTPULSE_HOSTED=1 \
    TALENTPULSE_SMTP_CHECK=0 \
    TALENTPULSE_DATA_DIR=/tmp/talentpulse \
    PORT=8000

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY app.py sales_nav_extractor.py job_titles.py locations.py world_geo.db ./
COPY extension ./extension

RUN useradd --create-home --uid 10001 talentpulse && mkdir -p /tmp/talentpulse && chown talentpulse /tmp/talentpulse
USER talentpulse

EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/api/status', timeout=4).status == 200 else 1)"

# One worker on purpose: running searches and live updates are kept in this process's memory.
# Keep-alive is longer than the load balancer's 300 s idle timeout, so it never reuses a closed connection (502s).
CMD ["sh", "-c", "uvicorn app:app --host 0.0.0.0 --port ${PORT} --proxy-headers --forwarded-allow-ips='*' --timeout-keep-alive 310"]
