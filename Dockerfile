FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN groupadd --system app && useradd --system --gid app --home /app app

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY family_planner_bot ./family_planner_bot

RUN mkdir -p /data /app/restore-safety && chown -R app:app /app /data

USER app

VOLUME ["/data"]

CMD ["python", "-m", "family_planner_bot"]
