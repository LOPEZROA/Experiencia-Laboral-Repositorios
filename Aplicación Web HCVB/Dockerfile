FROM python:3.8-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    APP_HOST=0.0.0.0 \
    APP_PORT=5000 \
    APP_THREADS=8 \
    PRELOAD_ON_STARTUP=1

WORKDIR /app

COPY requirements.txt .
RUN python -m pip install --upgrade pip \
    && pip install -r requirements.txt

COPY . .

RUN mkdir -p "/app/BASES DE DATOS" "/app/.flask_session"

EXPOSE 5000

CMD ["python", "app1.py"]
