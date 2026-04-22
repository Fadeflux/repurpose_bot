FROM python:3.11-slim

# Installation de FFmpeg + ffprobe (nécessaires pour le traitement vidéo)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install des deps Python (cache Docker optimisé : on copie d'abord requirements.txt)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copie du code
COPY . .

# Création des dossiers de travail
RUN mkdir -p uploads outputs logs

# Railway injecte $PORT automatiquement
ENV PORT=8000
EXPOSE 8000

# Lancement de l'API FastAPI via uvicorn
CMD uvicorn app.main:app --host 0.0.0.0 --port ${PORT}
