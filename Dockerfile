FROM python:3.11-slim

# Installation de FFmpeg + ffprobe + Tesseract OCR + polices Insta-style
# - fonts-noto-color-emoji : emojis colorés style mobile (équivalent libre Apple emojis)
# - fonts-inter : police Insta/TikTok-like (Helvetica-ish, arrondie, bold dispo)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    tesseract-ocr \
    tesseract-ocr-fra \
    tesseract-ocr-eng \
    fonts-dejavu-core \
    fonts-liberation \
    fonts-noto-color-emoji \
    fonts-noto-core \
    fonts-inter \
    fontconfig \
    && fc-cache -fv \
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
