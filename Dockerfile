FROM python:3.11-slim

# Installation de FFmpeg + ffprobe + Tesseract OCR + polices + outils de download
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    tesseract-ocr \
    tesseract-ocr-fra \
    tesseract-ocr-eng \
    fonts-dejavu-core \
    fonts-liberation \
    fonts-inter \
    fontconfig \
    curl \
    unzip \
    && fc-cache -fv \
    && rm -rf /var/lib/apt/lists/*

# Téléchargement des PNGs Apple emoji
# On clone juste le repo qui contient les PNGs (ils sont dans /160 généralement)
RUN mkdir -p /opt/apple-emoji && \
    curl -fsSL -o /tmp/emoji.zip \
      "https://github.com/samuelngs/apple-emoji-linux/archive/refs/heads/main.zip" \
    && unzip -q /tmp/emoji.zip -d /tmp/emoji-extract \
    && find /tmp/emoji-extract -name "*.png" -exec cp {} /opt/apple-emoji/ \; 2>/dev/null \
    && echo "Apple emoji PNGs : $(ls /opt/apple-emoji/ 2>/dev/null | wc -l) fichiers" \
    && rm -rf /tmp/emoji.zip /tmp/emoji-extract

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
