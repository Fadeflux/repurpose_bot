FROM python:3.11-slim

# Installation de FFmpeg + Tesseract OCR + polices Insta-style + outils
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
    ca-certificates \
    && fc-cache -fv \
    && rm -rf /var/lib/apt/lists/*

# Téléchargement de la font Apple Color Emoji (version Linux du repo apple-emoji-linux)
# Cette font contient les vraies images emoji Apple (sbix table) et fonctionne avec
# Pillow 10+ via embedded_color=True à la taille fixe 137px.
RUN mkdir -p /opt/fonts && \
    curl -fsSL -o /opt/fonts/AppleColorEmoji.ttf \
      "https://github.com/samuelngs/apple-emoji-linux/releases/latest/download/AppleColorEmoji-Linux.ttf" \
    && ls -la /opt/fonts/ \
    && echo "Apple Color Emoji font installée"

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
