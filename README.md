# Repurpose Bot API

Backend FastAPI pour générer N copies uniques d'une vidéo avec des variations randomisées (TikFusion-like). Chaque copie a des valeurs aléatoires de framerate, bitrate, colorimétrie, vitesse, zoom, bruit, vignette, rotation et cuts — ce qui change le hash perceptuel tout en gardant la vidéo regardable.

## Structure

```
repurpose_bot/
├── app/
│   ├── main.py                  # entrypoint FastAPI
│   ├── config.py                # settings + bornes min/max
│   ├── routes/
│   │   └── video.py             # endpoints /api/*
│   ├── services/
│   │   └── ffmpeg_service.py    # construction filter_complex + exec ffmpeg
│   └── utils/
│       ├── logger.py
│       └── randomizer.py
├── uploads/                     # fichiers source (auto-purgés)
├── outputs/                     # vidéos générées
├── logs/                        # logs rotatifs
└── requirements.txt
```

## Prérequis

- **Python 3.10+**
- **FFmpeg** installé et dans le PATH (doit inclure `ffprobe`)

### Installer FFmpeg

```bash
# macOS
brew install ffmpeg

# Ubuntu / Debian
sudo apt update && sudo apt install -y ffmpeg

# Windows (winget)
winget install Gyan.FFmpeg
```

Vérifier :
```bash
ffmpeg -version
ffprobe -version
```

## Installation

```bash
# 1. créer et activer un venv
python -m venv .venv
source .venv/bin/activate          # Linux / macOS
# .venv\Scripts\activate            # Windows

# 2. installer les dépendances
pip install -r requirements.txt
```

## Lancer le serveur

```bash
# dev (hot reload)
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

# prod
uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 2
```

Docs interactives Swagger : http://localhost:8000/docs

## Utilisation (curl)

Générer 5 copies d'une vidéo :

```bash
curl -X POST http://localhost:8000/api/process \
  -F "file=@source.mp4" \
  -F "copies=5" \
  -F "concurrency=2"
```

Réponse :
```json
{
  "job_id": "a1b2c3d4e5f6",
  "requested": 5,
  "succeeded": 5,
  "failed": 0,
  "results": [
    {
      "copy_index": 1,
      "success": true,
      "filename": "a1b2c3d4e5f6_copy01_9fe2a1c3.mp4",
      "path": "/.../outputs/a1b2c3d4e5f6_copy01_9fe2a1c3.mp4",
      "size_bytes": 4821234,
      "params": {
        "framerate": 47, "bitrate": 5623, "brightness": 0.021,
        "contrast": 1.04, "saturation": 1.08, "gamma": 1.01,
        "speed": 1.037, "zoom": 1.045, "noise": 9,
        "vignette": 0.31, "rotation": -0.22,
        "cut_start": 0.13, "cut_end": 0.11
      }
    }
  ],
  "download_base_url": "/api/download/"
}
```

Télécharger une vidéo :
```bash
curl -O http://localhost:8000/api/download/a1b2c3d4e5f6_copy01_9fe2a1c3.mp4
```

Autres endpoints :
- `GET /api/health`  – healthcheck
- `GET /api/params`  – voir les bornes min/max
- `GET /api/outputs` – lister les vidéos générées

## Paramètres randomisés

| Paramètre    | Min    | Max   | Description                         |
|--------------|--------|-------|-------------------------------------|
| framerate    | 30     | 60    | fps de sortie                       |
| bitrate      | 5000   | 6000  | kbps (vidéo)                        |
| brightness   | -0.05  | 0.05  | correction eq                       |
| contrast     | 0.95   | 1.10  | correction eq                       |
| saturation   | 0.95   | 1.15  | correction eq                       |
| gamma        | 0.95   | 1.05  | correction eq                       |
| speed        | 1.03   | 1.04  | multiplicateur vitesse (setpts+atempo) |
| zoom         | 1.03   | 1.06  | crop central + rescale              |
| noise        | 5      | 15    | bruit temporel                      |
| vignette     | 0.20   | 0.40  | angle vignette                      |
| rotation     | -0.5°  | 0.5°  | micro-rotation                      |
| cut_start    | 0.1 s  | 0.15 s| coupure au début                    |
| cut_end      | 0.1 s  | 0.15 s| coupure à la fin                    |

Modifie les bornes dans `app/config.py` → `PARAM_RANGES`.

## Optimisations FFmpeg appliquées

- **Single-pass** : tous les filtres chaînés en un `-vf` unique → pas de ré-encodage intermédiaire.
- **Seek rapide** : `-ss` placé en input pour un seek sur keyframe.
- **`-preset veryfast`** : bon compromis vitesse/qualité pour batch.
- **`-movflags +faststart`** : le moov atom est déplacé au début → lecture instantanée sur TikTok/web.
- **`scale=...:flags=lanczos`** : rescale de qualité (vs bilinear).
- **`yuv420p`** : compatibilité maximale (TikTok, iOS, Android).
- **Concurrency contrôlée** : par défaut 2 ffmpeg en parallèle ; FFmpeg est déjà multi-thread, inutile d'en lancer trop.

### Accélération GPU (optionnel)

Dans `app/config.py`, changer `VIDEO_ENCODER` :
- NVIDIA : `"h264_nvenc"` (+ `PRESET = "p5"` ou `"fast"`)
- Apple Silicon : `"h264_videotoolbox"`
- AMD : `"h264_amf"`

## Logs

Tous les traitements sont tracés dans `logs/app.log` (rotation à 5 MB × 3).

## Gestion des erreurs

- Upload > `MAX_UPLOAD_MB` → 413
- Extension non supportée → 400
- `copies > MAX_COPIES_PER_REQUEST` → 400
- Échec ffmpeg sur une copie → retournée dans `results` avec `success: false` et le log d'erreur ; les autres copies continuent.
- Path traversal sur `/download` → 400.
