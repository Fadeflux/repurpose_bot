"""
Module d'authentification simple pour le tools.

Variables d'environnement :
  TOOL_PASSWORD : mot de passe partagé pour accéder au tools (obligatoire pour activer le login)
  TOOL_SESSION_SECRET : clé pour signer les cookies (auto-générée si absente, mais
                       tu perds les sessions au redéploiement si tu ne le définis pas)

Si TOOL_PASSWORD n'est pas défini, le login est désactivé (pas de protection).
"""
import hashlib
import hmac
import os
import secrets
from datetime import datetime, timedelta
from typing import Optional

from fastapi import Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse

from app.utils.logger import get_logger

logger = get_logger("auth")

COOKIE_NAME = "tool_session"
SESSION_DURATION_DAYS = 30


def _get_password() -> Optional[str]:
    return os.getenv("TOOL_PASSWORD")


def _get_secret() -> str:
    """Clé pour signer les cookies. Auto-générée par instance si pas définie."""
    secret = os.getenv("TOOL_SESSION_SECRET")
    if not secret:
        # Fallback : génère une clé déterministe basée sur d'autres env vars
        seed = os.getenv("DISCORD_BOT_TOKEN", "") + os.getenv("GOOGLE_DRIVE_PARENT_ID", "")
        secret = hashlib.sha256(seed.encode()).hexdigest() if seed else "fallback-secret-change-me"
    return secret


def is_auth_enabled() -> bool:
    """Le login est activé si TOOL_PASSWORD est configuré."""
    return bool(_get_password())


def _make_token(username: str = "user") -> str:
    """Crée un token signé : username|expiry|signature"""
    expiry = (datetime.utcnow() + timedelta(days=SESSION_DURATION_DAYS)).isoformat()
    payload = f"{username}|{expiry}"
    signature = hmac.new(
        _get_secret().encode(),
        payload.encode(),
        hashlib.sha256,
    ).hexdigest()
    return f"{payload}|{signature}"


def _verify_token(token: str) -> bool:
    """Vérifie qu'un token est valide et non expiré."""
    if not token or token.count("|") != 2:
        return False
    try:
        username, expiry, signature = token.split("|")
        payload = f"{username}|{expiry}"
        expected_sig = hmac.new(
            _get_secret().encode(),
            payload.encode(),
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(signature, expected_sig):
            return False
        # Vérifie l'expiration
        expiry_dt = datetime.fromisoformat(expiry)
        if datetime.utcnow() > expiry_dt:
            return False
        return True
    except Exception:
        return False


def is_authenticated(request: Request) -> bool:
    """Retourne True si la requête a un cookie de session valide."""
    if not is_auth_enabled():
        return True  # Pas d'auth configurée = accès libre
    token = request.cookies.get(COOKIE_NAME)
    return _verify_token(token) if token else False


def check_password(password: str) -> bool:
    """Vérifie le mot de passe contre TOOL_PASSWORD (constant-time compare)."""
    expected = _get_password()
    if not expected or not password:
        return False
    return hmac.compare_digest(password, expected)


def make_login_response(redirect_to: str = "/") -> RedirectResponse:
    """Crée une réponse de redirection avec le cookie de session."""
    response = RedirectResponse(url=redirect_to, status_code=302)
    token = _make_token()
    response.set_cookie(
        key=COOKIE_NAME,
        value=token,
        max_age=SESSION_DURATION_DAYS * 24 * 3600,
        httponly=True,
        samesite="lax",
        secure=False,  # Railway sert via HTTPS mais le cookie marche aussi en HTTP local
    )
    return response


def make_logout_response() -> RedirectResponse:
    """Supprime le cookie de session."""
    response = RedirectResponse(url="/login", status_code=302)
    response.delete_cookie(COOKIE_NAME)
    return response


# =============================================================================
# Page de login HTML (simple, intégrée)
# =============================================================================
LOGIN_HTML = """<!DOCTYPE html>
<html lang="fr">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Connexion · Repurpose Bot</title>
  <style>
    :root {
      --bg: #0a0a0a;
      --panel: #141414;
      --panel-2: #1c1c1c;
      --border: #262626;
      --text: #f5f5f5;
      --text-muted: #a3a3a3;
      --accent: #3b82f6;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      background: var(--bg);
      color: var(--text);
      min-height: 100vh;
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 20px;
    }
    .login-box {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 40px;
      max-width: 400px;
      width: 100%;
      box-shadow: 0 20px 60px rgba(0, 0, 0, 0.5);
    }
    h1 {
      font-size: 22px;
      margin-bottom: 8px;
      text-align: center;
    }
    .subtitle {
      color: var(--text-muted);
      font-size: 13px;
      text-align: center;
      margin-bottom: 30px;
    }
    input[type="password"] {
      width: 100%;
      background: var(--panel-2);
      border: 1px solid var(--border);
      color: var(--text);
      padding: 12px 14px;
      border-radius: 8px;
      font-size: 14px;
      font-family: inherit;
      margin-bottom: 14px;
      outline: none;
      transition: border-color 0.15s;
    }
    input[type="password"]:focus {
      border-color: var(--accent);
    }
    button {
      width: 100%;
      background: var(--accent);
      border: none;
      color: white;
      padding: 12px;
      border-radius: 8px;
      font-size: 14px;
      font-weight: 600;
      cursor: pointer;
      font-family: inherit;
      transition: opacity 0.15s;
    }
    button:hover { opacity: 0.9; }
    .error {
      background: rgba(239, 68, 68, 0.15);
      color: #ef4444;
      padding: 10px;
      border-radius: 6px;
      font-size: 13px;
      margin-bottom: 14px;
      text-align: center;
    }
    .lock-icon {
      text-align: center;
      font-size: 32px;
      margin-bottom: 12px;
    }
  </style>
</head>
<body>
  <div class="login-box">
    <div class="lock-icon">🔐</div>
    <h1>Repurpose Bot</h1>
    <p class="subtitle">Accès restreint. Entre ton mot de passe.</p>
    {{ERROR_PLACEHOLDER}}
    <form method="POST" action="/login">
      <input type="password" name="password" placeholder="Mot de passe" autofocus required />
      <button type="submit">Se connecter</button>
    </form>
  </div>
</body>
</html>
"""


def render_login_page(error: str = "") -> HTMLResponse:
    error_html = f'<div class="error">{error}</div>' if error else ""
    html = LOGIN_HTML.replace("{{ERROR_PLACEHOLDER}}", error_html)
    return HTMLResponse(html)
