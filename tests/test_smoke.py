"""
Smoke tests minimaux pour les fonctions critiques.

Pourquoi smoke et pas unit complet : la priorité ici est d'attraper les
régressions évidentes (signature changée, import cassé, logique grossière
fausse) sans introduire de fixtures lourdes (DB, Discord, FFmpeg réels).

Run : `pytest tests/`
Run avec verbose : `pytest tests/ -v`

Note : certains tests skippent si DATABASE_URL n'est pas configuré
(ils touchent à des fonctions qui dépendent de la DB).
"""
import os
import re
from unittest.mock import patch

import pytest


# ============================================================================
# cf_caption_renderer — pure functions, pas de deps externes
# ============================================================================
class TestCleanCaptionText:
    def test_handles_empty(self):
        from app.services.cf_caption_renderer import _clean_caption_text
        assert _clean_caption_text("") == ""
        assert _clean_caption_text(None) == ""

    def test_normalizes_line_endings(self):
        from app.services.cf_caption_renderer import _clean_caption_text
        assert _clean_caption_text("a\r\nb") == "a\nb"
        assert _clean_caption_text("a\rb") == "a\nb"
        assert _clean_caption_text("a b") == "a\nb"
        assert _clean_caption_text("a b") == "a\nb"

    def test_strips_invisible_chars(self):
        from app.services.cf_caption_renderer import _clean_caption_text
        # zero-width space u200b doit être supprimé
        assert _clean_caption_text("a​b") == "ab"
        # variation selector u️ doit être PRÉSERVÉ (emojis)
        out = _clean_caption_text("a️b")
        assert "️" in out

    def test_trims_trailing_spaces_per_line(self):
        from app.services.cf_caption_renderer import _clean_caption_text
        assert _clean_caption_text("line  \nother") == "line\nother"

    def test_preserves_blank_lines(self):
        """Les blank lines (\\n\\n) doivent être PRÉSERVÉES après cleaning."""
        from app.services.cf_caption_renderer import _clean_caption_text
        out = _clean_caption_text("a\n\nb")
        assert out == "a\n\nb"


# ============================================================================
# cf_mixer — fonctions pures de choix (résolution, codec)
# ============================================================================
class TestChooseOutputResolution:
    def test_returns_valid_resolution(self):
        from app.services.cf_mixer import _choose_output_resolution, TARGET_W, TARGET_H
        for _ in range(20):
            w, h = _choose_output_resolution()
            assert (w, h) in [(TARGET_W, TARGET_H), (720, 1280)]

    def test_respects_disable_env(self):
        from app.services.cf_mixer import _choose_output_resolution, TARGET_W, TARGET_H
        with patch.dict(os.environ, {"CF_RESOLUTION_VARY": "0"}):
            for _ in range(20):
                assert _choose_output_resolution() == (TARGET_W, TARGET_H)


class TestShouldUseHevc:
    def test_returns_bool(self):
        from app.services.cf_mixer import _should_use_hevc
        result = _should_use_hevc({"model": "iPhone 16 Pro Max"})
        assert isinstance(result, bool)

    def test_non_iphone_never_hevc(self):
        from app.services.cf_mixer import _should_use_hevc
        for _ in range(20):
            assert _should_use_hevc({"model": "Samsung Galaxy S24"}) is False
            assert _should_use_hevc({"model": "Pixel 8"}) is False

    def test_no_metadata_never_hevc(self):
        from app.services.cf_mixer import _should_use_hevc
        assert _should_use_hevc(None) is False
        assert _should_use_hevc({}) is False

    def test_disable_env_blocks_all(self):
        from app.services.cf_mixer import _should_use_hevc
        with patch.dict(os.environ, {"CF_HEVC_ENABLED": "0"}):
            for _ in range(20):
                assert _should_use_hevc({"model": "iPhone 17 Pro Max"}) is False


# ============================================================================
# cf_mixer — audio sample rate detection
# ============================================================================
class TestGetAudioSampleRate:
    def test_returns_int(self):
        from app.services.cf_mixer import _get_audio_sample_rate
        # Path inexistant → fallback 44100
        result = _get_audio_sample_rate("/nonexistent/path/file.mp4")
        assert isinstance(result, int)
        assert result == 44100  # default


class TestBuildAudioSpoofFilter:
    def test_none_when_neutral(self):
        from app.services.cf_mixer import _build_audio_spoof_filter
        # speed=1.0 et pitch=1.0 → pas de filtre
        result = _build_audio_spoof_filter({"speed": 1.0, "audio_pitch": 1.0})
        assert result is None

    def test_atempo_only_when_speed_only(self):
        from app.services.cf_mixer import _build_audio_spoof_filter
        result = _build_audio_spoof_filter({"speed": 1.05, "audio_pitch": 1.0})
        assert result is not None
        assert "atempo=1.05" in result

    def test_uses_source_sample_rate(self):
        """Régression : asetrate doit utiliser le sample rate fourni, pas 44100 hardcodé."""
        from app.services.cf_mixer import _build_audio_spoof_filter
        result = _build_audio_spoof_filter(
            {"speed": 1.03, "audio_pitch": 1.01},
            source_sample_rate=48000,
        )
        assert result is not None
        # Doit contenir asetrate=48480 (48000 * 1.01)
        assert "asetrate=48480" in result
        assert "aresample=48000" in result


# ============================================================================
# cf_weekly_stats — formatting
# ============================================================================
class TestFormatWeeklyStats:
    def test_renders_minimal(self):
        from app.services.cf_weekly_stats import format_weekly_stats_message
        stats = {
            "overview": {
                "total_batches": 10,
                "total_requested": 100,
                "total_uploaded": 95,
                "unique_vas": 3,
                "avg_duration_seconds": 180.0,
            },
            "top_vas": [],
            "problem_vas": [],
            "days": 7,
        }
        msg = format_weekly_stats_message(stats)
        assert "10" in msg
        assert "95" in msg
        assert "100" in msg
        assert "Bilan" in msg
        # Pas de bloc top vas si vide
        assert "Top VAs" not in msg
        # Doit dire qu'aucun VA n'est en alerte
        assert "Aucun VA" in msg or "✅" in msg

    def test_renders_top_vas(self):
        from app.services.cf_weekly_stats import format_weekly_stats_message
        stats = {
            "overview": {
                "total_batches": 50, "total_requested": 500, "total_uploaded": 480,
                "unique_vas": 5, "avg_duration_seconds": 200.0,
            },
            "top_vas": [
                {"va_name": "Sara", "batch_count": 20, "videos_uploaded": 200},
                {"va_name": "Faudel", "batch_count": 15, "videos_uploaded": 150},
            ],
            "problem_vas": [],
            "days": 7,
        }
        msg = format_weekly_stats_message(stats)
        assert "Sara" in msg
        assert "Faudel" in msg
        assert "200" in msg


# ============================================================================
# cf_settings — validation env vars
# ============================================================================
class TestValidateEnvVars:
    def test_returns_dict_with_categories(self):
        from app.services.cf_settings import validate_env_vars, ENV_VARS
        report = validate_env_vars()
        assert isinstance(report, dict)
        # Toutes les catégories du catalogue sont représentées
        for var in ENV_VARS:
            assert var.category in report

    def test_secrets_are_redacted(self):
        from app.services.cf_settings import validate_env_vars
        with patch.dict(os.environ, {"DISCORD_BOT_TOKEN": "ultra_secret_token_xyz"}):
            report = validate_env_vars()
            display = report.get("discord", {}).get("DISCORD_BOT_TOKEN", {}).get("display", "")
            # La valeur secrète ne doit JAMAIS apparaître en clair dans le display
            assert "ultra_secret_token_xyz" not in display
            assert "set" in display or "chars" in display


# ============================================================================
# cf_storage — patterns de label resolution (sans DB)
# ============================================================================
class TestModelLabelMatching:
    """Test la logique de pattern matching de get_model_by_label_number SANS DB."""

    def test_exact_patterns_recognize_common_formats(self):
        # On reconstruit la logique ici pour la tester sans DB.
        n_str = "8"
        exact_patterns = {
            f"Modele {n_str}", f"Modèle {n_str}",
            f"modele {n_str}", f"modèle {n_str}",
            f"ID{n_str}", f"ID {n_str}",
            f"id{n_str}", f"id {n_str}",
            n_str,
        }
        # Tous ces labels doivent matcher exact :
        for label in ("Modele 8", "Modèle 8", "ID8", "ID 8", "id8", "id 8", "8"):
            assert label in exact_patterns

    def test_word_boundary_regex_avoids_substring_match(self):
        # \b1\b ne doit PAS matcher "Modele 18" (18 ≠ 1)
        pattern = re.compile(r"\b1\b")
        assert pattern.search("Modele 18") is None
        assert pattern.search("Modele 1") is not None
        assert pattern.search("ID 1") is not None
