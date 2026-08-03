"""
Servicio de transcripción con faster-whisper (modelo tiny - optimizado para Render Free).
"""

from __future__ import annotations

import logging
import os
import re
import tempfile
from typing import Final

from faster_whisper import WhisperModel

logger = logging.getLogger(__name__)

WHISPER_MODEL_NAME: Final[str] = os.getenv("WHISPER_MODEL", "tiny")
MIN_AUDIO_BYTES: Final[int] = int(os.getenv("MIN_AUDIO_BYTES", "1500"))

_HALLUCINATION_PATTERNS: Final[list[str]] = [
    r"^\s*\.*\s*$",
    r"^[\s\.\,\!\?\-\_]+$",
    r"suscr[ií]be",
    r"subtítulos?\s+por",
    r"subtitles?\s+by",
    r"www\.",
]
_HALLUCINATION_REGEXES = [re.compile(p, re.IGNORECASE) for p in _HALLUCINATION_PATTERNS]

# ─── Singleton ────────────────────────────────────────────────────────────────
_model: WhisperModel | None = None


def _get_model() -> WhisperModel:
    global _model
    if _model is None:
        logger.info("[Whisper] Cargando modelo '%s' (faster-whisper)...", WHISPER_MODEL_NAME)
        _model = WhisperModel(WHISPER_MODEL_NAME, device="cpu", compute_type="int8")
        logger.info("[Whisper] Modelo '%s' listo.", WHISPER_MODEL_NAME)
    return _model


def is_model_loaded() -> bool:
    return _model is not None


def _is_audio_too_short(audio_bytes: bytes) -> bool:
    return len(audio_bytes) < MIN_AUDIO_BYTES


def _contains_real_content(text: str) -> bool:
    real_words = re.findall(r"[a-zA-Z\u00C0-\u024F\u0400-\u04FF0-9]{2,}", text)
    if not real_words:
        return False
    for pattern in _HALLUCINATION_REGEXES:
        if pattern.search(text):
            logger.debug("[Whisper] Alucinación filtrada: %r", text[:80])
            return False
    return True


def transcribe(audio_bytes: bytes, source_lang: str = "es") -> str:
    """
    Transcribe bytes de audio a texto usando faster-whisper.
    Returns: texto transcrito.
    Raises: ValueError si no hay audio válido.
    """
    if _is_audio_too_short(audio_bytes):
        raise ValueError(
            f"Audio demasiado corto ({len(audio_bytes)} bytes < {MIN_AUDIO_BYTES} mínimo)."
        )

    model = _get_model()
    tmp_path: str | None = None

    try:
        with tempfile.NamedTemporaryFile(suffix=".webm", delete=False) as tmp_file:
            tmp_file.write(audio_bytes)
            tmp_path = tmp_file.name

        logger.debug("[Whisper] Transcribiendo %d bytes (lang=%s)...", len(audio_bytes), source_lang)

        # faster-whisper: transcribe() devuelve (segments_generator, info)
        segments, info = model.transcribe(
            tmp_path,
            language=source_lang if source_lang != "auto" else None,
            temperature=0.0,
            beam_size=1,
            best_of=1,
            no_speech_threshold=0.6,
            log_prob_threshold=-1.0,
            compression_ratio_threshold=2.4,
            condition_on_previous_text=False,
            task="transcribe",
        )

        # Consumir el generador de segmentos
        text_parts = [seg.text for seg in segments]
        text: str = " ".join(text_parts).strip()
        logger.debug("[Whisper] Output: %r (lang=%s)", text, info.language)

        if not text:
            raise ValueError("Whisper no produjo texto (silencio o audio sin voz).")

        if not _contains_real_content(text):
            raise ValueError(f"Texto descartado (posible alucinación): {text!r}")

        logger.info("[Whisper] OK: %r", text)
        return text

    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except OSError as e:
                logger.warning("[Whisper] No se pudo borrar tempfile: %s", e)
