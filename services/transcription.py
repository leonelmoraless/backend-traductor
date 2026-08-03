"""
Servicio de transcripción usando Google Web Speech API.

Soporta todos los idiomas que Google Web Speech API acepta:
  es → es-ES, en → en-US, fr → fr-FR, de → de-DE,
  pt → pt-BR, it → it-IT, ja → ja-JP, ko → ko-KR, zh → zh-CN
"""

from __future__ import annotations

import logging
import os
import re
import tempfile
import speech_recognition as sr
from typing import Final

logger = logging.getLogger(__name__)

MIN_AUDIO_BYTES: Final[int] = int(os.getenv("MIN_AUDIO_BYTES", "1500"))

# Mapeo completo: código de idioma → locale BCP-47 para Google Web Speech API
# Todos estos idiomas son soportados por recognize_google()
_LANG_TO_LOCALE: Final[dict[str, str]] = {
    "es": "es-ES",
    "en": "en-US",
    "fr": "fr-FR",
    "de": "de-DE",
    "pt": "pt-BR",
    "it": "it-IT",
    "ja": "ja-JP",
    "ko": "ko-KR",
    "zh": "zh-CN",
    "zh-cn": "zh-CN",
    "zh-tw": "zh-TW",
}

_HALLUCINATION_PATTERNS: Final[list[str]] = [
    r"^\s*\.*\s*$",
    r"^[\s\.\,\!\?\-\_]+$",
    r"suscr[ií]be",
    r"subtítulos?\s+por",
    r"subtitles?\s+by",
    r"www\.",
]
_HALLUCINATION_REGEXES = [re.compile(p, re.IGNORECASE) for p in _HALLUCINATION_PATTERNS]

_recognizer = sr.Recognizer()


def _resolve_locale(source_lang: str) -> str:
    """Convierte código de idioma a locale BCP-47. Fallback a en-US si no se conoce."""
    code = source_lang.lower().strip()
    # Primero buscar coincidencia exacta, luego por prefijo
    locale = _LANG_TO_LOCALE.get(code)
    if not locale:
        prefix = code.split("-")[0].split("_")[0]
        locale = _LANG_TO_LOCALE.get(prefix, "en-US")
    return locale


def _contains_real_content(text: str) -> bool:
    real_words = re.findall(r"[a-zA-Z\u00C0-\u024F\u0400-\u04FF\u4E00-\u9FFF\u3040-\u30FF\uAC00-\uD7AF0-9]{2,}", text)
    if not real_words:
        return False
    for pattern in _HALLUCINATION_REGEXES:
        if pattern.search(text):
            logger.debug("[STT] Alucinación filtrada: %r", text[:80])
            return False
    return True


def is_model_loaded() -> bool:
    return True  # Siempre activo, usa API web


def transcribe(audio_bytes: bytes, source_lang: str = "es") -> str:
    """
    Transcribe bytes de audio WAV a texto usando Google Web Speech API.

    Args:
        audio_bytes: Bytes del archivo WAV a transcribir.
        source_lang:  Código de idioma (es, en, fr, de, pt, it, ja, ko, zh, etc.)

    Returns:
        Texto transcrito (no vacío).

    Raises:
        ValueError:   Si el audio es demasiado corto, silencioso, o produce alucinaciones.
        RuntimeError: Si el servicio STT falla.
    """
    if len(audio_bytes) < MIN_AUDIO_BYTES:
        raise ValueError(
            f"Audio demasiado corto ({len(audio_bytes)} bytes < {MIN_AUDIO_BYTES} mínimo)."
        )

    locale = _resolve_locale(source_lang)
    logger.debug("[STT] Transcribiendo %d bytes (lang=%s → locale=%s)…", len(audio_bytes), source_lang, locale)

    tmp_path: str | None = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tmp_file:
            tmp_file.write(audio_bytes)
            tmp_path = tmp_file.name

        with sr.AudioFile(tmp_path) as source:
            audio_data = _recognizer.record(source)

        try:
            text = _recognizer.recognize_google(audio_data, language=locale).strip()
        except sr.UnknownValueError:
            raise ValueError("No se reconoció voz en el audio.")
        except sr.RequestError as e:
            raise RuntimeError(f"Error con el servicio STT: {e}")

        if not text:
            raise ValueError("El servicio no produjo texto (silencio o audio sin voz).")

        if not _contains_real_content(text):
            raise ValueError(f"Texto descartado (posible alucinación): {text!r}")

        logger.info("[STT] OK (locale=%s): %r", locale, text)
        return text

    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except OSError as e:
                logger.warning("[STT] No se pudo borrar tempfile: %s", e)
