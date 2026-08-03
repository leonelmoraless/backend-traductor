"""
Servicio de transcripción ultra-rápido para Render Free Tier.
Utiliza SpeechRecognition (Google Web Speech API) para 0MB de overhead de RAM.
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

def is_model_loaded() -> bool:
    # Siempre "cargado" porque usa API web
    return True

def _is_audio_too_short(audio_bytes: bytes) -> bool:
    return len(audio_bytes) < MIN_AUDIO_BYTES

def _contains_real_content(text: str) -> bool:
    real_words = re.findall(r"[a-zA-Z\u00C0-\u024F\u0400-\u04FF0-9]{2,}", text)
    if not real_words:
        return False
    for pattern in _HALLUCINATION_REGEXES:
        if pattern.search(text):
            logger.debug("[STT] Alucinación filtrada: %r", text[:80])
            return False
    return True

def transcribe(audio_bytes: bytes, source_lang: str = "es") -> str:
    """
    Transcribe bytes de audio a texto usando Google Web Speech API.
    Returns: texto transcrito.
    Raises: ValueError si no hay audio válido.
    """
    if _is_audio_too_short(audio_bytes):
        raise ValueError(
            f"Audio demasiado corto ({len(audio_bytes)} bytes < {MIN_AUDIO_BYTES} mínimo)."
        )

    tmp_path: str | None = None

    try:
        # Guardamos como wav (la app manda WAV aunque se llame .webm)
        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tmp_file:
            tmp_file.write(audio_bytes)
            tmp_path = tmp_file.name

        logger.debug("[STT] Transcribiendo %d bytes (lang=%s)...", len(audio_bytes), source_lang)

        # Mapear idioma origen a locale completo si es necesario
        # SpeechRecognition usa códigos como "es-ES", "en-US"
        lang_code = "es-ES" if source_lang.startswith("es") else "en-US"

        with sr.AudioFile(tmp_path) as source:
            audio_data = _recognizer.record(source)

        try:
            text = _recognizer.recognize_google(audio_data, language=lang_code)
            text = text.strip()
        except sr.UnknownValueError:
            raise ValueError("No se reconoció voz en el audio.")
        except sr.RequestError as e:
            raise RuntimeError(f"Error con el servicio STT: {e}")

        logger.debug("[STT] Output: %r", text)

        if not text:
            raise ValueError("El servicio no produjo texto (silencio o audio sin voz).")

        if not _contains_real_content(text):
            raise ValueError(f"Texto descartado (posible alucinación): {text!r}")

        logger.info("[STT] OK: %r", text)
        return text

    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except OSError as e:
                logger.warning("[STT] No se pudo borrar tempfile: %s", e)
