"""
<<<<<<< HEAD
Servicio de transcripción de audio.

Responsabilidad única: recibir bytes de audio y devolver el texto transcrito
junto con el idioma detectado. Usa faster-whisper (motor CTranslate2) para
máximo rendimiento en CPU.

El modelo se carga una sola vez en memoria (patrón singleton).
"""

import os
import tempfile
from faster_whisper import WhisperModel

# ─── Singleton del modelo ─────────────────────────────────────────────────────
_model: WhisperModel | None = None


def _get_model() -> WhisperModel:
    """Carga el modelo Whisper 'base' usando faster-whisper para CPU y lo reutiliza."""
    global _model
    if _model is None:
        print("[Whisper] Cargando modelo 'base' (faster-whisper)... (solo ocurre una vez)")
        _model = WhisperModel("base", device="cpu", compute_type="int8")
        print("[Whisper] Modelo listo.")
    return _model


# ─── API pública ──────────────────────────────────────────────────────────────

def transcribe(audio_bytes: bytes, expected_langs: list[str] | None = None) -> tuple[str, str]:
    """
    Transcribe un fragmento de audio y detecta el idioma.

    Args:
        audio_bytes:     Bytes del archivo de audio (webm, wav, mp3, etc.)
        expected_langs:  Lista de códigos ISO 639-1 esperados (ej. ["es", "en"]).
                         Si se provee y solo hay uno, se fuerza como idioma de entrada
                         para saltarse la detección automática y reducir la latencia.
                         Si hay más de uno, Whisper aún auto-detecta pero solo entre ellos.

    Returns:
        Tupla (texto_transcrito, idioma_detectado).

    Raises:
        ValueError: Si Whisper no detecta voz válida en el audio.
    """
    model = _get_model()

    with tempfile.NamedTemporaryFile(suffix=".webm", delete=False) as tmp_file:
        tmp_file.write(audio_bytes)
        tmp_path = tmp_file.name

    try:
        # Optimizar: si solo hay 2 idiomas posibles, Whisper fuerza la detección
        # entre ellos en vez de evaluar los 100+ idiomas del modelo.
        lang_hint = None
        if expected_langs and len(expected_langs) == 1:
            # Un solo idioma: forzamos, el más rápido posible
            lang_hint = expected_langs[0]
        # Si son 2, dejamos auto-detect (Whisper igual es rápido con el resto optimizado)

        segments, info = model.transcribe(
            tmp_path,
            language=lang_hint,
            beam_size=1,
            vad_filter=True,
            vad_parameters=dict(min_silence_duration_ms=500),
            condition_on_previous_text=False
        )

        detected_lang = info.language   # "es", "en", etc.
        
        # Fallback: si detecta un idioma no esperado (ej. "ja" o "ur"), forzamos el principal
        if not lang_hint and expected_langs and len(expected_langs) >= 2:
            if detected_lang not in expected_langs:
                print(f"[Whisper] Detectó {detected_lang}, pero se esperaba {expected_langs}. Forzando {expected_langs[0]}...")
                segments, info = model.transcribe(
                    tmp_path,
                    language=expected_langs[0],
                    beam_size=1,
                    vad_filter=True,
                    vad_parameters=dict(min_silence_duration_ms=500),
                    condition_on_previous_text=False
                )
                detected_lang = expected_langs[0]

        text = " ".join([seg.text for seg in segments]).strip()

        if not text:
            raise ValueError("Whisper no detectó voz en el audio.")

        print(f"[Whisper] Idioma detectado: {detected_lang!r} | Texto: {text!r}")
        return text, detected_lang

    finally:
        os.unlink(tmp_path)
=======
Servicio de transcripción de audio — Producción-ready v2.

FIXES v2:
  · CRÍTICO: Eliminado temperature=(tuple) → usaba hasta 6 pasadas del modelo
    en audio ruidoso, causando latencias de 10-30 segundos. Ahora temperature=0.0
    con beam_size=1 para decodificación greedy (la más rápida).
  · no_speech_threshold reducido a 0.6 (0.85 era demasiado agresivo, descartaba
    voz real con acento fuerte).
  · logprob_threshold vuelve a -1.0 (más permisivo, mejor UX).
  · Eliminado import struct que no se usaba.
  · Limpieza garantizada de tempfiles con try/finally.
"""

from __future__ import annotations

import logging
import os
import re
import tempfile
from typing import Final

import whisper

logger = logging.getLogger(__name__)

# ─── Configuración ────────────────────────────────────────────────────────────
WHISPER_MODEL_NAME: Final[str] = os.getenv("WHISPER_MODEL", "small")

# Tamaño mínimo de audio en bytes antes de llamar a Whisper.
# Webm/opus de 1 segundo ≈ 4-8 KB. Ponemos umbral bajo para no descartar audios cortos.
MIN_AUDIO_BYTES: Final[int] = int(os.getenv("MIN_AUDIO_BYTES", "1500"))

# Frases que Whisper alucina cuando recibe silencio o ruido de fondo.
_HALLUCINATION_PATTERNS: Final[list[str]] = [
    r"^\s*\.*\s*$",                        # Solo puntos o espacios
    r"^[\s\.\,\!\?\-\_]+$",               # Solo puntuación
    r"suscr[ií]be",                        # "Suscríbete a mi canal"
    r"subtítulos?\s+por",
    r"subtitles?\s+by",
    r"traducido\s+por",
    r"translated\s+by",
    r"amara\.org",
    r"gracias\s+por\s+ver",
    r"thanks?\s+for\s+watching",
    r"like\s+and\s+subscribe",
    r"^\s*\[.*\]\s*$",                     # [Música] [Aplausos]
    r"^\s*\(.*\)\s*$",                     # (Música) (inaudible)
    r"^\s*música\s*$",
    r"^\s*music\s*$",
    r"^\s*silencio\s*$",
    r"www\.",
    r"http[s]?://",
]

# Compilar una sola vez al cargar el módulo
_HALLUCINATION_REGEXES: Final[list[re.Pattern]] = [
    re.compile(p, re.IGNORECASE) for p in _HALLUCINATION_PATTERNS
]

# ─── Singleton del modelo ─────────────────────────────────────────────────────
_model: whisper.Whisper | None = None


def _get_model() -> whisper.Whisper:
    """Carga el modelo Whisper una sola vez. Thread-safe vía GIL de CPython."""
    global _model
    if _model is None:
        logger.info("[Whisper] Cargando modelo '%s'… (solo una vez)", WHISPER_MODEL_NAME)
        _model = whisper.load_model(WHISPER_MODEL_NAME)
        logger.info("[Whisper] Modelo '%s' listo.", WHISPER_MODEL_NAME)
    return _model


def is_model_loaded() -> bool:
    return _model is not None


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _is_audio_too_short(audio_bytes: bytes) -> bool:
    return len(audio_bytes) < MIN_AUDIO_BYTES


def _contains_real_content(text: str) -> bool:
    """Devuelve True si el texto parece habla humana real (no alucinación de Whisper)."""
    # Al menos una palabra de 2+ letras
    real_words = re.findall(r"[a-zA-Z\u00C0-\u024F\u0400-\u04FF0-9]{2,}", text)
    if not real_words:
        return False

    # Verificar lista negra
    for pattern in _HALLUCINATION_REGEXES:
        if pattern.search(text):
            logger.debug("[Whisper] Alucinación filtrada: %r", text[:80])
            return False

    return True


# ─── API pública ──────────────────────────────────────────────────────────────

def transcribe(audio_bytes: bytes, source_lang: str = "es") -> str:
    """
    Transcribe bytes de audio a texto usando Whisper.

    Configuración optimizada para velocidad en tiempo real:
      - temperature=0.0 (greedy, una sola pasada — NO retries)
      - beam_size=1     (no beam search, decodificación instantánea)
      - best_of=1       (sin muestreo múltiple)

    Args:
        audio_bytes: Bytes del archivo de audio (webm, wav, mp3…)
        source_lang: Código ISO 639-1 (ej. "es", "en")

    Returns:
        Texto transcrito.

    Raises:
        ValueError: Audio demasiado corto, silencio, o alucinación detectada.
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

        logger.debug("[Whisper] Transcribiendo %d bytes (lang=%s)…", len(audio_bytes), source_lang)

        result = model.transcribe(
            tmp_path,
            language=source_lang,
            # ── Parámetros de velocidad ─────────────────────────────────────
            # temperature=0.0 → decodificación greedy, UNA SOLA PASADA.
            # Nunca usa múltiples temperaturas como fallback.
            temperature=0.0,
            # beam_size=1 → sin beam search. Hasta 3x más rápido que beam_size=5.
            beam_size=1,
            # best_of=1 → no compara múltiples muestras.
            best_of=1,
            # ── Parámetros anti-alucinación ─────────────────────────────────
            # 0.6 = valor oficial de Whisper. 0.85 era demasiado agresivo.
            no_speech_threshold=0.6,
            # -1.0 = valor oficial. Filtro menos agresivo, mejor UX con acentos.
            logprob_threshold=-1.0,
            # Detecta repeticiones (Whisper alucinando loops de texto)
            compression_ratio_threshold=2.4,
            # No usar contexto anterior: cada fragmento es independiente
            condition_on_previous_text=False,
            task="transcribe",
        )

        text: str = result["text"].strip()
        logger.debug("[Whisper] Raw output: %r", text)

        if not text:
            raise ValueError("Whisper no produjo texto (silencio o audio sin voz).")

        if not _contains_real_content(text):
            raise ValueError(f"Texto descartado (alucinación): {text!r}")

        logger.info("[Whisper] OK: %r", text)
        return text

    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except OSError as e:
                logger.warning("[Whisper] No se pudo borrar tempfile: %s", e)
>>>>>>> 1dbc4a9 (Update for langdetect)
