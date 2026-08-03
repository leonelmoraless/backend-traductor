"""
<<<<<<< HEAD
Servicio de traducción de texto rápido usando deep-translator.
"""

import time
from functools import lru_cache
from deep_translator import GoogleTranslator
from langdetect import detect

def detect_language(text: str) -> str:
    """Detecta el idioma de un texto."""
    text = text.strip()
    if not text:
        return ""
    try:
        return detect(text)
    except Exception as e:
        print(f"[Translation] Error detectando idioma: {e}")
        return ""

@lru_cache(maxsize=256)
def translate(text: str, source_lang: str, target_lang: str) -> str:
    """
    Traduce un texto de un idioma a otro de forma rápida.
    Añadido: timeouts, reintentos y caché LRU para no bloquear infinitamente.
    """
    text = text.strip()
    if not text:
        return ""
        
    if source_lang == target_lang:
        return text

    for attempt in range(3):
        try:
            # timeout de 8 segundos para evitar bloqueos infinitos de la red
            translator = GoogleTranslator(source=source_lang, target=target_lang, timeout=8)
            result = translator.translate(text)
            if result:
                return result.strip()
        except Exception as e:
            print(f"[Translation] Error con deep-translator (intento {attempt+1}): {e}")
            time.sleep(0.5)
            
    raise RuntimeError("La traducción falló después de varios intentos.")
=======
Servicio de traducción de texto — Producción-ready v2.

FIXES v2:
  · CRÍTICO: Añadido timeout=8 a GoogleTranslator.
    Sin esto, la llamada HTTP podía bloquearse INFINITAMENTE si Google
    tardaba o había problemas de red, ocupando el thread del pool para siempre.
  · Retry delays reducidos: 0.3s, 0.6s (en vez de 0.5s, 1s, 2s).
    Total máximo de espera en retries: <1 segundo en vez de >3.5 segundos.
  · NO se reintenta si el error es ValueError (validación, no red).
    Antes, un texto inválido causaba 3 intentos + delays innecesarios.
  · Caché LRU: misma petición → respuesta O(1) sin red.
"""

from __future__ import annotations

import logging
import time
from functools import lru_cache

from deep_translator import GoogleTranslator

logger = logging.getLogger(__name__)

# ─── Configuración ────────────────────────────────────────────────────────────
_MAX_RETRIES = 3
# Delays entre reintentos en segundos. Total máximo: 0.3 + 0.6 = 0.9s de espera.
_RETRY_DELAYS = (0.3, 0.6, 0.0)  # índice 0=primer retry, 1=segundo, 2=no hay tercero
# Timeout HTTP para cada llamada a la API de Google Translate.
# CRÍTICO: sin esto el thread se bloquea indefinidamente en caso de red lenta.
_HTTP_TIMEOUT = 8  # segundos


# ─── Caché LRU ───────────────────────────────────────────────────────────────
@lru_cache(maxsize=256)
def _cached_translate(text: str, source_lang: str, target_lang: str) -> str:
    """
    Núcleo de traducción con caché LRU (256 entradas).
    La caché es hilo-segura en CPython (GIL protege el dict interno de lru_cache).
    """
    last_error: Exception | None = None

    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            # timeout=_HTTP_TIMEOUT → CRÍTICO para no bloquear threads infinitamente
            translator = GoogleTranslator(
                source=source_lang,
                target=target_lang,
                timeout=_HTTP_TIMEOUT,
            )
            result: str = translator.translate(text)

            if not result or not result.strip():
                # Error de API, no de red → no tiene sentido reintentar
                raise ValueError("La API devolvió traducción vacía.")

            # Si el resultado es idéntico al input con idiomas distintos → probable fallo
            if (
                result.strip().lower() == text.strip().lower()
                and source_lang != target_lang
            ):
                raise ValueError(
                    f"Traducción idéntica al input ({source_lang}→{target_lang}). "
                    "Posible fallo de la API."
                )

            logger.info(
                "[Translation] OK (intento %d/%d): %r → %r",
                attempt, _MAX_RETRIES, text[:60], result[:60],
            )
            return result.strip()

        except ValueError:
            # Error de validación del resultado: no reintentar, relanzar de inmediato
            raise

        except Exception as exc:
            last_error = exc
            delay = _RETRY_DELAYS[attempt - 1] if attempt <= len(_RETRY_DELAYS) else 0
            logger.warning(
                "[Translation] Intento %d/%d falló: %s. %s",
                attempt, _MAX_RETRIES, exc,
                f"Reintentando en {delay:.1f}s…" if attempt < _MAX_RETRIES else "Sin más reintentos.",
            )
            if attempt < _MAX_RETRIES and delay > 0:
                time.sleep(delay)

    raise RuntimeError(
        f"Traducción fallida tras {_MAX_RETRIES} intentos. Último error: {last_error}"
    )


# ─── API pública ──────────────────────────────────────────────────────────────

def translate(text: str, source_lang: str, target_lang: str) -> str:
    """
    Traduce texto de source_lang a target_lang.

    - Si los idiomas son iguales, devuelve el texto sin llamar a la API.
    - Usa caché LRU: segunda llamada con los mismos args es O(1).
    - Timeout HTTP de 8s garantiza que el thread NO se bloquea indefinidamente.

    Raises:
        ValueError:   Texto vacío o resultado inválido de la API.
        RuntimeError: API falla tras todos los reintentos.
    """
    text = text.strip()
    if not text:
        raise ValueError("El texto a traducir no puede estar vacío.")

    if source_lang == target_lang:
        logger.debug("[Translation] Idiomas iguales, texto sin modificar.")
        return text

    return _cached_translate(text, source_lang, target_lang)


def clear_translation_cache() -> None:
    """Limpia la caché de traducciones."""
    _cached_translate.cache_clear()
    logger.info("[Translation] Caché limpiada.")
>>>>>>> 1dbc4a9 (Update for langdetect)
