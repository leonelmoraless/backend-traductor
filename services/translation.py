"""
Servicio de traducci├│n de texto ÔÇö Producci├│n-ready v2.

FIXES v2:
  ┬À CR├ìTICO: A├▒adido timeout=8 a GoogleTranslator.
    Sin esto, la llamada HTTP pod├¡a bloquearse INFINITAMENTE si Google
    tardaba o hab├¡a problemas de red, ocupando el thread del pool para siempre.
  ┬À Retry delays reducidos: 0.3s, 0.6s (en vez de 0.5s, 1s, 2s).
    Total m├íximo de espera en retries: <1 segundo en vez de >3.5 segundos.
  ┬À NO se reintenta si el error es ValueError (validaci├│n, no red).
    Antes, un texto inv├ílido causaba 3 intentos + delays innecesarios.
  ┬À Cach├® LRU: misma petici├│n ÔåÆ respuesta O(1) sin red.
"""

from __future__ import annotations

import logging
import time
from functools import lru_cache

from deep_translator import GoogleTranslator

logger = logging.getLogger(__name__)

# ÔöÇÔöÇÔöÇ Configuraci├│n ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ
_MAX_RETRIES = 3
# Delays entre reintentos en segundos. Total m├íximo: 0.3 + 0.6 = 0.9s de espera.
_RETRY_DELAYS = (0.3, 0.6, 0.0)  # ├¡ndice 0=primer retry, 1=segundo, 2=no hay tercero
# Timeout HTTP para cada llamada a la API de Google Translate.
# CR├ìTICO: sin esto el thread se bloquea indefinidamente en caso de red lenta.
_HTTP_TIMEOUT = 8  # segundos


# ÔöÇÔöÇÔöÇ Cach├® LRU ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ
@lru_cache(maxsize=256)
def _cached_translate(text: str, source_lang: str, target_lang: str) -> str:
    """
    N├║cleo de traducci├│n con cach├® LRU (256 entradas).
    La cach├® es hilo-segura en CPython (GIL protege el dict interno de lru_cache).
    """
    last_error: Exception | None = None

    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            # timeout=_HTTP_TIMEOUT ÔåÆ CR├ìTICO para no bloquear threads infinitamente
            translator = GoogleTranslator(
                source=source_lang,
                target=target_lang,
                timeout=_HTTP_TIMEOUT,
            )
            result: str = translator.translate(text)

            if not result or not result.strip():
                # Error de API, no de red ÔåÆ no tiene sentido reintentar
                raise ValueError("La API devolvi├│ traducci├│n vac├¡a.")

            # Si el resultado es id├®ntico al input con idiomas distintos ÔåÆ probable fallo
            if (
                result.strip().lower() == text.strip().lower()
                and source_lang != target_lang
            ):
                raise ValueError(
                    f"Traducci├│n id├®ntica al input ({source_lang}ÔåÆ{target_lang}). "
                    "Posible fallo de la API."
                )

            logger.info(
                "[Translation] OK (intento %d/%d): %r ÔåÆ %r",
                attempt, _MAX_RETRIES, text[:60], result[:60],
            )
            return result.strip()

        except ValueError:
            # Error de validaci├│n del resultado: no reintentar, relanzar de inmediato
            raise

        except Exception as exc:
            last_error = exc
            delay = _RETRY_DELAYS[attempt - 1] if attempt <= len(_RETRY_DELAYS) else 0
            logger.warning(
                "[Translation] Intento %d/%d fall├│: %s. %s",
                attempt, _MAX_RETRIES, exc,
                f"Reintentando en {delay:.1f}sÔÇª" if attempt < _MAX_RETRIES else "Sin m├ís reintentos.",
            )
            if attempt < _MAX_RETRIES and delay > 0:
                time.sleep(delay)

    raise RuntimeError(
        f"Traducci├│n fallida tras {_MAX_RETRIES} intentos. ├Ültimo error: {last_error}"
    )


# ÔöÇÔöÇÔöÇ API p├║blica ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ

def translate(text: str, source_lang: str, target_lang: str) -> str:
    """
    Traduce texto de source_lang a target_lang.

    - Si los idiomas son iguales, devuelve el texto sin llamar a la API.
    - Usa cach├® LRU: segunda llamada con los mismos args es O(1).
    - Timeout HTTP de 8s garantiza que el thread NO se bloquea indefinidamente.

    Raises:
        ValueError:   Texto vac├¡o o resultado inv├ílido de la API.
        RuntimeError: API falla tras todos los reintentos.
    """
    text = text.strip()
    if not text:
        raise ValueError("El texto a traducir no puede estar vac├¡o.")

    if source_lang == target_lang:
        logger.debug("[Translation] Idiomas iguales, texto sin modificar.")
        return text

    return _cached_translate(text, source_lang, target_lang)


def clear_translation_cache() -> None:
    """Limpia la cach├® de traducciones."""
    _cached_translate.cache_clear()
    logger.info("[Translation] Cach├® limpiada.")
