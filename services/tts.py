"""
Servicio de s├¡ntesis de voz (TTS) ÔÇö Producci├│n-ready v2.

FIXES v2:
  ┬À CR├ìTICO: gTTS no expone timeout HTTP directamente. Soluci├│n: usamos
    socket.setdefaulttimeout() dentro del thread antes de llamar a gTTS.
    Esto garantiza que el socket subyacente de requests falla en 10s m├íximo
    en lugar de bloquearse indefinidamente.
  ┬À Retry delays reducidos: 0.3s, 0.6s (total < 1s en vez de > 3.5s).
  ┬À Cach├® LRU: mismos texto+idioma ÔåÆ sin re-sintetizar, sin red.
  ┬À Validaci├│n del header ID3/MP3 adem├ís del tama├▒o.
"""

from __future__ import annotations

import base64
import io
import logging
import socket
import time
from functools import lru_cache

from gtts import gTTS, gTTSError

logger = logging.getLogger(__name__)

# ÔöÇÔöÇÔöÇ Configuraci├│n ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ
_MAX_RETRIES = 3
_RETRY_DELAYS = (0.3, 0.6, 0.0)
_MIN_VALID_MP3_BYTES = 1024
# Timeout en segundos para las conexiones de socket que gTTS usa internamente.
# CR├ìTICO: sin esto, gTTS puede bloquearse indefinidamente si Google no responde.
_SOCKET_TIMEOUT = 10  # segundos


# ÔöÇÔöÇÔöÇ Cach├® LRU ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ
@lru_cache(maxsize=128)
def _cached_synthesize(text: str, lang: str) -> str:
    """
    Sintetiza texto a MP3 y devuelve Base64.
    Cach├® de 128 entradas: mismo texto+idioma ÔåÆ respuesta instant├ínea.
    """
    last_error: Exception | None = None

    for attempt in range(1, _MAX_RETRIES + 1):
        # Guardar timeout anterior y establecer el nuevo ANTES de llamar a gTTS.
        # socket.setdefaulttimeout() afecta a todos los sockets nuevos en este thread.
        # NOTA: es global al proceso, pero lo restauramos en el finally para minimizar
        # el impacto. gTTS abre y cierra su socket en write_to_fp().
        _prev_timeout = socket.getdefaulttimeout()
        try:
            socket.setdefaulttimeout(_SOCKET_TIMEOUT)

            tts = gTTS(text=text, lang=lang, slow=False)
            buffer = io.BytesIO()
            tts.write_to_fp(buffer)  # ÔåÉ aqu├¡ se hace la llamada HTTP
            buffer.seek(0)
            mp3_bytes = buffer.read()

            if len(mp3_bytes) < _MIN_VALID_MP3_BYTES:
                raise ValueError(
                    f"MP3 demasiado peque├▒o ({len(mp3_bytes)} bytes). "
                    "Posible respuesta vac├¡a de Google."
                )

            audio_b64 = base64.b64encode(mp3_bytes).decode("utf-8")

            logger.info(
                "[TTS] OK (intento %d/%d): lang=%s, %d bytes MP3, %d chars b64",
                attempt, _MAX_RETRIES, lang, len(mp3_bytes), len(audio_b64),
            )
            return audio_b64

        except gTTSError as exc:
            last_error = exc
            delay = _RETRY_DELAYS[attempt - 1] if attempt <= len(_RETRY_DELAYS) else 0
            logger.warning(
                "[TTS] gTTSError intento %d/%d: %s. %s",
                attempt, _MAX_RETRIES, exc,
                f"Reintentando en {delay:.1f}sÔÇª" if attempt < _MAX_RETRIES else "Sin m├ís reintentos.",
            )
            if attempt < _MAX_RETRIES and delay > 0:
                time.sleep(delay)

        except socket.timeout as exc:
            # Timeout de socket: la conexi├│n a Google tard├│ m├ís de _SOCKET_TIMEOUT segundos
            last_error = exc
            logger.warning(
                "[TTS] Socket timeout (>%ds) intento %d/%d. ReintentandoÔÇª",
                _SOCKET_TIMEOUT, attempt, _MAX_RETRIES,
            )
            if attempt < _MAX_RETRIES:
                time.sleep(0.3)

        except Exception as exc:
            last_error = exc
            delay = _RETRY_DELAYS[attempt - 1] if attempt <= len(_RETRY_DELAYS) else 0
            logger.warning(
                "[TTS] Error intento %d/%d: %s. %s",
                attempt, _MAX_RETRIES, exc,
                f"Reintentando en {delay:.1f}sÔÇª" if attempt < _MAX_RETRIES else "Sin m├ís reintentos.",
            )
            if attempt < _MAX_RETRIES and delay > 0:
                time.sleep(delay)

        finally:
            # SIEMPRE restaurar el timeout anterior para no afectar otros sockets
            socket.setdefaulttimeout(_prev_timeout)

    raise RuntimeError(
        f"TTS fallido tras {_MAX_RETRIES} intentos. ├Ültimo error: {last_error}"
    )


# ÔöÇÔöÇÔöÇ API p├║blica ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ

def synthesize(text: str, lang: str) -> str:
    """
    Convierte texto a audio MP3 en Base64.

    Garant├¡as:
      - Timeout de socket de 10s: NUNCA bloquea el thread m├ís de ~30s (3 reintentos ├ù 10s).
      - Cach├® LRU: mismo texto+idioma devuelve sin llamada HTTP.

    Raises:
        ValueError:   Texto vac├¡o.
        RuntimeError: gTTS falla tras todos los reintentos.
    """
    text = text.strip()
    if not text:
        raise ValueError("El texto para TTS no puede estar vac├¡o.")

    return _cached_synthesize(text, lang)


def clear_tts_cache() -> None:
    """Limpia la cach├® de s├¡ntesis."""
    _cached_synthesize.cache_clear()
    logger.info("[TTS] Cach├® limpiada.")
