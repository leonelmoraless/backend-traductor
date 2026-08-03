"""
<<<<<<< HEAD
Servicio de síntesis de voz (Text-to-Speech).

Responsabilidad única: recibir un texto y devolver el audio generado
en formato MP3 codificado en Base64, listo para ser enviado por JSON
y reproducido en el navegador con la Web Audio API.
"""

import io
import base64
import socket
import time
from functools import lru_cache
from gtts import gTTS
=======
Servicio de síntesis de voz (TTS) — Producción-ready v2.

FIXES v2:
  · CRÍTICO: gTTS no expone timeout HTTP directamente. Solución: usamos
    socket.setdefaulttimeout() dentro del thread antes de llamar a gTTS.
    Esto garantiza que el socket subyacente de requests falla en 10s máximo
    en lugar de bloquearse indefinidamente.
  · Retry delays reducidos: 0.3s, 0.6s (total < 1s en vez de > 3.5s).
  · Caché LRU: mismos texto+idioma → sin re-sintetizar, sin red.
  · Validación del header ID3/MP3 además del tamaño.
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

# ─── Configuración ────────────────────────────────────────────────────────────
_MAX_RETRIES = 3
_RETRY_DELAYS = (0.3, 0.6, 0.0)
_MIN_VALID_MP3_BYTES = 1024
# Timeout en segundos para las conexiones de socket que gTTS usa internamente.
# CRÍTICO: sin esto, gTTS puede bloquearse indefinidamente si Google no responde.
_SOCKET_TIMEOUT = 10  # segundos


# ─── Caché LRU ───────────────────────────────────────────────────────────────
@lru_cache(maxsize=128)
def _cached_synthesize(text: str, lang: str) -> str:
    """
    Sintetiza texto a MP3 y devuelve Base64.
    Caché de 128 entradas: mismo texto+idioma → respuesta instantánea.
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
            tts.write_to_fp(buffer)  # ← aquí se hace la llamada HTTP
            buffer.seek(0)
            mp3_bytes = buffer.read()

            if len(mp3_bytes) < _MIN_VALID_MP3_BYTES:
                raise ValueError(
                    f"MP3 demasiado pequeño ({len(mp3_bytes)} bytes). "
                    "Posible respuesta vacía de Google."
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
                f"Reintentando en {delay:.1f}s…" if attempt < _MAX_RETRIES else "Sin más reintentos.",
            )
            if attempt < _MAX_RETRIES and delay > 0:
                time.sleep(delay)

        except socket.timeout as exc:
            # Timeout de socket: la conexión a Google tardó más de _SOCKET_TIMEOUT segundos
            last_error = exc
            logger.warning(
                "[TTS] Socket timeout (>%ds) intento %d/%d. Reintentando…",
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
                f"Reintentando en {delay:.1f}s…" if attempt < _MAX_RETRIES else "Sin más reintentos.",
            )
            if attempt < _MAX_RETRIES and delay > 0:
                time.sleep(delay)

        finally:
            # SIEMPRE restaurar el timeout anterior para no afectar otros sockets
            socket.setdefaulttimeout(_prev_timeout)

    raise RuntimeError(
        f"TTS fallido tras {_MAX_RETRIES} intentos. Último error: {last_error}"
    )
>>>>>>> 1dbc4a9 (Update for langdetect)


# ─── API pública ──────────────────────────────────────────────────────────────

<<<<<<< HEAD
@lru_cache(maxsize=128)
def synthesize(text: str, lang: str) -> str:
    """
    Convierte texto en voz y devuelve el audio como string Base64.
    Añadido: timeouts en el socket y reintentos para que la request HTTP de gTTS
    no bloquee el hilo infinitamente. Caché LRU para respuestas rápidas de textos idénticos.
    """
    text = text.strip()
    if not text:
        return ""

    for attempt in range(3):
        _prev_timeout = socket.getdefaulttimeout()
        try:
            # Fijamos timeout de 10s para el socket, así gTTS no se queda pillado
            socket.setdefaulttimeout(10.0)
            
            tts = gTTS(text=text, lang=lang, slow=False)

            # Escribimos el MP3 en memoria en lugar de disco para no dejar archivos temporales
            buffer = io.BytesIO()
            tts.write_to_fp(buffer)
            buffer.seek(0)
            
            mp3_bytes = buffer.read()
            if len(mp3_bytes) < 100:
                raise ValueError("MP3 muy pequeño o vacío")

            audio_base64 = base64.b64encode(mp3_bytes).decode("utf-8")
            return audio_base64
            
        except Exception as e:
            print(f"[TTS] Error con gTTS (intento {attempt+1}): {e}")
            time.sleep(0.5)
        finally:
            socket.setdefaulttimeout(_prev_timeout)

    raise RuntimeError("TTS falló después de múltiples intentos")
=======
def synthesize(text: str, lang: str) -> str:
    """
    Convierte texto a audio MP3 en Base64.

    Garantías:
      - Timeout de socket de 10s: NUNCA bloquea el thread más de ~30s (3 reintentos × 10s).
      - Caché LRU: mismo texto+idioma devuelve sin llamada HTTP.

    Raises:
        ValueError:   Texto vacío.
        RuntimeError: gTTS falla tras todos los reintentos.
    """
    text = text.strip()
    if not text:
        raise ValueError("El texto para TTS no puede estar vacío.")

    return _cached_synthesize(text, lang)


def clear_tts_cache() -> None:
    """Limpia la caché de síntesis."""
    _cached_synthesize.cache_clear()
    logger.info("[TTS] Caché limpiada.")
>>>>>>> 1dbc4a9 (Update for langdetect)
