"""
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


# ─── API pública ──────────────────────────────────────────────────────────────

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
