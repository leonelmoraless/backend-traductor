"""
WebSocket handler para traducción en tiempo real.
Soporta dos modos:
  - meeting_chunk: audio raw del dispositivo (pantalla Audio)
  - text_utterance: texto ya transcrito (pantalla Inicio)

Arquitectura de Audio (meeting_chunk):
  ┌─────────────────────────────────────────────────────────┐
  │  chunk_queue  →  [vad_task]  →  batch_queue  →  [pipeline_task]  │
  └─────────────────────────────────────────────────────────┘
  
  vad_task:      Lee chunks de 0.5s, acumula PCM, detecta pausas/tiempo,
                 empuja batches listos a batch_queue. NUNCA bloquea por
                 transcripción — así siempre sigue escuchando.
                 
  pipeline_task: Consume batches uno a uno: transcribe → traduce → TTS →
                 envía resultado al cliente. Si hay múltiples batches
                 en la cola, los procesa en orden sin perder ninguno.
"""

from __future__ import annotations

import asyncio
import io
import logging
import math
import os
import re
import struct
import uuid
import wave
import base64
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import langdetect

from fastapi import WebSocket, WebSocketDisconnect

from services.transcription import transcribe
from services.translation import translate
from services.tts import synthesize
from services import history as history_service

logger = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
_WORKERS = int(os.getenv("WS_WORKERS", str((os.cpu_count() or 2) * 2)))
_TRANSLATE_TIMEOUT = float(os.getenv("WS_TRANSLATE_TIMEOUT_SEC", "12.0"))
_TTS_TIMEOUT = float(os.getenv("WS_TTS_TIMEOUT_SEC", "15.0"))

_executor = ThreadPoolExecutor(max_workers=_WORKERS, thread_name_prefix="translator")

_MEANINGFUL_TEXT_RE = re.compile(
    r"[a-zA-Z\u00C0-\u024F\u0400-\u04FF\u4E00-\u9FFF\u3040-\u30FF\u0600-\u06FF]{2,}"
)

logger.info(
    "[WS] Workers: %d | Translate timeout: %.0fs | TTS timeout: %.0fs",
    _WORKERS, _TRANSLATE_TIMEOUT, _TTS_TIMEOUT,
)


# ─── Helpers ──────────────────────────────────────────────────────────────────

async def _run_in_thread(fn, *args, timeout: float = 15.0) -> Any:
    loop = asyncio.get_running_loop()
    future = loop.run_in_executor(_executor, fn, *args)
    return await asyncio.wait_for(future, timeout=timeout)


def _is_meaningful_text(text: str) -> bool:
    return bool(_MEANINGFUL_TEXT_RE.search(text.strip()))


def _word_similarity(a: str, b: str) -> float:
    """Jaccard similarity a nivel de palabras para filtrar repeticiones."""
    if not a or not b:
        return 0.0
    sa = set(a.lower().split())
    sb = set(b.lower().split())
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)


async def _safe_send(websocket: WebSocket, payload: dict[str, Any]) -> bool:
    try:
        await websocket.send_json(payload)
        return True
    except Exception as exc:
        logger.debug("[WS] send fallido: %s", exc)
        return False


# ─── VAD helpers (sin dependencias externas) ─────────────────────────────────

def _get_rms(pcm_data: bytes) -> float:
    """Calcula el nivel de energía RMS de PCM 16-bit mono."""
    n = len(pcm_data) // 2
    if n == 0:
        return 0.0
    try:
        samples = struct.unpack(f"<{n}h", pcm_data[:n * 2])
        return math.sqrt(sum(s * s for s in samples) / n)
    except Exception:
        return 0.0


def _build_wav(pcm_data: bytes, sample_rate: int = 16000,
               channels: int = 1, bit_depth: int = 16) -> bytes:
    """Empaqueta PCM crudo en un archivo WAV en memoria."""
    buf = io.BytesIO()
    with wave.open(buf, "wb") as wf:
        wf.setnchannels(channels)
        wf.setsampwidth(bit_depth // 8)
        wf.setframerate(sample_rate)
        wf.writeframes(pcm_data)
    return buf.getvalue()


# ─── Handler principal ────────────────────────────────────────────────────────

async def handle_ws_session(websocket: WebSocket) -> None:
    await websocket.accept()

    session_id = str(uuid.uuid4())
    src_lang = "es"
    tgt_lang = "en"

    # chunk_queue: chunks raw de PCM de 0.5s enviados por el móvil
    # batch_queue: bloques acumulados listos para transcribir (máx 5 en espera)
    chunk_queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=100)
    batch_queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=5)
    closed = [False]

    logger.info("[WS] Nueva sesión: %s", session_id)
    await _safe_send(websocket, {"type": "session_id", "session_id": session_id})

    # ── VAD Task: acumula PCM y detecta frases. NUNCA espera transcripción. ───
    async def vad_task():
        """
        Lee chunks de 0.5s del chunk_queue y acumula PCM.
        Dispara un batch cuando:
          A) Detecta silencio (>= SILENCE_GATE_CHUNKS) tras voz
          B) Acumuló >= MAX_PHRASE_BYTES (máximo forzado)
        
        GARANTÍAS:
          - Nunca pierde un chunk de audio (buffer circular en chunk_queue)
          - Siempre sigue escuchando sin importar qué pase en el pipeline
          - Si hay audio continuo sin silencio, fuerza corte cada 3.5s
          - Si pasan 3s sin chunks (silencio total de pantalla), resetea estado
        """
        SAMPLE_RATE = 16000
        BYTES_PER_SAMPLE = 2
        BYTES_PER_SEC = SAMPLE_RATE * BYTES_PER_SAMPLE  # 32000

        SILENCE_GATE_CHUNKS = 1       # 0.5s silencio = fin de frase (ultra rápido)
        MAX_PHRASE_BYTES = int(BYTES_PER_SEC * 3.5)  # 3.5s máximo antes de forzar corte
        MIN_PHRASE_BYTES = int(BYTES_PER_SEC * 0.6)  # 0.6s mínimo (captura frases cortas)

        # Ruido de fondo adaptativo
        MIN_RMS = 40.0
        noise_floor = 400.0
        ALPHA = 0.93        # Un poco más reactivo que antes (era 0.95)

        accumulated = bytearray()
        silence_count = 0
        speech_detected = False
        chunks_since_last_flush = 0  # contador para forzar flush por volumen

        while not closed[0]:
            try:
                # Esperar hasta 1.5s por el siguiente chunk (silencio de pantalla)
                # Si no llega nada en 1.5s, forzar flush de lo acumulado y resetear
                try:
                    chunk = await asyncio.wait_for(chunk_queue.get(), timeout=1.5)
                except asyncio.TimeoutError:
                    # Silencio prolongado de pantalla — limpiar estado sin perder lo acumulado
                    if len(accumulated) >= MIN_PHRASE_BYTES:
                        logger.debug("[VAD] Timeout de silencio — forzando flush de %d bytes", len(accumulated))
                        await _push_batch(batch_queue, bytes(accumulated))
                    accumulated.clear()
                    speech_detected = False
                    silence_count = 0
                    chunks_since_last_flush = 0
                    # Resetear noise floor parcialmente para adaptarse al nuevo contexto
                    noise_floor = max(noise_floor * 0.7, MIN_RMS * 5)
                    continue

                if closed[0]:
                    break

                rms = _get_rms(chunk)
                chunks_since_last_flush += 1

                # Actualizar noise floor solo con audio que parece silencio
                if rms < noise_floor * 1.5:
                    noise_floor = (ALPHA * noise_floor) + ((1.0 - ALPHA) * max(MIN_RMS, rms))

                threshold = (noise_floor * 1.8) + 120.0

                logger.debug("[VAD] RMS=%.1f | Floor=%.1f | Thr=%.1f | acc=%dB | chunks=%d",
                             rms, noise_floor, threshold, len(accumulated), chunks_since_last_flush)

                if rms >= threshold:
                    speech_detected = True
                    silence_count = 0
                else:
                    if speech_detected:
                        silence_count += 1

                accumulated.extend(chunk)

                # Trigger A: silencio suficiente tras voz detectada
                if speech_detected and silence_count >= SILENCE_GATE_CHUNKS:
                    if len(accumulated) >= MIN_PHRASE_BYTES:
                        await _push_batch(batch_queue, bytes(accumulated))
                    accumulated.clear()
                    speech_detected = False
                    silence_count = 0
                    chunks_since_last_flush = 0

                # Trigger B: audio continuo sin pausa — cortar para no retrasarse
                elif len(accumulated) >= MAX_PHRASE_BYTES:
                    await _push_batch(batch_queue, bytes(accumulated))
                    accumulated.clear()
                    speech_detected = False
                    silence_count = 0
                    chunks_since_last_flush = 0

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("[VAD] Error inesperado: %s — continuando", e)
                # NUNCA salir del loop por un error — siempre seguir escuchando


    async def _push_batch(q: asyncio.Queue, pcm: bytes) -> None:
        """Empuja un batch al pipeline. Si está lleno, descarta el más antiguo."""
        if q.full():
            try:
                q.get_nowait()
                logger.debug("[VAD] batch_queue llena — descartando batch antiguo")
            except asyncio.QueueEmpty:
                pass
        q.put_nowait(pcm)

    # ── Pipeline Task: transcribe → traduce → TTS → envía al cliente ──────────
    async def pipeline_task():
        """
        Procesa batches de PCM en orden. Puede estar varios segundos en
        transcribir/traducir sin bloquear el VAD (que sigue acumulando en paralelo).
        """
        last_text = ""   # Para deduplicación: evitar repetir la misma frase

        while not closed[0]:
            try:
                pcm = await batch_queue.get()
                if closed[0]:
                    break

                wav = _build_wav(pcm)
                current_src = src_lang
                current_tgt = tgt_lang

                # 1. Transcripción (blocking → thread pool)
                try:
                    text = await _run_in_thread(transcribe, wav, current_src, timeout=25.0)
                    logger.info("[Pipeline] Transcripción: %r", text)
                except Exception as e:
                    logger.debug("[Pipeline] Sin texto: %s", e)
                    continue

                if not _is_meaningful_text(text):
                    continue

                # Deduplicación: omitir si es >75% idéntica a la transcripción anterior
                # (evita traducir repeticiones cuando el hablante repite palabras)
                if _word_similarity(text, last_text) > 0.75:
                    logger.debug("[Pipeline] Transcripción muy similar a la anterior, omitiendo: %r", text)
                    continue
                last_text = text

                # 2. Detección de idioma
                try:
                    detected = langdetect.detect(text)
                except Exception:
                    detected = current_src

                actual_tgt = current_tgt
                if detected == current_tgt or detected.startswith(current_tgt):
                    actual_tgt = current_src

                # 3. Traducción
                try:
                    translation = await _run_in_thread(
                        translate, text, detected, actual_tgt, timeout=_TRANSLATE_TIMEOUT
                    )
                    logger.info("[Pipeline] Traducción: %r", translation)
                except Exception as e:
                    logger.warning("[Pipeline] Error traduciendo: %s", e)
                    continue

                # 4. Síntesis TTS
                try:
                    audio_b64 = await _run_in_thread(
                        synthesize, translation, actual_tgt, timeout=_TTS_TIMEOUT
                    )
                except Exception as e:
                    logger.warning("[Pipeline] Error TTS: %s", e)
                    audio_b64 = ""

                # 5. Enviar resultado
                await _safe_send(websocket, {
                    "type":          "meeting_result",
                    "transcripcion": text,
                    "traduccion":    translation,
                    "source_lang":   detected,
                    "target_lang":   actual_tgt,
                    "audio_base64":  audio_b64,
                })

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("[Pipeline] Error inesperado: %s", e)

    vad = asyncio.create_task(vad_task())
    pipeline = asyncio.create_task(pipeline_task())

    try:
        while True:
            message = await websocket.receive_json()
            msg_type = message.get("type", "")

            # ── Configuración ──────────────────────────────────────────────────
            if msg_type == "config":
                src_lang = message.get("source_lang", src_lang)
                tgt_lang = message.get("target_lang", tgt_lang)
                logger.info("[WS:%s] Config: %s → %s", session_id, src_lang, tgt_lang)
                await _safe_send(websocket, {
                    "type":        "config_ack",
                    "source_lang": src_lang,
                    "target_lang": tgt_lang,
                    "session_id":  session_id,
                })

            # ── Audio raw del dispositivo (pantalla Audio) ─────────────────────
            elif msg_type == "meeting_chunk":
                if "data" not in message:
                    continue
                try:
                    audio_bytes = base64.b64decode(message["data"])
                    # Cola circular: si llena, descartamos el chunk más antiguo
                    if chunk_queue.full():
                        try:
                            chunk_queue.get_nowait()
                        except asyncio.QueueEmpty:
                            pass
                    chunk_queue.put_nowait(audio_bytes)
                except Exception as e:
                    logger.error("[WS] Error decodificando chunk: %s", e)

            # ── Texto ya transcrito (pantalla Inicio) ──────────────────────────
            elif msg_type == "text_utterance":
                texto = message.get("text", "").strip()

                if not _is_meaningful_text(texto):
                    await _safe_send(websocket, {
                        "type":    "no_speech",
                        "message": "No se detectó contenido útil.",
                    })
                    continue

                await _safe_send(websocket, {"type": "processing"})

                try:
                    await _safe_send(websocket, {"type": "translating"})

                    try:
                        detected = langdetect.detect(texto)
                    except Exception:
                        detected = src_lang

                    actual_src = src_lang
                    actual_tgt = tgt_lang
                    if detected.startswith(tgt_lang) or detected == tgt_lang:
                        actual_src = tgt_lang
                        actual_tgt = src_lang

                    translation = await _run_in_thread(
                        translate, texto, actual_src, actual_tgt,
                        timeout=_TRANSLATE_TIMEOUT,
                    )

                    await _safe_send(websocket, {"type": "synthesizing"})
                    audio_b64 = await _run_in_thread(
                        synthesize, translation, actual_tgt,
                        timeout=_TTS_TIMEOUT,
                    )

                    entry = await history_service.add_entry(
                        session_id=session_id,
                        source_lang=actual_src,
                        target_lang=actual_tgt,
                        transcripcion=texto,
                        traduccion=translation,
                    )

                    await _safe_send(websocket, {
                        "type":          "translation_result",
                        "transcripcion": texto,
                        "traduccion":    translation,
                        "audio_base64":  audio_b64,
                        "entry_id":      entry["id"],
                        "detected_lang": actual_src,
                    })

                except asyncio.TimeoutError:
                    await _safe_send(websocket, {
                        "type": "error", "message": "Tiempo de espera agotado."
                    })
                except asyncio.CancelledError:
                    raise
                except ValueError as ve:
                    await _safe_send(websocket, {
                        "type": "no_speech", "message": str(ve)
                    })
                except Exception as exc:
                    logger.error("[WS:%s] Error en utterance: %s", session_id, exc)
                    await _safe_send(websocket, {
                        "type": "error", "message": "Error interno del servidor."
                    })

    except WebSocketDisconnect:
        logger.info("[WS:%s] Cliente desconectado.", session_id)
    except Exception as exc:
        logger.error("[WS:%s] Error de sesión: %s", session_id, exc)
    finally:
        closed[0] = True
        vad.cancel()
        pipeline.cancel()
