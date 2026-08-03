"""
WebSocket handler para traducción en tiempo real.
Soporta dos modos:
  - meeting_chunk: audio raw del dispositivo (pantalla Audio)
  - text_utterance: texto ya transcrito (pantalla Inicio)
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import uuid
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


async def _safe_send(websocket: WebSocket, payload: dict[str, Any]) -> bool:
    try:
        await websocket.send_json(payload)
        return True
    except Exception as exc:
        logger.debug("[WS] send fallido: %s", exc)
        return False


# ─── Handler principal ────────────────────────────────────────────────────────

async def handle_ws_session(websocket: WebSocket) -> None:
    await websocket.accept()

    session_id = str(uuid.uuid4())
    src_lang = "es"
    tgt_lang = "en"

    chunk_queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=3)
    closed = [False]

    logger.info("[WS] Nueva sesión: %s", session_id)
    await _safe_send(websocket, {"type": "session_id", "session_id": session_id})

    # ── Loop de audio (meeting_chunk) ─────────────────────────────────────────
    async def audio_loop():
        nonlocal src_lang, tgt_lang
        while not closed[0]:
            try:
                audio_bytes = await chunk_queue.get()
                if closed[0]:
                    break

                # 1. Transcripción
                text = await _run_in_thread(transcribe, audio_bytes, src_lang, timeout=25.0)
                logger.info("[Meeting] Transcripción: %r", text)

                if not _is_meaningful_text(text):
                    continue

                # 2. Detección de idioma
                try:
                    detected = langdetect.detect(text)
                except Exception:
                    detected = src_lang

                # Determinar dirección de traducción
                actual_tgt = tgt_lang
                if detected.startswith(tgt_lang) or detected == tgt_lang:
                    actual_tgt = src_lang

                # 3. Traducción
                translation = await _run_in_thread(
                    translate, text, detected, actual_tgt, timeout=_TRANSLATE_TIMEOUT
                )
                logger.info("[Meeting] Traducción: %r", translation)

                # 4. Síntesis TTS
                audio_b64 = await _run_in_thread(
                    synthesize, translation, actual_tgt, timeout=_TTS_TIMEOUT
                )

                # 5. Enviar resultado
                await _safe_send(websocket, {
                    "type":          "meeting_result",
                    "transcripcion": text,
                    "traduccion":    translation,
                    "source_lang":   detected,
                    "target_lang":   actual_tgt,
                    "audio_base64":  audio_b64,
                })

            except asyncio.TimeoutError:
                logger.warning("[Meeting] Timeout procesando chunk de audio.")
            except ValueError as ve:
                logger.debug("[Meeting] Chunk inválido: %s", ve)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("[Meeting] Error inesperado: %s", e)

    loop_task = asyncio.create_task(audio_loop())

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
                    # Si la cola está llena, descartamos el más antiguo
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
        loop_task.cancel()
