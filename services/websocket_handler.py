"""
<<<<<<< HEAD
WebSocket handler para sesión de traducción bidireccional (Streaming).
"""

import asyncio
import base64
import re
from concurrent.futures import ThreadPoolExecutor
=======
WebSocket handler para traducción en tiempo real — Producción-ready v2.

FIXES v2:
  · CRÍTICO: asyncio.get_event_loop() → asyncio.get_running_loop()
    get_event_loop() está deprecado en Python 3.10+ desde un coroutine y puede
    levantar DeprecationWarning o fallar según configuración del event loop.

  · CRÍTICO: asyncio.CancelledError no era capturado en el bloque interior.
    CancelledError es BaseException (no Exception), así que escapaba del
    'except Exception' y mataba la sesión WebSocket entera sin notificar al cliente.
    Ahora se captura explícitamente y se envía mensaje de error al cliente.

  · CRÍTICO: _is_meaningful_text compilaba el regex en CADA llamada.
    Ahora el pattern es una constante a nivel de módulo.

  · asyncio.wait_for timeout separado por operación:
    - Traducción: _TRANSLATE_TIMEOUT (10s)
    - TTS:        _TTS_TIMEOUT (12s)
    - Cada servicio ya tiene timeouts de red propios, esto es la última barrera.

  · Añadido mensaje "processing" inmediatamente al recibir utterance,
    antes de cualquier operación async. El cliente sabe que el servidor
    recibió el texto.

  · _run_in_thread ahora usa asyncio.get_running_loop() correctamente.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any
import langdetect
>>>>>>> 1dbc4a9 (Update for langdetect)

from fastapi import WebSocket, WebSocketDisconnect

from services.transcription import transcribe
from services.translation import translate
from services.tts import synthesize
<<<<<<< HEAD

_executor = ThreadPoolExecutor(max_workers=5)

async def _run_in_thread(fn, *args, timeout=15.0, **kwargs):
    """Ejecuta una función síncrona con un límite de tiempo para evitar bloqueos."""
    loop = asyncio.get_running_loop()
    future = loop.run_in_executor(_executor, lambda: fn(*args, **kwargs))
    return await asyncio.wait_for(future, timeout=timeout)

async def _safe_send(websocket: WebSocket, payload: dict, closed_flag: list) -> None:
    """Envía un mensaje JSON solo si la conexión sigue abierta."""
    if closed_flag[0]:
        return
    try:
        await websocket.send_json(payload)
    except Exception:
        closed_flag[0] = True

async def handle_ws_session(websocket: WebSocket) -> None:
    await websocket.accept()
    print("[WS] Nueva sesión iniciada")

    lang1 = "es"
    lang2 = "en"

    closed = [False]
    processing_lock = asyncio.Lock()
    
    # Cola inteligente para no ahogar la CPU con meeting_chunks
    chunk_queue = asyncio.Queue(maxsize=1)
    
    async def process_chunks_loop():
        while not closed[0]:
            try:
                # Esperamos un audio
                audio_bytes = await chunk_queue.get()
                if closed[0]:
                    break
                    
                # Procesar el chunk más reciente
                text, detected_lang = await _run_in_thread(transcribe, audio_bytes, [lang1, lang2])
                print(f"[WS Meeting] Detectado: {detected_lang!r} | Texto: {text!r}")

                if text and re.search(r'[a-zA-ZáéíóúÁÉÍÓÚñÑüÜäöüßÄÖÜ]', text):
                    target_lang = lang2
                    if detected_lang == lang2:
                        target_lang = lang1

                    traduccion = await _run_in_thread(translate, text, detected_lang, target_lang)
                    print(f"[WS Meeting] Traducción: {traduccion!r}")

                    audio_b64 = await _run_in_thread(synthesize, traduccion, target_lang)

                    await _safe_send(websocket, {
                        "type":          "meeting_result",
                        "transcripcion": text,
                        "traduccion":    traduccion,
                        "source_lang":   detected_lang,
                        "target_lang":   target_lang,
                        "audio_base64":  audio_b64,
                    }, closed)
            except asyncio.TimeoutError:
                pass
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[WS Meeting Loop] Error ignorado: {e}")
                pass
                
    loop_task = asyncio.create_task(process_chunks_loop())

    try:
        while True:
            message = await websocket.receive_json()
            msg_type = message.get("type")

            if msg_type == "config":
                lang1 = message.get("lang1", "es")
                lang2 = message.get("lang2", "en")
                print(f"[WS] Config: {lang1} ↔ {lang2}")

            elif msg_type == "meeting_chunk":
                if "data" not in message: continue
                audio_bytes = base64.b64decode(message["data"])
                if chunk_queue.full():
                    try:
                        chunk_queue.get_nowait()
                    except asyncio.QueueEmpty:
                        pass
                chunk_queue.put_nowait(audio_bytes)

            elif msg_type == "translate_text":
                text_to_translate = message.get("text", "")
                if text_to_translate.strip():
                    try:
                        print(f"[WS] translate_text recibido: {text_to_translate!r}")
                        # Usamos nuestra función translate que ya tiene reintentos y caché
                        traduccion = await _run_in_thread(
                            translate, text_to_translate, lang1, lang2
                        )
                        await _safe_send(websocket, {
                            "type": "partial_translation_result",
                            "traduccion": traduccion
                        }, closed)
                    except Exception as e:
                        print(f"[WS] Error en translate_text: {e}")

            elif msg_type == "translate_and_speak_chunk":
                chunk = message.get("text", "")
                if chunk.strip():
                    try:
                        print(f"[WS] translate_and_speak_chunk recibido: {chunk!r}")
                        traduccion = await _run_in_thread(
                            translate, chunk, lang1, lang2
                        )
                        audio_b64 = await _run_in_thread(synthesize, traduccion, lang2)
                        await _safe_send(websocket, {
                            "type": "partial_audio",
                            "audio_base64": audio_b64
                        }, closed)
                    except Exception as e:
                        print(f"[WS] Error en TTS simultáneo: {e}")

            elif msg_type == "text_utterance":
                print("[WS] Mensaje text_utterance recibido")
                text_to_process = message.get("text", "").strip()
                
                asyncio.create_task(
                    _process_text_final(
                        websocket, text_to_process,
                        lang1, lang2,
                        processing_lock, closed
                    )
                )

            elif msg_type == "end_utterance":
                print("[WS] Mensaje end_utterance recibido")
                if "data" not in message:
                    continue
                audio_bytes = base64.b64decode(message["data"])

                asyncio.create_task(
                    _process_final(
                        websocket, audio_bytes,
                        lang1, lang2,
                        processing_lock, closed
                    )
                )

    except WebSocketDisconnect:
        closed[0] = True
        print("[WS] Sesión cerrada por el cliente")
    except asyncio.CancelledError:
        closed[0] = True
    except Exception as exc:
        closed[0] = True
        print(f"[WS] Error inesperado en sesión: {exc}")
    finally:
        loop_task.cancel()


async def _process_final(
    websocket: WebSocket,
    audio_bytes: bytes,
    lang1: str,
    lang2: str,
    lock: asyncio.Lock,
    closed: list,
) -> None:
    async with lock:
        if closed[0]:
            return

        try:
            text, detected_lang = await _run_in_thread(transcribe, audio_bytes, [lang1, lang2])
            print(f"[WS Final] Detectado: {detected_lang!r} | Texto: {text!r}")

            if not re.search(r'[a-zA-ZáéíóúÁÉÍÓÚñÑüÜäöüßÄÖÜ]', text):
                await _safe_send(websocket, {
                    "type": "no_speech",
                    "message": "No se entendió el audio. ¿Puedes repetirlo?"
                }, closed)
                return

            if detected_lang not in (lang1, lang2):
                await _safe_send(websocket, {
                    "type": "no_speech",
                    "message": f"Idioma diferente al seleccionado. Por favor habla en {lang1} o {lang2}."
                }, closed)
                return

            target_lang = lang2 if detected_lang == lang1 else lang1

            traduccion = await _run_in_thread(translate, text, detected_lang, target_lang)
            print(f"[WS Final] Traducción: {traduccion!r}")

            audio_b64 = await _run_in_thread(synthesize, traduccion, target_lang)

            await _safe_send(websocket, {
                "type":          "translation_result",
                "transcripcion": text,
                "traduccion":    traduccion,
                "source_lang":   detected_lang,
                "target_lang":   target_lang,
                "audio_base64":  audio_b64,
            }, closed)

        except ValueError:
            await _safe_send(websocket, {"type": "no_speech", "message": "No se detectó voz válida."}, closed)
        except asyncio.TimeoutError:
            await _safe_send(websocket, {"type": "error", "message": "El proceso tardó demasiado. Por favor, intenta de nuevo."}, closed)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"[WS Final] Error: {exc}")
            await _safe_send(websocket, {"type": "error", "message": "Error procesando la traducción o el audio."}, closed)


async def _process_text_final(
    websocket: WebSocket,
    text: str,
    lang1: str,
    lang2: str,
    lock: asyncio.Lock,
    closed: list,
) -> None:
    """Procesa una transcripción final proveniente directamente del texto (ej. SpeechRecognition del navegador)."""
    async with lock:
        if closed[0]:
            return

        try:
            print(f"[WS Final Text] Texto recibido: {text!r}")

            if not text or not re.search(r'[a-zA-ZáéíóúÁÉÍÓÚñÑüÜäöüßÄÖÜ]', text):
                await _safe_send(websocket, {
                    "type": "no_speech",
                    "message": "No se entendió o detectó contenido útil. ¿Puedes repetirlo?"
                }, closed)
                return

            from services.translation import detect_language
            detected = await _run_in_thread(detect_language, text, timeout=5.0)

            detected_lang = lang1
            target_lang = lang2

            if detected:
                # Extraer prefijo (ej. 'en' de 'en-US')
                l1_prefix = lang1.split('-')[0]
                l2_prefix = lang2.split('-')[0]
                
                if detected.startswith(l2_prefix):
                    detected_lang = lang2
                    target_lang = lang1
                elif detected.startswith(l1_prefix):
                    detected_lang = lang1
                    target_lang = lang2

            traduccion = await _run_in_thread(translate, text, detected_lang, target_lang)
            print(f"[WS Final Text] Traducción: {traduccion!r}")

            audio_b64 = await _run_in_thread(synthesize, traduccion, target_lang)

            await _safe_send(websocket, {
                "type":          "translation_result",
                "transcripcion": text,
                "traduccion":    traduccion,
                "source_lang":   detected_lang,
                "target_lang":   target_lang,
                "audio_base64":  audio_b64,
            }, closed)

        except asyncio.TimeoutError:
            await _safe_send(websocket, {"type": "error", "message": "El proceso tardó demasiado. Por favor, intenta de nuevo."}, closed)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"[WS Final Text] Error: {exc}")
            await _safe_send(websocket, {"type": "error", "message": "Error procesando la traducción o el audio final."}, closed)
=======
from services import history as history_service

logger = logging.getLogger(__name__)

# ─── Configuración ────────────────────────────────────────────────────────────
_WORKERS = int(os.getenv("WS_WORKERS", str((os.cpu_count() or 2) * 2)))
# Timeouts separados por operación (últimas barreras de seguridad)
# Los servicios ya tienen timeouts internos (socket/HTTP), estos son el fallback.
_TRANSLATE_TIMEOUT = float(os.getenv("WS_TRANSLATE_TIMEOUT_SEC", "12.0"))
_TTS_TIMEOUT = float(os.getenv("WS_TTS_TIMEOUT_SEC", "15.0"))

# Pool de threads para operaciones síncronas bloqueantes
_executor = ThreadPoolExecutor(max_workers=_WORKERS, thread_name_prefix="translator")

# Regex compilado UNA SOLA VEZ (no en cada llamada)
_MEANINGFUL_TEXT_RE = re.compile(
    r"[a-zA-Z\u00C0-\u024F\u0400-\u04FF\u4E00-\u9FFF\u3040-\u30FF\u0600-\u06FF]{2,}"
)

logger.info("[WS] ThreadPoolExecutor: %d workers. Translate timeout: %.0fs, TTS timeout: %.0fs",
            _WORKERS, _TRANSLATE_TIMEOUT, _TTS_TIMEOUT)


# ─── Helpers ─────────────────────────────────────────────────────────────────

async def _run_in_thread(fn, *args, timeout: float) -> Any:
    """
    Ejecuta una función síncrona en el pool de threads con timeout.

    Usa asyncio.get_running_loop() (correcto para Python 3.10+).
    El timeout es la ÚLTIMA barrera de seguridad. Los servicios internos
    (translation, tts) ya tienen sus propios timeouts HTTP/socket.

    Raises:
        asyncio.TimeoutError: Si la operación supera el timeout.
        asyncio.CancelledError: Si la tarea fue cancelada externamente.
    """
    loop = asyncio.get_running_loop()  # FIX: era get_event_loop() (deprecado)
    future = loop.run_in_executor(_executor, fn, *args)
    return await asyncio.wait_for(future, timeout=timeout)


def _is_meaningful_text(text: str) -> bool:
    """
    Verifica que el texto tiene contenido semántico útil.
    El regex se compiló una vez al cargar el módulo (no se recompila aquí).
    """
    return bool(_MEANINGFUL_TEXT_RE.search(text.strip()))


async def _safe_send(websocket: WebSocket, payload: dict[str, Any]) -> bool:
    """Envía JSON al cliente de forma segura. Devuelve False si la conexión está cerrada."""
    try:
        await websocket.send_json(payload)
        return True
    except Exception as exc:
        logger.debug("[WS] send fallido (conexión cerrada?): %s", exc)
        return False


# ─── Handler principal ────────────────────────────────────────────────────────

async def handle_ws_session(websocket: WebSocket) -> None:
    """
    Gestiona una sesión completa de traducción en tiempo real.

    Pipeline por utterance:
      receive text_utterance
        → filtro de contenido
        → [translate] con timeout _TRANSLATE_TIMEOUT
        → [synthesize] con timeout _TTS_TIMEOUT
        → guardar historial
        → enviar translation_result al cliente
    """
    await websocket.accept()

    session_id = str(uuid.uuid4())
    source_lang = "es"
    target_lang = "en"

    logger.info("[WS] Nueva sesión: %s", session_id)
    await _safe_send(websocket, {"type": "session_id", "session_id": session_id})

    try:
        while True:
            # receive_json puede lanzar WebSocketDisconnect si el cliente se va
            message = await websocket.receive_json()
            msg_type = message.get("type", "")

            # ── Config ──────────────────────────────────────────────────────
            if msg_type == "config":
                source_lang = message.get("source_lang", source_lang)
                target_lang = message.get("target_lang", target_lang)
                logger.info("[WS:%s] Config: %s → %s", session_id, source_lang, target_lang)
                await _safe_send(websocket, {
                    "type": "config_ack",
                    "source_lang": source_lang,
                    "target_lang": target_lang,
                    "session_id": session_id,
                })

            # ── Utterance (texto a traducir) ─────────────────────────────────
            elif msg_type == "text_utterance":
                transcripcion = message.get("text", "").strip()

                if not _is_meaningful_text(transcripcion):
                    logger.debug("[WS:%s] Texto sin contenido útil: %r", session_id, transcripcion)
                    await _safe_send(websocket, {
                        "type": "no_speech",
                        "message": "No se detectó contenido útil en el texto.",
                    })
                    continue

                logger.info("[WS:%s] Utterance recibida: %r", session_id, transcripcion[:80])

                # Avisar al cliente de inmediato que estamos procesando
                await _safe_send(websocket, {"type": "processing"})

                try:
                    # ── 1. Traducción (Con Auto-Detección) ───────────────────
                    await _safe_send(websocket, {"type": "translating"})
                    
                    try:
                        detected_lang = langdetect.detect(transcripcion)
                    except langdetect.LangDetectException:
                        detected_lang = source_lang # Fallback
                        
                    # Si el idioma detectado se parece más al target, invertimos
                    if detected_lang.startswith(target_lang) or detected_lang == target_lang:
                        actual_source = target_lang
                        actual_target = source_lang
                    else:
                        actual_source = source_lang
                        actual_target = target_lang

                    traduccion = await _run_in_thread(
                        translate, transcripcion, actual_source, actual_target,
                        timeout=_TRANSLATE_TIMEOUT,
                    )
                    logger.info("[WS:%s] Traducción OK: %r", session_id, traduccion[:80])

                    # ── 2. Síntesis de voz ───────────────────────────────────
                    await _safe_send(websocket, {"type": "synthesizing"})
                    audio_b64 = await _run_in_thread(
                        synthesize, traduccion, actual_target,
                        timeout=_TTS_TIMEOUT,
                    )

                    # ── 3. Guardar en historial ──────────────────────────────
                    entry = await history_service.add_entry(
                        session_id=session_id,
                        source_lang=actual_source,
                        target_lang=actual_target,
                        transcripcion=transcripcion,
                        traduccion=traduccion,
                    )

                    # ── 4. Resultado al cliente ──────────────────────────────
                    await _safe_send(websocket, {
                        "type":          "translation_result",
                        "transcripcion": transcripcion,
                        "traduccion":    traduccion,
                        "audio_base64":  audio_b64,
                        "entry_id":      entry["id"],
                        "detected_lang": actual_source,
                    })
                    logger.info("[WS:%s] Resultado enviado OK. entry_id=%s", session_id, entry["id"])

                except asyncio.TimeoutError:
                    # El servicio (translate o TTS) superó su timeout de asyncio
                    logger.error("[WS:%s] Timeout de operación.", session_id)
                    await _safe_send(websocket, {
                        "type": "error",
                        "message": "Tiempo de espera agotado. Intenta de nuevo.",
                    })

                except asyncio.CancelledError:
                    # FIX CRÍTICO: CancelledError es BaseException, no Exception.
                    # Sin este bloque, propagaba y mataba la sesión WebSocket.
                    logger.warning("[WS:%s] Tarea cancelada durante procesamiento.", session_id)
                    await _safe_send(websocket, {
                        "type": "error",
                        "message": "Procesamiento interrumpido. Intenta de nuevo.",
                    })
                    # Re-raise para que el event loop sepa que fue cancelado
                    raise

                except ValueError as ve:
                    # Errores de validación (texto vacío, audio sin voz, etc.)
                    logger.warning("[WS:%s] Error de validación: %s", session_id, ve)
                    await _safe_send(websocket, {
                        "type": "no_speech",
                        "message": str(ve),
                    })

                except RuntimeError as re_exc:
                    # Errores de servicio externo (API de traducción, TTS) tras reintentos
                    logger.error("[WS:%s] Error de servicio: %s", session_id, re_exc)
                    await _safe_send(websocket, {
                        "type": "error",
                        "message": f"Servicio temporalmente no disponible. Intenta de nuevo.",
                    })

                except Exception as exc:
                    logger.error("[WS:%s] Error inesperado: %s", session_id, exc, exc_info=True)
                    await _safe_send(websocket, {
                        "type": "error",
                        "message": "Error interno. Intenta de nuevo.",
                    })

            # ── Tipo desconocido ─────────────────────────────────────────────
            else:
                logger.debug("[WS:%s] Mensaje desconocido: %r", session_id, msg_type)

    except WebSocketDisconnect:
        logger.info("[WS:%s] Cliente desconectado.", session_id)

    except asyncio.CancelledError:
        logger.info("[WS:%s] Sesión cancelada por el servidor.", session_id)

    except Exception as exc:
        logger.error("[WS:%s] Error fatal en sesión: %s", session_id, exc, exc_info=True)
        await _safe_send(websocket, {"type": "error", "message": "Error interno del servidor."})
>>>>>>> 1dbc4a9 (Update for langdetect)
