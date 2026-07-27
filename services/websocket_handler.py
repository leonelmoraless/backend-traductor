"""
WebSocket handler para sesión de traducción bidireccional (Streaming).
"""

import asyncio
import base64
import re
from concurrent.futures import ThreadPoolExecutor

from fastapi import WebSocket, WebSocketDisconnect

from services.transcription import transcribe
from services.translation import translate
from services.tts import synthesize

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

                    await _safe_send(websocket, {
                        "type":          "meeting_result",
                        "transcripcion": text,
                        "traduccion":    traduccion,
                        "source_lang":   detected_lang,
                        "target_lang":   target_lang,
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
