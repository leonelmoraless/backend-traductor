"""
<<<<<<< HEAD
Backend del Traductor de Voz — FastAPI

Punto de entrada de la aplicación. Su única responsabilidad es:
  1. Configurar la aplicación FastAPI y CORS.
  2. Definir los endpoints HTTP.
  3. Orquestar los servicios (transcription, translation, tts).
     — La lógica real vive en cada servicio, no aquí.
=======
Backend del Traductor de Voz — FastAPI (Producción-ready)

Punto de entrada de la aplicación. Responsabilidades:
  1. Configurar la aplicación FastAPI y CORS.
  2. Definir y registrar todos los endpoints HTTP y WebSocket.
  3. Orquestar los servicios (transcription, translation, tts, history).
  4. Inicializar el historial al arrancar.
  5. Logging estructurado para producción.

Variables de entorno disponibles:
  WHISPER_MODEL        → Modelo de Whisper (default: small | opciones: tiny, base, small, medium, large)
  MIN_AUDIO_BYTES      → Tamaño mínimo de audio en bytes (default: 2000)
  WS_WORKERS           → Workers del ThreadPoolExecutor (default: CPU*2)
  WS_OP_TIMEOUT_SEC    → Timeout por operación en segundos (default: 15.0)
  HISTORY_FILE         → Ruta del archivo JSONL de historial (default: conversation_history.jsonl)
  MAX_HISTORY_ENTRIES  → Máximo de entradas en memoria/disco (default: 500)
>>>>>>> 1dbc4a9 (Update for langdetect)

Arrancar con:
    python main.py
    ó
    uvicorn main:app --reload
"""

<<<<<<< HEAD
from fastapi import FastAPI, File, Form, HTTPException, UploadFile, WebSocket
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from services.transcription import transcribe
from services.translation import translate
from services.tts import synthesize
from services.websocket_handler import handle_ws_session
=======
from __future__ import annotations

import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Optional

import uvicorn
from fastapi import FastAPI, File, Form, HTTPException, Query, UploadFile, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from services.transcription import transcribe, is_model_loaded
from services.translation import translate, clear_translation_cache
from services.tts import synthesize, clear_tts_cache
from services.websocket_handler import handle_ws_session
from services import history as history_service

# ─── Logging estructurado ────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ─── Lifespan: inicialización al arrancar ─────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Ejecuta tareas de inicialización antes de aceptar requests."""
    logger.info("=== Iniciando Voice Translator API ===")
    # Cargar historial desde disco al arrancar
    history_service.ensure_initialized()
    logger.info("=== API lista para recibir conexiones ===")
    yield
    logger.info("=== Voice Translator API detenida ===")

>>>>>>> 1dbc4a9 (Update for langdetect)

# ─── App ─────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Voice Translator API",
<<<<<<< HEAD
    description="Prototipo: graba → transcribe (Whisper) → traduce (googletrans) → sintetiza (gTTS)",
    version="0.1.0",
)

# ─── CORS ─────────────────────────────────────────────────────────────────────
# Permite que Angular (localhost:4200) pueda llamar al backend (localhost:8000).
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:4200"],
=======
    description=(
        "Backend de traducción de voz en tiempo real. "
        "Whisper → deep-translator → gTTS. "
        "WebSocket para traducción en vivo + historial de conversaciones."
    ),
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

# ─── CORS ─────────────────────────────────────────────────────────────────────
_ALLOWED_ORIGINS = [
    "http://localhost:4200",   # Angular dev
    "http://localhost:3000",   # React/Next dev
    "http://localhost:5173",   # Vite dev
    "http://127.0.0.1:4200",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_ALLOWED_ORIGINS,
>>>>>>> 1dbc4a9 (Update for langdetect)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


<<<<<<< HEAD
# ─── Endpoints ────────────────────────────────────────────────────────────────

@app.get("/")
def health_check():
    """Endpoint de salud para verificar que el servidor está activo."""
    return {"status": "ok", "message": "Voice Translator API corriendo correctamente."}


@app.websocket("/ws/translate")
async def websocket_endpoint(websocket: WebSocket):
    await handle_ws_session(websocket)


@app.post("/procesar-audio/")
async def procesar_audio(
    file: UploadFile = File(..., description="Archivo de audio grabado (webm/wav)"),
    source_lang: str = Form(..., description="Idioma origen, código ISO 639-1 (ej. 'es')"),
    target_lang: str = Form(..., description="Idioma destino, código ISO 639-1 (ej. 'en')"),
):
    """
    Recibe un audio, lo transcribe, traduce y sintetiza la voz traducida.

    Returns:
        JSON con:
          - transcripcion: texto original detectado en el audio
          - traduccion:    texto traducido al idioma destino
          - audio_base64:  audio MP3 de la traducción en Base64
    """
    try:
        # 1. Leer los bytes del audio
        audio_bytes = await file.read()
        print(f"[API] Audio recibido: {file.filename} ({len(audio_bytes)} bytes) | {source_lang} → {target_lang}")

        # 2. Transcribir con Whisper
        transcripcion = transcribe(audio_bytes, source_lang)
        print(f"[API] Transcripción: {transcripcion!r}")

        # 3. Traducir con googletrans
        traduccion = translate(transcripcion, source_lang, target_lang)
        print(f"[API] Traducción: {traduccion!r}")

        # 4. Sintetizar voz con gTTS
        audio_base64 = synthesize(traduccion, target_lang)
        print(f"[API] Audio generado ({len(audio_base64)} chars base64)")

        return {
            "transcripcion": transcripcion,
            "traduccion": traduccion,
            "audio_base64": audio_base64,
        }

    except ValueError as ve:
        # Error de validación (ej. audio sin voz detectada)
        raise HTTPException(status_code=422, detail=str(ve))

    except Exception as e:
        # Cualquier otro error interno
        print(f"[API] Error inesperado: {e}")
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")
=======
# ─── Endpoints de salud ───────────────────────────────────────────────────────

@app.get("/", tags=["Health"])
def root():
    """Endpoint de salud básico."""
    return {"status": "ok", "message": "Voice Translator API corriendo."}


@app.get("/health", tags=["Health"])
def health_check():
    """
    Health check detallado.
    Indica el estado de cada componente del sistema.
    """
    total_entries = len(history_service._entries)
    return {
        "status": "ok",
        "version": "1.0.0",
        "components": {
            "whisper_model": {
                "loaded": is_model_loaded(),
                "model": os.getenv("WHISPER_MODEL", "small"),
            },
            "history": {
                "entries_in_memory": total_entries,
                "file": str(history_service._HISTORY_FILE),
            },
        },
    }


# ─── Endpoint HTTP de procesamiento de audio ─────────────────────────────────

@app.post("/procesar-audio/", tags=["Translation"])
async def procesar_audio(
    file: UploadFile = File(..., description="Archivo de audio grabado (webm/wav/mp3)"),
    source_lang: str = Form(..., description="Idioma origen, código ISO 639-1 (ej. 'es')"),
    target_lang: str = Form(..., description="Idioma destino, código ISO 639-1 (ej. 'en')"),
    session_id: Optional[str] = Form(None, description="ID de sesión para agrupar en historial"),
):
    """
    Recibe un audio, lo transcribe (Whisper), traduce (deep-translator)
    y sintetiza la voz traducida (gTTS).

    Returns:
        JSON con:
          - transcripcion: texto original
          - traduccion:    texto traducido
          - audio_base64:  audio MP3 en Base64
          - entry_id:      ID de la entrada guardada en el historial
    """
    t0 = time.perf_counter()
    try:
        audio_bytes = await file.read()
        sid = session_id or "http-single"

        logger.info(
            "[API] Audio recibido: %s (%d bytes) | %s → %s | session=%s",
            file.filename, len(audio_bytes), source_lang, target_lang, sid,
        )

        # 1. Transcribir con Whisper
        transcripcion = transcribe(audio_bytes, source_lang)
        logger.info("[API] Transcripción: %r", transcripcion)

        # 2. Traducir con deep-translator
        traduccion = translate(transcripcion, source_lang, target_lang)
        logger.info("[API] Traducción: %r", traduccion)

        # 3. Sintetizar voz con gTTS
        audio_base64 = synthesize(traduccion, target_lang)

        # 4. Guardar en historial
        entry = await history_service.add_entry(
            session_id=sid,
            source_lang=source_lang,
            target_lang=target_lang,
            transcripcion=transcripcion,
            traduccion=traduccion,
        )

        elapsed = time.perf_counter() - t0
        logger.info("[API] Request completada en %.2fs. entry_id=%s", elapsed, entry["id"])

        return {
            "transcripcion": transcripcion,
            "traduccion":    traduccion,
            "audio_base64":  audio_base64,
            "entry_id":      entry["id"],
            "elapsed_sec":   round(elapsed, 3),
        }

    except ValueError as ve:
        logger.warning("[API] Audio rechazado: %s", ve)
        raise HTTPException(status_code=422, detail=str(ve))

    except RuntimeError as re:
        logger.error("[API] Error de servicio: %s", re)
        raise HTTPException(status_code=503, detail=str(re))

    except Exception as e:
        logger.error("[API] Error inesperado: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error interno: {e}")


# ─── WebSocket ────────────────────────────────────────────────────────────────

@app.websocket("/ws/translate")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket para traducción de voz en tiempo real con historial automático."""
    await handle_ws_session(websocket)


# ─── Endpoints de historial ───────────────────────────────────────────────────

@app.get("/history", tags=["History"])
async def get_history(
    page: int = Query(1, ge=1, description="Página (empieza en 1)"),
    page_size: int = Query(50, ge=1, le=100, description="Entradas por página (máx. 100)"),
    session_id: Optional[str] = Query(None, description="Filtrar por session_id"),
):
    """
    Devuelve el historial de traducciones paginado.
    Las entradas se devuelven con la más reciente primero.
    """
    return history_service.get_all(page=page, page_size=page_size, session_id=session_id)


@app.get("/history/sessions", tags=["History"])
async def get_sessions():
    """
    Devuelve un resumen de todas las sesiones de traducción registradas.
    Incluye: session_id, número de entradas, primera/última actividad e idiomas usados.
    """
    return {"sessions": history_service.get_sessions()}


@app.get("/history/{session_id}", tags=["History"])
async def get_session_history(
    session_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
):
    """Devuelve el historial de una sesión específica, paginado."""
    return history_service.get_all(page=page, page_size=page_size, session_id=session_id)


@app.delete("/history", tags=["History"])
async def delete_history():
    """Elimina TODO el historial de traducciones (memoria + disco)."""
    result = await history_service.clear_history()
    logger.warning("[API] Historial eliminado: %d entradas borradas.", result["deleted"])
    return {"message": f"Historial eliminado. {result['deleted']} entradas borradas."}


@app.delete("/cache", tags=["Cache"])
async def clear_caches():
    """Limpia las cachés LRU de traducción y TTS."""
    clear_translation_cache()
    clear_tts_cache()
    logger.info("[API] Cachés de traducción y TTS limpiadas.")
    return {"message": "Cachés limpiadas correctamente."}
>>>>>>> 1dbc4a9 (Update for langdetect)


# ─── Entry point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
<<<<<<< HEAD
    print("=== Iniciando Voice Translator API ===")
    print("Documentación: http://localhost:8000/docs")
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
=======
    logger.info("Documentación Swagger: http://localhost:8000/docs")
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info",
    )
>>>>>>> 1dbc4a9 (Update for langdetect)
