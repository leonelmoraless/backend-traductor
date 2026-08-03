"""
Backend del Traductor de Voz ÔÇö FastAPI (Producci├│n-ready)

Punto de entrada de la aplicaci├│n. Responsabilidades:
  1. Configurar la aplicaci├│n FastAPI y CORS.
  2. Definir y registrar todos los endpoints HTTP y WebSocket.
  3. Orquestar los servicios (transcription, translation, tts, history).
  4. Inicializar el historial al arrancar.
  5. Logging estructurado para producci├│n.

Variables de entorno disponibles:
  WHISPER_MODEL        ÔåÆ Modelo de Whisper (default: small | opciones: tiny, base, small, medium, large)
  MIN_AUDIO_BYTES      ÔåÆ Tama├▒o m├¡nimo de audio en bytes (default: 2000)
  WS_WORKERS           ÔåÆ Workers del ThreadPoolExecutor (default: CPU*2)
  WS_OP_TIMEOUT_SEC    ÔåÆ Timeout por operaci├│n en segundos (default: 15.0)
  HISTORY_FILE         ÔåÆ Ruta del archivo JSONL de historial (default: conversation_history.jsonl)
  MAX_HISTORY_ENTRIES  ÔåÆ M├íximo de entradas en memoria/disco (default: 500)

Arrancar con:
    python main.py
    ├│
    uvicorn main:app --reload
"""

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

# ÔöÇÔöÇÔöÇ Logging estructurado ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s ÔÇö %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ÔöÇÔöÇÔöÇ Lifespan: inicializaci├│n al arrancar ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Ejecuta tareas de inicializaci├│n antes de aceptar requests."""
    logger.info("=== Iniciando Voice Translator API ===")
    # Cargar historial desde disco al arrancar
    history_service.ensure_initialized()
    logger.info("=== API lista para recibir conexiones ===")
    yield
    logger.info("=== Voice Translator API detenida ===")


# ÔöÇÔöÇÔöÇ App ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ
app = FastAPI(
    title="Voice Translator API",
    description=(
        "Backend de traducci├│n de voz en tiempo real. "
        "Whisper ÔåÆ deep-translator ÔåÆ gTTS. "
        "WebSocket para traducci├│n en vivo + historial de conversaciones."
    ),
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

# ÔöÇÔöÇÔöÇ CORS ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ
_ALLOWED_ORIGINS = [
    "http://localhost:4200",   # Angular dev
    "http://localhost:3000",   # React/Next dev
    "http://localhost:5173",   # Vite dev
    "http://127.0.0.1:4200",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ÔöÇÔöÇÔöÇ Endpoints de salud ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ

@app.get("/", tags=["Health"])
def root():
    """Endpoint de salud b├ísico."""
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


# ÔöÇÔöÇÔöÇ Endpoint HTTP de procesamiento de audio ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ

@app.post("/procesar-audio/", tags=["Translation"])
async def procesar_audio(
    file: UploadFile = File(..., description="Archivo de audio grabado (webm/wav/mp3)"),
    source_lang: str = Form(..., description="Idioma origen, c├│digo ISO 639-1 (ej. 'es')"),
    target_lang: str = Form(..., description="Idioma destino, c├│digo ISO 639-1 (ej. 'en')"),
    session_id: Optional[str] = Form(None, description="ID de sesi├│n para agrupar en historial"),
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
            "[API] Audio recibido: %s (%d bytes) | %s ÔåÆ %s | session=%s",
            file.filename, len(audio_bytes), source_lang, target_lang, sid,
        )

        # 1. Transcribir con Whisper
        transcripcion = transcribe(audio_bytes, source_lang)
        logger.info("[API] Transcripci├│n: %r", transcripcion)

        # 2. Traducir con deep-translator
        traduccion = translate(transcripcion, source_lang, target_lang)
        logger.info("[API] Traducci├│n: %r", traduccion)

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


# ÔöÇÔöÇÔöÇ WebSocket ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ

@app.websocket("/ws/translate")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket para traducci├│n de voz en tiempo real con historial autom├ítico."""
    await handle_ws_session(websocket)


# ÔöÇÔöÇÔöÇ Endpoints de historial ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ

@app.get("/history", tags=["History"])
async def get_history(
    page: int = Query(1, ge=1, description="P├ígina (empieza en 1)"),
    page_size: int = Query(50, ge=1, le=100, description="Entradas por p├ígina (m├íx. 100)"),
    session_id: Optional[str] = Query(None, description="Filtrar por session_id"),
):
    """
    Devuelve el historial de traducciones paginado.
    Las entradas se devuelven con la m├ís reciente primero.
    """
    return history_service.get_all(page=page, page_size=page_size, session_id=session_id)


@app.get("/history/sessions", tags=["History"])
async def get_sessions():
    """
    Devuelve un resumen de todas las sesiones de traducci├│n registradas.
    Incluye: session_id, n├║mero de entradas, primera/├║ltima actividad e idiomas usados.
    """
    return {"sessions": history_service.get_sessions()}


@app.get("/history/{session_id}", tags=["History"])
async def get_session_history(
    session_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
):
    """Devuelve el historial de una sesi├│n espec├¡fica, paginado."""
    return history_service.get_all(page=page, page_size=page_size, session_id=session_id)


@app.delete("/history", tags=["History"])
async def delete_history():
    """Elimina TODO el historial de traducciones (memoria + disco)."""
    result = await history_service.clear_history()
    logger.warning("[API] Historial eliminado: %d entradas borradas.", result["deleted"])
    return {"message": f"Historial eliminado. {result['deleted']} entradas borradas."}


@app.delete("/cache", tags=["Cache"])
async def clear_caches():
    """Limpia las cach├®s LRU de traducci├│n y TTS."""
    clear_translation_cache()
    clear_tts_cache()
    logger.info("[API] Cach├®s de traducci├│n y TTS limpiadas.")
    return {"message": "Cach├®s limpiadas correctamente."}


# ÔöÇÔöÇÔöÇ Entry point ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ
if __name__ == "__main__":
    logger.info("Documentaci├│n Swagger: http://localhost:8000/docs")
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info",
    )
