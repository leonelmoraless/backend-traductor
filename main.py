"""
Backend del Traductor de Voz — FastAPI

Punto de entrada de la aplicación. Su única responsabilidad es:
  1. Configurar la aplicación FastAPI y CORS.
  2. Definir los endpoints HTTP.
  3. Orquestar los servicios (transcription, translation, tts).
     — La lógica real vive en cada servicio, no aquí.

Arrancar con:
    python main.py
    ó
    uvicorn main:app --reload
"""

from fastapi import FastAPI, File, Form, HTTPException, UploadFile, WebSocket
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from services.transcription import transcribe
from services.translation import translate
from services.tts import synthesize
from services.websocket_handler import handle_ws_session

# ─── App ─────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Voice Translator API",
    description="Prototipo: graba → transcribe (Whisper) → traduce (googletrans) → sintetiza (gTTS)",
    version="0.1.0",
)

# ─── CORS ─────────────────────────────────────────────────────────────────────
# Permite que Angular (localhost:4200) pueda llamar al backend (localhost:8000).
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:4200"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


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


# ─── Entry point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=== Iniciando Voice Translator API ===")
    print("Documentación: http://localhost:8000/docs")
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)