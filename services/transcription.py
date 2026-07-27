"""
Servicio de transcripción de audio.

Responsabilidad única: recibir bytes de audio y devolver el texto transcrito
junto con el idioma detectado. Usa faster-whisper (motor CTranslate2) para
máximo rendimiento en CPU.

El modelo se carga una sola vez en memoria (patrón singleton).
"""

import os
import tempfile
from faster_whisper import WhisperModel

# ─── Singleton del modelo ─────────────────────────────────────────────────────
_model: WhisperModel | None = None


def _get_model() -> WhisperModel:
    """Carga el modelo Whisper 'base' usando faster-whisper para CPU y lo reutiliza."""
    global _model
    if _model is None:
        print("[Whisper] Cargando modelo 'base' (faster-whisper)... (solo ocurre una vez)")
        _model = WhisperModel("base", device="cpu", compute_type="int8")
        print("[Whisper] Modelo listo.")
    return _model


# ─── API pública ──────────────────────────────────────────────────────────────

def transcribe(audio_bytes: bytes, expected_langs: list[str] | None = None) -> tuple[str, str]:
    """
    Transcribe un fragmento de audio y detecta el idioma.

    Args:
        audio_bytes:     Bytes del archivo de audio (webm, wav, mp3, etc.)
        expected_langs:  Lista de códigos ISO 639-1 esperados (ej. ["es", "en"]).
                         Si se provee y solo hay uno, se fuerza como idioma de entrada
                         para saltarse la detección automática y reducir la latencia.
                         Si hay más de uno, Whisper aún auto-detecta pero solo entre ellos.

    Returns:
        Tupla (texto_transcrito, idioma_detectado).

    Raises:
        ValueError: Si Whisper no detecta voz válida en el audio.
    """
    model = _get_model()

    with tempfile.NamedTemporaryFile(suffix=".webm", delete=False) as tmp_file:
        tmp_file.write(audio_bytes)
        tmp_path = tmp_file.name

    try:
        # Optimizar: si solo hay 2 idiomas posibles, Whisper fuerza la detección
        # entre ellos en vez de evaluar los 100+ idiomas del modelo.
        lang_hint = None
        if expected_langs and len(expected_langs) == 1:
            # Un solo idioma: forzamos, el más rápido posible
            lang_hint = expected_langs[0]
        # Si son 2, dejamos auto-detect (Whisper igual es rápido con el resto optimizado)

        segments, info = model.transcribe(
            tmp_path,
            language=lang_hint,
            beam_size=1,
            vad_filter=True,
            vad_parameters=dict(min_silence_duration_ms=500),
            condition_on_previous_text=False
        )

        detected_lang = info.language   # "es", "en", etc.
        
        # Fallback: si detecta un idioma no esperado (ej. "ja" o "ur"), forzamos el principal
        if not lang_hint and expected_langs and len(expected_langs) >= 2:
            if detected_lang not in expected_langs:
                print(f"[Whisper] Detectó {detected_lang}, pero se esperaba {expected_langs}. Forzando {expected_langs[0]}...")
                segments, info = model.transcribe(
                    tmp_path,
                    language=expected_langs[0],
                    beam_size=1,
                    vad_filter=True,
                    vad_parameters=dict(min_silence_duration_ms=500),
                    condition_on_previous_text=False
                )
                detected_lang = expected_langs[0]

        text = " ".join([seg.text for seg in segments]).strip()

        if not text:
            raise ValueError("Whisper no detectó voz en el audio.")

        print(f"[Whisper] Idioma detectado: {detected_lang!r} | Texto: {text!r}")
        return text, detected_lang

    finally:
        os.unlink(tmp_path)
