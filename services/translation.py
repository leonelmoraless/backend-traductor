"""
Servicio de traducción de texto rápido usando deep-translator.
"""

import time
from functools import lru_cache
from deep_translator import GoogleTranslator
from langdetect import detect

def detect_language(text: str) -> str:
    """Detecta el idioma de un texto."""
    text = text.strip()
    if not text:
        return ""
    try:
        return detect(text)
    except Exception as e:
        print(f"[Translation] Error detectando idioma: {e}")
        return ""

@lru_cache(maxsize=256)
def translate(text: str, source_lang: str, target_lang: str) -> str:
    """
    Traduce un texto de un idioma a otro de forma rápida.
    Añadido: timeouts, reintentos y caché LRU para no bloquear infinitamente.
    """
    text = text.strip()
    if not text:
        return ""
        
    if source_lang == target_lang:
        return text

    for attempt in range(3):
        try:
            # timeout de 8 segundos para evitar bloqueos infinitos de la red
            translator = GoogleTranslator(source=source_lang, target=target_lang, timeout=8)
            result = translator.translate(text)
            if result:
                return result.strip()
        except Exception as e:
            print(f"[Translation] Error con deep-translator (intento {attempt+1}): {e}")
            time.sleep(0.5)
            
    raise RuntimeError("La traducción falló después de varios intentos.")
