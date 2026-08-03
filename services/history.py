"""
Servicio de historial de conversaciones ÔÇö Nuevo.

Responsabilidad: persistir y recuperar el historial de traducciones de cada sesi├│n.

Arquitectura:
  ┬À Cach├® en memoria (dict) para acceso O(1) durante la sesi├│n activa.
  ┬À Persistencia en archivo JSONL (una l├¡nea JSON por entrada) para
    sobrevivir reinicios del servidor.
  ┬À Cada entrada tiene: id, timestamp, session_id, source_lang, target_lang,
    transcripcion, traduccion.
  ┬À Endpoints disponibles v├¡a main.py:
      GET  /history                ÔåÆ todas las entradas (paginado)
      GET  /history/{session_id}  ÔåÆ entradas de una sesi├│n espec├¡fica
      DELETE /history             ÔåÆ limpia el historial completo

Dise├▒o de concurrencia:
  ┬À asyncio.Lock para evitar escrituras simult├íneas al archivo.
  ┬À La cach├® en memoria se actualiza sin lock (las lecturas de dict son thread-safe
    en CPython gracias al GIL).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

# ÔöÇÔöÇÔöÇ Configuraci├│n ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ
_HISTORY_FILE = Path(os.getenv("HISTORY_FILE", "conversation_history.jsonl"))
_MAX_HISTORY_ENTRIES = int(os.getenv("MAX_HISTORY_ENTRIES", "500"))

# ÔöÇÔöÇÔöÇ Estado interno ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ
# Lista maestra de todas las entradas (m├ís reciente al final)
_entries: list[dict[str, Any]] = []
# ├ìndice por session_id para acceso O(1) por sesi├│n
_by_session: dict[str, list[dict[str, Any]]] = {}
# Lock para escrituras al archivo JSONL
_file_lock = asyncio.Lock()
_initialized = False


# ÔöÇÔöÇÔöÇ Inicializaci├│n ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ

def _load_history_sync() -> None:
    """Carga el historial desde disco al arrancar. Solo se llama una vez."""
    global _entries, _by_session, _initialized

    if not _HISTORY_FILE.exists():
        logger.info("[History] Archivo de historial no encontrado. Comenzando con historial vac├¡o.")
        _initialized = True
        return

    loaded = 0
    errors = 0
    with _HISTORY_FILE.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
                _entries.append(entry)
                sid = entry.get("session_id", "unknown")
                _by_session.setdefault(sid, []).append(entry)
                loaded += 1
            except json.JSONDecodeError:
                errors += 1
                logger.warning("[History] L├¡nea corrupta ignorada en historial.")

    # Mantener solo las ├║ltimas N entradas
    if len(_entries) > _MAX_HISTORY_ENTRIES:
        excess = len(_entries) - _MAX_HISTORY_ENTRIES
        _entries = _entries[excess:]
        # Reconstruir ├¡ndice por sesi├│n
        _by_session.clear()
        for entry in _entries:
            sid = entry.get("session_id", "unknown")
            _by_session.setdefault(sid, []).append(entry)

    logger.info(
        "[History] Historial cargado: %d entradas (%d errores ignorados).",
        loaded, errors,
    )
    _initialized = True


def ensure_initialized() -> None:
    """Garantiza que el historial est├í cargado. Idempotente."""
    if not _initialized:
        _load_history_sync()


# ÔöÇÔöÇÔöÇ API p├║blica ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇ

async def add_entry(
    session_id: str,
    source_lang: str,
    target_lang: str,
    transcripcion: str,
    traduccion: str,
) -> dict[str, Any]:
    """
    A├▒ade una nueva entrada al historial (memoria + disco).

    Args:
        session_id:   Identificador ├║nico de la sesi├│n WebSocket.
        source_lang:  C├│digo ISO 639-1 del idioma origen.
        target_lang:  C├│digo ISO 639-1 del idioma destino.
        transcripcion: Texto original transcrito.
        traduccion:   Texto traducido.

    Returns:
        La entrada creada (dict con todos sus campos).
    """
    ensure_initialized()

    entry: dict[str, Any] = {
        "id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "session_id": session_id,
        "source_lang": source_lang,
        "target_lang": target_lang,
        "transcripcion": transcripcion,
        "traduccion": traduccion,
    }

    # Actualizar cach├® en memoria
    _entries.append(entry)
    _by_session.setdefault(session_id, []).append(entry)

    # Rotar si superamos el m├íximo
    if len(_entries) > _MAX_HISTORY_ENTRIES:
        oldest = _entries.pop(0)
        old_sid = oldest.get("session_id", "unknown")
        if old_sid in _by_session and oldest in _by_session[old_sid]:
            _by_session[old_sid].remove(oldest)

    # Persistir en disco de forma as├¡ncrona y segura
    async with _file_lock:
        try:
            with _HISTORY_FILE.open("a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except OSError as exc:
            logger.error("[History] No se pudo escribir al archivo de historial: %s", exc)

    logger.debug("[History] Entrada a├▒adida: session=%s, %rÔåÆ%r", session_id, transcripcion[:40], traduccion[:40])
    return entry


def get_all(
    page: int = 1,
    page_size: int = 50,
    session_id: Optional[str] = None,
) -> dict[str, Any]:
    """
    Devuelve el historial paginado, opcionalmente filtrado por session_id.

    Args:
        page:       P├ígina (1-indexed).
        page_size:  Entradas por p├ígina (m├íx. 100).
        session_id: Si se provee, filtra solo las entradas de esa sesi├│n.

    Returns:
        Dict con: items, total, page, page_size, pages.
    """
    ensure_initialized()

    page_size = min(page_size, 100)
    page = max(page, 1)

    source = _by_session.get(session_id, []) if session_id else _entries

    # M├ís reciente primero
    sorted_entries = list(reversed(source))
    total = len(sorted_entries)
    start = (page - 1) * page_size
    end = start + page_size
    items = sorted_entries[start:end]

    return {
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
        "pages": max(1, (total + page_size - 1) // page_size),
    }


def get_sessions() -> list[dict[str, Any]]:
    """
    Devuelve un resumen de todas las sesiones en el historial.

    Returns:
        Lista de dicts con: session_id, entries_count, first_seen, last_seen,
        langs (set de pares origenÔåÆdestino usados).
    """
    ensure_initialized()

    sessions = []
    for sid, entries in _by_session.items():
        if not entries:
            continue
        langs = list({f"{e['source_lang']}ÔåÆ{e['target_lang']}" for e in entries})
        sessions.append({
            "session_id": sid,
            "entries_count": len(entries),
            "first_seen": entries[0]["timestamp"],
            "last_seen": entries[-1]["timestamp"],
            "langs": langs,
        })

    # Ordenar por actividad m├ís reciente primero
    sessions.sort(key=lambda s: s["last_seen"], reverse=True)
    return sessions


async def clear_history() -> dict[str, int]:
    """
    Elimina todo el historial (memoria + disco).

    Returns:
        Dict con el n├║mero de entradas eliminadas.
    """
    global _entries, _by_session

    count = len(_entries)
    _entries = []
    _by_session = {}

    async with _file_lock:
        try:
            if _HISTORY_FILE.exists():
                _HISTORY_FILE.unlink()
            logger.info("[History] Historial eliminado (%d entradas).", count)
        except OSError as exc:
            logger.error("[History] No se pudo borrar el archivo de historial: %s", exc)

    return {"deleted": count}
