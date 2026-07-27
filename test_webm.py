import base64
import os
import tempfile
from faster_whisper import WhisperModel

def test_incomplete_webm():
    # Creamos un modelo whisper
    model = WhisperModel("tiny", device="cpu", compute_type="int8")
    
    # Escribimos basura simulando un webm incompleto
    with tempfile.NamedTemporaryFile(suffix=".webm", delete=False) as f:
        f.write(b"\x1a\x45\xdf\xa3") # EBML header start
        f.write(b"random garbage data to simulate incomplete chunk")
        tmp = f.name
        
    try:
        segments, info = model.transcribe(tmp)
        print(list(segments))
    except Exception as e:
        print("ERROR_CAUGHT:", type(e).__name__, str(e))
        
test_incomplete_webm()
