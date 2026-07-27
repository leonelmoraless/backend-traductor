import google.generativeai as genai
import os

from dotenv import load_dotenv
load_dotenv()
api_key = os.getenv("GEMINI_API_KEY")
genai.configure(api_key=api_key)
model = genai.GenerativeModel('gemini-1.5-flash')

try:
    response = model.generate_content("Hello, how are you?")
    print("SUCCESS:", response.text)
except Exception as e:
    print("ERROR:", str(e))
