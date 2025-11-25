from dotenv import load_dotenv
import os

load_dotenv()
print("DSN:", os.getenv("WEATHER_DB_DSN"))
