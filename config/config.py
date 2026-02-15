import os
from dotenv import load_dotenv
load_dotenv()
SEARCH_SERVICE = os.getenv("AZURE_DOCUMENT_INTELLIGENCE_ENDPOINT")
print("Setup complete:", SEARCH_SERVICE)  # Run: python -c "import config"
