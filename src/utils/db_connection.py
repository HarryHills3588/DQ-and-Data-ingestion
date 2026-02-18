from supabase import Client, create_client
import os 
from dotenv import load_dotenv

class DBConnection(Client):
    load_dotenv()
    SUPABASE_URL = str(os.getenv("SUPABASE_URL"))
    SUPABASE_KEY = str(os.getenv("SUPABASE_KEY"))
    
    client = create_client(supabase_url=SUPABASE_URL,supabase_key=SUPABASE_KEY)