from supabase import Client, create_client
import os 
from dotenv import load_dotenv

class DBConnection(Client):
    load_dotenv()
    SUPABASE_URL = str(os.getenv("SUPABASE_URL"))
    SUPABASE_KEY = str(os.getenv("SUPABASE_KEY"))

    @staticmethod
    def get_client():
        return create_client(
            supabase_url=DBConnection.SUPABASE_URL,
            supabase_key=DBConnection.SUPABASE_KEY
        )