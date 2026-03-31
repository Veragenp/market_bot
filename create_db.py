import sys
import os
sys.path.insert(0, os.path.dirname(__file__))
from trading_bot.data.schema import init_db, run_migrations

if __name__ == "__main__":
    init_db()
    run_migrations()
    print("Database created successfully.")