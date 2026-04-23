from app.db import get_engine, Base
from app import create_app

app = create_app()
engine = get_engine()
Base.metadata.create_all(bind=engine)
print("DB tables created.")
