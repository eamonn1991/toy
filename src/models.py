from sqlalchemy import (
    create_engine, Column, Integer, String
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from src.config import settings

Base = declarative_base()

# Initialize database engine
engine = create_engine(settings.database_url)
Session = sessionmaker(bind=engine)

def get_db():
    """Get a new database session"""
    db = Session()
    try:
        yield db
    finally:
        db.close()

def get_engine(fresh_settings=False):
    """Get SQLAlchemy engine"""
    return create_engine(settings.database_url, echo=False)

def get_session_maker(engine=None):
    """Get a session maker for the given engine or create a new one"""
    if engine is None:
        engine = get_engine()
    return sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db(fresh_settings=False):
    """Get a database session"""
    engine = get_engine()
    SessionLocal = get_session_maker(engine)
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def create_tables():
    """Create all database tables"""
    engine = get_engine()
    Base.metadata.create_all(bind=engine)

# Main table
class Repository(Base):
    __tablename__ = "repositories"

    id = Column(String(255), primary_key=True)
    star_count = Column(Integer, default=0)