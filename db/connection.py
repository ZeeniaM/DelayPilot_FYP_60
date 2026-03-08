"""
PostgreSQL connection handling for the pipeline.
"""
import contextlib
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from config import get_connection_string

# Engine is created once and reused
_engine = None

def get_engine():
    """Get or create the SQLAlchemy engine."""
    global _engine
    if _engine is None:
        _engine = create_engine(
            get_connection_string(),
            pool_pre_ping=True,
            echo=False,  # Set True to log SQL
        )
    return _engine

@contextlib.contextmanager
def get_session():
    """Context manager for database sessions. Use: with get_session() as session:"""
    engine = get_engine()
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
