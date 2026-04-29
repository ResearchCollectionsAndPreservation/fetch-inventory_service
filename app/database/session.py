from sqlmodel import create_engine, Session
from sqlalchemy.orm import sessionmaker
from sqlmodel import text
from fastapi import Request

from contextlib import contextmanager
from app.config.config import get_settings

engine = create_engine(
    get_settings().DATABASE_URL, echo=get_settings().ENABLE_ORM_SQL_LOGGING
)

data_migration_engine = create_engine(
    get_settings().DATABASE_URL,
    echo=get_settings().ENABLE_ORM_SQL_LOGGING,
    pool_size=20,       # Increase the pool size
    max_overflow=20,    # Allow more overflow connections if needed
    pool_timeout=30,    # Timeout before raising an exception if no connections are available
)

sa_hybrid_session_local = sessionmaker(autocommit=False, autoflush=False, bind=data_migration_engine)

# ======================================================================
# ============ START: ADD NEW SESSION FACTORY FOR APP ==================
# ======================================================================
# This is a new session factory specifically for the main application engine.
# We will import and use THIS in our background task.
AppSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine, class_=Session)
# ======================================================================
# ============= END: ADD NEW SESSION FACTORY FOR APP ===================
# ======================================================================


#v1.02
from typing import Generator
def get_session() -> Generator[Session, None, None]:
    with Session(engine, autoflush=False) as session:
        with session.no_autoflush:
            try:
                yield session
            except Exception:
                session.rollback()
                raise

# ... (All of your other functions below this line remain completely unchanged) ...

def get_sqlalchemy_session():
    """
    Hybrid session that allows SQLAlchemy to register
    SQLModel base classes. This is used in data seeding
    to harness lower level power of SQLAlchemy without
    us having to do a refactor.
    """
    db = sa_hybrid_session_local()
    try:
        yield db
    finally:
        db.close()


def get_sqlalchemy_session_thread_safe():
    """Generic Thread Safe"""
    return sa_hybrid_session_local()


def get_sqlalchemy_session_for_item_migration():
    """ Thread safe version """
    return sa_hybrid_session_local()


def get_sqlalchemy_session_for_storage_migration():
    """ Thread safe version """
    return sa_hybrid_session_local()


@contextmanager
def session_manager():
    """
    For use when a generator is not valid
    Context manager for database sessions.
    Ensures sessions are properly scoped and closed.
    """
    session = Session(engine, autoflush=False)  # Disabling autoflush to prevent unintended mutations
    try:
        yield session
        # our commits are called explicitly on purpose
        # session.commit()  # Commit only if everything is successful
    except Exception:
        session.rollback()  # Rollback in case of error
        raise
    finally:
        session.close()  # Always close the session


def commit_record(session, record):
    audit_info = getattr(session, "audit_info", {"name": "System", "id": "0"})
    session.add(record)
    session.commit()
    session.refresh(record)
    from app.utilities import start_session_with_audit_info
    start_session_with_audit_info(audit_info, session)
    return record


def bulk_commit_records(session, records):
    audit_info = getattr(session, "audit_info", {"name": "System", "id": "0"})
    session.bulk_save_objects(records)
    session.commit()
    # session.refresh(records) # session.refresh() does not work on a list
    from app.utilities import start_session_with_audit_info
    start_session_with_audit_info(audit_info, session)
    return records


def remove_record(session, record):
    session.delete(record)
    session.commit()