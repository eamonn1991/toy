from sqlalchemy import (
    create_engine, Column, Integer, String, DateTime, Boolean,
    ForeignKey, Table, MetaData, Text, UniqueConstraint, CheckConstraint
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
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
    name = Column(String(255), nullable=False)
    star_count = Column(Integer, default=0)
    updated_at = Column(DateTime, nullable=False)
    last_crawled_at = Column(DateTime, nullable=False) 

    issues = relationship("Issue", back_populates="repository")
    pull_requests = relationship("PullRequest", back_populates="repository")

class Issue(Base):
    __tablename__ = "issues"

    id = Column(String(255), primary_key=True)
    repository_id = Column(String(255), ForeignKey("repositories.id"), nullable=False)
    number = Column(Integer, nullable=False)
    title = Column(String(255), nullable=False)
    created_at = Column(DateTime, nullable=False)

    repository = relationship("Repository", back_populates="issues")
    comments = relationship("Comment", back_populates="issue", foreign_keys="Comment.issue_id")

    __table_args__ = (
        UniqueConstraint("repository_id", "number", name="uix_repo_issue_number"),
    )

class PullRequest(Base):
    __tablename__ = "pull_requests"

    id = Column(String(255), primary_key=True)
    repository_id = Column(String(255), ForeignKey("repositories.id"), nullable=False)
    number = Column(Integer, nullable=False)
    title = Column(String(255), nullable=False)
    created_at = Column(DateTime, nullable=False)

    repository = relationship("Repository", back_populates="pull_requests")
    comments = relationship("Comment", back_populates="pull_request", foreign_keys="Comment.pull_request_id")
    reviews = relationship("Review", back_populates="pull_request", foreign_keys="Review.pull_request_id")
    ci_checks = relationship("CIcheck", back_populates="pull_request", foreign_keys="CIcheck.pull_request_id")

    __table_args__ = (
        UniqueConstraint("repository_id", "number", name="uix_repo_pr_number"),
    )

class Comment(Base):
    __tablename__ = "comments"

    id = Column(String(255), primary_key=True)
    issue_id = Column(String(255), ForeignKey("issues.id"), nullable=True) # NOTE: either issue or pull request can be None
    pull_request_id = Column(String(255), ForeignKey("pull_requests.id"), nullable=True)
    body = Column(Text, nullable=False)
    created_at = Column(DateTime, nullable=False)

    issue = relationship("Issue", back_populates="comments")
    pull_request = relationship("PullRequest", back_populates="comments")

    __table_args__ = (
        CheckConstraint(
            'issue_id IS NOT NULL OR pull_request_id IS NOT NULL',
            name='check_comment_has_parent'
        ),
    )

class Review(Base):
    __tablename__ = "reviews"

    id = Column(String(255), primary_key=True)
    pull_request_id = Column(String(255), ForeignKey("pull_requests.id"), nullable=False)
    body = Column(Text, nullable=False)
    created_at = Column(DateTime, nullable=False)

    pull_request = relationship("PullRequest", back_populates="reviews")

class CIcheck(Base):
    __tablename__ = "ci_checks"

    id = Column(String(255), primary_key=True)
    pull_request_id = Column(String(255), ForeignKey("pull_requests.id"), nullable=False)
    name = Column(String(255), nullable=False)
    created_at = Column(DateTime, nullable=False)

    pull_request = relationship("PullRequest", back_populates="ci_checks")