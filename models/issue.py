from sqlalchemy import create_engine, Column, Integer, String, UUID, MetaData, Table, inspect
from main import Base, session


class Issue(Base):
    __tablename__ = "issues"
    key = Column(UUID, primary_key=True)
    summary = Column(String)
    assignee = Column(String)
    reporter = Column(String)
    priority = Column(String)
    status = Column(String)
    resolution = Column(String)
    created = Column(String)
    closed = Column(String)
    due_date = Column(String)
    version = Column(String)
    storypoints = Column(Integer)

    def __repr__(self):
        return (f"<Issue(key={self.key}, summary={self.summary}, assignee={self.assignee}, reporter={self.reporter}, "
                f"priority={self.priority}, status={self.status}, resolution={self.resolution}, created={self.created},"
                f"closed={self.closed}, due_date={self.due_date}, version={self.version}, storypoints={self.storypoints})>"
                )
