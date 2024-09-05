from sqlalchemy import create_engine, Column, Integer, String, UUID, MetaData, Table, inspect
from main import session
from models.base import Base

class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True, autoincrement=True)
    firstname = Column(String(100))
    lastname = Column(String(100))
    email = Column(String(100), unique=True)

    def __repr__(self):
        return f"<User(id={self.id}, firstname={self.firstname}, lastname={self.lastname}, email={self.email})>"

    @classmethod
    def add(cls, firstname, lastname, email):
        new_user = User(
            firstname=firstname,
            lastname=lastname,
            email=email
        )
        session.add(new_user)
        session.commit()
