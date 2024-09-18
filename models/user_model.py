from sqlalchemy import Column, String, DateTime, Boolean
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base
import datetime
import uuid
from sqlalchemy import ForeignKey, Integer, Table


Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    username = Column(String, unique=True)
    first_name = Column(String)
    last_name = Column(String)
    password = Column(String)
    account_created = Column(DateTime, default=datetime.datetime.now)
    account_updated = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
    verified = Column(Boolean, default=False)

    def __init__(self, **kwargs):
        super(User, self).__init__(**kwargs)  # This ensures proper initialization by Base

    #Getter for user attributes except password
    def get_user_dict(self):
        return {
            "id": self.id,
            "username": self.username,
            "first_name": self.first_name,
            "last_name": self.last_name,
            "account_created": self.account_created,
            "account_updated": self.account_updated, 
            "account_verified": self.verified
        }
    
class EmailTracker(Base):
    __tablename__ = 'email_tracker'
    verification_token = Column(String, primary_key=True)
    email = Column(String)
    expire_time = Column(DateTime)

class Like(Base):
    __tablename__ = 'likes'
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    username = Column(String, ForeignKey('users.username'))
    content_id = Column(String)
    content_type = Column(String)  # 'movie' or 'tv_show'
   