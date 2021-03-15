import datetime as dt
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

from sqlalchemy import Column, Integer, String, ForeignKey, Table, DateTime, Boolean

Base = declarative_base()


class IdMixin:
    id = Column(Integer, primary_key=True, autoincrement=True)


class NameMixin:
    name = Column(String, nullable=False)


class UrlMixin:
    url = Column(String, nullable=False, unique=True)


tag_post = Table(
    'tag_post',
    Base.metadata,
    Column("post_id", Integer, ForeignKey("post.id")),
    Column("tag_id", Integer, ForeignKey("tag.id"))
)


class Post(Base, IdMixin, UrlMixin):
    __tablename__ = "post"
    title = Column(String, nullable=False)
    author_id = Column(Integer, ForeignKey("author.id"))
    image = Column(String)
    publication_date = Column(DateTime)
    author = relationship("Author")
    tags = relationship("Tag", secondary=tag_post)
    comments = relationship("Comment")


class Author(Base, IdMixin, UrlMixin, NameMixin):
    __tablename__ = "author"
    posts = relationship('Post')


class Tag(Base, IdMixin,NameMixin, UrlMixin):
    __tablename__ = "tag"
    posts = relationship("Post", secondary=tag_post)


class Comment(Base, IdMixin):
    __tablename__ = "comment"
    comment_id = Column(Integer, nullable=False)
    parent_id = Column(Integer)
    author = Column(String, nullable=False)
    body = Column(String, nullable=False)
    post_id = Column(Integer, ForeignKey("post.id"))
