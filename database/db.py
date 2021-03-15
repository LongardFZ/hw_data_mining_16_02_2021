from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from . import models


class Database:
    def __init__(self, db_url):
        engine = create_engine(db_url)
        models.Base.metadata.create_all(bind=engine)  # создать бд
        self.maker = sessionmaker(bind=engine)

    def _get_or_create(self, session, model, uniq_field, uniq_value, **data):
        db_data = session.query(model).filter(uniq_field == data[uniq_value]).first()
        if not db_data:
            db_data = model(**data)
        return db_data

    def _get_or_create_comments(self, session, data: list) -> list:
        result = []
        for comment in data:
            #db_data = session.query(models.Comment).filter(models.Comment.comment_id == comment["comment_id"]).first()
            db_comment = self._get_or_create(session,
                                             models.Comment,               # session,
                                             models.Comment.comment_id,    #models.Comment,
                                             "comment_id",                 #comment["comment_id"],
                                             **comment                     #models.Comment.comment_id.key,
                                                                           #**comment
            )
            result.append(db_comment)
        return result

    def create_post(self, data):
        session = self.maker()
        comments = self._get_or_create_comments(session, data["comments_data"])
        author = self._get_or_create(session,
                                     models.Author,
                                     models.Author.url,
                                     'url',
                                     **data['author'])
        #post = models.Post(**data['post_data'], author=author)
        post = self._get_or_create(session,
                                   models.Post,
                                   models.Post.url,
                                   'url',
                                   **data['post_data'],
                                   author=author)
        post.tags.extend(map(
            lambda tag_data: self._get_or_create(
                session, models.Tag, models.Tag.url, "url", **tag_data
            ),
            data["tags"],
        ))
        post.comments.extend(comments)
        session.add(post)

        try:
            session.commit()
        except Exception as exc:
            print(exc)
            session.rollback()
        finally:
            session.close()
