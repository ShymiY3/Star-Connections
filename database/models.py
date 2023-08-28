from sqlalchemy import Column, Integer, String, ForeignKey, Float
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Movies(Base):
    # title.basics
    __tablename__ = "movies"

    id = Column(
        String, primary_key=True, nullable=False, index=True, autoincrement=False
    )  # tconst
    original_title = Column(String, nullable=False)  # originalTitle
    start_year = Column(Integer)  # startYear

    aka = relationship("Akas", back_populates="movie", cascade='all, delete-orphan')
    rating = relationship("Ratings", back_populates="movie", cascade='all, delete-orphan')
    cast = relationship("Cast", back_populates="movie", cascade='all, delete-orphan')


class Akas(Base):
    # title.akas
    __tablename__ = "akas"

    id = Column(Integer, primary_key=True, autoincrement=True)
    movie_id = Column(String, ForeignKey("movies.id", ondelete='CASCADE'), nullable=False)  # titleId
    title = Column(String, index=True, nullable=False)  # title
    region = Column(String, nullable=False)  # region

    movie = relationship("Movies", back_populates="aka")


class Actors(Base):
    __tablename__ = "actors"

    id = Column(String, index=True, primary_key=True, autoincrement=False)  # nconst
    name = Column(String, index=True, nullable=False)  # primaryName
    birth_year = Column(Integer, nullable=False)  # birthYear
    death_year = Column(Integer)  # deathYear

    cast = relationship("Cast", back_populates="actor", cascade='all, delete-orphan')


class Cast(Base):
    # title.principals
    __tablename__ = "cast"

    id = Column(Integer, index=True, primary_key=True, autoincrement=True)
    movie_id = Column(String, ForeignKey("movies.id", ondelete='CASCADE'), nullable=False)
    actor_id = Column(String, ForeignKey("actors.id", ondelete='CASCADE'), nullable=False)
    characters = Column(String)

    movie = relationship("Movies", back_populates="cast")
    actor = relationship("Actors", back_populates="cast")


class Ratings(Base):
    # title.ratings
    __tablename__ = "ratings"

    id = Column(Integer, index=True, primary_key=True, autoincrement=True)
    movie_id = Column(String, ForeignKey("movies.id", ondelete='CASCADE'), nullable=False)
    average = Column(Float, nullable=False)
    num_votes = Column(Integer, nullable=False)

    movie = relationship("Movies", back_populates="rating")
