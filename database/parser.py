import pandas as pd
import numpy as np
import os
import functools
import math
import multiprocessing as mp
from multiprocessing.managers import BaseManager
from database import DATABASE_URL
from models import Movies
from typing import Iterable, Callable
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import time


def print_status(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        message = f'{kwargs.get("file")}: Parsing data from file'
        print(message, flush=True)
        result = func(*args, **kwargs)
        print(f'{kwargs.get("file")}: Data parsed from file')
        return result

    return wrapper


class Parser:
    class CustomManager(BaseManager):
        pass

    def __init__(self, db_url) -> None:
        self.db_url = db_url
        self.data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
        self.CustomManager.register("set", set)
        self.title_filter = set()
        self.name_filter = set()

    def chunk_loader(
        self, file: str, cols: Iterable | None = None, chunksize: int = 1000
    ):
        for chunk in pd.read_csv(
            os.path.join(self.data_dir, file),
            delimiter="\t",
            chunksize=1000,
            na_values="\\N",
            usecols=cols,
        ):
            yield chunk

    def connect_db(self):
        engine = create_engine(self.db_url)
        Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        return engine, Session()

    def load_to_db(self, chunk: pd.DataFrame, table: str):
        engine, session = self.connect_db()
        chunk.to_sql(table, engine, if_exists="append", index=False)
        session.commit()
        session.close()

    def get_movies(self):
        _, session = self.connect_db()
        return set(map(lambda x: x[0], session.query(Movies.id).all()))
    
    @print_status
    def multi_process(
        self,
        func: Callable,
        file: str,
        chunksize: int = 1000,
        num_processes: int = mp.cpu_count(),
        cols: Iterable | None = None,
    ):
        start = time.time_ns()
        # file_path = os.path.join(self.data_dir, file)
        # count = 0

        # with open(file_path, encoding="UTF-8") as f:
        #     num_records = sum(1 for _ in f) - 1

        # chunksize *= num_processes
        

        # with mp.Pool(processes=num_processes) as pool:
        #     for chunk in self.chunk_loader(file, cols, chunksize):
        #         message = f"Processing chunk: {count+1}/{math.ceil(num_records/chunksize)}"
        #         print("\r" + " " * len(message), end="", flush=True)
        #         print("\r", message, end="", flush=True)
        #         pool.map(func, np.array_split(chunk, num_processes))
        #         count += 1
        #     else:
        #         print()
        #print(f'\nRuntime of function {__name__}: {(time.time_ns() - start)/10e9:.3f}')
        start = time.time_ns()
        with mp.Pool(processes=num_processes) as pool:
            pool.map(func, self.chunk_loader(file, cols, chunksize), num_processes)
        print(f'\nRuntime of function {__name__}: {(time.time_ns() - start)/10e9:.3f}')
    # -----------------------------------------
    # LOADING FUNCTIONS
    # -----------------------------------------
    def load_title_basics(self, chunk: pd.DataFrame):
        column_mapping = {
            "tconst": "id",
            "originalTitle": "original_title",
            "startYear": "start_year",
        }

        chunk_copy = chunk.rename(columns=column_mapping)
        movie_filter = (
            (chunk_copy["titleType"] == "movie")
            & (chunk_copy["start_year"] >= 1945)
            & (chunk_copy["start_year"] <= 2023)
        )
        chunk_copy = chunk_copy[movie_filter]
        chunk_copy.drop(columns=["titleType"], inplace=True)
        chunk_copy.dropna(how="any", inplace=True)
        if not chunk_copy.empty:
            self.title_filter.update(chunk_copy["id"])
            self.load_to_db(chunk_copy, "movies")

    def load_title_akas(self, chunk: pd.DataFrame):
        column_mapping = {"titleId": "movie_id"}

    def load_title_principals(self, chunk: pd.DataFrame):
        column_mapping = {
            "tconst": "movie_id",
            "nconst": "actor_id",
        }

    def load_name_basics(self, chunk: pd.DataFrame):
        column_mapping = {
            "nconst": "id",
            "primaryName": "name",
            "birthYear": "birth_year",
            "deathYear": "death_year",
        }

    def load_ratings(self, chunk: pd.DataFrame):
        column_mapping = {
            "tconst": "movie_id",
            "averageRating": "average",
            "numVotes": "num_votes",
        }
        chunk_copy = chunk.rename(columns=column_mapping)
        chunk_copy.dropna(how="any", inplace=True)
        ratings_filter = chunk_copy["movie_id"].isin(self.title_filter)
        chunk_copy = chunk_copy[ratings_filter]
        print('\r', ''*30, end='', flush=True)
        print(f'\rLoading records number: {chunk_copy.shape[0]}', end='', flush=True)
        if not chunk_copy.empty:
            self.load_to_db(chunk_copy, "ratings")


        
    def run(self):
        errors = []
        name_flag = True
        title_flag = True
        with self.CustomManager() as manager:
            self.title_filter = manager.set()
            self.name_filter = manager.set()

            # func = self.load_title_basics
            # file = "title_basics.tsv"
            # try:
            #     self.multi_process(
            #         func=func,
            #         file=file,
            #         cols=["tconst", "titleType", "originalTitle", "startYear"],
            #     )
            #     title_flag = False
            #     self.flag = True
            # except Exception as e:
            #     errors.append((func.__name__, file, e))
            #     print(f"\n{file}: Failed to parse from file")
            self.title_filter = self.title_filter._getvalue()
            if title_flag:
                self.title_filter = self.get_movies()
                if not self.title_filter:
                    errors_str = "\n\n".join(
                        [
                            f"Function: {func} operating on file {file} raised error:\n{error}"
                            for func, file, error in errors
                        ]
                    )
                    raise Exception(
                        f"\nFirst must be loaded the title_basics file\n{errors_str}"
                    )
                print("Downloading ids of movies")
                print(f'Downloaded {len(self.title_filter)} ids')

            # AKAS
            # func = self.load_title_akas
            # file = "title_akas.tsv"
            # try:
            #     self.multi_process(func=func, file=file)
            # except Exception as e:
            #     errors.append((func.__name__, file, e))
            #     print(f"\n{file}: Failed to parse from file")

            # CAST
            # func = self.load_title_principals
            # file = "title_principals.tsv"
            # try:
            #     self.multi_process(func=func, file=file)
            #     name_flag = False
            # except Exception as e:
            #     errors.append((func.__name__, file, e))
            #     print(f"\n{file}: Failed to parse from file")

            # RATINGS
            self.name_filter = self.name_filter._getvalue()
            func = self.load_ratings
            file = "title_ratings.tsv"
            try:
                self.multi_process(func=func, file=file)
            except Exception as e:
                errors.append((func.__name__, file, e))
                print(f"\n{file}: Failed to parse from file")

            # if name_flag:
            #     errors_str = "\n\n".join(
            #         [
            #             f"Function: {func} operating on file {file} raised error:\n{error}"
            #             for func, file, error in errors
            #         ]
            #     )
            #     raise Exception(
            #         f"\nFirst must be loaded the title_principals file\n{errors_str}"
            #     )

            # ACTORS
            # func = self.load_name_basics
            # file = "name_basics.tsv"
            # try:
            #     self.multi_process(func=func, file=file)
            # except Exception as e:
            #     errors.append((func.__name__, file, e))
            #     print(f"\n{file}: Failed to parse from file")

            if errors:
                errors_str = "\n\n".join(
                    [
                        f"Function: {func} operating on file {file} raised error:\n{error}"
                        for func, file, error in errors
                    ]
                )
                print(errors_str)

    @classmethod
    def parse(cls, db_url: str):
        try:
            instance = cls(db_url)
            instance.run()
            return instance
        except Exception as e:
            print(f"\nError occurred: {e}\n\nTrying to execute one more time.\n")
            try:
                instance.run()
            except Exception as e:
                print(f"\nError occurred again: {e}\n\nAborting parsing")


if __name__ == "__main__":
    p = Parser(DATABASE_URL)
    p.run()
    # Parser.parse(DATABASE_URL)
