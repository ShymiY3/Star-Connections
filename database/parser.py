import pandas as pd
import os
import functools
import math
import multiprocessing as mp
import time
import shutil
from sqlalchemy import create_engine
from multiprocessing.managers import BaseManager
from typing import Iterable, Callable
from database import DATABASE_URL


def init_pool_processes(the_lock):
    """Initialize each process with a global variable lock."""
    global lock
    lock = the_lock


def print_status(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time_ns()
        message = f'{kwargs.get("file")}: Transforming data from file'
        print(message, flush=True)
        result = func(*args, **kwargs)
        print(
            f"\nRuntime of function {func.__name__}: {(time.time_ns() - start)/1e9:.3f}"
        )
        print(f'{kwargs.get("file")}: Data transformed from file')
        return result

    return wrapper


class Counter(object):
    def __init__(self, initval=0):
        self.val = mp.Value("i", initval)
        self.lock = mp.Lock()

    def value(self):
        with self.lock:
            return self.val.value

    def increment(self):
        with self.lock:
            self.val.value += 1


class Parser:
    class CustomManager(BaseManager):
        pass

    def __init__(self, db_url) -> None:
        self.data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
        self.raw_dir = os.path.join(self.data_dir, "raw")
        self.transformed_dir = os.path.join(self.data_dir, "transformed")
        self.database_url = db_url
        if not os.path.exists(self.transformed_dir):
            os.mkdir(self.transformed_dir)
        self.CustomManager.register("set", set)
        self.CustomManager.register("Counter", Counter)
        self.title_filter = set()
        self.name_filter = set()
        self.current_chunk = 0
        self.num_records = 0

    def chunk_loader(
        self, file: str, cols: Iterable | None = None, chunksize: int = 1000
    ):
        for chunk in pd.read_csv(
            file,
            delimiter="\t",
            chunksize=chunksize,
            na_values="\\N",
            usecols=cols,
        ):
            yield chunk

    def load_to_db(
        self,
        file_path,
        table: str,
    ):
        try:
            with open(file_path, encoding="UTF-8") as file:
                conn = create_engine(self.database_url).raw_connection()
                cursor = conn.cursor()
                cmd = f"COPY {table}({file.readline()}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)"
                cursor.copy_expert(cmd, file)
                conn.commit()
        except Exception as e:
            raise f"Couldn't load data to table {table}. Error: {e}"
        finally:
            cursor.close()
            conn.close()

    def save_to_file(self, chunk: pd.DataFrame, table: str):
        file_path = os.path.join(self.transformed_dir, f"{table}.csv")

        lock.acquire()
        with open(file_path, "a", encoding="UTF-8") as f:
            if f.tell() == 0:
                chunk.to_csv(f, header=True, index=False, lineterminator="\n")
                lock.release()
                return
            chunk.to_csv(f, header=False, index=False, lineterminator="\n")
        lock.release()

    def print_chunk_info(self, size):
        self.current_chunk.increment()
        message = f"Processing chunk: {self.current_chunk.value()}/{math.ceil(self.num_records/size)}"
        print("\r" + " " * len(message), end="", flush=True)
        print(f"\r{message}" + " " * 6, end="", flush=True)

    @print_status
    def multi_process_transform(
        self,
        func: Callable,
        file: str,
        chunksize: int = 1000,
        num_processes: int = mp.cpu_count(),
        cols: Iterable | None = None,
    ):
        file_path = os.path.join(self.raw_dir, file)

        with open(file_path, encoding="UTF-8") as f:
            self.num_records = sum(1 for _ in f)
        lock = mp.Lock()
        with mp.Pool(
            processes=num_processes, initializer=init_pool_processes, initargs=(lock,)
        ) as pool:
            pool.map(func, self.chunk_loader(file_path, cols, chunksize))

    def load_all(self, ignore_files: Iterable = []):
        transformed_files = [
            # "movies.csv",
            # "actors.csv",
            # "cast.csv",
            "akas.csv",
            "ratings.csv",
        ]

        files = filter(lambda x: x not in ignore_files, transformed_files)

        for file in files:
            print(f"Starting loading data {file}")
            start_time = time.time_ns()
            try:
                self.load_to_db(
                    os.path.join(self.transformed_dir, file), file.rsplit(".csv", 1)[0]
                )
            except Exception as e:
                print(e)
            else:
                print("End of loading data")
                print(
                    f"Data {file} loaded. Runtime: {(time.time_ns() - start_time)/1e9:.3f}"
                )

    # -----------------------------------------
    # Transforming FUNCTIONS
    # -----------------------------------------
    def transform_title_basics(
        self,
        chunk: pd.DataFrame,
    ):
        column_mapping = {
            "tconst": "id",
            "originalTitle": "original_title",
            "startYear": "start_year",
        }
        self.print_chunk_info(chunk.shape[0])
        chunk_copy = chunk.rename(columns=column_mapping)
        movie_filter = (
            (chunk_copy["titleType"] == "movie")
            & (chunk_copy["start_year"] >= 1945)
            & (chunk_copy["start_year"] <= 2023)
        )
        chunk_copy = chunk_copy[movie_filter]
        chunk_copy.drop(columns=["titleType"], inplace=True)
        chunk_copy.dropna(how="any", inplace=True)
        chunk_copy["start_year"] = chunk_copy["start_year"].astype(int)
        if not chunk_copy.empty:
            self.title_filter.update(chunk_copy["id"])
            self.save_to_file(chunk_copy, "movies")

    def transform_title_akas(self, chunk: pd.DataFrame):
        column_mapping = {"titleId": "movie_id"}

        self.print_chunk_info(chunk.shape[0])
        chunk_copy = chunk.rename(columns=column_mapping)
        chunk_copy.dropna(how="any", inplace=True)
        akas_filter = (chunk_copy["movie_id"].isin(self.title_filter)) & (
            chunk_copy["region"].isin(("PL", "US"))
        )
        chunk_copy = chunk_copy[akas_filter]
        if not chunk_copy.empty:
            self.save_to_file(chunk_copy, "akas")

    def transform_title_principals(self, chunk: pd.DataFrame):
        column_mapping = {
            "tconst": "movie_id",
            "nconst": "actor_id",
        }
        self.print_chunk_info(chunk.shape[0])
        chunk_copy = chunk.rename(columns=column_mapping)
        cols_without_na = [col for col in chunk_copy.columns if col != "characters"]
        cast_filter = (chunk_copy["movie_id"].isin(self.title_filter)) & (
            chunk_copy["category"].isin(("actor", "actress"))
        )
        chunk_copy = chunk_copy[cast_filter]
        chunk_copy.dropna(how="any", subset=cols_without_na, inplace=True)
        chunk_copy.drop(columns=["category"], inplace=True)

        if not chunk_copy.empty:
            self.name_filter.update(chunk_copy["actor_id"])
            self.save_to_file(chunk_copy, "cast")

    def transform_name_basics(self, chunk: pd.DataFrame):
        column_mapping = {
            "nconst": "id",
            "primaryName": "name",
            "birthYear": "birth_year",
            "deathYear": "death_year",
        }
        self.print_chunk_info(chunk.shape[0])
        chunk_copy = chunk.rename(columns=column_mapping)
        cols_without_na = [col for col in chunk_copy.columns if col != "death_year"]
        chunk_copy.dropna(how="any", subset=cols_without_na, inplace=True)
        ratings_filter = chunk_copy["id"].isin(self.name_filter)
        chunk_copy = chunk_copy[ratings_filter]
        chunk_copy[["birth_year", "death_year"]] = chunk_copy[
            ["birth_year", "death_year"]
        ].astype(int)

        if not chunk_copy.empty:
            self.save_to_file(chunk_copy, "actors")

    def transform_ratings(self, chunk: pd.DataFrame):
        column_mapping = {
            "tconst": "movie_id",
            "averageRating": "average",
            "numVotes": "num_votes",
        }
        self.print_chunk_info(chunk.shape[0])
        chunk_copy = chunk.rename(columns=column_mapping)
        chunk_copy.dropna(how="any", inplace=True)
        ratings_filter = chunk_copy["movie_id"].isin(self.title_filter)
        chunk_copy = chunk_copy[ratings_filter]
        if not chunk_copy.empty:
            self.save_to_file(chunk_copy, "ratings")

    def run(
        self, load_db=False, delete_raw=False, delete_transformed=False, transform=True
    ):
        errors = []
        name_flag = False
        title_flag = False
        if transform:
            with self.CustomManager() as manager:
                self.title_filter = manager.set()
                self.name_filter = manager.set()

                # MOVIES
                self.current_chunk = manager.Counter(0)
                func = self.transform_title_basics
                file = "title_basics.tsv"
                try:
                    self.multi_process_transform(
                        func=func,
                        file=file,
                        cols=["tconst", "titleType", "originalTitle", "startYear"],
                        chunksize=5000,
                    )
                    self.title_filter = self.title_filter._getvalue()
                    if self.title_filter:
                        with open(
                            os.path.join(self.data_dir, "title_filter.csv"),
                            "w",
                            encoding="utf-8",
                        ) as f:
                            f.write(",".join(self.title_filter))
                    title_flag = True
                except Exception as e:
                    errors.append((func.__name__, file, e))
                    print(f"\n{file}: Failed to parse from file")

                if not title_flag:
                    self.title_filter = set()
                    filter_path = os.path.join(self.data_dir, "title_filter.csv")
                    if os.path.exists(filter_path):
                        with open(filter_path, encoding="UTF-8") as f:
                            self.title_filter.update(f.readline().split(","))
                        print("Downloading ids of movies")
                        print(f"Downloaded {len(self.title_filter)} ids")
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

                # AKAS
                self.current_chunk = manager.Counter(0)
                func = self.transform_title_akas
                file = "title_akas.tsv"
                try:
                    self.multi_process_transform(
                        func=func,
                        file=file,
                        cols=["titleId", "title", "region"],
                        chunksize=5000,
                    )
                except Exception as e:
                    errors.append((func.__name__, file, e))
                    print(f"\n{file}: Failed to parse from file")

                # CAST
                self.current_chunk = manager.Counter(0)
                func = self.transform_title_principals
                file = "title_principals.tsv"
                try:
                    self.multi_process_transform(
                        func=func,
                        file=file,
                        cols=["tconst", "nconst", "category", "characters"],
                        chunksize=5000,
                    )
                    if self.title_filter:
                        with open(
                            os.path.join(self.data_dir, "name_filter.csv"),
                            "w",
                            encoding="utf-8",
                        ) as f:
                            f.write(",".join(self.name_filter))
                    name_flag = True
                except Exception as e:
                    errors.append((func.__name__, file, e))
                    print(f"\n{file}: Failed to parse from file")

                # RATINGS
                self.name_filter = self.name_filter._getvalue()
                self.current_chunk = manager.Counter(0)
                func = self.transform_ratings
                file = "title_ratings.tsv"
                try:
                    self.multi_process_transform(func=func, file=file, chunksize=500)
                except Exception as e:
                    errors.append((func.__name__, file, e))
                    print(f"\n{file}: Failed to parse from file")

                if not name_flag:
                    self.name_filter = set()
                    filter_path = os.path.join(self.data_dir, "name_filter.csv")
                    if os.path.exists(filter_path):
                        with open(filter_path, encoding="UTF-8") as f:
                            self.name_filter.update(f.readline().split(","))
                        print("Downloading ids of actors")
                        print(f"Downloaded {len(self.name_filter)} ids")
                    if not self.name_filter:
                        errors_str = "\n\n".join(
                            [
                                f"Function: {func} operating on file {file} raised error:\n{error}"
                                for func, file, error in errors
                            ]
                        )
                        raise Exception(
                            f"\nFirst must be loaded the title_principals file\n{errors_str}"
                        )

                # ACTORS
                self.current_chunk = manager.Counter(0)
                func = self.transform_name_basics
                file = "name_basics.tsv"
                try:
                    self.multi_process_transform(
                        func=func,
                        file=file,
                        cols=["nconst", "primaryName", "birthYear", "deathYear"],
                        chunksize=5000,
                    )
                except Exception as e:
                    errors.append((func.__name__, file, e))
                    print(f"\n{file}: Failed to parse from file")

                if errors:
                    errors_str = "\n\n".join(
                        [
                            f"Function: {func} operating on file {file} raised error:\n{error}"
                            for func, file, error in errors
                        ]
                    )
                    print(errors_str)

        if load_db:
            if not os.listdir(self.transformed_dir):
                print("No data to transform")
            else:
                self.load_all()

        if delete_raw:
            try:
                shutil.rmtree(self.raw_dir)
            except Exception as e:
                print(f"Could't remove raw data. Error: {e}")
            else:
                print("Raw data deleted successfully")

        if delete_transformed:
            try:
                shutil.rmtree(self.transformed_dir)
            except Exception as e:
                print(f"Could't remove transformed data. Error: {e}")
            else:
                print("Transformed data deleted successfully")


def load_arg_parser():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-l", "--load_db", action="store_true", help="Enable loading the database."
    )

    parser.add_argument(
        "-dr", "--delete_raw", action="store_true", help="Enable deleting raw data."
    )

    parser.add_argument(
        "-dt" "--delete_transformed",
        action="store_true",
        help="Enable deleting transformed data.",
    )

    parser.add_argument(
        "-nt",
        "--not_transform",
        action="store_true",
        help="Disable data transformation",
    )
    return parser


if __name__ == "__main__":
    arg_parser = load_arg_parser()
    p = Parser(DATABASE_URL)
    # p.run(load_db=False, delete_raw=False, delete_transformed=False, transform=True)
    p.run(
        load_db=arg_parser.load_db,
        delete_raw=arg_parser.delete_raw,
        delete_transformed=arg_parser.delete_transformed,
        transform= not (arg_parser.not_transform),
    )
