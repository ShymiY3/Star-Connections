from database.database import get_session
from database.models import Movies, Actors, Cast
from queue import Queue
from sqlalchemy.orm import joinedload
from sqlalchemy import select, distinct, and_
import time


class Node:
    def __init__(self, state, parent, common_movie, layer) -> None:
        self.actor = state
        self.parent = parent
        self.common_movie = common_movie
        self.layer = layer


class BFS:
    def __init__(self, start_id, goal_id) -> None:
        self.session = get_session()
        self.start_id = start_id
        self.goal_id = goal_id
        self.max_layer = 6
        self.solution = None
        self.current_layer = -1
        self.explored_movies = set()

    def get_neighbors(self, actor_id):
    #     query = (
    #     select([Cast.movie_id, Cast.actor_id])
    #     .filter(Cast.movie_id.in_(
    #         select([Cast.movie_id])
    #         .filter(Cast.actor_id == actor_id)
    #     ))
    #     .filter(Cast.actor_id != actor_id)
    # )
    #     cast = {}
    #     results = self.session.execute(query).fetchall()
        
    #     for movie, actor in results:
    #         # if movie in self.explored_movies:
    #         #     continue
    #         if movie not in cast:
    #             cast[movie] = []
    #         cast[movie].append(actor)
        
    #     # self.explored_movies.update(cast.keys())
        
    #     return cast
           # Load all movies and their cast in a single query
        movies = (
        self.session.query(Movies)
        .filter(Movies.cast.any(actor_id=actor_id))
        .options(joinedload(Movies.cast).joinedload(Cast.actor))
        .all()
    )
        
        cast = {}
        for movie in movies:
            cast[movie.id] = [cast.actor.id for cast in movie.cast]
        return cast
        # subquery = (
        #         self.session.query(Cast.movie_id)
        #         .filter(Cast.actor_id == actor_id)
        #         .subquery()
        #     )

        # query = (
        #     self.session.query(Cast.movie_id, Cast.actor_id)
        #     .filter(
        #         and_(
        #             Cast.movie_id.in_(subquery),
        #             Cast.actor_id != actor_id
        #         )
        #     )
        # )

        # results = query.all()

        # cast = {}
        # for movie, actor in results:
        #     if movie not in cast:
        #         cast[movie] = []
        #     cast[movie].append(actor)      
        # return cast
    
    def find_actor(self):
        self.explored_movies = set()
        start = Node(self.start_id, None, None, 0)
        explored = set()
        frontier = Queue()
        frontier.put(start)
        start_time = time.time_ns()

        while True:
            if frontier.empty():
                raise Exception("No solution")
            loop_time = time.time_ns()
            node = frontier.get()

            if node.layer > self.current_layer:
                self.current_layer = node.layer
                print(f"\nQueue now on layer {node.layer}")

            if node.layer > self.max_layer:
                raise Exception("Can't connect to actor in 6 steps")

            if node.actor == self.goal_id:
                actors = []
                movies = []
                while node.parent is not None:
                    actors.append(node.actor)
                    movies.append(node.common_movie)
                    node = node.parent
                actors.reverse()
                movies.reverse()
                self.solution = {movie: actor for movie, actor in zip(movies, actors)}
                self.runtime = (time.time_ns() - start_time)/1e9 
                return

            explored.add(node.actor)
            before_neigh = time.time_ns()
            for movie, cast in self.get_neighbors(node.actor).items():
                if movie in self.explored_movies: continue
                for actor in cast:
                    if actor in explored:
                        # print(actor, explored)
                        continue
                    if any(actor == i for i in frontier.queue):
                        continue
                    # print('\r' + ' '*40, end='', flush=True)
                    # print(f'\rActor: {actor.name}', end='', flush=True)
                    layer = node.layer + 1
                    child = Node(actor, node, movie, layer)
                    frontier.put(child)
                self.explored_movies.add(movie)
            print("\r" + " " * 60, end="", flush=True)
            now = time.time_ns()
            print(
                f"\rLoop Time: {(now-loop_time)/1e9:.3f} Neighbor Time: {(now-before_neigh)/1e9:.3f} Explored: {len(explored)}, In Q: {frontier.qsize()}, Layer: {node.layer}",
                end="",
                flush=True,
            )
            # print(f'\rExplored: {len(explored)}, In Q: {frontier.qsize()}', end='', flush=True)

    def print_solution(self):
        print()
        print(f'{self.runtime:.3f}')
        for movie, actor in self.solution.items():
            print(movie, actor)


if __name__ == "__main__":
    obj = BFS("nm0331516", "nm3053338")
    obj.find_actor()
    obj.print_solution()
    