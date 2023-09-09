from database.database import get_session
from database.models import Cast
from queue import Queue
from sqlalchemy import select
from typing_extensions import Self
import time


class Node:
    def __init__(
        self,
        state: str,
        parent: Self | None = None,
        common_movie: str | None = None,
        layer: int = 0,
    ) -> None:
        """Init of Node class

        Args:
            state (str): current actor id
            parent (Node; | None): A parent Node
            common_movie (str | None): Movie where current and parent stared in
            layer (int): Degree of Kevin Bacon's Law
        """
        self.actor = state
        self.parent = parent
        self.common_movie = common_movie
        self.layer = layer


class BFS:
    def __init__(self, start_id: str, goal_id: str) -> None:
        """Init of BFS class

        Args:
            start_id (str): Id of start actor
            goal_id (str): Id of goal actor
        """
        self.session = get_session()
        self.start_id = start_id
        self.goal_id = goal_id
        self.max_layer = 6
        self.solution = None
        self.current_layer = -1
        self.explored_movies = set()

    def get_neighbors(self, actor_id: str):
        """Getting all actors who worked with current actor

        Args:
            actor_id (str): current actor id

        Returns:
            Iterable: Returns pairs of common movie and neighbor actor
        """
        subquery = select(Cast.movie_id).where(Cast.actor_id == actor_id).alias()

        query = select(Cast.movie_id, Cast.actor_id).where(
            Cast.movie_id.in_(select(subquery.c.movie_id)) & (Cast.actor_id != actor_id)
        )

        return self.session.execute(query).all()

    def find_actor(self):
        """Function with implemented algorithm to search for shortest path between 2 actors

        Raises:
            Exception: When two actors can't find the way in 6 steps
            Exception: When all actors where searched

        Returns:
            List: List of pairs (movie, actors) where movie is common movie and actor is person that worked with previous; 
            In first tuple movie is empty string
        """
        self.explored_movies = set()
        start = Node(self.start_id)
        marked = set(self.start_id)
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
                self.solution = [(movie, actor) for movie, actor in zip(movies, actors)]
                self.solution.insert(0, ("", self.start_id))
                self.runtime = (time.time_ns() - start_time) / 1e9
                return self.solution

            before_neigh = time.time_ns()
            for movie, actor in self.get_neighbors(node.actor):
                if actor in marked:
                    continue
                marked.add(actor)
                layer = node.layer + 1
                child = Node(actor, node, movie, layer)
                if actor == self.goal_id:
                    frontier = Queue()
                    frontier.put(child)
                    break
                frontier.put(child)

            print("\r" + " " * 60, end="", flush=True)
            now = time.time_ns()
            print(
                f"\rLoop Time: {(now-loop_time)/1e9:.3f} Neighbor Time: {(now-before_neigh)/1e9:.3f} Explored: , In Q: {frontier.qsize()}, Layer: {node.layer}",
                end="",
                flush=True,
            )

    def print_solution(self):
        """Prints solution and runtime of function
        """
        print()
        print(f"{self.runtime:.3f}")
        for movie, actor in self.solution:
            print(movie, actor)


def main():
    obj = BFS("nm1500155", "nm3053338")
    obj.find_actor()
    obj.print_solution()
    return


if __name__ == "__main__":
    main()
