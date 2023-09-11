from itertools import pairwise
from .models import Cast, Movies
import json

def get_movie_info(movie_obj, language):
    akas = movie_obj.aka.filter(region = language).first()
    original_title = movie_obj.original_title
    start_year = movie_obj.start_year
    rating = movie_obj.rating.first()
    
    movie_info = ''
    if akas and not akas.title == original_title:
        movie_info += f'{akas.title} ({original_title})'
    else:
        movie_info += original_title
    if start_year:
        movie_info += f' ({start_year})'
    
    if rating:
       rating = rating.average
    else: rating = "-" 
    
    return movie_info, rating

def get_actor_info(actor_obj):
    actor_str = str(actor_obj.actor)
    characters = json.loads(actor_obj.characters)
    return actor_str, characters

def format_results(results, language:str = 'US'):
    items = []
    
    for (_, prev_actor), (movie, next_actor) in pairwise(results):
        prev_actor_obj = Cast.objects.get(actor_id = prev_actor, movie_id = movie)
        next_actor_obj = Cast.objects.get(actor_id = next_actor, movie_id = movie)
        movie_obj = Movies.objects.get(id = movie)
        
        movie_info, rating  = get_movie_info(movie_obj, language)
        prev_actor_info, prev_characters_info = get_actor_info(prev_actor_obj)
        next_actor_info, next_characters_info = get_actor_info(next_actor_obj)
        
        items.append(
            {
                'movie':movie_info,
                'rating':rating,
                'prev_actor':prev_actor_info,
                'next_actor':next_actor_info,
                'prev_characters':" ".join(prev_characters_info),
                'next_characters':" ".join(next_characters_info)
            }
        )    
    return items
            