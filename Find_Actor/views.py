from django.shortcuts import render, HttpResponse, redirect
from django.http import JsonResponse
from django.conf import settings
from .forms import Actors_submit, Actors_choice
from find_actors import Find_Actor_BFS
from time import time_ns
from .models import Movies, Ratings, Actors, Akas, Cast
from .utils import format_results


def index(request):
    context = {}
    if request.method == 'POST':
        form = Actors_submit(request.POST)
        if form.is_valid():
            start_list = form.cleaned_data['start_list']
            goal_list = form.cleaned_data['goal_list']
            if start_list.count() == 1 and goal_list.count() == 1:
                start_id, goal_id = start_list.first().id, goal_list.first().id
                res = Find_Actor_BFS.run(start_id, goal_id, print_solution=True)
                request.session['results'] = res
                return redirect('results')
            
            start_list = [(actor.id, str(actor)) for actor in start_list]
            goal_list = [(actor.id, str(actor)) for actor in goal_list]
            choices = (start_list, goal_list)
            request.session['choices'] = choices
            return redirect('choices')
        else:
            pass
    else:
        context['form'] = Actors_submit()
    
    return render(request, 'index.html', context=context)

def choices(request):
    if not (choices:=request.session.get('choices', None)) and not request.method == 'POST':
        return redirect('index')
    start_list, goal_list = choices
    
    if request.method == 'POST':
        form = Actors_choice(request.POST, start_list=start_list, goal_list=goal_list)
        if form.is_valid():
            start_id = form.cleaned_data['start_actor']
            goal_id = form.cleaned_data['goal_actor']
            res = Find_Actor_BFS.run(start_id, goal_id, print_solution=True)
            request.session['results'] = res
            return redirect('results')
        else:
            # Form is invalid, print validation errors for each field
            for field, errors in form.errors.items():
                print(f"Field: {field}")
                for error in errors:
                    print(f"Error: {error}")
            return redirect('choices')
    else:
        form = Actors_choice(start_list=start_list, goal_list=goal_list)
        return render(request, 'choices.html', context={'form':form})

def results(request):
    if not (results:=request.session.get('results', None)):
        return redirect('index')
    
    data = format_results(results)
    
    return render(request, 'results.html', {'data':data})

def about(request):
    return render(request, 'about.html')