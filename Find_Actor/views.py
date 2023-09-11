from django.shortcuts import render, HttpResponse, redirect
from django.http import JsonResponse
from django.conf import settings
from .forms import Actors_submit, Actors_choice
from find_actors import Find_Actor_BFS
from time import time_ns

def format_results(results):
    formatted = []
    _, start = results.pop(0)
    formatted.append(start)
    for movie, actor in results:
        formatted.append(movie)
        formatted.append(actor)
    return formatted


def index(request):
    context = {}
    if request.method == 'POST':
        form = Actors_submit(request.POST)
        if form.is_valid():
            start_list = form.cleaned_data['start_list']
            goal_list = form.cleaned_data['goal_list']
            if start_list.count() == 1 and goal_list.count() == 1:
                start_id, goal_id = start_list.first().id, goal_list.first().id
                res = Find_Actor_BFS.run(start_id, goal_id)
                request.session['results'] = format_results(res)
                return redirect('results')
            
            start_list = tuple((actor.id, str(actor)) for actor in start_list)
            goal_list = tuple((actor.id, str(actor)) for actor in goal_list)
            choices = (start_list, goal_list)
            request.session['choices'] = choices
            return redirect('choices')
        else:
            pass
    else:
        context['form'] = Actors_submit()
    
    return render(request, 'index.html', context=context)

def choices(request):
    if not (choices:=request.session.get('choices', None)):
        return redirect('index')
    
    if request.method == 'POST':
        form = Actors_choice(request.POST)
        if form.is_valid():
            start_id = form.cleaned_data['start_actor']
            goal_id = form.cleaned_data['goal_actor']
            res = Find_Actor_BFS.run(start_id, goal_id)
            request.session['results'] = format_results(res)
            return redirect('results')

    start_list, goal_list = choices
    form = Actors_choice(start_list=start_list, goal_list=goal_list)
    del request.session['choices']
    return render(request, 'choices.html', context={'form':form})

def results(request):
    if not (results:=request.session.get('results', None)):
        return JsonResponse({'data': "No results"})
    
    
    return JsonResponse({'data':results})