from django.shortcuts import render, HttpResponse
from django.conf import settings
from .forms import Actors_submit, Actors_choice
import find_actors



def get_results():
    pass

def index(request):
    context = {}
    if request.method == 'POST':
        form = Actors_submit(request.POST)
        if form.is_valid():
            start_list = form.cleaned_data['start_list']
            goal_list = form.cleaned_data['goal_list']
        
            if len(start_list)==1 and len(goal_list)==1:
                pass
            
        else:
            pass
    else:
        context['form'] = Actors_submit()
    
    return render(request, 'index.html', context=context)

def choices(request):
    pass


def results(request):
    pass