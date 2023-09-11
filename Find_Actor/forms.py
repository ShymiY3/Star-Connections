from collections.abc import Mapping
from typing import Any
from django import forms
from django.core.exceptions import ValidationError
from django.forms.utils import ErrorList
from .models import Actors

class Actors_submit(forms.Form):
    start_actor = forms.CharField(max_length=200, required=True, widget=forms.TextInput(attrs={'class':'form-control'}))
    goal_actor = forms.CharField(max_length=200, required=True, widget=forms.TextInput(attrs={'class':'form-control'}))
    
    def clean(self) -> dict[str, Any]:
        cleaned_data = super().clean()
        start_actor = cleaned_data.get('start_actor')
        goal_actor = cleaned_data.get('goal_actor')
        start_list = Actors.objects.filter(name__icontains=start_actor).order_by('name')[:20]
        goal_list = Actors.objects.filter(name__icontains=goal_actor).order_by('name')[:20]
        
        msg = 'Actor {} not in database'
        if not start_list.exists():
            self.add_error('start_actor', msg.format(start_actor))
        
        if not goal_list.exists():
            self.add_error('goal_actor', msg.format(goal_actor))
                
        cleaned_data['start_list'] = start_list
        cleaned_data['goal_list'] = goal_list
        
        return cleaned_data

class Actors_choice(forms.Form):
    start_actor = forms.ChoiceField(choices=[], widget=forms.Select(attrs={'class':'form-select'}), required=True)
    goal_actor = forms.ChoiceField(choices=[], widget=forms.Select(attrs={'class':'form-select'}), required=True)
    
    def __init__(self, *args, **kwargs):
        start_list = kwargs.pop('start_list', [])
        goal_list = kwargs.pop('goal_list', [])
        
        super().__init__(*args, **kwargs)
        
        self.fields['start_actor'].choices = start_list
        self.fields['goal_actor'].choices = goal_list