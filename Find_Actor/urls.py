from django.contrib import admin
from django.urls import path
from .views import index, choices, results, about

urlpatterns = [
    path('', index, name="index"),
    path('choices/', choices, name="choices"),
    path('results/', results, name="results"),
    path('about/', about, name="about"),
]
