from django.contrib import admin
from django.urls import path
from .views import index, choices, results

urlpatterns = [
    path('', index, name="index"),
    path('choices/', choices, name="choices"),
    path('results/', results, name="results"),
]
