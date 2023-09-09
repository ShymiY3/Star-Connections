from django.db import models


class Actors(models.Model):
    id = models.CharField(primary_key=True, max_length=20)
    name = models.CharField(max_length=100)
    birth_year = models.FloatField(blank=True, null=True)
    death_year = models.FloatField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'actors'
        
    def __str__(self):
        if not self.birth_year:
            return f'{self.name}'
        if self.death_year:
            return f'{self.name} ({int(self.birth_year)}-{int(self.death_year)})'
        return f'{self.name} ({int(self.birth_year)})'


class Akas(models.Model):
    movie = models.ForeignKey('Movies', on_delete=models.CASCADE)
    title = models.CharField(max_length=250)
    region = models.CharField(max_length=10)

    class Meta:
        managed = False
        db_table = 'akas'


class Cast(models.Model):
    movie = models.ForeignKey('Movies', on_delete=models.CASCADE)
    actor = models.ForeignKey(Actors, on_delete=models.CASCADE)
    characters = models.CharField(blank=True, null=True, max_length=400)

    class Meta:
        managed = False
        db_table = 'cast'


class Movies(models.Model):
    id = models.CharField(primary_key=True, max_length=20)
    original_title = models.CharField(max_length=200)
    start_year = models.IntegerField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'movies'


class Ratings(models.Model):
    movie = models.ForeignKey(Movies, on_delete=models.CASCADE)
    average = models.FloatField()
    num_votes = models.IntegerField()

    class Meta:
        managed = False
        db_table = 'ratings'
