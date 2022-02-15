try:
    from django.urls import re_path
except:
    from django.conf.urls import url as re_path
from . import views

urlpatterns = [
    re_path( r'^upload.json$', views.upload, name="upload" ),
    re_path( r'^specs.json$', views.get_spec,name="get datasets spec")
]
