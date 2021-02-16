"""moviews_stemf URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/3.1/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path
from home import views

urlpatterns = [
    path('admin/', admin.site.urls),
    path('recommon/', views.Recommon.as_view()),
    path('movieslist/', views.Movies_Type.as_view()),
    path('realtime/', views.Realtime.as_view()),
    path('hot/', views.Hot.as_view()),
    path('heigth/', views.Higth.as_view()),
    path('new/', views.New.as_view()),
    path('detailed/', views.Detailed.as_view()),
    path('search/', views.Search.as_view()),
]
