from django.shortcuts import render,redirect,HttpResponse
from rest_framework.views import APIView
# Create your views here.

class Recommon(APIView):
    def get(self,request):
        return render(request,'index.html')
    def post(self,request):
        movie_id=request.POST.get('movieid')
        print(movie_id)
        return redirect('/search?name=%s'%movie_id)

class Movies_Type(APIView):
    def get(self,request):
        return render(request, 'Moviestype.html')

class Realtime(APIView):
    def get(self,request):
        return render(request,'Realtime.html')

class Hot(APIView):
    def get(self,request):
        return render(request,'Hot.html')

class Higth(APIView):
    def get(self,request):
        return render(request,'Height.html')

class New(APIView):
    def get(self,request):
        return render(request,'New.html')

class Detailed(APIView):
    def get(self,request):
        return render(request,'Detailed.html')

class Search(APIView):
    def get(self,request):
        print(request.GET.get('name'))
        return render(request,'Search.html')