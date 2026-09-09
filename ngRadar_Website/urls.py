from django.urls import path
from .views import views
from django.contrib.auth.views import LogoutView


urlpatterns = [
    # Home page URLs
    path('home/', views.home_view, name='home'),
    path('home/status/', views.status_partial, name="status"),
    path('home/gbtevent/', views.gbt_event_partial, name='gbt_events'),
    path('home/dsocevent/', views.dsoc_event_partial, name='dsoc_events'),
    path('home/submit-waveform/', views.submit_waveform, name='submit_waveform'),
    path('home/lock-status/', views.lock_status, name='lock_status'),
    path("home/image/<uuid:uuid>/", views.serve_image, name="serve_image"),

    # Dashboard page URLs
    path('dashboard/', views.dashboard_view, name='dashboard_home'),
    path('dashboard/updates/', views.event_table_partial, name='event_table_update'),
    path('dashboard/latency/', views.latency_graphing, name='latency_graphing'),

    # SSE (Server-Sent Events) URLs:
    # We have two separate SSE endpoints: one for progress updates and one for UI events,
    # but maybe we can combine them into a single endpoint in the future so we can maintain a single connection to the server.
    path("progress/stream/", views.progress_sse, name="progress_sse"),
    path("events/stream/", views.sse_stream, name="sse_stream"),
    
    # Logout paths 
    path('logout/', views.logout_view, name='logout')
]
