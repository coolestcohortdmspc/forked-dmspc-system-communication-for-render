"""
ASGI config for prototype_project.

It exposes the ASGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/6.0/howto/deployment/asgi/
"""

import os
from django.core.asgi import get_asgi_application
from fastapi_app import app as fastapi_app

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'settings')

django_application = get_asgi_application()

#create an asynchronous server which works with both Django and FastAPI
application = fastapi_app
application.mount("/", django_application)
