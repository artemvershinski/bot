from aiohttp import web
import logging
import os

logger = logging.getLogger(__name__)

async def health_check(request):
    """Эндпоинт для проверки здоровья приложения"""
    return web.Response(text="Bot is alive!")

async def start_background_tasks(app):
    """Фоновые задачи (если нужны)"""
    pass

async def cleanup_background_tasks(app):
    """Очистка фоновых задач"""
    pass

def create_keep_alive_server(port=8080):
    """Создание сервера для поддержания активности"""
    app = web.Application()
    
    # Регистрация маршрутов
    app.router.add_get('/', health_check)
    app.router.add_get('/health', health_check)
    app.router.add_get('/ping', health_check)
    
    # Фоновые задачи
    app.on_startup.append(start_background_tasks)
    app.on_cleanup.append(cleanup_background_tasks)
    
    return app
