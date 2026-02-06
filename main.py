import asyncio
import logging
import os
import sys 
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from dataclasses import dataclass, field
from contextlib import asynccontextmanager

from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, ContentType
from aiogram.filters import CommandStart, Command
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.fsm.storage.memory import MemoryStorage

# Импорт сервера для поддержания активности
from keep_alive import create_keep_alive_server
from aiohttp import web

# Конфигурация
YOUR_PEER_ID = 989062605  # Ваш peer ID
RATE_LIMIT_MINUTES = 10  # Ограничение на отправку сообщений (в минутах)
MAX_BAN_HOURS = 720  # Максимальное время бана (30 дней)
KEEP_ALIVE_PORT = int(os.getenv("PORT", 8080))  # Порт для Render.com

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Хранение данных
@dataclass
class UserData:
    user_id: int
    username: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    last_message_time: Optional[datetime] = None
    is_banned: bool = False
    ban_until: Optional[datetime] = None
    ban_reason: str = ""
    messages_sent: int = 0
    created_at: datetime = field(default_factory=datetime.now)

class MessageForwardingBot:
    def __init__(self, token: str):
        self.token = token
        self.storage = MemoryStorage()
        self.bot = Bot(
            token=token,
            default=DefaultBotProperties(parse_mode=ParseMode.HTML)
        )
        self.dp = Dispatcher(storage=self.storage)
        self.router = Router()
        self.dp.include_router(self.router)
        
        # Хранилище данных пользователей
        self.user_data: Dict[int, UserData] = {}
        
        # Статистика
        self.stats = {
            "total_messages": 0,
            "successful_forwards": 0,
            "failed_forwards": 0,
            "bans_issued": 0,
            "users_blocked_by_rate_limit": 0
        }
        
        # Регистрация обработчиков
        self.register_handlers()
    
    def get_user_info(self, user_id: int) -> str:
        """Получение информации о пользователе в читаемом формате"""
        if user_id in self.user_data:
            user = self.user_data[user_id]
            if user.username:
                return f"@{user.username}"
            elif user.first_name or user.last_name:
                return f"{user.first_name or ''} {user.last_name or ''}".strip()
        return f"ID: {user_id}"
    
    def check_ban_status(self, user_id: int) -> tuple[bool, str]:
        """Проверка статуса блокировки пользователя"""
        if user_id not in self.user_data:
            return False, ""
        
        user_data = self.user_data[user_id]
        
        if not user_data.is_banned:
            return False, ""
        
        # Проверка срока бана
        if user_data.ban_until:
            if datetime.now() > user_data.ban_until:
                # Срок бана истек
                user_data.is_banned = False
                user_data.ban_until = None
                user_data.ban_reason = ""
                return False, ""
            else:
                ban_time = user_data.ban_until.strftime("%d.%m.%Y %H:%M")
                return True, f"до {ban_time}"
        else:
            return True, "навсегда"
    
    def check_rate_limit(self, user_id: int) -> tuple[bool, int]:
        """Проверка ограничения по времени отправки сообщений"""
        if user_id not in self.user_data or not self.user_data[user_id].last_message_time:
            return True, 0  # Можно отправлять
        
        last_time = self.user_data[user_id].last_message_time
        time_diff = (datetime.now() - last_time).total_seconds() / 60
        
        if time_diff < RATE_LIMIT_MINUTES:
            remaining = RATE_LIMIT_MINUTES - int(time_diff)
            return False, remaining
        return True, 0
    
    def update_user_data(self, message: Message):
        """Обновление данных пользователя"""
        user = message.from_user
        user_id = user.id
        
        if user_id not in self.user_data:
            self.user_data[user_id] = UserData(
                user_id=user_id,
                username=user.username,
                first_name=user.first_name,
                last_name=user.last_name
            )
        else:
            # Обновляем данные, если они изменились
            self.user_data[user_id].username = user.username
            self.user_data[user_id].first_name = user.first_name
            self.user_data[user_id].last_name = user.last_name
        
        return self.user_data[user_id]
    
    def register_handlers(self):
        """Регистрация всех обработчиков сообщений"""
        
        # Команда /start
        @self.router.message(CommandStart())
        async def cmd_start(message: Message):
            user_data = self.update_user_data(message)
            
            welcome_text = (
                "👋 <b>Привет, {name}!</b>\n\n"
                "Этот бот создан чтобы вы могли отправить мне сообщение. "
                "Задержка между отправкой сообщений 10 минут.\n"
                "Пишите всё в одном сообщении\n"
                "Поддерживаются любые типы сообщений."
                "<b>Просто отправьте ваше сообщение</b>"
            ).format(
                name=message.from_user.first_name or "пользователь"
            )
            
            await message.answer(welcome_text)
            
            try:
                if message.from_user.id != YOUR_PEER_ID:
                    user_info = self.get_user_info(message.from_user.id)
                    await self.bot.send_message(
                        YOUR_PEER_ID,
                        f"👤 <b>Новый пользователь запустил бота:</b>\n"
                        f"• {user_info}\n"
                        f"• ID: {message.from_user.id}\n"
                        f"• Всего пользователей: {len(self.user_data)}"
                    )
            except Exception as e:
                logger.error(f"Ошибка при уведомлении о новом пользователе: {e}")
        
        @self.router.message(Command("ban"))
        async def cmd_ban(message: Message):
            if message.from_user.id != YOUR_PEER_ID:
                await message.answer("⛔ У вас нет прав для использования этой команды.")
                return
            
            try:
                args = message.text.split()[1:]
                if len(args) < 2:
                    await message.answer(
                        "❌ <b>Неверный формат команды</b>\n\n"
                        "Используйте: <code>/ban PEER_ID Причина [Время в часах]</code>\n\n"
                        "<b>Примеры:</b>\n"
                        "<code>/ban 123456 Спам</code> - бан навсегда\n"
                        "<code>/ban 123456 Флуд 24</code> - бан на 24 часа\n"
                        "<code>/ban 123456 Нарушение правил 168</code> - бан на неделю"
                    )
                    return
                
                peer_id = int(args[0])
                reason = " ".join(args[1:-1]) if len(args) > 2 and args[-1].isdigit() else " ".join(args[1:])
                hours = int(args[-1]) if len(args) > 2 and args[-1].isdigit() else None
                
                if hours and (hours <= 0 or hours > MAX_BAN_HOURS):
                    await message.answer(
                        f"❌ Время бана должно быть от 1 до {MAX_BAN_HOURS} часов "
                        f"(максимум {MAX_BAN_HOURS // 24} дней)"
                    )
                    return
                
                # Блокировка пользователя
                if peer_id not in self.user_data:
                    self.user_data[peer_id] = UserData(user_id=peer_id)
                
                user_data = self.user_data[peer_id]
                user_data.is_banned = True
                user_data.ban_reason = reason
                
                if hours:
                    user_data.ban_until = datetime.now() + timedelta(hours=hours)
                    ban_duration = f"на {hours} часов ({hours // 24} дней {hours % 24} часов)"
                else:
                    user_data.ban_until = None
                    ban_duration = "навсегда"
                
                self.stats["bans_issued"] += 1
                
                # Формирование ответа
                user_info = self.get_user_info(peer_id)
                response = (
                    f"✅ <b>Пользователь заблокирован</b>\n\n"
                    f"<b>Информация:</b> {user_info}\n"
                    f"<b>Peer ID:</b> <code>{peer_id}</code>\n"
                    f"<b>Причина:</b> {reason}\n"
                    f"<b>Длительность:</b> {ban_duration}\n\n"
                    f"<i>Всего заблокировано: {self.stats['bans_issued']}</i>"
                )
                
                await message.answer(response)
                
                # Уведомление пользователю
                try:
                    ban_message = (
                        f"🚫 <b>Вы заблокированы</b>\n\n"
                        f"<b>Причина:</b> {reason}\n"
                        f"<b>Длительность:</b> {ban_duration}\n\n"
                    )
                    
                    if hours:
                        unban_time = user_data.ban_until.strftime("%d.%m.%Y в %H:%M")
                        ban_message += f"<b>Разблокировка:</b> {unban_time}\n\n"
                    
                    ban_message += (
                        f"<i>Если вы считаете, что это ошибка, "
                        f"свяжитесь с администратором.</i>"
                    )
                    
                    await self.bot.send_message(peer_id, ban_message)
                    
                except Exception as e:
                    logger.error(f"Не удалось уведомить пользователя {peer_id} о бане: {e}")
                    await message.answer(
                        f"⚠️ <b>Пользователь заблокирован, но не получил уведомление</b>\n\n"
                        f"<i>Причина: {str(e)}</i>"
                    )
                    
            except ValueError as e:
                await message.answer(f"❌ Ошибка в формате данных: {str(e)}")
            except Exception as e:
                logger.error(f"Ошибка при бане: {e}")
                await message.answer(f"❌ Неизвестная ошибка: {str(e)}")
        
        # Команда /unban
        @self.router.message(Command("unban"))
        async def cmd_unban(message: Message):
            if message.from_user.id != YOUR_PEER_ID:
                await message.answer("⛔ У вас нет прав для использования этой команды.")
                return
            
            try:
                args = message.text.split()[1:]
                if len(args) < 1:
                    await message.answer("❌ Используйте: <code>/unban PEER_ID</code>")
                    return
                
                peer_id = int(args[0])
                
                if peer_id in self.user_data:
                    user_data = self.user_data[peer_id]
                    user_info = self.get_user_info(peer_id)
                    
                    if user_data.is_banned:
                        user_data.is_banned = False
                        user_data.ban_until = None
                        reason = user_data.ban_reason
                        user_data.ban_reason = ""
                        
                        response = (
                            f"✅ <b>Пользователь разблокирован</b>\n\n"
                            f"<b>Информация:</b> {user_info}\n"
                            f"<b>Peer ID:</b> <code>{peer_id}</code>\n"
                            f"<b>Был заблокирован за:</b> {reason}"
                        )
                        
                        await message.answer(response)
                        
                        # Уведомление пользователю
                        try:
                            await self.bot.send_message(
                                peer_id,
                                "✅ <b>Вы разблокированы</b>\n\n"
                                "Теперь вы снова можете отправлять сообщения через бота."
                            )
                        except Exception as e:
                            logger.error(f"Не удалось уведомить пользователя {peer_id} о разблокировке: {e}")
                    else:
                        await message.answer(f"ℹ️ Пользователь {user_info} (<code>{peer_id}</code>) не заблокирован")
                else:
                    await message.answer(f"ℹ️ Пользователь с ID <code>{peer_id}</code> не найден в базе данных")
                    
            except ValueError:
                await message.answer("❌ Неверный формат Peer ID")
        
        # Команда /stats (только для администратора)
        @self.router.message(Command("stats"))
        async def cmd_stats(message: Message):
            if message.from_user.id != YOUR_PEER_ID:
                return
            
            total_users = len(self.user_data)
            banned_users = sum(1 for data in self.user_data.values() if data.is_banned)
            active_users = total_users - banned_users
            
            # Находим самого активного пользователя
            most_active = max(self.user_data.values(), key=lambda x: x.messages_sent, default=None)
            
            stats_text = (
                f"📊 <b>Статистика бота</b>\n\n"
                f"<b>Пользователи:</b>\n"
                f"• Всего: {total_users}\n"
                f"• Активных: {active_users}\n"
                f"• Заблокированных: {banned_users}\n\n"
                f"<b>Сообщения:</b>\n"
                f"• Всего отправлено: {self.stats['total_messages']}\n"
                f"• Успешно переслано: {self.stats['successful_forwards']}\n"
                f"• Ошибок при пересылке: {self.stats['failed_forwards']}\n"
                f"• Блокировок по лимиту: {self.stats['users_blocked_by_rate_limit']}\n"
                f"• Выдано банов: {self.stats['bans_issued']}\n\n"
            )
            
            if most_active and most_active.messages_sent > 0:
                stats_text += (
                    f"<b>Самый активный:</b>\n"
                    f"• {self.get_user_info(most_active.user_id)}\n"
                    f"• Сообщений: {most_active.messages_sent}\n"
                    f"• Первое сообщение: {most_active.created_at.strftime('%d.%m.%Y')}\n\n"
                )
            
            stats_text += f"<i>Обновлено: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}</i>"
            
            await message.answer(stats_text)
        
        # Команда /users - список пользователей
        @self.router.message(Command("users"))
        async def cmd_users(message: Message):
            if message.from_user.id != YOUR_PEER_ID:
                return
            
            if not self.user_data:
                await message.answer("📭 <b>Пользователей пока нет</b>")
                return
            
            users_text = "👥 <b>Список пользователей:</b>\n\n"
            
            for i, (user_id, user_data) in enumerate(list(self.user_data.items())[:50], 1):
                status = "🚫" if user_data.is_banned else "✅"
                user_info = self.get_user_info(user_id)
                messages = user_data.messages_sent
                
                users_text += f"{i}. {status} {user_info} | ID: <code>{user_id}</code> | Сообщений: {messages}\n"
            
            if len(self.user_data) > 50:
                users_text += f"\n<i>Показано 50 из {len(self.user_data)} пользователей</i>"
            
            await message.answer(users_text)
        
        # Обработка всех типов сообщений от пользователей
        @self.router.message()
        async def handle_user_message(message: Message):
            user_id = message.from_user.id
            self.stats["total_messages"] += 1
            
            # Обновляем данные пользователя
            user_data = self.update_user_data(message)
            
            # Проверка на бан
            is_banned, ban_info = self.check_ban_status(user_id)
            if is_banned:
                await message.answer(
                    f"🚫 <b>Вы заблокированы {ban_info}</b>\n"
                    f"<b>Причина:</b> {user_data.ban_reason}\n\n"
                    f"<i>Если вы считаете, что это ошибка, свяжитесь с администратором.</i>"
                )
                return
            
            # Проверка ограничения по времени
            can_send, remaining = self.check_rate_limit(user_id)
            if not can_send:
                self.stats["users_blocked_by_rate_limit"] += 1
                await message.answer(
                    f"⏳ <b>Подождите {remaining} минут</b>\n\n"
                    "Вы можете отправить только одно сообщение за 10 минут.\n"
                    "Пожалуйста, соберите все мысли в одно сообщение."
                )
                return
            
            try:
                # Подготовка информации о пользователе
                user_info = self.get_user_info(user_id)
                
                # Формируем заголовок сообщения
                caption = (
                    f"📩 <b>Новое сообщение от {user_info}</b>\n"
                    f"<b>ID:</b> <code>{user_id}</code>\n"
                    f"<b>Время:</b> {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n\n"
                )
                
                # Обрабатываем разные типы контента
                content_handlers = {
                    ContentType.TEXT: self.handle_text,
                    ContentType.PHOTO: self.handle_photo,
                    ContentType.VIDEO: self.handle_video,
                    ContentType.VOICE: self.handle_voice,
                    ContentType.AUDIO: self.handle_audio,
                    ContentType.DOCUMENT: self.handle_document,
                    ContentType.LOCATION: self.handle_location,
                    ContentType.CONTACT: self.handle_contact,
                    ContentType.STICKER: self.handle_sticker,
                    ContentType.ANIMATION: self.handle_animation,
                    ContentType.VIDEO_NOTE: self.handle_video_note,
                }
                
                handler = content_handlers.get(message.content_type, self.handle_unknown)
                await handler(message, caption)
                
                # Обновляем статистику
                user_data.last_message_time = datetime.now()
                user_data.messages_sent += 1
                self.stats["successful_forwards"] += 1
                
                # Подтверждение пользователю
                confirmation = (
                    "✅ <b>Сообщение успешно отправлено!</b>\n\n"
                    "📌 <b>Важная информация:</b>\n"
                    "• Ответ поступит только в личные сообщения (ЛС)\n"
                    "• Следующее сообщение можно отправить через 10 минут\n"
                    "• Пожалуйста, пишите всё в одном сообщении\n\n"
                    "⏰ <i>Спасибо за понимание!</i>"
                )
                
                await message.answer(confirmation)
                
                logger.info(f"Сообщение от {user_id} успешно переслано")
                
            except Exception as e:
                logger.error(f"Ошибка при пересылке сообщения от {user_id}: {e}")
                self.stats["failed_forwards"] += 1
                
                # Уведомление пользователю
                error_message = (
                    "❌ <b>Не удалось отправить сообщение</b>\n\n"
                    "Произошла техническая ошибка. Пожалуйста:\n"
                    "1. Попробуйте отправить сообщение позже\n"
                    "2. Убедитесь, что сообщение не слишком большое\n"
                    "3. Если ошибка повторяется, свяжитесь с администратором\n\n"
                    f"<i>Код ошибки: {type(e).__name__}</i>"
                )
                
                await message.answer(error_message)
                
                # Уведомление администратору
                try:
                    await self.bot.send_message(
                        YOUR_PEER_ID,
                        f"⚠️ <b>Ошибка при получении сообщения</b>\n\n"
                        f"<b>От:</b> {self.get_user_info(user_id)}\n"
                        f"<b>ID:</b> <code>{user_id}</code>\n"
                        f"<b>Тип ошибки:</b> {type(e).__name__}\n"
                        f"<b>Описание:</b> {str(e)[:200]}\n\n"
                        f"<i>Всего ошибок: {self.stats['failed_forwards']}</i>"
                    )
                except Exception as notify_error:
                    logger.error(f"Не удалось отправить уведомление об ошибке: {notify_error}")
    
    # Обработчики разных типов контента
    async def handle_text(self, message: Message, caption: str):
        caption += f"💬 <b>Текст:</b>\n{message.text}"
        await self.bot.send_message(YOUR_PEER_ID, caption)
    
    async def handle_photo(self, message: Message, caption: str):
        caption += "🖼 <b>Фото</b>"
        if message.caption:
            caption += f"\n\n<b>Подпись:</b>\n{message.caption}"
        await self.bot.send_photo(
            YOUR_PEER_ID,
            message.photo[-1].file_id,
            caption=caption
        )
    
    async def handle_video(self, message: Message, caption: str):
        caption += "🎬 <b>Видео</b>"
        if message.caption:
            caption += f"\n\n<b>Подпись:</b>\n{message.caption}"
        await self.bot.send_video(
            YOUR_PEER_ID,
            message.video.file_id,
            caption=caption
        )
    
    async def handle_voice(self, message: Message, caption: str):
        caption += "🎤 <b>Голосовое сообщение</b>"
        if message.caption:
            caption += f"\n\n<b>Подпись:</b>\n{message.caption}"
        await self.bot.send_voice(
            YOUR_PEER_ID,
            message.voice.file_id,
            caption=caption
        )
    
    async def handle_audio(self, message: Message, caption: str):
        caption += "🎵 <b>Аудио</b>"
        if message.caption:
            caption += f"\n\n<b>Подпись:</b>\n{message.caption}"
        await self.bot.send_audio(
            YOUR_PEER_ID,
            message.audio.file_id,
            caption=caption
        )
    
    async def handle_document(self, message: Message, caption: str):
        caption += "📎 <b>Документ</b>"
        if message.caption:
            caption += f"\n\n<b>Подпись:</b>\n{message.caption}"
        await self.bot.send_document(
            YOUR_PEER_ID,
            message.document.file_id,
            caption=caption
        )
    
    async def handle_location(self, message: Message, caption: str):
        caption += "📍 <b>Геолокация</b>"
        await self.bot.send_location(
            YOUR_PEER_ID,
            message.location.latitude,
            message.location.longitude
        )
        await self.bot.send_message(YOUR_PEER_ID, caption)
    
    async def handle_contact(self, message: Message, caption: str):
        caption += "👤 <b>Контакт</b>"
        await self.bot.send_contact(
            YOUR_PEER_ID,
            phone_number=message.contact.phone_number,
            first_name=message.contact.first_name,
            last_name=message.contact.last_name
        )
        await self.bot.send_message(YOUR_PEER_ID, caption)
    
    async def handle_sticker(self, message: Message, caption: str):
        caption += "😊 <b>Стикер</b>"
        await self.bot.send_sticker(YOUR_PEER_ID, message.sticker.file_id)
        await self.bot.send_message(YOUR_PEER_ID, caption)
    
    async def handle_animation(self, message: Message, caption: str):
        caption += "🎭 <b>GIF</b>"
        if message.caption:
            caption += f"\n\n<b>Подпись:</b>\n{message.caption}"
        await self.bot.send_animation(
            YOUR_PEER_ID,
            message.animation.file_id,
            caption=caption
        )
    
    async def handle_video_note(self, message: Message, caption: str):
        caption += "📹 <b>Видеосообщение</b>"
        await self.bot.send_video_note(
            YOUR_PEER_ID,
            message.video_note.file_id
        )
        await self.bot.send_message(YOUR_PEER_ID, caption)
    
    async def handle_unknown(self, message: Message, caption: str):
        caption += "❓ <b>Неизвестный тип сообщения</b>"
        await self.bot.send_message(YOUR_PEER_ID, caption)
        await self.bot.send_message(
            YOUR_PEER_ID,
            f"⚠️ Получен неподдерживаемый тип контента: {message.content_type}"
        )
    
    async def start_keep_alive_server(self):
        """Запуск сервера для поддержания активности"""
        app = create_keep_alive_server(KEEP_ALIVE_PORT)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', KEEP_ALIVE_PORT)
        await site.start()
        logger.info(f"Keep-alive сервер запущен на порту {KEEP_ALIVE_PORT}")
        return runner
    
    async def run(self):
        """Запуск бота и keep-alive сервера"""
        try:
            # Запускаем keep-alive сервер
            runner = await self.start_keep_alive_server()
            
            # Запускаем бота
            logger.info("🤖 Бот запускается...")
            logger.info(f"👑 Peer ID администратора: {YOUR_PEER_ID}")
            logger.info(f"⏱ Лимит сообщений: {RATE_LIMIT_MINUTES} минут")
            logger.info(f"🌐 Keep-alive порт: {KEEP_ALIVE_PORT}")
            
            await self.dp.start_polling(self.bot)
            
        except Exception as e:
            logger.error(f"Критическая ошибка при запуске бота: {e}")
        finally:
            await self.bot.session.close()
            if 'runner' in locals():
                await runner.cleanup()

@asynccontextmanager
async def lifespan():
    """Контекстный менеджер для управления жизненным циклом"""
    logger.info("Запуск приложения...")
    yield
    logger.info("Завершение работы приложения...")

def main():
    """Основная функция запуска"""
    import os
    
    # Получение токена из переменной окружения
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    
    if not BOT_TOKEN:
        logger.error("❌ Не найден BOT_TOKEN в переменных окружения!")
        logger.info("ℹ️ Убедитесь, что вы установили переменную BOT_TOKEN на Render.com")
        return
    
    # Проверка, что токен валидный
    if not BOT_TOKEN.startswith("7") or ":" not in BOT_TOKEN:
        logger.error("❌ Неверный формат BOT_TOKEN!")
        return
    
    # Создаем и запускаем бота
    bot = MessageForwardingBot(BOT_TOKEN)
    
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"Фатальная ошибка: {e}")

if __name__ == "__main__":
    main()

