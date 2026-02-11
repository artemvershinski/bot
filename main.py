import asyncio, logging, os, sys, signal, asyncpg
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from contextlib import asynccontextmanager
from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, ContentType, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.filters import CommandStart, Command
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.fsm.storage.memory import MemoryStorage
from keep_alive import create_keep_alive_server
from aiohttp import web
import json
from datetime import timezone

# Конфигурация
OWNER_ID = 989062605  # Владелец бота (нельзя удалить/забанить)
RATE_LIMIT_MINUTES = 10
MAX_BAN_HOURS = 720
KEEP_ALIVE_PORT = int(os.getenv("PORT", 8080))
DATABASE_URL = os.getenv("DATABASE_URL")  # Переменная окружения для PostgreSQL

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

class Database:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool = None
    
    async def create_pool(self):
        """Создание пула соединений с БД"""
        self.pool = await asyncpg.create_pool(self.dsn)
        await self.init_db()
        logger.info("✅ Подключение к PostgreSQL установлено")
    
    async def init_db(self):
        """Инициализация таблиц"""
        async with self.pool.acquire() as conn:
            # Таблица пользователей
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    last_message_time TIMESTAMP,
                    is_banned BOOLEAN DEFAULT FALSE,
                    ban_until TIMESTAMP,
                    ban_reason TEXT,
                    messages_sent INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Таблица администраторов
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS admins (
                    user_id BIGINT PRIMARY KEY,
                    added_by BIGINT NOT NULL,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_active BOOLEAN DEFAULT TRUE,
                    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
                )
            ''')
            
            # Таблица статистики
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS stats (
                    id SERIAL PRIMARY KEY,
                    total_messages INTEGER DEFAULT 0,
                    successful_forwards INTEGER DEFAULT 0,
                    failed_forwards INTEGER DEFAULT 0,
                    bans_issued INTEGER DEFAULT 0,
                    rate_limit_blocks INTEGER DEFAULT 0,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Добавляем владельца как администратора
            await conn.execute('''
                INSERT INTO admins (user_id, added_by) 
                VALUES ($1, $1) 
                ON CONFLICT (user_id) DO NOTHING
            ''', OWNER_ID)
            
            # Инициализируем статистику если её нет
            await conn.execute('''
                INSERT INTO stats (id, total_messages, successful_forwards, failed_forwards, bans_issued, rate_limit_blocks)
                VALUES (1, 0, 0, 0, 0, 0)
                ON CONFLICT (id) DO NOTHING
            ''')
    
    async def get_user(self, user_id: int) -> Optional[Dict]:
        """Получение пользователя по ID"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT * FROM users WHERE user_id = $1', user_id)
            return dict(row) if row else None
    
    async def save_user(self, user_id: int, **kwargs):
        """Сохранение или обновление пользователя"""
        async with self.pool.acquire() as conn:
            # Проверяем существует ли пользователь
            exists = await conn.fetchval('SELECT EXISTS(SELECT 1 FROM users WHERE user_id = $1)', user_id)
            
            if exists:
                # Обновляем существующего
                set_clause = ', '.join([f"{k} = ${i+2}" for i, k in enumerate(kwargs.keys())])
                set_clause += ", updated_at = CURRENT_TIMESTAMP"
                query = f'UPDATE users SET {set_clause} WHERE user_id = $1'
                await conn.execute(query, user_id, *kwargs.values())
            else:
                # Создаем нового
                fields = ['user_id'] + list(kwargs.keys())
                values = [user_id] + list(kwargs.values())
                placeholders = ', '.join([f'${i+1}' for i in range(len(values))])
                query = f'INSERT INTO users ({", ".join(fields)}) VALUES ({placeholders})'
                await conn.execute(query, *values)
    
    async def update_user_stats(self, user_id: int, increment_messages: bool = True):
        """Обновление статистики пользователя"""
        async with self.pool.acquire() as conn:
            if increment_messages:
                await conn.execute('''
                    UPDATE users SET messages_sent = messages_sent + 1, 
                                     updated_at = CURRENT_TIMESTAMP 
                    WHERE user_id = $1
                ''', user_id)
            else:
                await conn.execute('''
                    UPDATE users SET updated_at = CURRENT_TIMESTAMP 
                    WHERE user_id = $1
                ''', user_id)
    
    async def update_user_last_message(self, user_id: int, message_time: datetime):
        """Обновление времени последнего сообщения"""
        async with self.pool.acquire() as conn:
            await conn.execute('''
                UPDATE users SET last_message_time = $1, updated_at = CURRENT_TIMESTAMP 
                WHERE user_id = $2
            ''', message_time, user_id)
    
    async def ban_user(self, user_id: int, reason: str, ban_until: Optional[datetime] = None):
        """Блокировка пользователя"""
        async with self.pool.acquire() as conn:
            await conn.execute('''
                UPDATE users SET is_banned = TRUE, 
                                 ban_reason = $1, 
                                 ban_until = $2,
                                 updated_at = CURRENT_TIMESTAMP 
                WHERE user_id = $3
            ''', reason, ban_until, user_id)
    
    async def unban_user(self, user_id: int):
        """Разблокировка пользователя"""
        async with self.pool.acquire() as conn:
            await conn.execute('''
                UPDATE users SET is_banned = FALSE, 
                                 ban_reason = NULL, 
                                 ban_until = NULL,
                                 updated_at = CURRENT_TIMESTAMP 
                WHERE user_id = $1
            ''', user_id)
    
    async def get_all_users(self) -> List[Dict]:
        """Получение всех пользователей"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch('SELECT * FROM users ORDER BY created_at DESC')
            return [dict(row) for row in rows]
    
    async def add_admin(self, user_id: int, added_by: int) -> bool:
        """Добавление администратора"""
        try:
            async with self.pool.acquire() as conn:
                await conn.execute('''
                    INSERT INTO admins (user_id, added_by) 
                    VALUES ($1, $2) 
                    ON CONFLICT (user_id) DO UPDATE SET 
                        is_active = TRUE,
                        added_by = EXCLUDED.added_by,
                        added_at = CURRENT_TIMESTAMP
                ''', user_id, added_by)
                return True
        except Exception as e:
            logger.error(f"Ошибка добавления админа {user_id}: {e}")
            return False
    
    async def remove_admin(self, user_id: int) -> bool:
        """Удаление администратора"""
        if user_id == OWNER_ID:
            return False  # Нельзя удалить владельца
        try:
            async with self.pool.acquire() as conn:
                await conn.execute('DELETE FROM admins WHERE user_id = $1', user_id)
                return True
        except Exception as e:
            logger.error(f"Ошибка удаления админа {user_id}: {e}")
            return False
    
    async def get_admins(self) -> List[int]:
        """Получение списка ID администраторов"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch('SELECT user_id FROM admins WHERE is_active = TRUE')
            return [row['user_id'] for row in rows]
    
    async def is_admin(self, user_id: int) -> bool:
        """Проверка является ли пользователь администратором"""
        if user_id == OWNER_ID:
            return True
        async with self.pool.acquire() as conn:
            exists = await conn.fetchval(
                'SELECT EXISTS(SELECT 1 FROM admins WHERE user_id = $1 AND is_active = TRUE)',
                user_id
            )
            return exists
    
    async def update_stats(self, **kwargs):
        """Обновление глобальной статистики"""
        async with self.pool.acquire() as conn:
            set_clause = ', '.join([f"{k} = {k} + ${i+1}" for i, k in enumerate(kwargs.keys())])
            set_clause += ", updated_at = CURRENT_TIMESTAMP"
            query = f'UPDATE stats SET {set_clause} WHERE id = 1'
            await conn.execute(query, *kwargs.values())
    
    async def get_stats(self) -> Dict:
        """Получение глобальной статистики"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('SELECT * FROM stats WHERE id = 1')
            if not row:
                return {
                    'total_messages': 0,
                    'successful_forwards': 0,
                    'failed_forwards': 0,
                    'bans_issued': 0,
                    'rate_limit_blocks': 0
                }
            return dict(row)
    
    async def get_users_count(self) -> Dict:
        """Получение статистики по пользователям"""
        async with self.pool.acquire() as conn:
            total = await conn.fetchval('SELECT COUNT(*) FROM users')
            banned = await conn.fetchval('SELECT COUNT(*) FROM users WHERE is_banned = TRUE')
            active_today = await conn.fetchval('''
                SELECT COUNT(*) FROM users 
                WHERE updated_at > CURRENT_TIMESTAMP - INTERVAL '24 hours'
            ''')
            return {
                'total': total,
                'banned': banned,
                'active_today': active_today
            }
    
    async def get_most_active_user(self) -> Optional[Dict]:
        """Получение самого активного пользователя"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow('''
                SELECT * FROM users 
                WHERE messages_sent > 0 
                ORDER BY messages_sent DESC 
                LIMIT 1
            ''')
            return dict(row) if row else None
    
    async def close(self):
        """Закрытие соединения с БД"""
        if self.pool:
            await self.pool.close()

class MessageForwardingBot:
    def __init__(self, token: str, db: Database):
        self.token = token
        self.db = db
        self.storage = MemoryStorage()
        self.bot = Bot(token=token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        self.dp = Dispatcher(storage=self.storage)
        self.router = Router()
        self.dp.include_router(self.router)
        self.is_running = True
        self.cached_admins = []  # Кэш администраторов
        self.register_handlers()
    
    async def notify_admins(self, message: str, exclude_user_id: int = None):
        """Отправка уведомления всем администраторам"""
        admins = await self.db.get_admins()
        for admin_id in admins:
            if exclude_user_id and admin_id == exclude_user_id:
                continue
            try:
                await self.bot.send_message(admin_id, message)
            except Exception as e:
                logger.error(f"Не удалось отправить уведомление админу {admin_id}: {e}")
    
    def get_user_info(self, user_data: Dict) -> str:
        """Форматирование информации о пользователе"""
        if user_data.get('username'):
            return f"@{user_data['username']}"
        elif user_data.get('first_name') or user_data.get('last_name'):
            return f"{user_data.get('first_name', '')} {user_data.get('last_name', '')}".strip()
        return f"ID: {user_data['user_id']}"
    
    async def check_ban_status(self, user_id: int) -> tuple[bool, str]:
        """Проверка статуса блокировки"""
        user_data = await self.db.get_user(user_id)
        if not user_data or not user_data.get('is_banned'):
            return False, ""
        
        ban_until = user_data.get('ban_until')
        if ban_until and datetime.now(timezone.utc) > ban_until.replace(tzinfo=timezone.utc):
            await self.db.unban_user(user_id)
            return False, ""
        
        if ban_until:
            return True, f"до {ban_until.strftime('%d.%m.%Y %H:%M')}"
        return True, "навсегда"
    
    async def check_rate_limit(self, user_id: int) -> tuple[bool, int]:
        """Проверка лимита сообщений"""
        user_data = await self.db.get_user(user_id)
        if not user_data or not user_data.get('last_message_time'):
            return True, 0
        
        last_time = user_data['last_message_time']
        if last_time.tzinfo:
            last_time = last_time.replace(tzinfo=timezone.utc)
        
        time_diff = (datetime.now(timezone.utc) - last_time).total_seconds() / 60
        if time_diff < RATE_LIMIT_MINUTES:
            return False, RATE_LIMIT_MINUTES - int(time_diff)
        return True, 0
    
    async def save_user_from_message(self, message: Message):
        """Сохранение пользователя из сообщения"""
        user = message.from_user
        user_id = user.id
        
        await self.db.save_user(
            user_id=user_id,
            username=user.username,
            first_name=user.first_name,
            last_name=user.last_name
        )
    
    def register_handlers(self):
        @self.router.message(CommandStart())
        async def cmd_start(message: Message):
            await self.save_user_from_message(message)
            await message.answer(
                f"👋 <b>Привет, {message.from_user.first_name or 'пользователь'}!</b>\n\n"
                f"Это бот-пересыльщик сообщений.\n"
                f"Задержка между отправкой сообщений - {RATE_LIMIT_MINUTES} минут.\n"
                f"Пишите всё в одном сообщении.\n"
                f"Поддерживаются любые типы сообщений.\n\n"
                f"<b>Просто отправьте ваше сообщение.</b>"
            )
            
            # Уведомление администраторам о новом пользователе
            user_data = await self.db.get_user(message.from_user.id)
            await self.notify_admins(
                f"👤 <b>Новый пользователь запустил бота:</b>\n"
                f"• {self.get_user_info(user_data)}\n"
                f"• ID: {message.from_user.id}\n"
                f"• Всего пользователей: {(await self.db.get_users_count())['total']}",
                exclude_user_id=message.from_user.id
            )
        
        @self.router.message(Command("admin"))
        async def cmd_admin(message: Message):
            """Управление администраторами"""
            if not await self.db.is_admin(message.from_user.id):
                return await message.answer("❌ У вас нет прав для использования этой команды.")
            
            text = message.text.split()
            if len(text) == 1:
                # Показываем список команд
                await message.answer(
                    "👑 <b>Управление администраторами</b>\n\n"
                    "<b>Команды:</b>\n"
                    "• <code>/admin add ID</code> - добавить администратора\n"
                    "• <code>/admin remove ID</code> - удалить администратора\n"
                    "• <code>/admin list</code> - список администраторов\n\n"
                    "<i>Владелец бота (ID: {OWNER_ID}) всегда является администратором</i>"
                )
            elif len(text) >= 3:
                action = text[1].lower()
                try:
                    target_id = int(text[2])
                    
                    if action == "add":
                        if target_id == OWNER_ID:
                            return await message.answer("👑 Владелец уже является администратором.")
                        
                        # Сохраняем пользователя если его нет
                        try:
                            user = await self.bot.get_chat(target_id)
                            await self.db.save_user(
                                user_id=target_id,
                                username=user.username,
                                first_name=user.first_name,
                                last_name=user.last_name
                            )
                        except:
                            pass
                        
                        if await self.db.add_admin(target_id, message.from_user.id):
                            user_data = await self.db.get_user(target_id) or {'user_id': target_id}
                            await message.answer(
                                f"✅ <b>Администратор добавлен</b>\n\n"
                                f"Пользователь: {self.get_user_info(user_data)}\n"
                                f"ID: <code>{target_id}</code>\n"
                                f"Добавил: {message.from_user.first_name}"
                            )
                            
                            # Уведомляем нового администратора
                            try:
                                await self.bot.send_message(
                                    target_id,
                                    f"👑 <b>Вы назначены администратором</b>\n\n"
                                    f"Теперь вам будут приходить все сообщения пользователей, "
                                    f"и вы можете управлять ботом.\n\n"
                                    f"Назначил: {message.from_user.first_name}"
                                )
                            except:
                                pass
                        else:
                            await message.answer("❌ Не удалось добавить администратора.")
                    
                    elif action == "remove":
                        if target_id == OWNER_ID:
                            return await message.answer("❌ Нельзя удалить владельца бота.")
                        
                        if await self.db.remove_admin(target_id):
                            await message.answer(
                                f"✅ <b>Администратор удален</b>\n\n"
                                f"ID: <code>{target_id}</code>"
                            )
                        else:
                            await message.answer("❌ Не удалось удалить администратора.")
                
                except ValueError:
                    await message.answer("❌ Неверный формат ID.")
            
            elif len(text) == 2 and text[1].lower() == "list":
                admins = await self.db.get_admins()
                text = "👑 <b>Список администраторов:</b>\n\n"
                
                for i, admin_id in enumerate(admins, 1):
                    user_data = await self.db.get_user(admin_id) or {}
                    if admin_id == OWNER_ID:
                        text += f"{i}. 👑 {self.get_user_info(user_data)} | <code>{admin_id}</code> (владелец)\n"
                    else:
                        text += f"{i}. {self.get_user_info(user_data)} | <code>{admin_id}</code>\n"
                
                await message.answer(text)
        
        @self.router.message(Command("ban"))
        async def cmd_ban(message: Message):
            if not await self.db.is_admin(message.from_user.id):
                return await message.answer("❌ У вас нет прав для использования этой команды.")
            
            try:
                args = message.text.split()[1:]
                if len(args) < 2:
                    return await message.answer(
                        "❌ <b>Неверный формат команды</b>\n\n"
                        "Используйте: <code>/ban PEER_ID Причина [Время в часах]</code>\n\n"
                        "<b>Примеры:</b>\n"
                        "<code>/ban 123456 Спам</code> - бан навсегда\n"
                        "<code>/ban 123456 Флуд 24</code> - бан на 24 часа\n"
                        "<code>/ban 123456 Нарушение правил 168</code> - бан на неделю"
                    )
                
                peer_id = int(args[0])
                
                # Проверяем, не является ли пользователь администратором
                if await self.db.is_admin(peer_id):
                    return await message.answer("❌ Нельзя заблокировать администратора.")
                
                reason = " ".join(args[1:-1]) if len(args) > 2 and args[-1].isdigit() else " ".join(args[1:])
                hours = int(args[-1]) if len(args) > 2 and args[-1].isdigit() else None
                
                if hours and (hours <= 0 or hours > MAX_BAN_HOURS):
                    return await message.answer(
                        f"❌ Время бана должно быть от 1 до {MAX_BAN_HOURS} часов "
                        f"(максимум {MAX_BAN_HOURS // 24} дней)"
                    )
                
                # Сохраняем пользователя если его нет
                try:
                    user = await self.bot.get_chat(peer_id)
                    await self.db.save_user(
                        user_id=peer_id,
                        username=user.username,
                        first_name=user.first_name,
                        last_name=user.last_name
                    )
                except:
                    await self.db.save_user(user_id=peer_id)
                
                ban_until = datetime.now(timezone.utc) + timedelta(hours=hours) if hours else None
                await self.db.ban_user(peer_id, reason, ban_until)
                await self.db.update_stats(bans_issued=1)
                
                ban_duration = f"на {hours} часов ({hours // 24} дней {hours % 24} часов)" if hours else "навсегда"
                user_data = await self.db.get_user(peer_id)
                
                await message.answer(
                    f"✅ <b>Пользователь заблокирован</b>\n\n"
                    f"<b>Информация:</b> {self.get_user_info(user_data)}\n"
                    f"<b>Peer ID:</b> <code>{peer_id}</code>\n"
                    f"<b>Причина:</b> {reason}\n"
                    f"<b>Длительность:</b> {ban_duration}\n\n"
                    f"<i>Заблокировал: {message.from_user.first_name}</i>"
                )
                
                # Уведомление другим администраторам
                await self.notify_admins(
                    f"🔨 <b>Пользователь заблокирован</b>\n\n"
                    f"Пользователь: {self.get_user_info(user_data)}\n"
                    f"ID: <code>{peer_id}</code>\n"
                    f"Причина: {reason}\n"
                    f"Длительность: {ban_duration}\n"
                    f"Заблокировал: {message.from_user.first_name}",
                    exclude_user_id=message.from_user.id
                )
                
                # Уведомление заблокированного пользователя
                try:
                    await self.bot.send_message(
                        peer_id,
                        f"🚫 <b>Вы заблокированы</b>\n\n"
                        f"<b>Причина:</b> {reason}\n"
                        f"<b>Длительность:</b> {ban_duration}\n\n"
                        f"{'' if not hours else f'<b>Разблокировка:</b> {ban_until.strftime("%d.%m.%Y в %H:%M")}\n\n'}"
                        f"<i>Если вы считаете, что это ошибка, свяжитесь с администратором.</i>"
                    )
                except Exception as e:
                    logger.error(f"Не удалось уведомить пользователя {peer_id} о бане: {e}")
            
            except ValueError:
                await message.answer("❌ Неверный формат Peer ID")
            except Exception as e:
                await message.answer(f"❌ Ошибка: {str(e)}")
        
        @self.router.message(Command("unban"))
        async def cmd_unban(message: Message):
            if not await self.db.is_admin(message.from_user.id):
                return await message.answer("❌ У вас нет прав для использования этой команды.")
            
            try:
                args = message.text.split()[1:]
                if len(args) < 1:
                    return await message.answer("❌ Используйте: <code>/unban PEER_ID</code>")
                
                peer_id = int(args[0])
                user_data = await self.db.get_user(peer_id)
                
                if user_data and user_data.get('is_banned'):
                    await self.db.unban_user(peer_id)
                    
                    await message.answer(
                        f"✅ <b>Пользователь разблокирован</b>\n\n"
                        f"<b>Информация:</b> {self.get_user_info(user_data)}\n"
                        f"<b>Peer ID:</b> <code>{peer_id}</code>\n"
                        f"<b>Был заблокирован за:</b> {user_data.get('ban_reason', 'Не указано')}"
                    )
                    
                    # Уведомление другим администраторам
                    await self.notify_admins(
                        f"✅ <b>Пользователь разблокирован</b>\n\n"
                        f"Пользователь: {self.get_user_info(user_data)}\n"
                        f"ID: <code>{peer_id}</code>\n"
                        f"Разблокировал: {message.from_user.first_name}",
                        exclude_user_id=message.from_user.id
                    )
                    
                    try:
                        await self.bot.send_message(
                            peer_id,
                            "✅ <b>Вы разблокированы</b>\n\n"
                            "Теперь вы снова можете отправлять сообщения через бота."
                        )
                    except:
                        pass
                else:
                    await message.answer(f"ℹ️ Пользователь {peer_id} не заблокирован")
            
            except ValueError:
                await message.answer("❌ Неверный формат Peer ID")
        
        @self.router.message(Command("stats"))
        async def cmd_stats(message: Message):
            if not await self.db.is_admin(message.from_user.id):
                return
            
            stats = await self.db.get_stats()
            user_stats = await self.db.get_users_count()
            most_active = await self.db.get_most_active_user()
            admins = await self.db.get_admins()
            
            text = (
                f"📊 <b>Статистика бота</b>\n\n"
                f"<b>Пользователи:</b>\n"
                f"• Всего: {user_stats['total']}\n"
                f"• Активных (24ч): {user_stats['active_today']}\n"
                f"• Заблокированных: {user_stats['banned']}\n"
                f"• Администраторов: {len(admins)}\n\n"
                f"<b>Сообщения:</b>\n"
                f"• Всего отправлено: {stats['total_messages']}\n"
                f"• Успешно переслано: {stats['successful_forwards']}\n"
                f"• Ошибок при пересылке: {stats['failed_forwards']}\n"
                f"• Блокировок по лимиту: {stats['rate_limit_blocks']}\n"
                f"• Выдано банов: {stats['bans_issued']}\n\n"
            )
            
            if most_active and most_active.get('messages_sent', 0) > 0:
                text += (
                    f"<b>Самый активный:</b>\n"
                    f"• {self.get_user_info(most_active)}\n"
                    f"• Сообщений: {most_active['messages_sent']}\n"
                    f"• Первое сообщение: {most_active['created_at'].strftime('%d.%m.%Y')}\n\n"
                )
            
            text += f"<i>Обновлено: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}</i>"
            await message.answer(text)
        
        @self.router.message(Command("users"))
        async def cmd_users(message: Message):
            if not await self.db.is_admin(message.from_user.id):
                return
            
            users = await self.db.get_all_users()
            if not users:
                return await message.answer("📭 <b>Пользователей пока нет</b>")
            
            text = "👥 <b>Список пользователей:</b>\n\n"
            for i, user in enumerate(users[:50], 1):
                status = '🚫' if user.get('is_banned') else '✅'
                is_admin = await self.db.is_admin(user['user_id'])
                admin_star = '👑 ' if is_admin else ''
                text += f"{i}. {status} {admin_star}{self.get_user_info(user)} | ID: <code>{user['user_id']}</code> | Сообщений: {user.get('messages_sent', 0)}\n"
            
            if len(users) > 50:
                text += f"\n<i>Показано 50 из {len(users)} пользователей</i>"
            
            await message.answer(text)
        
        @self.router.message(Command("help"))
        async def cmd_help(message: Message):
            if await self.db.is_admin(message.from_user.id):
                await message.answer(
                    "🤖 <b>Команды администратора:</b>\n\n"
                    "👑 <b>Управление администраторами:</b>\n"
                    "• /admin - список команд\n"
                    "• /admin add ID - добавить админа\n"
                    "• /admin remove ID - удалить админа\n"
                    "• /admin list - список админов\n\n"
                    "🔨 <b>Управление пользователями:</b>\n"
                    "• /ban ID причина [часы] - заблокировать\n"
                    "• /unban ID - разблокировать\n"
                    "• /users - список пользователей\n\n"
                    "📊 <b>Статистика:</b>\n"
                    "• /stats - статистика бота\n"
                    "• /help - это сообщение\n\n"
                    f"<i>Владелец бота: {OWNER_ID}</i>"
                )
            else:
                await message.answer(
                    "🤖 <b>Команды пользователя:</b>\n\n"
                    "• /start - начать работу\n"
                    "• /help - помощь\n\n"
                    f"<b>Лимит сообщений:</b> {RATE_LIMIT_MINUTES} минут\n"
                    "Пожалуйста, пишите всё в одном сообщении."
                )
        
        @self.router.message()
        async def handle_user_message(message: Message):
            user_id = message.from_user.id
            
            # Обновляем статистику
            await self.db.update_stats(total_messages=1)
            
            # Сохраняем пользователя
            await self.save_user_from_message(message)
            
            # Проверяем блокировку
            is_banned, ban_info = await self.check_ban_status(user_id)
            if is_banned:
                user_data = await self.db.get_user(user_id)
                return await message.answer(
                    f"🚫 <b>Вы заблокированы {ban_info}</b>\n"
                    f"<b>Причина:</b> {user_data.get('ban_reason', 'Не указана')}\n\n"
                    f"<i>Если вы считаете, что это ошибка, свяжитесь с администратором.</i>"
                )
            
            # Проверяем лимит сообщений (для обычных пользователей)
            if not await self.db.is_admin(user_id):
                can_send, remaining = await self.check_rate_limit(user_id)
                if not can_send:
                    await self.db.update_stats(rate_limit_blocks=1)
                    return await message.answer(
                        f"⏳ <b>Подождите {remaining} минут</b>\n\n"
                        f"Вы можете отправить только одно сообщение за {RATE_LIMIT_MINUTES} минут.\n"
                        f"Пожалуйста, соберите все мысли в одно сообщение."
                    )
            
            try:
                # Формируем подпись для сообщения
                user_data = await self.db.get_user(user_id)
                caption = (
                    f"📩 <b>Новое сообщение от {self.get_user_info(user_data)}</b>\n"
                    f"<b>ID:</b> <code>{user_id}</code>\n"
                    f"<b>Время:</b> {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n\n"
                )
                
                # Отправляем всем администраторам
                admins = await self.db.get_admins()
                handlers = {
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
                    ContentType.VIDEO_NOTE: self.handle_video_note
                }
                
                handler = handlers.get(message.content_type, self.handle_unknown)
                
                # Отправляем сообщение каждому администратору
                success_count = 0
                for admin_id in admins:
                    try:
                        await handler(message, caption, admin_id)
                        success_count += 1
                    except Exception as e:
                        logger.error(f"Ошибка отправки админу {admin_id}: {e}")
                
                if success_count > 0:
                    # Обновляем статистику пользователя
                    await self.db.update_user_last_message(user_id, datetime.now(timezone.utc))
                    await self.db.update_user_stats(user_id, increment_messages=True)
                    await self.db.update_stats(successful_forwards=success_count)
                    
                    await message.answer(
                        "<b>✅ Сообщение успешно отправлено!</b>\n\n"
                        "<b>Важная информация:</b>\n"
                        "• Ответ поступит только в личные сообщения (ЛС)\n"
                        f"• Следующее сообщение можно отправить через {RATE_LIMIT_MINUTES} минут\n"
                        "• Пожалуйста, пишите всё в одном сообщении"
                    )
                    
                    logger.info(f"Сообщение от {user_id} успешно переслано {success_count} админам")
                else:
                    raise Exception("Не удалось отправить сообщение ни одному администратору")
            
            except Exception as e:
                logger.error(f"Ошибка при пересылке сообщения от {user_id}: {e}")
                await self.db.update_stats(failed_forwards=1)
                
                await message.answer(
                    f"❌ <b>Не удалось отправить сообщение</b>\n\n"
                    f"Произошла техническая ошибка. Пожалуйста:\n"
                    f"1. Попробуйте отправить сообщение позже\n"
                    f"2. Убедитесь, что сообщение не слишком большое\n"
                    f"3. Если ошибка повторяется, свяжитесь с администратором\n\n"
                    f"<i>Код ошибки: {type(e).__name__}</i>"
                )
                
                # Уведомление администраторам об ошибке
                await self.notify_admins(
                    f"⚠️ <b>Ошибка при получении сообщения</b>\n\n"
                    f"<b>От:</b> {self.get_user_info(user_data)}\n"
                    f"<b>ID:</b> <code>{user_id}</code>\n"
                    f"<b>Тип ошибки:</b> {type(e).__name__}\n"
                    f"<b>Описание:</b> {str(e)[:200]}"
                )
    
    async def handle_text(self, message: Message, caption: str, admin_id: int):
        await self.bot.send_message(admin_id, caption + f"💬 <b>Текст:</b>\n{message.text}")
    
    async def handle_photo(self, message: Message, caption: str, admin_id: int):
        await self.bot.send_photo(
            admin_id,
            message.photo[-1].file_id,
            caption=caption + "🖼 <b>Фото</b>" + 
                    (f"\n\n<b>Подпись:</b>\n{message.caption}" if message.caption else "")
        )
    
    async def handle_video(self, message: Message, caption: str, admin_id: int):
        await self.bot.send_video(
            admin_id,
            message.video.file_id,
            caption=caption + "🎬 <b>Видео</b>" + 
                    (f"\n\n<b>Подпись:</b>\n{message.caption}" if message.caption else "")
        )
    
    async def handle_voice(self, message: Message, caption: str, admin_id: int):
        await self.bot.send_voice(
            admin_id,
            message.voice.file_id,
            caption=caption + "🎤 <b>Голосовое сообщение</b>" + 
                    (f"\n\n<b>Подпись:</b>\n{message.caption}" if message.caption else "")
        )
    
    async def handle_audio(self, message: Message, caption: str, admin_id: int):
        await self.bot.send_audio(
            admin_id,
            message.audio.file_id,
            caption=caption + "🎵 <b>Аудио</b>" + 
                    (f"\n\n<b>Подпись:</b>\n{message.caption}" if message.caption else "")
        )
    
    async def handle_document(self, message: Message, caption: str, admin_id: int):
        await self.bot.send_document(
            admin_id,
            message.document.file_id,
            caption=caption + "📎 <b>Документ</b>" + 
                    (f"\n\n<b>Подпись:</b>\n{message.caption}" if message.caption else "")
        )
    
    async def handle_location(self, message: Message, caption: str, admin_id: int):
        await self.bot.send_location(
            admin_id,
            message.location.latitude,
            message.location.longitude
        )
        await self.bot.send_message(admin_id, caption + "📍 <b>Геолокация</b>")
    
    async def handle_contact(self, message: Message, caption: str, admin_id: int):
        await self.bot.send_contact(
            admin_id,
            message.contact.phone_number,
            message.contact.first_name,
            last_name=message.contact.last_name
        )
        await self.bot.send_message(admin_id, caption + "👤 <b>Контакт</b>")
    
    async def handle_sticker(self, message: Message, caption: str, admin_id: int):
        await self.bot.send_sticker(admin_id, message.sticker.file_id)
        await self.bot.send_message(admin_id, caption + "😊 <b>Стикер</b>")
    
    async def handle_animation(self, message: Message, caption: str, admin_id: int):
        await self.bot.send_animation(
            admin_id,
            message.animation.file_id,
            caption=caption + "🎭 <b>GIF</b>" + 
                    (f"\n\n<b>Подпись:</b>\n{message.caption}" if message.caption else "")
        )
    
    async def handle_video_note(self, message: Message, caption: str, admin_id: int):
        await self.bot.send_video_note(admin_id, message.video_note.file_id)
        await self.bot.send_message(admin_id, caption + "📹 <b>Видеосообщение</b>")
    
    async def handle_unknown(self, message: Message, caption: str, admin_id: int):
        await self.bot.send_message(admin_id, caption + "❓ <b>Неизвестный тип сообщения</b>")
        await self.bot.send_message(admin_id, f"⚠️ Получен неподдерживаемый тип контента: {message.content_type}")
    
    async def start_keep_alive_server(self):
        """Запуск keep-alive сервера"""
        app = create_keep_alive_server(KEEP_ALIVE_PORT)
        runner = web.AppRunner(app)
        await runner.setup()
        await web.TCPSite(runner, '0.0.0.0', KEEP_ALIVE_PORT).start()
        logger.info(f"Keep-alive сервер запущен на порту {KEEP_ALIVE_PORT}")
        return runner
    
    async def shutdown(self, sig=None):
        """Грациозное завершение работы"""
        logger.info(f"Получен сигнал {sig}, завершение работы...")
        self.is_running = False
        await self.dp.stop_polling()
        await self.bot.session.close()
        await self.db.close()
    
    async def run(self):
        """Запуск бота"""
        try:
            # Настройка сигналов для Unix
            if sys.platform != 'win32':
                loop = asyncio.get_running_loop()
                for sig in [signal.SIGTERM, signal.SIGINT]:
                    loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(self.shutdown(s)))
            
            # Запуск keep-alive сервера
            runner = await self.start_keep_alive_server()
            
            logger.info("🤖 Бот запускается...")
            logger.info(f"👑 Владелец бота: {OWNER_ID}")
            logger.info(f"⏱ Лимит сообщений: {RATE_LIMIT_MINUTES} минут")
            logger.info(f"🌐 Keep-alive порт: {KEEP_ALIVE_PORT}")
            
            # Основной цикл
            while self.is_running:
                try:
                    await self.dp.start_polling(self.bot)
                except Exception as e:
                    logger.error(f"Ошибка polling: {e}")
                    if self.is_running:
                        logger.info("Перезапуск через 5 секунд...")
                        await asyncio.sleep(5)
                    else:
                        break
        except Exception as e:
            logger.error(f"Критическая ошибка: {e}")
        finally:
            await self.bot.session.close()
            await self.db.close()
            await runner.cleanup()

def main():
    """Точка входа"""
    # Проверка наличия токена
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    if not BOT_TOKEN:
        logger.error("❌ Не найден BOT_TOKEN в переменных окружения!")
        return
    if ":" not in BOT_TOKEN:
        logger.error("❌ Неверный формат BOT_TOKEN!")
        return
    
    # Проверка наличия DATABASE_URL
    DATABASE_URL = os.getenv("DATABASE_URL")
    if not DATABASE_URL:
        logger.error("❌ Не найден DATABASE_URL в переменных окружения!")
        logger.error("📌 Добавьте переменную DATABASE_URL в настройках Render (Internal Database)")
        return
    
    # Инициализация и запуск
    async def run_bot():
        db = Database(DATABASE_URL)
        await db.create_pool()
        bot = MessageForwardingBot(BOT_TOKEN, db)
        await bot.run()
    
    try:
        asyncio.run(run_bot())
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"Фатальная ошибка: {e}")

if __name__ == "__main__":
    main()
