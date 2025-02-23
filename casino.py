import logging
import asyncio
import aiosqlite
import time  
import requests
import json
import aiohttp 
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, InputFile, ContentTypes
from aiogram.contrib.middlewares.logging import LoggingMiddleware
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import StatesGroup, State
from aiogram.contrib.fsm_storage.memory import MemoryStorage  
from aiogram.utils import executor
from aiogram.dispatcher.middlewares import BaseMiddleware
from aiogram.dispatcher.handler import CancelHandler
from aiogram.dispatcher.filters import BoundFilter
from aiogram.utils import exceptions
from aiogram.utils.exceptions import MessageNotModified, BadRequest, ChatNotFound, RetryAfter, MessageCantBeEdited, MessageNotModified

import random
from aiogram.utils.markdown import escape_md
import os
import sys
import shutil
import re
from aiogram.utils.callback_data import CallbackData









BOT_TOKEN = "7738821678:AAF6pIQUHrXcs_VleVMazfxjRBfIPaeAOkg"
AI_API_KEY = "sk-gwKRrMg1LnmDkNphFw3F7ckzFyxYyqpKb0XtiYC6xGTrZbZC"
CRYPTO_BOT_TOKEN = "327917:AAcX3wHCaajQ8WJRBVsvXNLape9czGc2c2i"
CRYPTO_BOT_API_URL = "https://pay.crypt.bot/api/"
SUPPORT_CHAT_ID = -1002495174863
REFERRAL_BONUS = 10
REFERRAL_INITIAL = 5
MIN_AMOUNT_USD = 0.02
MINIMAL_DEPOSIT_RUB = 20

DB_NAME = "bot.db"
BACKUP_DIR = 'backups'
MAX_BACKUPS = 1


game_lock = asyncio.Lock()
temp_payments = {}
notified_trial = set()
notified_paid = set()



bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot, storage=MemoryStorage())
dp.middleware.setup(LoggingMiddleware())


class States(StatesGroup):
    SELECT_GAME = State()
    ENTER_BET = State()
    ENTER_DEPOSIT_AMOUNT = State()
    ENTER_AMOUNT = State()
    DEPOSIT_PHOTO = State()
    SUPPORT_MESSAGE = State()
    ADMIN_REPLY = State()
    WITHDRAW_AMOUNT = State()
    WITHDRAW_DETAILS = State()
    BROADCAST_MESSAGE = State()
    GAME_LOOP = State()
    USER_PROFILE = State()
    EDIT_BALANCE = State()
    ACTIVATE_PROMO = State()
    ADMIN_MESSAGE = State()
    LUCKY_JET_BET = State()
    LUCKY_JET_GAME = State()
    SELECT_PAYMENT_METHOD = State()
    SELECT_CURRENCY = State()
    CHECK_PAYMENT = State()
    SELECT_CASE = State()
    CASE_DETAILS = State()
    EDIT_LUCKY_NUMBER = State()
    EDIT_LUCKY_CHANCE = State()
    waiting_for_appeal = State()
    
    


class BanCheckMiddleware(BaseMiddleware):
    def __init__(self):
        super().__init__()
        self.checked_users = set()

    async def auto_unban_loop(self):
        while True:
            try:
                users = await fetch_query(
                    "SELECT user_id, username, ban_reason, ban_until, ban_time FROM users WHERE banned = 1 AND ban_until IS NOT NULL"
                )
                for user in users:
                    if datetime.now() >= datetime.strptime(user['ban_until'], '%Y-%m-%d %H:%M:%S'):
                        await execute_query(
                            "UPDATE users SET banned=0, ban_reason=NULL, ban_time=NULL, ban_until=NULL WHERE user_id=?",
                            (user['user_id'],)
                        )
                        try:
                            await bot.send_message(user['user_id'], "✅ Вы разблокированы!")
                            username = f"@{user['username']}" if user['username'] else f"ID: {user['user_id']}"
                            ban_time = self.format_ban_time(user['ban_time'])
                            ban_reason = user['ban_reason'] if user['ban_reason'] else "не указана"
                            ban_until = datetime.strptime(user['ban_until'], '%Y-%m-%d %H:%M:%S').strftime('%d.%m.%Y %H:%M')
                            support_message = (
                                f"✅ Пользователь {username} разбанен\n"
                                f"🆔 ID: {user['user_id']}\n"
                                f"⏳ Время бана: {ban_time}\n"
                                f"💬 Причина: {ban_reason}\n"
                                f"📅 Дата окончания: {ban_until}"
                            )
                            profile_button = InlineKeyboardMarkup().add(
                                InlineKeyboardButton(
                                    "👤 Профиль",
                                    callback_data=f"user_profile_{user['user_id']}"
                                )
                            )
                            await bot.send_message(SUPPORT_CHAT_ID, support_message, reply_markup=profile_button)
                        except Exception as e:
                            logging.error(f"Unban message error: {e}")
            except Exception as e:
                logging.error(f"Auto-unban error: {e}")
            await asyncio.sleep(3)

    def format_ban_time(self, ban_time):
        import re

        def declension(n, forms):
            n = abs(n) % 100
            n1 = n % 10
            if 11 <= n <= 19:
                return forms[2]
            if n1 == 1:
                return forms[0]
            if 2 <= n1 <= 4:
                return forms[1]
            return forms[2]

        if ban_time == "permanent":
            return "навсегда"

        m = re.match(r"(\d+)([smhd])", ban_time)
        if m:
            value = int(m.group(1))
            unit = m.group(2)
            if unit == "s":
                return f"{value} {declension(value, ('секунда', 'секунды', 'секунд'))}"
            elif unit == "m":
                return f"{value} {declension(value, ('минута', 'минуты', 'минут'))}"
            elif unit == "h":
                return f"{value} {declension(value, ('час', 'часа', 'часов'))}"
            elif unit == "d":
                return f"{value} {declension(value, ('день', 'дня', 'дней'))}"
        return ban_time

    async def on_pre_process_message(self, message: types.Message, data: dict):
        state = dp.current_state(user=message.from_user.id, chat=message.chat.id)
        current_state = await state.get_state()
        
        if current_state and "waiting_for_appeal" in current_state:
            return
        if message.chat.id == SUPPORT_CHAT_ID or not message.from_user:
            return
        user = await fetch_query(
            'SELECT banned FROM users WHERE user_id = ?',
            (message.from_user.id,),
            fetch_one=True
        )
        if user and user.get('banned'):
            raise CancelHandler()

    async def on_pre_process_callback_query(self, callback: types.CallbackQuery, data: dict):
       
        if callback.data and callback.data.startswith("appeal_"):
            return
        if not callback.message or callback.message.chat.id == SUPPORT_CHAT_ID:
            return
        state = dp.current_state(user=callback.from_user.id, chat=callback.message.chat.id)
        current_state = await state.get_state()
        if current_state and "waiting_for_appeal" in current_state:
            return
        user = await fetch_query(
            'SELECT banned, ban_reason, ban_until, ban_time FROM users WHERE user_id = ?',
            (callback.from_user.id,),
            fetch_one=True
        )
        if user and user.get('banned'):
            text = (
                f"🚫 Вы забанены!\n"
                f"⏳ Время бана: {self.format_ban_time(user['ban_time'])}\n"
                f"💬 Причина: {user['ban_reason'] or 'не указана'}"
            )
            try:
                await callback.answer(text, show_alert=True, cache_time=5)
            except Exception as e:
                logging.error(f"Callback error: {e}")
            raise CancelHandler()




class ActivityMiddleware(BaseMiddleware):
    async def on_pre_process_message(self, message: types.Message, data: dict):
        
        if message.chat.id == SUPPORT_CHAT_ID:
            return
            
        if message.from_user:
            await self._handle_activity(message.from_user.id)

    async def on_pre_process_callback_query(self, callback: types.CallbackQuery, data: dict):
        
        if callback.message and callback.message.chat.id == SUPPORT_CHAT_ID:
            return
            
        if callback.from_user:
            await self._handle_activity(callback.from_user.id)

    async def _handle_activity(self, user_id: int):
        try:
            utc_time = datetime.utcnow()
            corrected_time = (utc_time + timedelta(hours=3)).strftime('%Y-%m-%d %H:%M:%S')
            
            async with aiosqlite.connect(DB_NAME) as db:
                cursor = await db.execute("SELECT banned FROM users WHERE user_id = ?", (user_id,))
                user_data = await cursor.fetchone()
                
                if user_data and not user_data[0]:
                    await db.execute(
                        """INSERT INTO activity (user_id, last_activity)
                           VALUES (?, ?)
                           ON CONFLICT(user_id) DO UPDATE 
                           SET last_activity = ?""",
                        (user_id, corrected_time, corrected_time)
                    )
                    await db.commit()
                    
        except Exception as e:
            logging.error(f"Activity error: {str(e)}")
            print(f"DEBUG | Failed to update activity: {str(e)}")




                    
class AdvancedSpamMiddleware(BaseMiddleware):
    def __init__(self):
        self.users = {}
        super().__init__()

    async def on_pre_process_message(self, message: types.Message, data: dict):
        await self._handle_request(message.from_user.id, "message", message)

    async def on_pre_process_callback_query(self, query: types.CallbackQuery, data: dict):
        await self._handle_request(query.from_user.id, "button", query)

    async def _handle_request(self, user_id: int, action_type: str, event: types.Message | types.CallbackQuery):
        if isinstance(event, types.Message):
            if event.chat.id == SUPPORT_CHAT_ID:
                return
        elif isinstance(event, types.CallbackQuery):
            if event.message and event.message.chat.id == SUPPORT_CHAT_ID:
                return
        now = time.time()
        if user_id not in self.users:
            self.users[user_id] = {
                "message_last_action": now,
                "button_last_action": now,
                "message_timeout": 1,
                "button_timeout": 0.6,  
                "message_spam": 0,
                "button_spam": 0,
                "blocked_until": None
            }
            return
        user = self.users[user_id]
        if user["blocked_until"]:
            if now < user["blocked_until"]:
                if isinstance(event, types.CallbackQuery):
                    await event.answer("🚫 Вы ограничены на 1 минуту", show_alert=True)
                raise CancelHandler()
            user.update({
                "message_timeout": 1,
                "button_timeout": 0.6,
                "message_spam": 0,
                "button_spam": 0,
                "blocked_until": None
            })
        if action_type == "message":
            time_diff = now - user["message_last_action"]
            current_timeout = user["message_timeout"]
        else:
            time_diff = now - user["button_last_action"]
            current_timeout = user["button_timeout"]
        if time_diff < current_timeout:
            if action_type == "message":
                user["message_spam"] += 1
                spam_count = user["message_spam"]
                limit = 10
            else:
                user["button_spam"] += 1
                spam_count = user["button_spam"]
                limit = 20
            if spam_count >= limit:
                user["blocked_until"] = now + 60
                raw_username = f"@{event.from_user.username}" if event.from_user.username else "Нет username"
                escaped_username = raw_username.replace("_", "\\_")
                text = (
                    f"*Ограничение пользователя на минуту за спам*\n\n"
                    f"👤 Пользователь: {escaped_username}\n"
                    f"🆔 ID: `{user_id}`\n"
                    f"🚨 Количество нарушений: `{spam_count}`"
                )
                keyboard = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton("👤 Профиль", callback_data=f"user_profile_{user_id}")]
                ])
                await bot.send_message(SUPPORT_CHAT_ID, text, reply_markup=keyboard, parse_mode="Markdown")
                if isinstance(event, types.Message):
                    await event.answer("🚫 Вы ограничены на 1 минуту")
                raise CancelHandler()
            if action_type == "message":
                display_timeout = user["message_timeout"]
            else:
                
                display_timeout = int(user["button_timeout"] - 0.6) + 1
            timeout_display = str(int(display_timeout)) if float(display_timeout).is_integer() else f"{display_timeout:.1f}"
            warning_msg = f"⚠️ Слишком много {'сообщений' if action_type == 'message' else 'нажатий'}! Подождите {timeout_display} сек"
            if isinstance(event, types.Message):
                await event.answer(warning_msg)
            elif isinstance(event, types.CallbackQuery):
                await event.answer(warning_msg, show_alert=True)
            if action_type == "message":
                user["message_timeout"] = min(user["message_timeout"] + 1, 5)
                user["message_last_action"] = now
            else:
                user["button_timeout"] = min(user["button_timeout"] + 1, 5)
                user["button_last_action"] = now
            raise CancelHandler()
        else:
            if time_diff > 10:
                if action_type == "message":
                    user["message_timeout"] = 1
                    user["message_spam"] = 0
                else:
                    user["button_timeout"] = 0.6
                    user["button_spam"] = 0
            if action_type == "message":
                user["message_last_action"] = now
            else:
                user["button_last_action"] = now



dp.middleware.setup(ActivityMiddleware())
dp.middleware.setup(AdvancedSpamMiddleware())

            
                        
                                    
                                                            
            
            
GAME_RULES = {
    'luckyjet': (
        "✈️ **Правила Lucky Jet**\n\n"
        "1. Самолет стартует с множителем x1.00\n"
        "2. Множитель растет каждую секунду\n"
        "3. Нажмите кнопку Стоп до остановки множителя\n"
        "4. Выигрыш = ставка × текущий множитель\n\n"
        "💰 Баланс: {balance:.1f}₽\n"
        "💶 Введите сумму ставки:"
    ),
    'dice': (
        "🎲 **Правила игры в кубик**\n"
        "Выигрыш если выпадет 4, 5 или 6\n"
        "Проигрыш если 1, 2 или 3\n\n"
        "💰 Баланс: {balance:.1f}₽\n"
        "💶 Введите сумму ставки:"
    ),
    'football': (
        "⚽ **Правила футбола**\n"
        "Выигрыш если мяч попадет в ворота\n"
        "Проигрыш в любом другом случае\n\n"
        "💰 Баланс: {balance:.1f}₽\n"
        "💶 Введите сумму ставки:"
    ),
    'basketball': (
        "🏀 **Правила баскетбола**\n"
        "Выигрыш если мяч попадет в кольцо\n"
        "Проигрыш в любом другом случае\n\n"
        "💰 Баланс: {balance:.1f}₽\n"
        "💶 Введите сумму ставки:"
    ),
    'darts': (
        "🎯 **Правила Дартса**\n\n"
        "Выигрыш если дротик попадет в яблочко\n"
        "Проигрыш в любом другом случае\n\n"
        "💰 Баланс: {balance:.1f}₽\n"
        "💶 Введите сумму ставки:"
    ),
    'bowling': (
        "🎳 **Правила Боулинга**\n\n"
        "Выигрыш если выбит страйк\n"
        "Проигрыш в любом другом случае\n\n"
        "💰 Баланс: {balance:.1f}₽\n"
        "💶 Введите сумму ставки:"
    ),
    'roulette': (
        "🎰 **Правила Слотов**\n\n"
		"Выигрыш, если все три символа одинаковые\n"
		"Проигрыш в любом другом случае\n"
        "Коэффициент выигрыша: 14х\n\n"
        "💰 Баланс: {balance:.1f}₽\n"
        "💶 Введите сумму ставки:"
    )
}



    	
 
 
CASES = {
    'simple': {
        'name': 'Простой',
        'price': 5,
        'min': 1,
        'max': 50,
        'rewards': [1, 5, 10, 25, 50],
        'probabilities':  [65, 25, 5, 3, 2]  
    },
    'classic': {
        'name': 'Классический', 
        'price': 10,
        'min': 2,
        'max': 100,
        'rewards': [2, 10, 25, 50, 100],
        'probabilities':  [65, 20, 8, 4, 3] 
    },
    'medium': {
        'name': 'Средний',
        'price': 50,
        'min': 10,
        'max': 500,
        'rewards': [10, 50, 100, 250, 500],
        'probabilities':  [65, 25, 5, 3, 2]  
    },
    'premium': {
        'name': 'Дорогой',
        'price': 200,
        'min': 20,
        'max': 2000,
        'rewards': [20, 100, 500, 1000, 2000],
        'probabilities':  [65, 25, 5, 3, 2]  
    },
    'exclusive': {
        'name': 'Эксклюзивный',
        'price': 500,
        'min': 100,
        'max': 5000,
        'rewards': [100, 500, 1000, 2500, 5000],
        'probabilities':  [65, 25, 5, 3, 2]  
    }
}




async def init_db():
    async with aiosqlite.connect(DB_NAME, timeout=10.0) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA foreign_keys = ON")

        await db.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                balance REAL DEFAULT 0.0,
                banned BOOLEAN DEFAULT FALSE,
                ban_reason TEXT,
                ban_time TEXT,
                ban_until TEXT,
                referral_used BOOLEAN DEFAULT FALSE,
                referred_by INTEGER,
                request_counter INTEGER DEFAULT 0,
                registration_date TEXT DEFAULT (STRFTIME('%d.%m.%Y', 'now', 'localtime')),
                lucky_number TEXT DEFAULT 'default',
                lucky_chance TEXT DEFAULT 'default'
            )''')

        await db.execute('''
            CREATE TABLE IF NOT EXISTS activity (
                user_id INTEGER PRIMARY KEY,
                last_activity TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S', 'now', 'localtime')),
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            )''')

        await db.execute('''
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                description TEXT,
                amount INTEGER,
                date TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )''')

        await db.execute('''
            CREATE TABLE IF NOT EXISTS withdraw_requests (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                amount INTEGER,
                details TEXT,
                status TEXT DEFAULT 'pending',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )''')

        await db.execute('''
            CREATE TABLE IF NOT EXISTS payment_requests (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                amount INTEGER,
                photo TEXT,
                status TEXT DEFAULT 'pending',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )''')

        await db.execute('''
            CREATE TABLE IF NOT EXISTS promocodes (
                code TEXT PRIMARY KEY,
                amount INTEGER,
                max_uses INTEGER,
                uses_left INTEGER,
                message TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )''')

        await db.execute('''
            CREATE TABLE IF NOT EXISTS used_promocodes (
                user_id INTEGER,
                code TEXT,
                used_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, code),
                FOREIGN KEY (user_id) REFERENCES users(user_id),
                FOREIGN KEY (code) REFERENCES promocodes(code)
            )''')

        await db.execute('''
            CREATE TABLE IF NOT EXISTS admin_actions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                admin_id INTEGER,
                action_type TEXT,
                target_user INTEGER,
                details TEXT,
                media_data TEXT,
                timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (admin_id) REFERENCES users(user_id),
                FOREIGN KEY (target_user) REFERENCES users(user_id)
            )''')

        await db.execute('''
            CREATE TABLE IF NOT EXISTS all_games (
                user_id INTEGER,
                game_name TEXT,
                play_count INTEGER DEFAULT 0,
                PRIMARY KEY (user_id, game_name),
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )''')

        await db.execute('''
            CREATE TABLE IF NOT EXISTS favorite_games (
                user_id INTEGER PRIMARY KEY,
                game_name TEXT,
                play_count INTEGER DEFAULT 0,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )''')

        await db.execute('''
            CREATE TABLE IF NOT EXISTS subscriptions (
                user_id INTEGER PRIMARY KEY,
                start_time TEXT,
                end_time TEXT,
                is_trial BOOLEAN DEFAULT FALSE,
                trial_activated BOOLEAN DEFAULT FALSE,
                next_activation_date TEXT,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )''')

        await db.execute('''
            CREATE TABLE IF NOT EXISTS private_channel_purchases (
                user_id INTEGER PRIMARY KEY,
                amount INTEGER,
                purchase_date TEXT,
                status TEXT,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )''')

        await db.execute('''
            CREATE TABLE IF NOT EXISTS bot_info (
                first_start TEXT,
                creator TEXT
            )''')

        await migrate_table(db)
        await db.commit()



async def migrate_table(db):
    try:
        await db.execute('ALTER TABLE users ADD COLUMN ban_time TEXT')
    except aiosqlite.OperationalError:
        pass
    
    try:
        await db.execute('ALTER TABLE users ADD COLUMN ban_until TEXT')
    except aiosqlite.OperationalError:
        pass

    try:
        await db.execute('ALTER TABLE bot_info ADD COLUMN first_start TEXT')
    except aiosqlite.OperationalError:
        pass

    try:
        await db.execute('ALTER TABLE bot_info ADD COLUMN creator TEXT')
    except aiosqlite.OperationalError:
        pass

        
        

async def backup_db():
    try:
        if not os.path.exists(BACKUP_DIR):
            os.makedirs(BACKUP_DIR)

       
        backups = [os.path.join(BACKUP_DIR, f) for f in os.listdir(BACKUP_DIR) if f.endswith('.db')]
        backups.sort(reverse=True)  
        for old_backup in backups[MAX_BACKUPS-1:]:
            os.remove(old_backup)
            logging.info(f"Удален старый бэкап: {old_backup}")

        now = datetime.now()
        backup_name = f"backup_{now.strftime('%Y%m%d_%H%M%S')}.db"
        backup_path = os.path.join(BACKUP_DIR, backup_name)
        
        if not os.path.exists(DB_NAME):
            raise FileNotFoundError(f"Файл базы данных {DB_NAME} не найден")

        shutil.copy(DB_NAME, backup_path)
        logging.info(f"Бэкап базы данных создан: {backup_path}")
        
        if not os.path.exists(backup_path):
            raise IOError(f"Файл резервной копии {backup_path} не был создан")
        
        return True
    except Exception as e:
        logging.error(f"Ошибка при создании бэкапа базы данных: {str(e)}")
        return False
        
        

async def scheduled_hourly_backup():
    while True:
        await backup_db() 
        await asyncio.sleep(3600)
        
        
        
        
async def update_game_count(user_id: int, game_name: str):
    game_translation = {
    'football': '⚽ Футбол',
    'basketball': '🏀 Баскетбол',
    'darts': '🎯 Дартс',
    'bowling': '🎳 Боулинг',
    'dice': '🎲 Кубик',
    'luckyjet': '✈ Lucky Jet'
}
    
    
    translated_game_name = game_translation.get(game_name.lower(), game_name)

    async with aiosqlite.connect(DB_NAME, timeout=10.0) as db:
        await db.execute("BEGIN TRANSACTION")
        try:
            game_exists = await db.execute_fetchall('SELECT play_count FROM all_games WHERE user_id = ? AND game_name = ?', (user_id, translated_game_name))
            
            if game_exists:
                current_count = game_exists[0][0]
                new_count = current_count + 1
                await db.execute('UPDATE all_games SET play_count = ? WHERE user_id = ? AND game_name = ?', (new_count, user_id, translated_game_name))
            else:
                await db.execute('INSERT INTO all_games (user_id, game_name, play_count) VALUES (?, ?, 1)', (user_id, translated_game_name))

            await update_favorite_game(db, user_id)
            await db.commit()
        except aiosqlite.OperationalError as e:
            logging.error(f"Ошибка в транзакции: {str(e)}")
            await db.rollback()
            raise

async def update_favorite_game(db: aiosqlite.Connection, user_id: int):
    favorite = await db.execute_fetchall('''
        SELECT game_name, play_count FROM all_games 
        WHERE user_id = ? 
        ORDER BY play_count DESC LIMIT 1
    ''', (user_id,))
    
    if favorite:
        await db.execute('''
            INSERT OR REPLACE INTO favorite_games (user_id, game_name, play_count) 
            VALUES (?, ?, ?)
        ''', (user_id, favorite[0][0], favorite[0][1]))
        
                        
        
        

async def execute_query(query: str, params: tuple = None):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(query, params or ())
        await db.commit()


async def fetch_query(query: str, params: tuple = (), fetch_one: bool = False):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        try:
            cursor = await db.execute(query, params)
            if fetch_one:
                result = await cursor.fetchone()
                return dict(result) if result else {}
            else:
                results = await cursor.fetchall()
                return [dict(row) for row in results] if results else []
        except Exception as e:
            logging.error(f"Ошибка запроса: {str(e)}")
            return {} if fetch_one else []
        finally:
            await cursor.close()


async def get_balance(user_id: int) -> float:
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute('SELECT balance FROM users WHERE user_id = ?', (user_id,))
        result = await cursor.fetchone()
        return round(result[0], 1) if result else 0.0
        
        
async def update_balance(user_id: int, amount: int):
    await execute_query('UPDATE users SET balance = balance + ? WHERE user_id = ?', (amount, user_id))


async def add_transaction(user_id: int, description: str, amount: int):
    await execute_query('''INSERT INTO transactions (user_id, description, amount, date)
        VALUES (?, ?, ?, ?)''', (user_id, description, amount, datetime.now().strftime("%d.%m %H:%M")))








@dp.message_handler(commands=["start"], state="*")
async def cmd_start(message: types.Message, state: FSMContext):
    await state.finish()
    user = message.from_user
    user_id = user.id
    username = user.username or ""
    try:
        registration_date = (datetime.utcnow() + timedelta(hours=3)).strftime('%d.%m.%Y')
        current_time = (datetime.utcnow() + timedelta(hours=3)).strftime('%Y-%m-%d %H:%M:%S')
        async with aiosqlite.connect(DB_NAME) as db:
            user_record = await fetch_query('SELECT * FROM users WHERE user_id = ?', (user_id,), fetch_one=True)
            is_new = not user_record
            
            if user_record:
                await db.execute("UPDATE users SET username = ? WHERE user_id = ?", (username, user_id))
            else:
                await db.execute(
                    "INSERT INTO users (user_id, username, registration_date) VALUES (?, ?, ?)",
                    (user_id, username, registration_date)
                )
            
            await db.execute(
                """
                INSERT INTO activity (user_id, last_activity)
                VALUES (?, ?)
                ON CONFLICT(user_id) DO UPDATE SET last_activity = ?
                """,
                (user_id, current_time, current_time)
            )
            await db.commit()
            user_record = await fetch_query('SELECT * FROM users WHERE user_id = ?', (user_id,), fetch_one=True) 
        
        text_parts = message.text.split()
        if len(text_parts) > 1:
            await process_referral(user_id=user_id, referrer_str=text_parts[1], is_new=is_new)
            user_record = await fetch_query('SELECT * FROM users WHERE user_id = ?', (user_id,), fetch_one=True)  
        await send_main_menu(message, user_record, False)

        if is_new:
            profile_btn = InlineKeyboardButton(text="📊 Профиль", callback_data=f"user_profile_{user_id}")
            keyboard = InlineKeyboardMarkup().add(profile_btn)
            
            asyncio.create_task(bot.send_message(
                SUPPORT_CHAT_ID,
                f"🎉 Новый пользователь!\n"
                f"🆔 ID: {user_id}\n"
                f"📅 Дата регистрации: {registration_date}\n"
                f"📧 Юзернейм: @{username or 'нет'}",
                reply_markup=keyboard
            ))
    except Exception as e:
        logging.error(f"Start error: {str(e)}")
        await message.answer("⚠️ Произошла ошибка, попробуйте позже")
        
        
        

async def process_referral(user_id: int, referrer_str: str, is_new: bool):
    try:
        if not is_new:
            await bot.send_message(user_id, "❌ Рефералы доступны только новым пользователям!")
            return

        existing_ref = await fetch_query('SELECT referral_used FROM users WHERE user_id = ?', (user_id,), fetch_one=True)
        if existing_ref and existing_ref.get('referral_used') == 1:
            await bot.send_message(user_id, "❌ Реферальная ссылка уже была использована!")
            return

        referrer_id = int(referrer_str)
        if referrer_id == user_id:
            await bot.send_message(user_id, "❌ Нельзя использовать свою реферальную ссылку!")
            return

        referrer_exists = await fetch_query('SELECT 1 FROM users WHERE user_id = ?', (referrer_id,), fetch_one=True)
        if not referrer_exists:
            await bot.send_message(user_id, "⚠️ Недействительная реферальная ссылка!")
            return

        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute('''
                UPDATE users 
                SET balance = balance + ?, 
                    referral_used = 1 
                WHERE user_id = ?
            ''', (REFERRAL_INITIAL, user_id))
            
            referral_bonus = REFERRAL_BONUS
            subscription = await fetch_query('''
                SELECT end_time FROM subscriptions 
                WHERE user_id = ? AND end_time > CURRENT_TIMESTAMP
            ''', (referrer_id,), fetch_one=True)
            
            if subscription:
                referral_bonus *= 2
            
            await db.execute('''
                UPDATE users 
                SET balance = balance + ? 
                WHERE user_id = ?
            ''', (referral_bonus, referrer_id))
            await db.commit()
            
            await bot.send_message(referrer_id, f"🎉 Новый реферал!\nНачислено {referral_bonus}₽ на баланс!")
    
    except ValueError:
        await bot.send_message(user_id, "⚠️ Некорректный формат реферальной ссылки")
    except Exception as e:
        logging.error(f"Referral processing error: {str(e)}")
        await bot.send_message(user_id, "⚠️ Ошибка обработки реферальной программы")
        
        

async def send_main_menu(message: types.Message, user_data: dict, ref_processed: bool):
    user_id = message.from_user.id
    balance = await get_balance(user_id)
    reg_date_str = user_data.get('registration_date', 'Неизвестно')
    bot_username = (await message.bot.get_me()).username
    days_ago = ""
    subscription = await fetch_query("SELECT end_time FROM subscriptions WHERE user_id = ?", (user_id,), fetch_one=True)
    has_subscription = False
    subscription_status = "не активирована"
    if subscription:
        try:
            end_time = datetime.strptime(subscription['end_time'], '%Y-%m-%d %H:%M:%S')
            if end_time > datetime.now():
                has_subscription = True
                subscription_status = "активирована"
        except Exception:
            pass
    if reg_date_str != 'Неизвестно':
        try:
            reg_date = datetime.strptime(reg_date_str, "%d.%m.%Y")
            days_ago = f" ({(datetime.now() - reg_date).days} д. назад)"
        except ValueError:
            days_ago = " (Неверный формат даты)"
    try:
        balance = float(balance)
        formatted_balance = f"{int(balance)}₽" if balance.is_integer() else f"{balance:.1f}₽"
    except (ValueError, TypeError):
        formatted_balance = "Ошибка баланса"
    text = (
        f"👋 Привет, {message.from_user.first_name}!\n"
        f"💰 Баланс: {formatted_balance}\n"
        f"📅 Дата регистрации: {reg_date_str}{days_ago}\n"
        f"🔗 Ваша реферальная ссылка: t.me/{bot_username}?start={user_id}\n"
        f"💎 Подписка: {subscription_status}"
    )
    keyboard = InlineKeyboardMarkup()
    keyboard.row(InlineKeyboardButton("🎮 Играть", callback_data="play"), InlineKeyboardButton("📦 Кейсы", callback_data="cases"))
    keyboard.row(InlineKeyboardButton("💳 Пополнить", callback_data="deposit"), InlineKeyboardButton("📤 Вывод", callback_data="withdraw"))
    keyboard.row(InlineKeyboardButton("📊 Профиль", callback_data="profile"), InlineKeyboardButton("📞 Поддержка", callback_data="support"))
    keyboard.row(InlineKeyboardButton("🎁 Активировать промокод", callback_data="activate_promo"), InlineKeyboardButton("📚 FAQ", url="https://telegra.ph/Kapel-kazino-01-23"))
    keyboard.row(InlineKeyboardButton("🔒 Приватный канал", callback_data="private_channel"))
    buttons = [InlineKeyboardButton("💎 Подписка", callback_data="subscription_info")]
    if has_subscription:
        buttons.append(InlineKeyboardButton("💫 ИИ прогноз", callback_data="ai_forecast"))
    keyboard.row(*buttons)
    await message.answer(text, reply_markup=keyboard)





@dp.callback_query_handler(lambda c: c.data == "back_to_main", state="*")
async def back_to_main_menu(call: types.CallbackQuery, state: FSMContext):
    await state.finish()
    user_id = call.from_user.id
    
    user_data = await fetch_query('SELECT registration_date FROM users WHERE user_id = ?', (user_id,), True)
    subscription = await fetch_query("SELECT end_time FROM subscriptions WHERE user_id = ?", (user_id,), fetch_one=True)
    
    has_subscription = False
    subscription_status = "не активирована"
    if subscription:
        try:
            end_time = datetime.strptime(subscription['end_time'], '%Y-%m-%d %H:%M:%S')
            if end_time > datetime.now():
                has_subscription = True
                subscription_status = "активирована"
        except Exception:
            pass

    reg_date_str = user_data.get('registration_date', 'Неизвестно')
    days_ago = ""
    if reg_date_str != 'Неизвестно':
        try:
            reg_date = datetime.strptime(reg_date_str, "%d.%m.%Y" if '.' in reg_date_str else "%Y-%m-%d")
            days_ago = f" ({(datetime.now() - reg_date).days} д. назад)"
            reg_date_str = reg_date.strftime("%d.%m.%Y")
        except ValueError:
            days_ago = " (Неверный формат даты)"
    
    balance = await get_balance(user_id)
    bot_username = (await call.bot.get_me()).username
    
    try:
        balance = float(balance)
        formatted_balance = f"{int(balance)}₽" if balance.is_integer() else f"{balance:.1f}₽"
    except (ValueError, TypeError):
        formatted_balance = "Ошибка баланса"

    text = (
        f"👋 Привет, {call.from_user.first_name}!\n"
        f"💰 Баланс: {formatted_balance}\n"
        f"📅 Дата регистрации: {reg_date_str}{days_ago}\n"
        f"🔗 Ваша реферальная ссылка: t.me/{bot_username}?start={user_id}\n"
        f"💎 Подписка: {subscription_status}"
    )

    keyboard = InlineKeyboardMarkup()
    keyboard.row(InlineKeyboardButton("🎮 Играть", callback_data="play"), InlineKeyboardButton("📦 Кейсы", callback_data="cases"))
    keyboard.row(InlineKeyboardButton("💳 Пополнить", callback_data="deposit"), InlineKeyboardButton("📤 Вывод", callback_data="withdraw"))
    keyboard.row(InlineKeyboardButton("📊 Профиль", callback_data="profile"), InlineKeyboardButton("📞 Поддержка", callback_data="support"))
    keyboard.row(InlineKeyboardButton("🎁 Активировать промокод", callback_data="activate_promo"), InlineKeyboardButton("📚 FAQ", url="https://telegra.ph/Kapel-kazino-01-23"))
    keyboard.row(InlineKeyboardButton("🔒 Приватный канал", callback_data="private_channel"))
    

    buttons = [InlineKeyboardButton("💎 Подписка", callback_data="subscription_info")]
    if has_subscription:
        buttons.append(InlineKeyboardButton("💫 ИИ прогноз", callback_data="ai_forecast"))
    keyboard.row(*buttons)

    try:
        await call.message.edit_text(text, reply_markup=keyboard)
    except Exception:
        await call.message.answer(text, reply_markup=keyboard)
    
    await call.answer()











@dp.callback_query_handler(lambda c: c.data == "private_channel", state="*")
async def handle_private_channel(call: CallbackQuery, state: FSMContext):
    user_id = call.from_user.id
    purchase = await fetch_query(
        "SELECT * FROM private_channel_purchases WHERE user_id = ?", 
        (user_id,), fetch_one=True
    )

    if purchase and purchase.get("status") == "yes":
        text = (
            "🔒 Приватный канал\n"
            "🎁 Вы получите доступ к:\n"
            "👨‍💻 - Исходному коду бота\n"
            "🤖 - Способ как использовать ChatGPT o3-mini, o3-mini-hight, o1 и другие флагманские модели бесплатно\n\n"
            "🔒 Приватный канал куплен\n🔗 Ссылка: https://t.me/+BboLHYDsz_85ODIy"
        )
        keyboard = InlineKeyboardMarkup().add(
            InlineKeyboardButton("◀️ Назад", callback_data="back_to_main")
        )
    else:
        balance = await get_balance(user_id)
        try:
            balance_val = float(balance)
        except Exception:
            balance_val = 0

        text = (
            "🔒 Приватный канал\n"
            "Вы получите доступ к:\n"
            "👨‍💻 - Исходному коду бота\n"
            "🤖 - Способ как использовать ChatGPT o3-mini, o3-mini-hight, o1 и другие флагманские модели бесплатно\n\n"
            "💳 Цена: 999₽\n"
            f"💰 Ваш баланс: {balance_val}₽"
        )
        keyboard = InlineKeyboardMarkup().row(
            InlineKeyboardButton("💳 Купить", callback_data="buy_private_channel"),
            InlineKeyboardButton("◀️ Назад", callback_data="back_to_main")
        )

    await call.message.edit_text(text, reply_markup=keyboard)
    await call.answer()






@dp.callback_query_handler(lambda c: c.data == "buy_private_channel", state="*")
async def buy_private_channel(call: CallbackQuery, state: FSMContext):
    try:
        user_id = call.from_user.id
        username = call.from_user.username or "без юзернейма"
        balance = await get_balance(user_id)
        try:
            balance_val = float(balance)
        except Exception:
            balance_val = 0
        price = 999
        keyboard_back = InlineKeyboardMarkup().row(
            InlineKeyboardButton("◀️ Назад", callback_data="back_to_main")
        )
        if balance_val >= price:
            await execute_query(
                "UPDATE users SET balance = balance - ? WHERE user_id = ?", 
                (price, user_id)
            )
            now = datetime.now()
            await execute_query(
                "INSERT OR REPLACE INTO private_channel_purchases (user_id, amount, purchase_date, status) VALUES (?, ?, ?, ?)",
                (user_id, price, now.strftime('%Y-%m-%d %H:%M:%S'), "yes")
            )
            new_balance = await get_balance(user_id)
            text = "🔗 Ссылка на приватный канал: https://t.me/+BboLHYDsz\\_85ODIy"
            keyboard = InlineKeyboardMarkup()
            support_msg = (
                f"💳 *Покупка приватного канала*\n\n"
                f"👤 Пользователь: @{escape_md(username)}\n"
                f"🆔 ID: `{user_id}`\n"
                f"💰 Сумма: {price}₽\n"
                f"🕒 Дата: {now.strftime('%d.%m.%y %H:%M')}\n"
                f"💰 Баланс после покупки: {new_balance:.1f}₽"
            )
            await bot.send_message(SUPPORT_CHAT_ID, support_msg, parse_mode="Markdown")
            await handle_edit(call, text, keyboard)
        else:
            kb = InlineKeyboardMarkup().row(
                InlineKeyboardButton("Пополнить баланс", callback_data="deposit"),
                InlineKeyboardButton("◀️ Назад", callback_data="back_to_main")
            )
            text = (
                f"💰 Ваш баланс: {balance_val:.1f}₽\n\n"
                "❌ Недостаточно средств для оплаты приватного канала!"
            )
            await handle_edit(call, text, kb)
    except Exception as e:
        logging.error(f"Ошибка в buy_private_channel: {e}")
        await call.answer("Произошла ошибка при обработке платежа", show_alert=True)
    finally:
        await state.finish()





async def get_user_all_data(user_id):
    data = {}
    data["user"] = await fetch_query(
        "SELECT * FROM users WHERE user_id = ?", (user_id,), fetch_one=True
    )
    data["transactions"] = await fetch_query(
        "SELECT COUNT(*) as count FROM transactions WHERE user_id = ?", (user_id,), fetch_one=True
    )
    data["withdraw_requests"] = await fetch_query(
        "SELECT COUNT(*) as count FROM withdraw_requests WHERE user_id = ?", (user_id,), fetch_one=True
    )
    data["payment_requests"] = await fetch_query(
        "SELECT COUNT(*) as count FROM payment_requests WHERE user_id = ?", (user_id,), fetch_one=True
    )
    data["used_promocodes"] = await fetch_query(
        "SELECT COUNT(*) as count FROM used_promocodes WHERE user_id = ?", (user_id,), fetch_one=True
    )
    data["activity"] = await fetch_query(
        "SELECT * FROM activity WHERE user_id = ?", (user_id,), fetch_one=True
    )
    data["all_games"] = await fetch_query(
        "SELECT COUNT(*) as count FROM all_games WHERE user_id = ?", (user_id,), fetch_one=True
    )
    data["favorite_games"] = await fetch_query(
        "SELECT * FROM favorite_games WHERE user_id = ?", (user_id,), fetch_one=True
    )
    data["subscriptions"] = await fetch_query(
        "SELECT * FROM subscriptions WHERE user_id = ?", (user_id,), fetch_one=True
    )
    data["private_channel_purchases"] = await fetch_query(
        "SELECT * FROM private_channel_purchases WHERE user_id = ?", (user_id,), fetch_one=True
    )
    data["admin_actions_as_admin"] = await fetch_query(
        "SELECT COUNT(*) as count FROM admin_actions WHERE admin_id = ?", (user_id,), fetch_one=True
    )
    data["admin_actions_as_target"] = await fetch_query(
        "SELECT COUNT(*) as count FROM admin_actions WHERE target_user = ?", (user_id,), fetch_one=True
    )
    return data

def summarize_user_data(original_data):
    summary = {}
    user = original_data.get("user", {})
    summary["user_id"] = user.get("user_id")
    summary["username"] = user.get("username")
    summary["balance"] = user.get("balance")
    summary["registration_date"] = user.get("registration_date")
    summary["banned"] = user.get("banned")
    summary["ban_reason"] = user.get("ban_reason")
    summary["ban_time"] = user.get("ban_time")
    summary["ban_until"] = user.get("ban_until")
    summary["referral_used"] = user.get("referral_used")
    summary["referred_by"] = user.get("referred_by")
    summary["request_counter"] = user.get("request_counter")
    summary["lucky_number"] = user.get("lucky_number")
    summary["lucky_chance"] = user.get("lucky_chance")
    summary["transactions_count"] = original_data.get("transactions", {}).get("count")
    summary["withdraw_requests_count"] = original_data.get("withdraw_requests", {}).get("count")
    summary["payment_requests_count"] = original_data.get("payment_requests", {}).get("count")
    summary["used_promocodes_count"] = original_data.get("used_promocodes", {}).get("count")
    activity = original_data.get("activity", {})
    summary["last_activity"] = activity.get("last_activity") if activity else None
    summary["all_games_count"] = original_data.get("all_games", {}).get("count")
    fav = original_data.get("favorite_games", {})
    summary["favorite_game"] = fav.get("game_name") if fav else None
    summary["subscription_active"] = bool(original_data.get("subscriptions"))
    summary["private_channel_purchase"] = original_data.get("private_channel_purchases")
    summary["admin_actions_as_admin_count"] = original_data.get("admin_actions_as_admin", {}).get("count")
    summary["admin_actions_as_target_count"] = original_data.get("admin_actions_as_target", {}).get("count")
    return summary






async def call_ai_forecast(user_data):
    summarized = summarize_user_data(user_data)
    prompt = (
        "Сгенерируй прогноз-совет, где указаны:\n"
        "1) Назови Любимую игру пользователя БЕЗ форматирования (например **). Не используй шаблон, но основываясь на примере ниже, напиши персонализированное сообщение. Пример: Ваша любимая игра: Рулетка.\n"
        "2) Конкретный и полезный совет с акцентом на рекомендации, без лишних слов и шаблонных фраз (например, не используй 'получай удовольствие').\n"
        "Пиши естественно, сосредотачиваясь на советах.\n"
        "Данные: " + json.dumps(summarized, ensure_ascii=False)
    )
    payload = {
        "model": "o3-mini",
        "messages": [
            {"role": "system", "content": "Ты эксперт в анализе данных и выдаёшь краткие, полезные советы."},
            {"role": "user", "content": prompt}
        ]
    }
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {AI_API_KEY}"
    }
    async with aiohttp.ClientSession() as session:
        async with session.post("https://chat01.ai/v1/chat/completions", json=payload, headers=headers) as resp:
            result = await resp.json()
            if "choices" in result and result["choices"]:
                return result["choices"][0]["message"]["content"]
            return "Не удалось получить прогноз"





@dp.callback_query_handler(lambda c: c.data == "ai_forecast", state="*")
async def ai_forecast_handler(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await call.message.edit_text("💫 Генерирую прогноз...")
    user_id = call.from_user.id
    all_data = await get_user_all_data(user_id)
    forecast = await call_ai_forecast(all_data)
    if "## Ответ:" in forecast:
        forecast = forecast.split("## Ответ:", 1)[1].strip()
    if not forecast:
        forecast = "Прогноз отсутствует"
    keyboard = InlineKeyboardMarkup().add(InlineKeyboardButton("◀️ Назад", callback_data="back_to_main"))
    await call.message.edit_text(f"{forecast}\n\n🤖 Прогноз сгенерирован моделью ChatGPT o3-mini", reply_markup=keyboard)
    username = call.from_user.username if call.from_user.username else "без юзернейма"
    if username != "без юзернейма":
        username = username.replace("_", "\\_")
    support_message = (
        f"💫 *Использование ИИ прогноза*\n\n"
        f"👤 Пользователь: @{username}\n"
        f"🆔 ID: {user_id}\n"
        f"🤖 Прогноз:\n{forecast}"
    )
    await bot.send_message(SUPPORT_CHAT_ID, support_message, parse_mode="Markdown")





@dp.callback_query_handler(lambda c: c.data == "subscription_info")
async def show_subscription_info(call: types.CallbackQuery):
    user_id = call.from_user.id
    subscription = await fetch_query(
        "SELECT start_time, end_time, is_trial, trial_activated FROM subscriptions WHERE user_id = ?",
        (user_id,),
        fetch_one=True
    )
    now = datetime.now()
    balance = await get_balance(user_id)
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("◀️ Назад", callback_data="back_to_main"))
    
    if subscription:
        try:
            start_time = datetime.strptime(subscription["start_time"], "%Y-%m-%d %H:%M:%S.%f") if '.' in subscription["start_time"] else datetime.strptime(subscription["start_time"], "%Y-%m-%d %H:%M:%S")
            end_time = datetime.strptime(subscription["end_time"], "%Y-%m-%d %H:%M:%S.%f") if '.' in subscription["end_time"] else datetime.strptime(subscription["end_time"], "%Y-%m-%d %H:%M:%S")
            formatted_start = start_time.strftime("%d.%m.%y %H:%M")
            formatted_end = end_time.strftime("%d.%m.%y %H:%M")
        except Exception:
            formatted_start = subscription["start_time"][:16]
            formatted_end = subscription["end_time"][:16]
            
        if end_time > now:
            status = "активирована"
            subscription_text = f"Подписка: {status}\nДата начала: {formatted_start}\nЗакончится: {formatted_end}"
            keyboard = InlineKeyboardMarkup().add(InlineKeyboardButton("◀️ Назад", callback_data="back_to_main"))
        else:
            subscription_text = "Подписка: не активирована"
            if subscription.get("trial_activated"):
                keyboard = InlineKeyboardMarkup().row(InlineKeyboardButton("Оплатить", callback_data="pay_subscription"), InlineKeyboardButton("◀️ Назад", callback_data="back_to_main"))
            else:
                keyboard = InlineKeyboardMarkup().row(InlineKeyboardButton("Активировать пробную подписку", callback_data="activate_trial_subscription"), InlineKeyboardButton("◀️ Назад", callback_data="back_to_main"))
    else:
        subscription_text = "Подписка: не активирована"
        keyboard = InlineKeyboardMarkup().row(InlineKeyboardButton("💎 Активировать пробный период", callback_data="activate_trial_subscription"), InlineKeyboardButton("◀️ Назад", callback_data="back_to_main"))

    text = f"💰 Ваш баланс: {balance}\n\n{subscription_text}\n\nЦена подписки: 249₽/мес\n💎 С подпиской у вас будет доступ к:\n- 💫 ИИ прогнозам\n- 🎁 Двойным бонусам за рефералов\n- 📦 Бесплатному кейсу каждые 24 часа"
    
    try:
        if call.message.caption:
            await call.message.edit_caption(caption=text, reply_markup=keyboard)
        else:
            await call.message.edit_text(text=text, reply_markup=keyboard)
    except:
        await call.message.answer(text, reply_markup=keyboard)
    await call.answer()
    
    






@dp.callback_query_handler(lambda c: c.data == "activate_trial_subscription")
async def activate_trial_subscription(call: types.CallbackQuery):
    user_id = call.from_user.id
    username = call.from_user.username or "без юзернейма"
    now = datetime.now()
    trial_end = now + timedelta(days=3)
    next_daily = (now + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    subscription = await fetch_query(
        "SELECT trial_activated, end_time FROM subscriptions WHERE user_id = ?",
        (user_id,),
        fetch_one=True
    )
    user_keyboard = InlineKeyboardMarkup().add(InlineKeyboardButton("◀️ Назад", callback_data="back_to_main"))
    
    if subscription:
        try:
            end_time_str = subscription.get("end_time", "")
            end_time = (datetime.strptime(end_time_str, "%Y-%m-%d %H:%M:%S.%f")
                        if '.' in end_time_str
                        else datetime.strptime(end_time_str, "%Y-%m-%d %H:%M:%S"))
        except Exception:
            end_time = now
        
        if subscription.get("trial_activated") or end_time > now:
            await handle_edit(call, "Пробный период уже был использован или активная подписка существует", user_keyboard)
            return

    await execute_query(
        "INSERT OR REPLACE INTO subscriptions (user_id, start_time, end_time, is_trial, trial_activated, next_activation_date) VALUES (?, ?, ?, ?, ?, ?)",
        (
            user_id,
            now.strftime("%Y-%m-%d %H:%M:%S"),
            trial_end.strftime("%Y-%m-%d %H:%M:%S"),
            True,
            True,
            next_daily
        )
    )
    
    await give_daily_case(user_id)
    
    await handle_edit(call, f"✅ Пробный период активирован до {trial_end.strftime('%d.%m.%Y %H:%M')}", user_keyboard)
    
    try:
        support_keyboard = InlineKeyboardMarkup().add(
            InlineKeyboardButton("👤 Профиль", callback_data=f"user_profile_{user_id}")
        )
        support_msg = (
            f"💎 *Активация пробного периода*\n\n"
            f"👤 Пользователь: @{escape_md(username)}\n"
            f"🆔 ID: `{user_id}`\n"
            f"📅 Начало: {now.strftime('%d.%m.%Y %H:%M')}\n"
            f"📅 Окончание: {trial_end.strftime('%d.%m.%Y %H:%M')}"
        )
        await bot.send_message(SUPPORT_CHAT_ID, support_msg, reply_markup=support_keyboard, parse_mode="Markdown")
    except Exception as e:
        logging.error(f"Ошибка отправки в поддержку: {e}")







@dp.callback_query_handler(lambda c: c.data == "pay_subscription")
async def pay_subscription(call: types.CallbackQuery, state: FSMContext):
    try:
        user_id = call.from_user.id
        username = call.from_user.username or "без юзернейма"
        balance = await get_balance(user_id)
        user_keyboard = InlineKeyboardMarkup().add(InlineKeyboardButton("◀️ Назад", callback_data="back_to_main"))
        
        if balance >= 249:
            now = datetime.now()
            end_time = now + timedelta(days=30)
            next_daily = (now + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
            
            await execute_query(
                "INSERT OR REPLACE INTO subscriptions (user_id, start_time, end_time, is_trial, trial_activated, next_activation_date) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    user_id,
                    now.strftime("%Y-%m-%d %H:%M:%S"),
                    end_time.strftime("%Y-%m-%d %H:%M:%S"),
                    False,
                    True,
                    next_daily
                )
            )
            await execute_query("UPDATE users SET balance = balance - 249 WHERE user_id = ?", (user_id,))
            new_balance = await get_balance(user_id)
            
            await give_daily_case(user_id)
            
            await handle_edit(
                call,
                f"💰 Ваш баланс: {new_balance:.1f}₽\n\n✅ Подписка активирована до {end_time.strftime('%d.%m.%y %H:%M')}",
                user_keyboard
            )
            
            support_keyboard = InlineKeyboardMarkup().add(
                InlineKeyboardButton("👤 Профиль", callback_data=f"user_profile_{user_id}")
            )
            support_msg = (
                f"💳 *Оплата подписки*\n\n"
                f"👤 Пользователь: @{escape_md(username)}\n"
                f"🆔 ID: `{user_id}`\n"
                f"📅 Срок: {now.strftime('%d.%m.%y %H:%M')} - {end_time.strftime('%d.%m.%y %H:%M')}"
            )
            await bot.send_message(SUPPORT_CHAT_ID, support_msg, reply_markup=support_keyboard, parse_mode="Markdown")
        else:
            kb = InlineKeyboardMarkup().row(
                InlineKeyboardButton("💳 Пополнить баланс", callback_data="deposit"),
                InlineKeyboardButton("◀️ Назад", callback_data="back_to_main")
            )
            await handle_edit(call, f"💰 Ваш баланс: {balance:.1f}₽\n\n❌ Недостаточно средств для оплаты подписки!", kb)
    except Exception as e:
        logging.error(f"Ошибка в pay_subscription: {e}")
        await call.answer("Произошла ошибка при обработке платежа", show_alert=True)
    finally:
        await state.finish()




async def handle_edit(call, text, markup):
    try:
        if call.message.caption:
            await call.message.edit_caption(caption=text, reply_markup=markup, parse_mode="Markdown")
        else:
            await call.message.edit_text(text=text, reply_markup=markup, parse_mode="Markdown")
    except Exception as e:
        logging.error(f"Ошибка при редактировании сообщения: {e}")
        await call.message.answer(text, reply_markup=markup, parse_mode="Markdown")
        
        







async def manage_subscriptions():
    if not hasattr(manage_subscriptions, "notified"):
        manage_subscriptions.notified = set()

    while True:
        now = datetime.now()
        async with aiosqlite.connect(DB_NAME) as db:
            async with db.execute(
                "SELECT user_id, next_activation_date, end_time, is_trial FROM subscriptions"
            ) as cursor:
                rows = await cursor.fetchall()
                for user_id, next_activation_date, end_time_str, is_trial in rows:
                    
                    try:
                        if end_time_str:
                            if '.' in end_time_str:
                                end_time = datetime.strptime(end_time_str, "%Y-%m-%d %H:%M:%S.%f")
                            else:
                                end_time = datetime.strptime(end_time_str, "%Y-%m-%d %H:%M:%S")
                        else:
                            end_time = None
                    except Exception as e:
                        logging.error(f"Ошибка разбора end_time для user_id {user_id}: {e}")
                        continue

                    if end_time is not None and now >= end_time:
                        if user_id not in manage_subscriptions.notified:
                            try:
                                if is_trial:
                                    await bot.send_message(user_id, "⚠️ Пробный период закончился")
                                else:
                                    await bot.send_message(user_id, "⚠️ Подписка закончилась")
                            except exceptions.BotBlocked:
                                logging.warning(f"Пользователь {user_id} заблокировал бота")
                            except Exception as e:
                                logging.error(f"Ошибка отправки сообщения пользователю {user_id}: {e}")
                            manage_subscriptions.notified.add(user_id)
                        continue
                    else:
                        
                        manage_subscriptions.notified.discard(user_id)

                    if not next_activation_date:
                        new_date = (now + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
                        await db.execute(
                            "UPDATE subscriptions SET next_activation_date = ? WHERE user_id = ?",
                            (new_date, user_id)
                        )
                        continue

                    try:
                        act_datetime = datetime.strptime(next_activation_date, "%Y-%m-%d %H:%M:%S")
                    except Exception as e:
                        logging.error(f"Ошибка разбора next_activation_date для user_id {user_id}: {e}")
                        continue

                    if now >= act_datetime:
                       
                        try:
                            await give_daily_case(user_id)
                        except exceptions.BotBlocked:
                            logging.warning(f"Пользователь {user_id} заблокировал бота")
                        except Exception as e:
                            logging.error(f"Ошибка при отправке ежедневного кейса пользователю {user_id}: {e}")
                        new_date = (act_datetime + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
                        await db.execute(
                            "UPDATE subscriptions SET next_activation_date = ? WHERE user_id = ?",
                            (new_date, user_id)
                        )
            await db.commit()
        await asyncio.sleep(60)
        
        
        
        
        

@dp.callback_query_handler(lambda c: c.data == "activate_promo", state="*")
async def activate_promo(call: types.CallbackQuery):
    await States.ACTIVATE_PROMO.set()
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("◀️ Назад", callback_data="back_to_main"))
    
    await call.message.edit_text(
        "🎁 Введите промокод:",
        reply_markup=markup
    )
    await call.answer()




@dp.message_handler(commands=["promo"], chat_id=SUPPORT_CHAT_ID)
async def cmd_promo(message: types.Message):
    try:
        args = message.text.split(maxsplit=4)
        
        if len(args) == 1:
            promocodes = await fetch_query('''
                SELECT 
                    code,
                    amount,
                    message,
                    max_uses,
                    uses_left,
                    created_at,
                    (SELECT COUNT(*) FROM used_promocodes WHERE code = promocodes.code) as used_count
                FROM promocodes
                WHERE max_uses IS NULL OR uses_left > 0
                ORDER BY created_at DESC
            ''')

            if not promocodes:
                await message.answer("🔴 Нет активных промокодов")
                return

            response = ["🎟 <b>АКТИВНЫЕ ПРОМОКОДЫ</b> 🎟\n"]
            
            for promo in promocodes:
                created_at = datetime.strptime(promo['created_at'], "%Y-%m-%d %H:%M:%S").strftime("%d.%m.%Y")
                status = "🟢 Активен" if (promo['uses_left'] or 1) > 0 else "🔴 Завершен"
                uses_info = f"♻ Использовано: {promo['used_count']}"
                
                if promo['max_uses']:
                    uses_info += f"/{promo['max_uses']}"
                
                promo_info = [
                    f"▫️ Код: <code>{promo['code']}</code>",
                    f"▫️ Сумма: {promo['amount']}₽",
                    f"▫️ {status}",
                    f"▫️ {uses_info}",
                    f"▫️ Создан: {created_at}"
                ]

                if promo['message']:
                    promo_info.append(f"📝 Сообщение: {promo['message']}")

                response.append("\n".join(promo_info))
                response.append("▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬")

            await message.answer("\n\n".join(response), parse_mode="HTML")
            return

        if len(args) < 3:
            await message.answer("❌ Формат: /promo <код> <сумма> [макс_использований] [сообщение]")
            return

        code = args[1].upper().strip()
        amount = args[2]
        max_uses = None
        custom_message = None

        try:
            amount = int(amount)
            if amount <= 0:
                raise ValueError
        except:
            await message.answer("❗ Некорректная сумма. Используйте целое число больше 0")
            return

        if len(args) >= 4:
            if args[3].isdigit():
                max_uses = int(args[3])
                custom_message = args[4] if len(args) > 4 else None
            else:
                custom_message = ' '.join(args[3:])

        try:
            await execute_query('''
                INSERT INTO promocodes (
                    code, 
                    amount, 
                    max_uses, 
                    uses_left,
                    message
                ) VALUES (?, ?, ?, ?, ?)
            ''', (code, amount, max_uses, max_uses, custom_message))

            result = [
                "✅ Промокод создан ✅",
                f"🎟 Код: <code>{code}</code>",
                f"💵 Сумма: {amount}₽",
                f"♻ Лимит: {max_uses or '∞'}"
            ]

            if custom_message:
                result.append(f"📝 Сообщение: {custom_message}")

            await message.answer("\n".join(result), parse_mode="HTML")

        except aiosqlite.IntegrityError:
            await message.answer(f"❗ Промокод {code} уже существует!")

    except Exception as e:
        logging.error(f"Ошибка в команде promo: {str(e)}", exc_info=True)
        await message.answer("⚠ Произошла критическая ошибка при обработке запроса!")
        

        




@dp.message_handler(state=States.ACTIVATE_PROMO)
async def process_promo(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    code = message.text.upper().strip()

    try:
        promocode = await fetch_query(
            '''SELECT * FROM promocodes 
            WHERE code = ?''',
            (code,), 
            fetch_one=True
        )
        
        if not promocode:
            await message.answer("❌ Промокод не найден!")
            return

        if promocode['max_uses'] is not None and promocode['uses_left'] <= 0:
            await message.answer("⚠️ Промокод закончился!")
            return

        used = await fetch_query(
            '''SELECT 1 FROM used_promocodes 
            WHERE user_id = ? AND code = ?''',
            (user_id, code), 
            fetch_one=True
        )
        
        if used:
            await message.answer("⚠️ Вы уже активировали этот промокод!")
            return

        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute(
                'UPDATE users SET balance = balance + ? WHERE user_id = ?',
                (promocode['amount'], user_id)
            )
            
            await db.execute(
                '''INSERT INTO used_promocodes (user_id, code)
                VALUES (?, ?)''',
                (user_id, code)
            )
            
            if promocode['max_uses'] is not None:
                await db.execute(
                    'UPDATE promocodes SET uses_left = uses_left - 1 WHERE code = ?',
                    (code,)
                )
            
            await db.commit()

        user_data = await fetch_query(
            'SELECT username, balance FROM users WHERE user_id = ?',
            (user_id,), 
            fetch_one=True
        )
        
        response = f"🎉 Промокод активирован! +{promocode['amount']}₽"
        if promocode.get('message'):
            response += f"\n\n📝 Сообщение: {promocode['message']}"
        
        support_keyboard = InlineKeyboardMarkup().add(
            InlineKeyboardButton("👤 Профиль", callback_data=f"user_profile_{user_id}")
        )
        
        support_msg = (
            "🆕 *Новая активация промокода*\n"
            f"👤 Юзернейм: {escape_md('@' + (user_data['username'] or 'Нет юзернейма'))}\n"
            f"🆔 ID: `{user_id}`\n"
            f"🎟 Промокод: `{code}`\n"
            f"💰 Награда: {promocode['amount']}₽\n"
            f"💳 Новый баланс: {user_data['balance']:.1f}₽"
        )
        
        try:
            await bot.send_message(
                SUPPORT_CHAT_ID,
                support_msg,
                reply_markup=support_keyboard,
                parse_mode="Markdown"
            )
        except Exception as e:
            logging.error(f"Ошибка отправки в SUPPORT_CHAT: {str(e)}")

        await add_transaction(user_id, f"Промокод {code}", promocode['amount'])
        await message.answer(response)

    except Exception as e:
        logging.error(f"Ошибка активации промокода: {str(e)}", exc_info=True)
        await message.answer("⚠️ Произошла ошибка при активации!")




@dp.message_handler(commands=["unpromo"], chat_id=SUPPORT_CHAT_ID)
async def cmd_unpromo(message: types.Message):
    try:
        args = message.text.split(maxsplit=1)
        if len(args) < 2:
            await message.answer("❌ Формат: /unpromo <код>")
            return

        code = args[1].upper().strip()
        promocode = await fetch_query('SELECT * FROM promocodes WHERE code = ?', (code,), True)

        if not promocode:
            await message.answer(f"❌ Промокод {code} не найден!")
            return

        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute('DELETE FROM promocodes WHERE code = ?', (code,))
            await db.execute('DELETE FROM used_promocodes WHERE code = ?', (code,))
            await db.commit()

        await message.answer(f"✅ Промокод {code} полностью удален!")

    except Exception as e:
        logging.error(f"Ошибка удаления промокода: {str(e)}")
        await message.answer("⚠ Ошибка при удалении промокода!")
        
        





@dp.callback_query_handler(lambda c: c.data == "play", state="*")
async def select_game(call: types.CallbackQuery, state: FSMContext):
    try:
        await state.finish()
        markup = InlineKeyboardMarkup(row_width=2)
        markup.add(
        	InlineKeyboardButton("✈️ Lucky Jet", callback_data="game_luckyjet"),
        	InlineKeyboardButton("🎰 Слоты", callback_data="game_roulette"),
            InlineKeyboardButton("🎲 Кубик", callback_data="game_dice"),
            InlineKeyboardButton("⚽ Футбол", callback_data="game_football"),
            InlineKeyboardButton("🏀 Баскетбол", callback_data="game_basketball"),
            InlineKeyboardButton("🎯 Дартс", callback_data="game_darts"),
            InlineKeyboardButton("🎳 Боулинг", callback_data="game_bowling"),
            InlineKeyboardButton("◀️ Назад", callback_data="back_to_main")
        )
        await call.message.edit_text("🎮 Выберите игру:", reply_markup=markup)
        await States.SELECT_GAME.set()
        await call.answer()
    except Exception as e:
        logging.error(f"Ошибка в select_game: {str(e)}")
        await call.answer("⚠️ Произошла ошибка")




@dp.callback_query_handler(lambda c: c.data.startswith('game_'), state=States.SELECT_GAME)
async def process_game(call: types.CallbackQuery, state: FSMContext):
    try:
        user_id = call.from_user.id
        game_type = call.data.split('_')[1]
        
        
        balance = await get_balance(user_id)
        balance = round(balance, 1)
        balance = abs(balance) if balance == 0 else balance  

        if game_type not in GAME_RULES:
            await call.answer("⚠️ Игра не найдена!")
            return

        rules = GAME_RULES[game_type].format(balance=balance)
        await state.update_data(game_type=game_type)
        
        markup = InlineKeyboardMarkup().add(
            InlineKeyboardButton("◀️ Назад", callback_data="play")
        )
        
        await call.message.edit_text(
            text=rules,
            parse_mode="Markdown",
            reply_markup=markup
        )
        await States.GAME_LOOP.set()
        await call.answer()

    except Exception as e:
        logging.error(f"Ошибка запуска игры: {str(e)}")
        await call.answer("⚠️ Ошибка запуска игры!")
        await state.finish()








@dp.message_handler(state=States.GAME_LOOP)
async def game_loop_handler(message: types.Message, state: FSMContext):
    try:
        data = await state.get_data()
        game_type = data['game_type']
        user_id = message.from_user.id
        
        try:
            bet = round(float(message.text.replace(',', '.')), 1)
            if bet <= 0: 
                raise ValueError
        except (ValueError, TypeError):
            markup = InlineKeyboardMarkup().add(InlineKeyboardButton("◀️ Назад", callback_data="play"))
            await message.answer("❌ Некорректная сумма! Введите число больше нуля:", reply_markup=markup)
            return

        balance = await get_balance(user_id)
        if balance < bet:
            markup = InlineKeyboardMarkup().row(
                InlineKeyboardButton("💳 Пополнить баланс", callback_data="deposit"),
                InlineKeyboardButton("◀️ Назад", callback_data="play")
            )
            await message.answer(f"❌ Недостаточно средств! Баланс: {balance:.1f}₽", reply_markup=markup)
            return

        await update_balance(user_id, -bet)
        await add_transaction(user_id, f"Ставка {game_type}", -bet)

        if game_type == 'luckyjet':
            async with game_lock:
                data = await state.get_data()
                if data.get('is_active', False):
                    await message.answer("⚠️ Завершите текущую игру перед новой ставкой!")
                    return
                await state.update_data(is_active=True)
            await handle_luckyjet_game(message, state, bet, user_id)

        elif game_type == 'roulette':
            dice_msg = await message.answer_dice(emoji="🎰")
            await asyncio.sleep(2)
            val = dice_msg.dice.value
            combo = get_slots_symbols(val)
            
            WIN_COMBINATIONS = {
                ("seven", "seven", "seven"),
                ("bar", "bar", "bar"),
                ("grapes", "grapes", "grapes"),
                ("lemon", "lemon", "lemon")
            }
            
            if tuple(combo) in WIN_COMBINATIONS:
                win_amount = round(bet * 14, 2) 
                await update_balance(user_id, win_amount)
                await add_transaction(user_id, "Выигрыш в рулетке", win_amount)
                result_text = f"🎉 Победа! +{win_amount}₽"
            else:
                result_text = "😢 Проигрыш"
            
            await update_game_count(user_id, "Рулетка")
            balance = await get_balance(user_id)
            markup = InlineKeyboardMarkup().add(InlineKeyboardButton("◀️ Назад", callback_data="play"))
            await message.answer(f"{result_text}\n💰 Текущий баланс: {balance:.1f}₽\n\n💶 Введите сумму ставки:", reply_markup=markup)

        else:
            emoji_map = {
                'dice': '🎲',
                'football': '⚽',
                'basketball': '🏀',
                'darts': '🎯',
                'bowling': '🎳'
            }
            game_name = {
                'dice': 'Кости',
                'football': 'Футбол',
                'basketball': 'Баскетбол',
                'darts': 'Дартс',
                'bowling': 'Боулинг'
            }.get(game_type, game_type.capitalize())
            dice = await message.answer_dice(emoji=emoji_map.get(game_type, '🎲'))
            await asyncio.sleep(3)
            win_rules = {
                'dice': lambda val: val in [4, 5, 6],
                'football': lambda val: val == 5,
                'basketball': lambda val: val in [4, 5],
                'darts': lambda val: val == 6,
                'bowling': lambda val: val == 6
            }
            multiplier = 1.8
            if win_rules.get(game_type, lambda val: False)(dice.dice.value):
                win_amount = round(bet * multiplier, 2)
                await update_balance(user_id, win_amount)
                await add_transaction(user_id, f"Выигрыш в {game_name}", win_amount)
                await update_game_count(user_id, game_name)
                result_text = f"🎉 Победа! +{win_amount}₽"
            else:
                await update_game_count(user_id, game_name)
                result_text = "😢 Проигрыш"
            balance = await get_balance(user_id)
            markup = InlineKeyboardMarkup().add(InlineKeyboardButton("◀️ Назад", callback_data="play"))
            await message.answer(f"{result_text}\n💰 Текущий баланс: {balance:.1f}₽\n\n💶 Введите сумму ставки:", reply_markup=markup)

    except Exception as e:
        logging.error(f"Ошибка в game_loop: {str(e)}")
        markup = InlineKeyboardMarkup().add(InlineKeyboardButton("◀️ Назад", callback_data="play"))
        await message.answer("⚠️ Критическая ошибка. Перейдите в главное меню и запустите игру снова", reply_markup=markup)
        await handle_error_and_restart(exception)
        await state.finish()
        
        







async def handle_roulette_game(message: types.Message, bet: float, user_id: int):
    dice_msg = await message.answer_dice(emoji="🎰")
    await asyncio.sleep(2)
    val = dice_msg.dice.value
    combo = get_slots_symbols(val)
    
    win_combinations = {
        ("seven", "seven", "seven"),
        ("bar", "bar", "bar"),
        ("grapes", "grapes", "grapes"),
        ("lemon", "lemon", "lemon")
    }
    
    is_win = tuple(combo) in win_combinations
    
    if is_win:
        win_amount = round(bet * 14, 2) 
        await update_balance(user_id, win_amount)
        await add_transaction(user_id, "Выигрыш рулетки", win_amount)
        await update_game_count(user_id, "Рулетка")
        return f"🎉 Победа! +{win_amount}₽"
    
    await update_game_count(user_id, "Рулетка")
    return "😢 Проигрыш"
    



def get_slots_symbols(value: int) -> tuple:
    value -= 1  
    symbols = ["bar", "grapes", "lemon", "seven"]
    return (
        symbols[(value >> 0) & 0b11],  
        symbols[(value >> 2) & 0b11],   
        symbols[(value >> 4) & 0b11]    
    )
    
    
    

async def handle_luckyjet_game(message: types.Message, state, bet: int, user_id: int):
    async with game_lock:
        await add_transaction(user_id, "Ставка Lucky Jet", -bet)
        target = await generate_target_multiplier(user_id)
        await state.update_data(
            bet=bet,
            current_multiplier=1.0,
            speed=1.0,
            target=target,
            is_active=True,
            user_id=user_id,
            last_update=time.time(),
            start_time=time.time()
        )
        msg = await message.answer("✈️ Запуск Lucky Jet...")
        keyboard = InlineKeyboardMarkup().add(
            InlineKeyboardButton("🛑 Стоп", callback_data="luckyjet_stop")
        )
        await msg.edit_text("✈️ Множитель: 1.00x", reply_markup=keyboard)
        asyncio.create_task(luckyjet_timer(message.bot, msg.chat.id, msg.message_id, state))
        
        
        
async def luckyjet_timer(bot: Bot, chat_id: int, message_id: int, state):
    try:
        next_ui_update = 0
        total_increments = 0
        keyboard = InlineKeyboardMarkup().add(
            InlineKeyboardButton("🛑 Стоп", callback_data="luckyjet_stop")
        )
        while True:
            start_time_loop = time.time()
            async with game_lock:
                data = await state.get_data()
                if not data.get('is_active', False):
                    return
                now = time.time()
                if total_increments >= 30:
                    new_speed = data['speed'] * 1.2
                    total_increments = 0
                else:
                    new_speed = data['speed']
                new_multiplier = round(data['current_multiplier'] + 0.01 * new_speed, 2)
                await state.update_data(
                    current_multiplier=new_multiplier,
                    speed=new_speed,
                    last_update=now
                )
                total_increments += 1
                if new_multiplier >= data['target']:
                    await finish_game(bot, chat_id, message_id, state, success=False)
                    return
            if now >= next_ui_update:
                try:
                    await bot.edit_message_text(
                        f"✈️ Множитель: {new_multiplier:.2f}x",
                        chat_id,
                        message_id,
                        reply_markup=keyboard
                    )
                except exceptions.MessageNotModified:
                    pass
                except exceptions.RetryAfter as e:
                    await asyncio.sleep(e.timeout)
                    async with game_lock:
                        data = await state.get_data()
                        if not data.get('is_active', False):
                            return
                        current = data['current_multiplier']
                    try:
                        await bot.edit_message_text(
                            f"✈️ Множитель: {current:.2f}x",
                            chat_id,
                            message_id,
                            reply_markup=keyboard
                        )
                    except exceptions.MessageNotModified:
                        pass
                next_ui_update = now + 0.2
            elapsed = time.time() - start_time_loop
            await asyncio.sleep(max(0.1 - elapsed, 0))
    except Exception as e:
        logging.error(f"Ошибка Lucky Jet: {str(e)}")
    finally:
        async with game_lock:
            await state.update_data(is_active=False)






async def finish_game(bot: Bot, chat_id: int, message_id: int, state, success: bool):
    await state.update_data(is_active=False)
    data = await state.get_data()
    user_id = data['user_id']
    bet = data['bet']
    current_balance = await get_balance(user_id)
    current_balance = round(current_balance, 1)
    markup = InlineKeyboardMarkup()
    
    if not success and current_balance <= 0:
        markup.row(
            InlineKeyboardButton("💳 Пополнить баланс", callback_data="deposit"),
            InlineKeyboardButton("◀️ Назад", callback_data="play")
        )
    else:
        markup.add(InlineKeyboardButton("◀️ Назад", callback_data="play"))

    if success:
        win_amount = round(bet * data['current_multiplier'], 1)
        await update_balance(user_id, win_amount)
        await add_transaction(user_id, "Выигрыш Lucky Jet", win_amount)
        await update_game_count(user_id, "Lucky Jet")
        new_balance = await get_balance(user_id)
        text = (
            f"✈️ Вы успели забрать на: {data['current_multiplier']:.2f}x\n"
            f"🎯 Максимальный множитель: {data['target']:.2f}x\n"
            f"💰 Выигрыш: +{win_amount:.1f}₽\n"
            f"💵 Баланс: {new_balance:.1f}₽\n\n"
            "➡️ Введите новую ставку:"
        )
    else:
        await add_transaction(user_id, "Проигрыш Lucky Jet", -bet)
        await update_game_count(user_id, "Lucky Jet")
        new_balance = await get_balance(user_id)
        text = (
            f"✈️ Игра остановилась на: {data['current_multiplier']:.2f}x\n"
            f"🎯 Максимальный множитель: {data['target']:.2f}x\n"
            f"💸 Проигрыш: -{bet:.1f}₽\n"
            f"💰 Баланс: {new_balance:.1f}₽\n\n"
            "➡️ Введите новую ставку:"
        )
    
    try:
        await bot.edit_message_text(
            text,
            chat_id,
            message_id,
            reply_markup=markup
        )
    except Exception as e:
        logging.error(f"Ошибка завершения игры: {str(e)}")
        await bot.send_message(chat_id, "⚠️ Произошла ошибка при завершении игры. Попробуйте снова")
        
        

@dp.callback_query_handler(lambda c: c.data == "luckyjet_stop", state=States.GAME_LOOP)
async def stop_luckyjet(call: types.CallbackQuery, state: FSMContext):
    async with game_lock:
        await finish_game(call.bot, call.message.chat.id, call.message.message_id, state, success=True)
    await call.answer()

async def generate_target_multiplier(user_id: int):
    user_lucky = await fetch_query("SELECT lucky_number, lucky_chance FROM users WHERE user_id = ?", (user_id,), fetch_one=True)
    if user_lucky and user_lucky.get('lucky_number') != 'default' and user_lucky.get('lucky_chance') != 'default':
        try:
            ln = float(user_lucky['lucky_number'])
            lc = float(user_lucky['lucky_chance'])
        except (ValueError, TypeError):
            ln = None
        if ln is not None:
            if random.random() <= (lc / 100):
                lower_bound = ln if ln >= 1.0 else 1.0
                return round(random.uniform(lower_bound, lower_bound + 1.0), 2)
            else:
                if ln > 1.0:
                    return round(random.uniform(1.0, ln - 0.01), 2)
                else:
                    return round(random.uniform(1.0, ln), 2)
    rand = random.random()
    if rand <= 0.35:
        return round(random.uniform(1.0, 1.5), 2)
    elif rand <= 0.60:
        return round(random.uniform(1.5, 2.0), 2)
    elif rand <= 0.75:
        return round(random.uniform(2.0, 2.5), 2)
    elif rand <= 0.85:
        return round(random.uniform(2.5, 3.0), 2)
    elif rand <= 0.91:
        return round(random.uniform(3.0, 5.0), 2)
    elif rand <= 0.96:
        return round(random.uniform(5.0, 10.0), 2)
    elif rand <= 0.98:
        return round(random.uniform(10.0, 25.0), 2)
    else:
        return round(random.uniform(25.0, 100.0), 2)




async def get_withdraw_amount(user_id: int) -> int:
    result = await fetch_query(
        'SELECT COALESCE(SUM(amount), 0) as total FROM withdraw_requests '
        'WHERE user_id = ? AND status = "pending"',
        (user_id,),
        fetch_one=True
    )
    return result['total'] if result else 0


@dp.callback_query_handler(lambda c: c.data == "withdraw")
async def start_withdraw(call: types.CallbackQuery, state: FSMContext):
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("◀️ Назад", callback_data="back_to_main"))
    
    await States.WITHDRAW_AMOUNT.set()
    await call.message.edit_text(
        "💸 Введите сумму для вывода (минимальная сумма 50₽):",
        reply_markup=markup
    )
    await call.answer()
    







@dp.message_handler(state=States.WITHDRAW_AMOUNT)
async def process_withdraw_amount(message: types.Message, state: FSMContext):
    try:
        amount = int(message.text)
        if amount < 50:
            await message.answer("❌ Минимальная сумма вывода 50₽!")
            return

        if await get_balance(message.from_user.id) < amount:
            await message.answer("❌ Недостаточно средств!")
            return

        await state.update_data(amount=amount)
        await States.WITHDRAW_DETAILS.set()
        await message.answer("📝 Введите реквизиты для вывода (номер карты/кошелька):")

    except ValueError:
        await message.answer("❌ Введите корректную сумму!")





@dp.message_handler(state=States.WITHDRAW_DETAILS)
async def process_withdraw_details(message: types.Message, state: FSMContext):
    data = await state.get_data()
    user_id = message.from_user.id
    username = message.from_user.username or "нет username"

    user_record = await fetch_query('SELECT * FROM users WHERE user_id = ?', (user_id,), fetch_one=True)
    if user_record:
        await execute_query('UPDATE users SET username = ? WHERE user_id = ?', (username, user_id))
    else:
        await execute_query(
            'INSERT INTO users (user_id, username, request_counter) VALUES (?, ?, ?)',
            (user_id, username, 0)
        )

    user_data = await fetch_query('SELECT request_counter FROM users WHERE user_id = ?', (user_id,), fetch_one=True)
    req_id = user_data['request_counter'] + 1 if user_data else 1

    await execute_query(
        '''INSERT INTO withdraw_requests (id, user_id, amount, details) 
           VALUES (?, ?, ?, ?)''',
        (req_id, user_id, data['amount'], message.text)
    )
    await execute_query('UPDATE users SET request_counter = ? WHERE user_id = ?', (req_id, user_id))

    keyboard = InlineKeyboardMarkup()
    keyboard.row(
        InlineKeyboardButton("✅ Подтвердить выплату", callback_data=f"confirm_withdraw_{req_id}"),
        InlineKeyboardButton("❌ Отклонить", callback_data=f"reject_withdraw_{req_id}")
    )

    text = (
        f"📤 Новая заявка на вывод #{req_id}\n"
        f"👤 Пользователь: @{username}\n"
        f"🆔 ID: {user_id}\n"
        f"💵 Сумма: {data['amount']}₽\n"
        f"📋 Реквизиты: {message.text}"
    )

    await bot.send_message(SUPPORT_CHAT_ID, text, reply_markup=keyboard, parse_mode=None)
    await message.answer("📨 Заявка отправлена на модерацию. Ожидайте подтверждения")
    await state.finish()
    
    
    


@dp.callback_query_handler(lambda c: c.data.startswith(('confirm_withdraw_', 'reject_withdraw_')))
async def handle_withdraw(call: types.CallbackQuery):
    try:
        parts = call.data.split('_')
        action = parts[0] 
        req_id = int(parts[2])

        request = await fetch_query(
            '''SELECT wr.*, u.username 
               FROM withdraw_requests wr
               LEFT JOIN users u ON wr.user_id = u.user_id 
               WHERE wr.id = ?''', 
            (req_id,), True
        )
        if not request:
            await call.answer("⚠ Заявка не найдена!")
            return

        user_id = request['user_id']
        amount = request['amount']
        details = request['details']

        try:
            chat = await bot.get_chat(user_id)
            username = chat.username or "нет username"
        except Exception as e:
            logging.error(f"Ошибка получения чата для {user_id}: {e}")
            username = request['username'] or "нет username"

        if action == 'confirm':
            await execute_query('UPDATE withdraw_requests SET status = "approved" WHERE id = ?', (req_id,))
            text = (
                f"✅ Заявка #{req_id} одобрена\n"
                f"💵 Сумма: {amount}₽\n"
                f"📝 Реквизиты: {details}\n"
                f"👤 Пользователь: @{username}\n🆔 ID: {user_id}\n"
                "➡️ Подтвердите выплату:"
            )
            keyboard = InlineKeyboardMarkup().add(
                InlineKeyboardButton("✅ Я оплатил", callback_data=f"paid_withdraw_{req_id}")
            )
            await call.message.edit_text(text, reply_markup=keyboard, parse_mode=None)
            await bot.send_message(user_id, f"✅ Ваша заявка одобрена. Ожидайте выплаты в течение 24 часов", parse_mode=None)

        elif action == 'reject':
            await execute_query('UPDATE withdraw_requests SET status = "rejected" WHERE id = ?', (req_id,))
            text = (
                f"❌ Заявка #{req_id} отклонена\n"
                f"💵 Сумма: {amount}₽\n"
                f"📝 Реквизиты: {details}\n"
                f"👤 Пользователь: @{username}\n🆔 ID: {user_id}\n"
                f"👨‍💼 Админ: @{call.from_user.username}"
            )
            await call.message.edit_text(text, parse_mode=None)
            await bot.send_message(user_id, "❌ Ваша заявка отклонена", parse_mode=None)

        await call.answer()

    except Exception as e:
        logging.error(f"Ошибка обработки вывода: {e}")
        await call.answer("⚠ Ошибка!")




@dp.callback_query_handler(lambda c: c.data.startswith("paid_withdraw_"))
async def confirm_payment(call: types.CallbackQuery):
    try:
        req_id = int(call.data.split('_')[2])
        request = await fetch_query('SELECT * FROM withdraw_requests WHERE id = ?', (req_id,), True)
        if not request:
            await call.answer("⚠ Заявка не найдена!")
            return

        user_id = request['user_id']
        amount = request['amount']
        details = request['details']

        try:
            chat = await bot.get_chat(user_id)
            username = chat.username or "нет username"
        except Exception as e:
            logging.error(f"Ошибка получения чата для {user_id}: {e}")
            username = "нет username"

        await update_balance(user_id, -amount)
        await add_transaction(user_id, "Вывод", -amount)
        await execute_query('DELETE FROM withdraw_requests WHERE id = ?', (req_id,))

        text = (
            f"✅ Выплата #{req_id} подтверждена\n"
            f"💵 Сумма: {amount}₽\n"
            f"📝 Реквизиты: {details}\n"
            f"👤 Пользователь: @{username}\n"
            f"🆔 ID: {user_id}\n"
            f"👨‍💼 Админ: @{call.from_user.username}"
        )
        await call.message.edit_text(text, parse_mode=None)
        await bot.send_message(user_id, "✅ Ваша выплата завершена", parse_mode=None)
        await call.answer()

    except Exception as e:
        logging.error(f"Ошибка подтверждения: {e}")
        await call.answer("⚠ Ошибка!")
        
        

@dp.callback_query_handler(lambda c: c.data == "deposit", state="*")
async def deposit_handler(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await state.finish()
    
    try:
        markup = InlineKeyboardMarkup(row_width=2)
        markup.row(
            InlineKeyboardButton("Crypto Bot", callback_data="method_crypto"),
            InlineKeyboardButton("Банковская карта", callback_data="method_card")
        )
        markup.row(InlineKeyboardButton("◀️ Назад", callback_data="back_to_main"))
        
        await call.message.edit_text(
            "💰 Выберите способ пополнения:",
            reply_markup=markup
        )
        await state.set_state(States.SELECT_PAYMENT_METHOD)
    except Exception as e:
        logging.error(f"Ошибка при выборе способа пополнения: {str(e)}")
        await call.message.answer(
            "💰 Выберите способ пополнения:",
            reply_markup=markup
        )
        await state.set_state(States.SELECT_PAYMENT_METHOD) 
    
    

@dp.callback_query_handler(lambda c: c.data == "method_crypto", state=States.SELECT_PAYMENT_METHOD)
async def crypto_payment_handler(callback: types.CallbackQuery, state: FSMContext):
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton("USDT", callback_data="currency_USDT"),
        InlineKeyboardButton("TON", callback_data="currency_TON"),
        InlineKeyboardButton("GRAM", callback_data="currency_GRAM"),
        InlineKeyboardButton("Notcoin", callback_data="currency_NOT")
    )
    await callback.message.edit_text(
        "💱 Выберите валюту:",
        reply_markup=markup
    )
    await state.set_state(States.SELECT_CURRENCY)
    await callback.answer()
    


@dp.callback_query_handler(lambda c: c.data == "method_card", state=States.SELECT_PAYMENT_METHOD)
async def card_payment_handler(callback: types.CallbackQuery, state: FSMContext):
    try:
        await state.set_state(States.ENTER_DEPOSIT_AMOUNT)
        await callback.message.edit_text(
            "💳 Введите сумму пополнения (минимум 20₽):\n\n",
            reply_markup=None  
        )
        await callback.answer()
    except MessageNotModified:
        await callback.answer()
    except Exception as e:
        logging.error(f"Card method error: {str(e)}")
        await callback.answer("❌ Ошибка обработки запроса", show_alert=True)
        
        
        

@dp.callback_query_handler(lambda c: c.data.startswith("currency_"), state=States.SELECT_CURRENCY)
async def select_currency(callback: types.CallbackQuery, state: FSMContext):
    currency = callback.data.split("_")[1]
    try:
        
        response = requests.get(
            f"{CRYPTO_BOT_API_URL}getExchangeRates",
            headers={"Crypto-Pay-API-Token": CRYPTO_BOT_TOKEN}
        )
        rates = response.json().get('result', [])
        
        
        rate = next((r for r in rates if r['source'] == currency and r['target'] == 'RUB'), None)
        
        if not rate:
            raise ValueError("Курс не найден")
            
        
        min_rub = MINIMAL_DEPOSIT_RUB  
        min_crypto = min_rub / float(rate['rate'])
        
        
        formatted_min = f"{min_crypto:.4f}".rstrip('0').rstrip('.') if '.' in f"{min_crypto:.4f}" else f"{min_crypto:.4f}"
        formatted_rub = f"{min_rub:.0f}₽"
        
        await state.update_data(currency=currency)
        await callback.message.edit_text(
            f"💵 Введите сумму в {currency} (мин: {formatted_min} {currency} ~{formatted_rub}):",
            reply_markup=None
        )
        await state.set_state(States.ENTER_AMOUNT)
        
    except Exception as e:
        logging.error(f"Ошибка: {str(e)}")
        await callback.message.answer("❌ Ошибка получения курса валют")
        await state.finish()
    await callback.answer()
    



@dp.message_handler(state=States.ENTER_AMOUNT)
async def process_crypto_amount(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    try:
        data = await state.get_data()
        currency = data['currency']
        amount = float(message.text.replace(',', '.'))
        
        
        try:
            response = requests.get(
                f"{CRYPTO_BOT_API_URL}getExchangeRates",
                headers={"Crypto-Pay-API-Token": CRYPTO_BOT_TOKEN},
                timeout=10
            )
            response.raise_for_status()
            rates = response.json().get('result', [])
        except requests.exceptions.RequestException as e:
            logging.error(f"Ошибка при запросе курса: {str(e)}")
            await message.answer("🔧 Сервис оплаты временно недоступен. Попробуйте позже")
            await state.finish()
            return

        rate = next((r for r in rates if r['source'] == currency and r['target'] == 'RUB'), None)
        if not rate:
            await message.answer("❌ Курс для выбранной валюты не найден")
            await state.finish()
            return

        min_rub = MINIMAL_DEPOSIT_RUB
        min_crypto = round(min_rub / float(rate['rate']), 4)
        if amount < min_crypto:
            await message.answer(f"❌ Минимальная сумма: {min_crypto} {currency} (~{min_rub}₽)")
            return

       
        try:
            invoice_response = requests.get(
                f"{CRYPTO_BOT_API_URL}createInvoice",
                headers={"Crypto-Pay-API-Token": CRYPTO_BOT_TOKEN},
                params={"asset": currency, "amount": amount, "description": "Пополнение баланса"},
                timeout=10
            )
            invoice_response.raise_for_status()
            invoice = invoice_response.json().get('result')
        except requests.exceptions.RequestException as e:
            logging.error(f"Ошибка создания инвойса: {str(e)}")
            await message.answer("❌ Не удалось создать счет. Попробуйте снова")
            await state.finish()
            return

        if not invoice:
            await message.answer("❌ Ошибка генерации платежной ссылки")
            await state.finish()
            return

        temp_payments[user_id] = {
            'invoice_id': invoice['invoice_id'],
            'currency': currency,
            'amount_rub': None
        }

        markup = InlineKeyboardMarkup().add(InlineKeyboardButton("✅ Я оплатил", callback_data="check_payment"))
        await message.answer(f"💸 Ссылка для оплаты: {invoice['pay_url']}", reply_markup=markup)
        await state.set_state(States.CHECK_PAYMENT)

    except ValueError:
        await message.answer("❌ Введите число")
    except Exception as e:
        logging.error(f"Критическая ошибка: {str(e)}")
        await message.answer("⚠️ Произошла непредвиденная ошибка. Попробуйте другой способ оплаты")
        await state.finish()




@dp.callback_query_handler(lambda c: c.data == "check_payment", state=States.CHECK_PAYMENT)
async def check_payment(callback: types.CallbackQuery, state: FSMContext):
    user_data = temp_payments.get(callback.from_user.id)
    
    if not user_data:
        await callback.answer("🚫 Сессия истекла", show_alert=True)
        return

    try:
        invoice_response = requests.get(
            f"{CRYPTO_BOT_API_URL}getInvoices",
            headers={"Crypto-Pay-API-Token": CRYPTO_BOT_TOKEN},
            params={"invoice_ids": user_data['invoice_id']}
        )
        invoice_data = invoice_response.json()

        if invoice_data.get('result', {}).get('items', []):
            invoice = invoice_data['result']['items'][0]
            
            if invoice.get('status') == 'paid':
                crypto_amount = float(invoice['amount'])
                asset = invoice['asset'].lower()
                
                rates_response = requests.get(
                    f"{CRYPTO_BOT_API_URL}getExchangeRates",
                    headers={"Crypto-Pay-API-Token": CRYPTO_BOT_TOKEN}
                )
                rates_data = rates_response.json()
                
                rate = next(
                    (r['rate'] for r in rates_data.get('result', []) 
                     if r['source'] == asset.upper() and r['target'] == 'RUB'),
                    None
                )
                
                if not rate:
                    raise ValueError()

                rub_amount = int(float(crypto_amount) * float(rate))
                
                await update_balance(callback.from_user.id, rub_amount)
                await add_transaction(callback.from_user.id, f"Пополнение {asset.upper()}", rub_amount)
                
                user_info = await fetch_query(
                    'SELECT username, balance FROM users WHERE user_id = ?',  
                    (callback.from_user.id,),
                    fetch_one=True
                )
                username = user_info.get('username', 'без юзернейма')
                new_balance = user_info.get('balance', 0)  
                
                support_text = (
                    "💰 Новое пополнение\n"
                    f"👤 Пользователь: @{username}\n"
                    f"🆔 ID: {callback.from_user.id}\n"
                    f"💳 Способ: Crypto Bot ({asset.upper()})\n"
                    f"💸 Сумма: {rub_amount}₽\n"
                    f"🏦 Новый баланс: {new_balance}₽"  
                )
                
                support_keyboard = InlineKeyboardMarkup().add(
                    InlineKeyboardButton("👤 Профиль", callback_data=f"user_profile_{callback.from_user.id}")
                )
                
                await bot.send_message(SUPPORT_CHAT_ID, support_text, reply_markup=support_keyboard)

                user_keyboard = InlineKeyboardMarkup().add(
                    InlineKeyboardButton("◀️ Назад", callback_data="back_to_main")
                )
                
                try:
                    await callback.message.edit_text(
                        f"✅ Баланс пополнен на {rub_amount}₽!", 
                        reply_markup=user_keyboard
                    )
                except:
                    pass
                
                del temp_payments[callback.from_user.id]
                await state.finish()  
                return
            
            await callback.answer("⚠ Оплата не подтверждена", show_alert=True)
        else:
            await callback.answer("⚠ Инвойс не найден", show_alert=True)

    except Exception as e:
        logging.error(f"Payment error: {str(e)}")
        await callback.answer("❌ Временная ошибка системы", show_alert=True)

        
        
        
@dp.message_handler(state=States.ENTER_DEPOSIT_AMOUNT)
async def process_deposit(message: types.Message, state: FSMContext):
    try:
        amount = int(message.text)
        if amount < 20:
            await message.answer("❌ Минимальная сумма пополнения 20₽!")
            return
            
        await state.update_data(amount=amount)
        await message.answer(
            "💳 Реквизиты для оплаты:\n"
            "Тбанк (Тинькофф): 2200701783253781\nАртем И.\n"
            "📸 После оплаты отправьте скриншот чека"
        )
        await States.DEPOSIT_PHOTO.set()
        
    except ValueError:
        await message.answer("❌ Введите корректную сумму!")





@dp.message_handler(state=States.DEPOSIT_PHOTO, content_types=types.ContentType.PHOTO)
async def process_deposit_photo(message: types.Message, state: FSMContext):
    data = await state.get_data()
    user_id = message.from_user.id
    photo_id = message.photo[-1].file_id
    username = message.from_user.username or "Нет юзернейма"
    balance = await get_balance(user_id)
    
    user_data = await fetch_query(
        'SELECT request_counter FROM users WHERE user_id = ?',
        (user_id,), fetch_one=True
    )
    req_id = user_data['request_counter'] + 1 if user_data else 1
    
    await execute_query(
        '''INSERT INTO payment_requests (id, user_id, amount, photo) 
        VALUES (?, ?, ?, ?)''',
        (req_id, user_id, data['amount'], photo_id)
    )
    await execute_query(
        'UPDATE users SET request_counter = ? WHERE user_id = ?',
        (req_id, user_id)
    )
    
    keyboard = InlineKeyboardMarkup()
    keyboard.row(
        InlineKeyboardButton("✅ Подтвердить", callback_data=f"approve_{req_id}"),
        InlineKeyboardButton("❌ Отклонить", callback_data=f"reject_{req_id}")
    )
    
    caption = (
        "🟡 Заявка на пополнение\n\n"
        f"👤 Юзернейм: @{username}\n"
        f"🆔 ID: {user_id}\n"
        f"💳 Сумма: {data['amount']}₽\n"
        f"💰 Текущий баланс: {balance:.1f}₽"
    )
    
    await bot.send_photo(
        SUPPORT_CHAT_ID,
        photo_id,
        caption=caption,
        reply_markup=keyboard
    )
    await message.answer("📨 Заявка отправлена на модерацию. Обычно проверка занимает до 15 минут")
    await state.finish()
    
    







@dp.callback_query_handler(lambda c: c.data.startswith(('approve_', 'reject_')))
async def handle_payment(call: types.CallbackQuery):
    try:
        action = call.data.split('_')[0]
        req_id = int(call.data.split('_')[1])
        
        request = await fetch_query(
            'SELECT * FROM payment_requests WHERE id = ?',
            (req_id,), fetch_one=True
        )
        if not request:
            await call.answer("⚠ Ошибка!")
            return
    
        user_id = request['user_id']
        amount = request['amount']
        
        
        user_data = await fetch_query(
            'SELECT username, balance FROM users WHERE user_id = ?',
            (user_id,), fetch_one=True
        )
        username = user_data.get('username', 'Нет юзернейма') if user_data else 'Нет юзернейма'
        balance = user_data.get('balance', 0.0) if user_data else 0.0

        if action == 'approve':
            
            new_balance = balance + amount
            await update_balance(user_id, amount)
            await add_transaction(user_id, "Пополнение", amount)
            
            
            admin_msg = (
                f"✅ Пополнение подтверждено\n\n"
                f"👤 Юзернейм: @{username}\n"
                f"🆔 ID: {user_id}\n"
                f"💳 Сумма: {amount}₽\n"
                f"💰 Новый баланс: {new_balance:.1f}₽"
            )
            
            user_msg = (
                f"✅ Баланс пополнен на {amount}₽!\n"
            )
            
            user_keyboard = InlineKeyboardMarkup().add(
                InlineKeyboardButton("◀️ Назад", callback_data="back_to_main")
            )

            await bot.send_message(user_id, user_msg, reply_markup=user_keyboard)
            await call.message.edit_caption(admin_msg)
        else:
           
            admin_msg = (
                f"❌ Пополнение отклонено\n\n"
                f"👤 Юзернейм: @{username}\n"
                f"🆔 ID: {user_id}\n"
                f"💳 Сумма: {amount}₽\n"
                f"💰 Текущий баланс: {balance:.1f}₽"
            )
            
            user_keyboard = InlineKeyboardMarkup().add(
                InlineKeyboardButton("◀️ Назад", callback_data="back_to_main")
            )

            await bot.send_message(user_id, "❌ Ваша заявка отклонена!", reply_markup=user_keyboard)
            await call.message.edit_caption(admin_msg)

        await execute_query(
            'DELETE FROM payment_requests WHERE id = ?',
            (req_id,)
        )
        await call.answer()

    except Exception as e:
        logging.error(f"Ошибка обработки платежа: {e}")
        await call.answer("⚠ Ошибка при обработке заявки!")






@dp.callback_query_handler(lambda c: c.data == "profile")
async def show_profile(call: types.CallbackQuery):
    try:
        user_id = call.from_user.id
        
        user_data = await fetch_query(
            '''SELECT 
                balance,
                registration_date,
                (SELECT COUNT(*) FROM users WHERE referred_by = ?) as referrals,
                (SELECT SUM(p.amount) 
                 FROM used_promocodes up
                 JOIN promocodes p ON up.code = p.code
                 WHERE up.user_id = ?) as promo_sum,
                (SELECT COUNT(*) 
                 FROM transactions 
                 WHERE user_id = ? 
                 AND description LIKE 'Покупка кейса%%') as cases_opened
            FROM users 
            WHERE user_id = ?''', 
            (user_id, user_id, user_id, user_id), 
            True
        )
        
        subscription = await fetch_query(
            "SELECT end_time FROM subscriptions WHERE user_id = ?",
            (user_id,),
            fetch_one=True
        )
        subscription_status = "не активирована"
        if subscription:
            try:
                end_time = datetime.strptime(subscription['end_time'], '%Y-%m-%d %H:%M:%S')
                if end_time > datetime.now():
                    subscription_status = "активирована"
            except Exception:
                pass

        favorite_game = await fetch_query(
            '''SELECT game_name, play_count FROM favorite_games WHERE user_id = ?''', 
            (user_id,), 
            True
        )

        reg_date = user_data.get('registration_date', 'Неизвестно')
        if '-' in reg_date:
            year, month, day = reg_date.split('-')
            reg_date = f"{day}.{month}.{year}"
            
        balance = user_data.get('balance', 0)
        formatted_balance = f"{balance:.1f}₽".rstrip('0').rstrip('.') if balance else "0₽"
        referrals = user_data.get('referrals', 0)
        total_bonus = (referrals * REFERRAL_BONUS) + (user_data.get('promo_sum', 0) or 0)
        cases_opened = user_data.get('cases_opened', 0)  
        
        favorite_game_str = "Нет данных"
        if favorite_game:
            game_name = favorite_game.get('game_name', 'Неизвестно')
            play_count = favorite_game.get('play_count', 0)
            favorite_game_str = f"{game_name} ({play_count} раз)"

        private_channel_purchase = await fetch_query(
            "SELECT * FROM private_channel_purchases WHERE user_id = ?", 
            (user_id,), fetch_one=True
        )
        private_channel_status = "не куплен"
        if private_channel_purchase and private_channel_purchase.get("status") == "yes":
            private_channel_status = "куплен"

        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("◀️ Назад", callback_data="back_to_main"))
        
        await call.message.edit_text(
            f"📊 Профиль\n\n"
            f"🆔 ID: {user_id}\n"
            f"📅 Дата регистрации: {reg_date}\n"
            f"💸 Баланс: {formatted_balance}\n"
            f"🎁 Бонусы: {total_bonus}₽\n"
            f"🎲 Открыто кейсов: {cases_opened}\n"
            f"🎮 Любимая игра: {favorite_game_str}\n"
            f"💎 Подписка: {subscription_status}\n"
            f"🔒 Приватный канал: {private_channel_status}", 
            reply_markup=markup
        )
        
    except Exception as e:
        logging.error(f"Ошибка профиля: {str(e)}")
        await call.message.answer("⚠ Ошибка загрузки профиля")
    
    await call.answer()


    
    
    

@dp.callback_query_handler(lambda c: c.data == "support")
async def start_support(call: types.CallbackQuery, state: FSMContext):
    try:
        
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("◀️ Назад", callback_data="back_to_main"))
        
        
        await States.SUPPORT_MESSAGE.set()
        await call.message.edit_text(
            "📝 Напишите сообщение. Вы можете отправить любой тип контента: 💬 текст, 📸 фото, 🎥 видео, 🎵 голос, 🎶 музыку,📁 файл, 📍геопозицию, 👤 контакт, 🖼 стикер и 🎞 GIF",
            reply_markup=markup
        )
        
    except Exception as e:
        
        await call.message.answer(
            "📝 Напишите сообщение. Вы можете отправить любой тип контента: 💬 текст, 📸 фото, 🎥 видео, 🎵 голос, 🎶 музыку,📁 файл, 📍геопозицию, 👤 контакт, 🖼 стикер и 🎞 GIF",
            reply_markup=markup
        )
    
    await call.answer()




CONTENT_TYPES = {
    'text': '📝 Текст',
    'photo': '📸 Фото',
    'video': '🎥 Видео',
    'voice': '🎵 Голос',
    'audio': '🎶 Музыка',  
    'document': '📁 Файл',
    'location': '📍 Геопозиция',
    'contact': '👤 Контакт',
    'sticker': '🖼 Стикер',
    'animation': '🎞 GIF'
}


@dp.callback_query_handler(lambda c: c.data.startswith("reply_"))
async def handle_reply_button(call: types.CallbackQuery, state: FSMContext):
    try:
        user_id = int(call.data.split('_')[1])
        await States.ADMIN_REPLY.set()
        await state.update_data(target_user=user_id)
        await call.message.answer("💬 Введите ответ для пользователя:")
        await call.answer()
    except Exception as e:
        logging.error(f"Ошибка обработки кнопки ответа: {e}")
        await call.answer("⚠ Произошла ошибка!")




@dp.message_handler(state=States.SUPPORT_MESSAGE, content_types=types.ContentTypes.ANY)
async def process_support(message: types.Message, state: FSMContext):
    user = message.from_user
    keyboard = InlineKeyboardMarkup().add(InlineKeyboardButton("✉️ Ответить", callback_data=f"reply_{user.id}"))
    content_type = message.content_type
    type_description = CONTENT_TYPES.get(content_type, '📌 неизвестный формат')
    
    username = f"@{user.username}" if user.username else "Без юзернейма"
    header = f"📝 Сообщение в поддержку\n\n{type_description}\n👤 {username}\n🆔 ID: {user.id}\n"
    caption = message.caption or ""
    text = message.text or ""

    try:
        
        if content_type in ['photo', 'video', 'audio', 'document', 'voice']:
            full_caption = f"{header}💬 {caption}"
            
            if content_type == 'photo':
                await bot.send_photo(SUPPORT_CHAT_ID, message.photo[-1].file_id, 
                                   caption=full_caption, reply_markup=keyboard)
            elif content_type == 'video':
                await bot.send_video(SUPPORT_CHAT_ID, message.video.file_id, 
                                    caption=full_caption, reply_markup=keyboard)
            elif content_type == 'audio':
                await bot.send_audio(SUPPORT_CHAT_ID, message.audio.file_id,
                                    caption=full_caption, reply_markup=keyboard)
            elif content_type == 'document':
                await bot.send_document(SUPPORT_CHAT_ID, message.document.file_id,
                                       caption=full_caption, reply_markup=keyboard)
            elif content_type == 'voice':
                await bot.send_voice(SUPPORT_CHAT_ID, message.voice.file_id,
                                    caption=full_caption, reply_markup=keyboard)
            
            if text and content_type != 'text':
                await bot.send_message(SUPPORT_CHAT_ID, f"{header}📝 Текст:\n{text}", reply_markup=keyboard)

        else:
            if content_type == 'text':
                await bot.send_message(SUPPORT_CHAT_ID, f"{header}{text}", reply_markup=keyboard)
            else:
                await bot.send_message(SUPPORT_CHAT_ID, header, reply_markup=keyboard)
                if content_type == 'location':
                    await bot.send_location(SUPPORT_CHAT_ID, 
                                          latitude=message.location.latitude,
                                          longitude=message.location.longitude)
                elif content_type == 'contact':
                    await bot.send_contact(SUPPORT_CHAT_ID, 
                                         phone_number=message.contact.phone_number,
                                         first_name=message.contact.first_name)
                elif content_type == 'sticker':
                    await bot.send_sticker(SUPPORT_CHAT_ID, message.sticker.file_id)
                elif content_type == 'animation':
                    await bot.send_animation(SUPPORT_CHAT_ID, message.animation.file_id)

        await message.answer("✅ Обращение отправлено!")

    except Exception as e:
        logging.error(f"Ошибка отправки в поддержку: {e}")
        await message.answer("❌ Не удалось отправить обращение!")
    
    await state.finish()

@dp.message_handler(state=States.ADMIN_REPLY, content_types=types.ContentTypes.ANY)
async def handle_admin_reply(message: types.Message, state: FSMContext):
    try:
        data = await state.get_data()
        target_user = data['target_user']
        content_type = message.content_type
        caption = message.caption or ""
        text = message.text or ""

        await bot.send_message(target_user, "📨 Ответ от поддержки:")

        if content_type in ['photo', 'video', 'audio', 'document', 'voice']:
            if content_type == 'photo':
                await bot.send_photo(target_user, message.photo[-1].file_id, caption=caption)
            elif content_type == 'video':
                await bot.send_video(target_user, message.video.file_id, caption=caption)
            elif content_type == 'audio':
                await bot.send_audio(target_user, message.audio.file_id, caption=caption)
            elif content_type == 'document':
                await bot.send_document(target_user, message.document.file_id, caption=caption)
            elif content_type == 'voice':
                await bot.send_voice(target_user, message.voice.file_id, caption=caption)
            
            if text:
                await bot.send_message(target_user, text)

        else:
            if content_type == 'text':
                await bot.send_message(target_user, text)
            elif content_type == 'location':
                await bot.send_location(target_user,
                                       latitude=message.location.latitude,
                                       longitude=message.location.longitude)
            elif content_type == 'contact':
                await bot.send_contact(target_user,
                                     phone_number=message.contact.phone_number,
                                     first_name=message.contact.first_name)
            elif content_type == 'sticker':
                await bot.send_sticker(target_user, message.sticker.file_id)
            elif content_type == 'animation':
                await bot.send_animation(target_user, message.animation.file_id)

        await message.answer("✅ Ответ успешно отправлен!")

    except Exception as e:
        logging.error(f"Ошибка отправки ответа: {e}")
        await message.answer("❌ Не удалось отправить ответ!")
    
    await state.finish()


@dp.message_handler(commands=["admin"], chat_id=SUPPORT_CHAT_ID)
async def admin_panel(message: types.Message):
    try:
        member = await bot.get_chat_member(SUPPORT_CHAT_ID, message.from_user.id)
        if member.status not in [types.ChatMemberStatus.ADMINISTRATOR, types.ChatMemberStatus.CREATOR]:
            return

        await message.answer(
    "🔐 <b>Список админ-команд:</b>\n\n"
    "🔃 /restart - Перезагрузка бота\n"
    "🗄 /backup - Резервная копия базы данных\n"
    "ℹ /info - Информация о боте\n"
    "📊 /stat - Статистика бота\n"
    "🏆 /top - Топ пользователей бота\n"
    "👥 /users - Список пользователей\n"
    "📨 /message [сообщение] - Рассылка сообщений\n"
    "🚫 /ban [ID/username] [причина] - Забанить пользователя\n"
    "✅ /unban [ID/username] - Разбанить пользователя\n"
    "👤 /user [ID/username] - Инфо о пользователе\n"
    "🎁 /promo [код] [сумма] [кол-во активаций] - Создать промокод\n"
    "❌ /unpromo [код] - Удалить промокод",
    parse_mode="HTML"
)

    except Exception as e:
        logging.error(f"Ошибка админ-панели: {e}")
        await message.answer("❌ Ошибка доступа!")





@dp.message_handler(commands=["stat"], chat_id=SUPPORT_CHAT_ID)
async def cmd_stat(message: types.Message):
    try:
        stats = {
            'total_users': (await fetch_query('SELECT COUNT(*) as cnt FROM users', (), True)).get('cnt', 0),
            'total_banned': (await fetch_query('SELECT COUNT(*) as cnt FROM users WHERE banned = 1', (), True)).get('cnt', 0),
            'total_games': (await fetch_query('SELECT COUNT(*) as cnt FROM transactions WHERE description LIKE "%Ставка%"', (), True)).get('cnt', 0),
            'total_deposits': (await fetch_query('SELECT COALESCE(SUM(amount), 0) as dep FROM transactions WHERE description LIKE "%Пополнение%"', (), True)).get('dep', 0),
            'total_withdrawals': abs((await fetch_query('SELECT COALESCE(SUM(amount), 0) as wd FROM transactions WHERE description LIKE "%Вывод%"', (), True)).get('wd', 0)),
            'total_wins': (await fetch_query('SELECT COALESCE(SUM(amount), 0) as win FROM transactions WHERE description LIKE "%Выигрыш%"', (), True)).get('win', 0),
            'total_ref_bonus': (await fetch_query('SELECT COALESCE(SUM(amount), 0) as ref FROM transactions WHERE description LIKE "%Реферальный%"', (), True)).get('ref', 0),
            'active_requests': (await fetch_query('SELECT COUNT(*) as cnt FROM withdraw_requests WHERE status = "pending"', (), True)).get('cnt', 0),
            'total_cases': (await fetch_query('SELECT COUNT(*) as cnt FROM transactions WHERE description LIKE "%Покупка кейса%"', (), True)).get('cnt', 0),
            'total_cases_spent': abs((await fetch_query('SELECT COALESCE(SUM(amount), 0) as spent FROM transactions WHERE description LIKE "%Покупка кейса%"', (), True)).get('spent', 0)),
            'active_users': (await fetch_query('''
            SELECT COUNT(*) as cnt 
            FROM activity 
            WHERE last_activity > datetime('now', '-5 minutes', 'localtime')
            ''', (), True)).get('cnt', 0)
        }

        stat_text = (
            f"📊 Статистика бота:\n\n"
            f"👤 Всего пользователей: {stats['total_users']}\n"
            f"🚫 Забанено: {stats['total_banned']}\n"
            f"🎮 Игр сыграно: {stats['total_games']}\n"
            f"🎲 Открыто кейсов: {stats['total_cases']}\n"  
            f"💸 Потрачено на кейсы: {stats['total_cases_spent']:,}₽\n"  
            f"💳 Пополнений: {stats['total_deposits']:,}₽\n"
            f"📤 Выводов: {stats['total_withdrawals']:,}₽\n"
            f"🎉 Выигрыши: {stats['total_wins']:,}₽\n"
            f"🎁 Реферальные бонусы: {stats['total_ref_bonus']:,}₽\n"
            f"📨 Активные заявки на вывод: {stats['active_requests']}\n"
            f"🟢 Онлайн: {stats['active_users']}"
        )

        keyboard = InlineKeyboardMarkup()
        keyboard.add(InlineKeyboardButton("⚠️ Очистить ВСЕ данные", callback_data="clear_db_confirm"))
        
        await message.answer(stat_text, reply_markup=keyboard)
        
    except Exception as e:
        logging.error(f"Ошибка статистики: {str(e)}", exc_info=True)
        await message.answer("❌ Ошибка получения данных. Проверьте логи")
        
        
        
        
        
@dp.callback_query_handler(lambda c: c.data.startswith("clear_db"), chat_id=SUPPORT_CHAT_ID)
async def handle_clear_db(call: types.CallbackQuery):
    action = call.data.split("_")[-1]
    
    if action == "confirm":
        
        keyboard = InlineKeyboardMarkup()
        keyboard.row(
            InlineKeyboardButton("❌ Нет", callback_data="clear_db_cancel"),
            InlineKeyboardButton("✅ Да, удалить ВСЁ", callback_data="clear_db_execute")
        )
        await call.message.edit_text(
            "🚨 Вы уверены, что хотите удалить ВСЕ данные? Это действие нельзя отменить!",
            reply_markup=keyboard
        )
        
    elif action == "execute":
        
        try:
            tables = [
                "users", "transactions", 
                "withdraw_requests", "payment_requests",
                "promocodes", "used_promocodes", "activity", "all_games", "favorite_games", "subscriptions", "private_channel_purchases", "bot_info"
            ]
            
            async with aiosqlite.connect(DB_NAME) as db:
                for table in tables:
                    await db.execute(f"DELETE FROM {table}")
                await db.commit()
                
            await call.message.edit_text("✅ ВСЕ данные удалены!")
            logging.warning(f"Админ {call.from_user.id} очистил базу данных!")
            
        except Exception as e:
            await call.message.edit_text(f"❌ Ошибка: {str(e)}")
            logging.error(f"Ошибка очистки БД: {str(e)}")
            
    elif action == "cancel":
        await call.message.edit_text("❌ Очистка отменена")
        
    await call.answer()



@dp.callback_query_handler(lambda c: c.data == "cases", state="*")
async def show_cases(call: types.CallbackQuery):
    cases_text = "📦 *Кейсы*\n\n"
    for case in CASES.values():
        cases_text += (
            f"🔸 *{case['name']}*\n"
            f"Цена: {case['price']}₽\n"
            f"Содержимое: {case['min']}-{case['max']}₽\n\n"
        )

    markup = InlineKeyboardMarkup(row_width=2)
    buttons = []
    for key in CASES.keys():
        case = CASES[key]
        buttons.append(InlineKeyboardButton(
            f"{case['name']} ({case['price']}₽)", 
            callback_data=f"case_{key}"
        ))
    markup.add(*buttons)
    markup.add(InlineKeyboardButton("◀️ Назад", callback_data="back_to_main"))

    await call.message.edit_text(
        cases_text,
        reply_markup=markup,
        parse_mode="Markdown"
    )
    await States.SELECT_CASE.set()
    await call.answer()



@dp.callback_query_handler(lambda c: c.data.startswith("case_"), state=States.SELECT_CASE)
async def case_details(call: types.CallbackQuery, state: FSMContext):
    case_key = call.data.split("_")[1]
    case = CASES[case_key]
    user_id = call.from_user.id
    balance = await get_balance(user_id)
    
    details_text = (
        f"📦 *{case['name']} Кейс*\n\n"
        f"💵 Цена: {case['price']}₽\n"
        f"🎁 Содержимое: {case['min']}₽ - {case['max']}₽\n"
        f"💰 Ваш баланс: {balance:.1f}₽\n\n"
    )
    
    markup = InlineKeyboardMarkup()
    markup.row(
        InlineKeyboardButton("✅ Купить", callback_data=f"buy_{case_key}"),
        InlineKeyboardButton("◀️ Назад", callback_data="cases")
    )
    
    await call.message.edit_text(
        details_text,
        reply_markup=markup,
        parse_mode="Markdown"
    )
    await States.CASE_DETAILS.set()
    await call.answer()
    
    

@dp.callback_query_handler(lambda c: c.data.startswith("buy_"), state=States.CASE_DETAILS)
async def buy_case(call: types.CallbackQuery):
    case_key = call.data.split("_")[1]
    case = CASES[case_key]
    user_id = call.from_user.id
    username = call.from_user.username or "без юзернейма"
    
    balance = await get_balance(user_id)
    if balance < case['price']:
        await call.answer("❌ Недостаточно средств на балансе!", show_alert=True)
        return
    
    await update_balance(user_id, -case['price'])
    await add_transaction(user_id, f"Покупка кейса {case['name']}", -case['price'])  
    await call.message.edit_text("🕒 Открываю кейс...")
    await asyncio.sleep(2)
    
    reward = random.choices(case['rewards'], weights=case['probabilities'])[0]
    await update_balance(user_id, reward)
    await add_transaction(user_id, f"Выигрыш из кейса {case['name']}", reward)  
    new_balance = await get_balance(user_id)
    support_msg = (
        f"📦 *Открытие кейса*\n"
        f"👤 Юзернейм: @{escape_md(username)}\n"
        f"🆔 ID: `{user_id}`\n"
        f"🎁 Кейс: {case['name']}\n"
        f"💰 Выпало: {reward}₽\n"
        f"🏦 Новый баланс: {new_balance}₽"
    )
    
    try:
        await bot.send_message(
            SUPPORT_CHAT_ID,
            support_msg,
            parse_mode="Markdown"
        )
    except Exception as e:
        logging.error(f"Ошибка отправки в SUPPORT_CHAT: {str(e)}")
    
 
    result_text = (
        f"🎉 *Результат открытия:*\n"
        f"▫️ Выпало: {reward}₽\n"
        f"▫️ Новый баланс: {new_balance}₽\n\n"
        f"📦 Попробуйте еще кейсы!"
    )
    
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("◀️ Назад", callback_data="cases"))
    
    await call.message.edit_text(
        result_text,
        reply_markup=markup,
        parse_mode="Markdown"
    )
    await call.answer()
    
    
    



async def give_daily_case(user_id):
    case_key = 'classic'
    case = CASES.get(case_key)
    if not case:
        return
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("Открыть", callback_data=f"open_daily_{case_key}"))
    await bot.send_message(
        user_id,
        f"Вы получили ежедневный кейс \"{case['name']}\"",
        reply_markup=markup
    )



@dp.callback_query_handler(lambda c: c.data.startswith("open_daily_"), state="*")
async def open_daily_case(call: CallbackQuery):
    try:
        await call.answer()
        user_id = call.from_user.id
        case_key = call.data.replace("open_daily_", "", 1)
        case = CASES.get(case_key)
        if not case:
            await call.answer("Кейс не найден!", show_alert=True)
            return
        if await is_processing(user_id):
            await call.answer("Подождите, кейс уже открывается...", show_alert=True)
            return
        await set_processing(user_id, True)
        await call.message.edit_text("🕒 Открываем кейс...")
        await asyncio.sleep(2)
        reward = random.choices(
            case['rewards'],
            weights=case['probabilities']
        )[0]
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute("BEGIN TRANSACTION")
            try:
                await db.execute(
                    "UPDATE users SET balance = balance + ? WHERE user_id = ?",
                    (reward, user_id)
                )
                await db.execute(
                    """INSERT INTO transactions 
                    (user_id, description, amount, date)
                    VALUES (?, ?, ?, ?)""",
                    (user_id, f"Бесплатный кейс {case['name']}", reward,
                     datetime.now().strftime("%d.%m %H:%M"))
                )
                await db.commit()
            except Exception as e:
                await db.rollback()
                logging.error(f"Ошибка базы данных: {str(e)}")
                await call.answer("Ошибка при открытии кейса", show_alert=True)
                return
        new_balance = await get_balance(user_id)
        result_text = (
            f"🎉 *Результат открытия:*\n"
            f"▫️ Выпало: {reward}₽\n"
            f"▫️ Новый баланс: {new_balance:.1f}₽\n\n"
            f"📦 Попробуйте еще кейсы!"
        )
        await call.message.edit_text(
            result_text,
            reply_markup=InlineKeyboardMarkup().add(
                InlineKeyboardButton("◀️ Назад", callback_data="cases")
            ),
            parse_mode="Markdown"
        )
        username = call.from_user.username or "без юзернейма"
        support_msg = (
            f"📦 *Бесплатный кейс*\n"
            f"👤 @{escape_md(username)}\n"
            f"🆔 `{user_id}`\n"
            f"🎁 {case['name']}\n"
            f"💰 {reward}₽ → {new_balance}₽"
        )
        try:
            await bot.send_message(SUPPORT_CHAT_ID, support_msg, parse_mode="Markdown")
        except Exception as e:
            logging.error(f"Ошибка отправки в поддержку: {str(e)}")
    except Exception as e:
        logging.error(f"Ошибка в open_daily_case: {str(e)}")
        await call.answer("Произошла ошибка", show_alert=True)
    finally:
        await set_processing(user_id, False)




processing_users = set()

async def is_processing(user_id: int) -> bool:
    return user_id in processing_users





async def set_processing(user_id: int, status: bool):
    if status:
        processing_users.add(user_id)
    else:
        processing_users.discard(user_id)


        
    
    

@dp.message_handler(commands=["users"], chat_id=SUPPORT_CHAT_ID)
async def cmd_users(message: types.Message):
    try:
        current_date = (datetime.utcnow() + timedelta(hours=3)).strftime('%d.%m.%Y')
        
        users_data = await fetch_query('''
            SELECT 
                (SELECT COUNT(*) FROM users) as total,
                (SELECT COUNT(*) FROM users WHERE registration_date = ?) as today
        ''', (current_date,), fetch_one=True)
        
        total_users = users_data['total']
        today_users = users_data['today']
        
        users = await fetch_query('SELECT user_id, username FROM users ORDER BY user_id DESC LIMIT 500')
        
        header = (
            f"👥 Всего пользователей: {total_users}\n"
            f"🆕 Зарегистрировано сегодня: {today_users}\n"
            "────────────────────"
        )
        
        response = [header]

        def number_to_emoji(n: int) -> str:
            mapping = {
                '0': "0️⃣", '1': "1️⃣", '2': "2️⃣", '3': "3️⃣",
                '4': "4️⃣", '5': "5️⃣", '6': "6️⃣", '7': "7️⃣",
                '8': "8️⃣", '9': "9️⃣"
            }
            return "".join(mapping[d] for d in str(n)) 

        for i, user in enumerate(users, 1):
            index_emoji = number_to_emoji(i)
            username = f"@{user['username']}" if user['username'] else "нет username"
            response.append(f"{index_emoji} {username} | ID: {user['user_id']}")

        for chunk in [response[i:i+50] for i in range(0, len(response), 50)]:
            await message.answer("\n".join(chunk))
            
    except Exception as e:
        logging.error(f"Ошибка получения пользователей: {e}")
        await message.answer("Ошибка получения списка пользователей")    

        

        
       

CONTENT_TYPES = {
    'text': '📝 Текст',
    'photo': '📸 Фото',
    'video': '🎥 Видео',
    'voice': '🎵 Голос',
    'audio': '🎶 Музыка',
    'document': '📁 Файл',
    'location': '📍 Геопозиция',
    'contact': '👤 Контакт',
    'sticker': '🖼 Стикер',
    'animation': '🎞 GIF'
}

pending_broadcast = {}

class BroadcastFilter(BoundFilter):
    key = 'is_broadcast'
    def __init__(self, is_broadcast):
        self.is_broadcast = is_broadcast
    async def check(self, message: types.Message):
        if message.chat.id != SUPPORT_CHAT_ID:
            return False
        if message.text:
            return message.text.startswith('/message')
        if message.caption:
            return message.caption.startswith('/message')
        return False

dp.filters_factory.bind(BroadcastFilter)

@dp.message_handler(is_broadcast=True, content_types=types.ContentTypes.ANY)
async def handle_broadcast(message: types.Message):
    try:
        ct = message.content_type
        type_description = CONTENT_TYPES.get(ct, '📌 неизвестный формат')
        content = {"content_type": ct}
        if ct == 'text':
            parts = message.text.split(' ', 1)
            content['text'] = parts[1] if len(parts) > 1 else ""
        elif ct in ['photo', 'video', 'audio', 'document', 'voice']:
            parts = message.caption.split(' ', 1) if message.caption else [""]
            content['text'] = parts[1] if len(parts) > 1 else ""
            if ct == 'photo':
                content['file_id'] = message.photo[-1].file_id
            elif ct == 'video':
                content['file_id'] = message.video.file_id
            elif ct == 'audio':
                content['file_id'] = message.audio.file_id
            elif ct == 'document':
                content['file_id'] = message.document.file_id
            elif ct == 'voice':
                content['file_id'] = message.voice.file_id
        elif ct == 'location':
            content['location'] = {
                'latitude': message.location.latitude,
                'longitude': message.location.longitude
            }
            parts = message.caption.split(' ', 1) if message.caption else [""]
            content['text'] = parts[1] if len(parts) > 1 else ""
        elif ct == 'contact':
            content['contact'] = {
                'phone_number': message.contact.phone_number,
                'first_name': message.contact.first_name,
                'last_name': message.contact.last_name or ""
            }
            parts = message.caption.split(' ', 1) if message.caption else [""]
            content['text'] = parts[1] if len(parts) > 1 else ""
        elif ct in ['sticker', 'animation']:
            if ct == 'sticker':
                content['file_id'] = message.sticker.file_id
            else:
                content['file_id'] = message.animation.file_id
            parts = message.caption.split(' ', 1) if message.caption else [""]
            content['text'] = parts[1] if len(parts) > 1 else ""
        else:
            await message.answer("❌ Неподдерживаемый тип контента для рассылки")
            return

        if ct == 'text' and not content.get('text'):
            await message.answer("❌ Формат: /message [сообщение]")
            return
        if ct in ['photo', 'video', 'audio', 'document', 'voice', 'sticker', 'animation'] and not content.get('file_id'):
            await message.answer("❌ Формат: /message [фото/видео/аудио/файл/стикер/GIF с опциональным текстом]")
            return

        users = await fetch_query('SELECT user_id FROM users')
        pending_broadcast[message.from_user.id] = {
            'content': content,
            'users': users
        }
        info = (
            f"✉️ Рассылка\n"
            f"Тип контента: {type_description}\n"
            f"💬 Текст: {content.get('text', '')}\n"
            f"📨 Вы уверены, что хотите отправить это сообщение {len(users)} пользователям?"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton("✅ Да", callback_data="broadcast_confirm:yes"),
             InlineKeyboardButton("❌ Нет", callback_data="broadcast_confirm:no")]
        ])
        await message.answer(info, reply_markup=kb)
    except Exception as e:
        logging.error(f"Ошибка рассылки: {e}")
        await message.answer("⚠ Произошла ошибка при рассылке!")








broadcast_sent = {}     
broadcast_details = {}   

@dp.callback_query_handler(lambda c: c.data == "broadcast_confirm:yes")
async def broadcast_confirm_yes(callback: types.CallbackQuery):
    admin_id = callback.from_user.id
    data = pending_broadcast.get(admin_id)
    if not data:
        await callback.answer("❌ Нет данных для рассылки")
        return
    content = data['content']
    users = data['users']
    ct = content.get("content_type")
    text = content.get("text") or ""
    file_id = content.get("file_id")
    location = content.get("location")
    contact = content.get("contact")
    
    success = 0
    failed = 0
    sent_messages = [] 
    delivered_details = []  
    failed_details = []   
    
    for user in users:
        user_id = user['user_id']
        try:
            if ct in ['photo', 'video', 'audio', 'document', 'voice']:
                if ct == 'photo':
                    msg = await bot.send_photo(user_id, file_id, caption=text)
                elif ct == 'video':
                    msg = await bot.send_video(user_id, file_id, caption=text)
                elif ct == 'audio':
                    msg = await bot.send_audio(user_id, file_id, caption=text)
                elif ct == 'document':
                    msg = await bot.send_document(user_id, file_id, caption=text)
                elif ct == 'voice':
                    msg = await bot.send_voice(user_id, file_id, caption=text)
            elif ct == 'location':
                msg = await bot.send_location(user_id, latitude=location['latitude'], longitude=location['longitude'])
                if text:
                    await bot.send_message(user_id, text)
            elif ct == 'contact':
                msg = await bot.send_contact(user_id, phone_number=contact['phone_number'], 
                                             first_name=contact['first_name'], 
                                             last_name=contact.get('last_name', ""))
                if text:
                    await bot.send_message(user_id, text)
            elif ct in ['sticker', 'animation']:
                if ct == 'sticker':
                    msg = await bot.send_sticker(user_id, file_id)
                else:
                    msg = await bot.send_animation(user_id, file_id)
                if text:
                    await bot.send_message(user_id, text)
            elif ct == 'text':
                msg = await bot.send_message(user_id, text)
            else:
                msg = await bot.send_message(user_id, text)
            success += 1
            sent_messages.append({'user_id': user_id, 'message_id': msg.message_id})
            delivered_details.append({
                "user_id": user_id,
                "message_id": msg.message_id,
                "time": datetime.now().strftime("%d.%m.%Y в %H:%M:%S")
            })
        except Exception as e:
            failed += 1
            failed_details.append({
                "user_id": user_id,
                "error": str(e)
            })
        await asyncio.sleep(0.1)
    
    broadcast_sent[admin_id] = sent_messages
    broadcast_details[admin_id] = {
        "delivered": delivered_details,
        "failed": failed_details
    }
    
    summary_text = (f"✉ Рассылка завершена\n"
                    f"✅ Успешно: {len(delivered_details)}\n"
                    f"❌ Не доставлено: {len(failed_details)}")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton("Посмотреть подробно", callback_data="broadcast_details")],
        [InlineKeyboardButton("🗑 Удалить сообщение", callback_data="broadcast_delete")]
    ])
    await callback.message.edit_text(summary_text, reply_markup=kb)
    pending_broadcast.pop(admin_id, None)
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data == "broadcast_confirm:no")
async def broadcast_confirm_no(callback: types.CallbackQuery):
    admin_id = callback.from_user.id
    pending_broadcast.pop(admin_id, None)
    await callback.message.edit_text("❌ Рассылка отменена")
    await callback.answer()







@dp.callback_query_handler(lambda c: c.data == "broadcast_details")
async def show_broadcast_details(callback: types.CallbackQuery):
    admin_id = callback.from_user.id
    details = broadcast_details.get(admin_id)
    if not details:
        await callback.answer("❌ Нет подробной информации")
        return

    delivered = details.get("delivered", [])
    failed = details.get("failed", [])
    
    delivered_tasks = [bot.get_chat(item['user_id']) for item in delivered]
    failed_tasks = [bot.get_chat(item['user_id']) for item in failed]
    delivered_chats = await asyncio.gather(*delivered_tasks, return_exceptions=True)
    failed_chats = await asyncio.gather(*failed_tasks, return_exceptions=True)

    text_lines = [f"✅ Доставлено: {len(delivered)}"]
    for idx, (item, chat) in enumerate(zip(delivered, delivered_chats), start=1):
        if isinstance(chat, Exception) or not getattr(chat, "username", None):
            username_display = f"ID:{item['user_id']}"
        else:
            
            username_display = "@" + "\\_".join(chat.username.split('_'))
        text_lines.append(f"{idx}. {username_display} | ID: {item['user_id']} - доставлено {item['time']}")
    
    text_lines.append("")
    text_lines.append("")
    text_lines.append(f"❌ Не доставлено: {len(failed)}")
    for idx, (item, chat) in enumerate(zip(failed, failed_chats), start=1):
        if isinstance(chat, Exception) or not getattr(chat, "username", None):
            username_display = f"ID:{item['user_id']}"
        else:
            username_display = "@" + "\\_".join(chat.username.split('_'))
        text_lines.append(f"{idx}. {username_display} | ID: {item['user_id']} - не доставлено. Причина: {item['error']}")
    
    detailed_text = "\n".join(text_lines)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton("◀️ Назад", callback_data="broadcast_back")]
    ])
    await callback.message.edit_text(detailed_text, reply_markup=kb, parse_mode="Markdown")
    await callback.answer()
    
    
    

@dp.callback_query_handler(lambda c: c.data == "broadcast_back")
async def back_to_summary(callback: types.CallbackQuery):
    admin_id = callback.from_user.id
    details = broadcast_details.get(admin_id)
    if not details:
        await callback.answer("❌ Нет данных для возврата")
        return
    summary_text = (f"✉ Рассылка завершена\n"
                    f"✅ Успешно: {len(details.get('delivered', []))}\n"
                    f"❌ Не доставлено: {len(details.get('failed', []))}")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton("Посмотреть подробно", callback_data="broadcast_details")],
        [InlineKeyboardButton("🗑 Удалить сообщение", callback_data="broadcast_delete")]
    ])
    await callback.message.edit_text(summary_text, reply_markup=kb)
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "broadcast_delete")
async def broadcast_delete(callback: types.CallbackQuery):
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("🗑 Удалить", callback_data="confirm_broadcast_delete"),
        InlineKeyboardButton("❌ Отменить", callback_data="cancel_broadcast_delete")
    )
    await callback.message.edit_text("⚠ Вы уверены, что хотите удалить это сообщение?", reply_markup=keyboard)
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data == "confirm_broadcast_delete")
async def confirm_broadcast_delete(callback: types.CallbackQuery):
    admin_id = callback.from_user.id
    sent_messages = broadcast_sent.get(admin_id, [])
    del_success = 0
    del_failed = 0
    for item in sent_messages:
        user_id = item['user_id']
        message_id = item['message_id']
        try:
            await bot.delete_message(user_id, message_id)
            del_success += 1
        except Exception:
            del_failed += 1
        await asyncio.sleep(0.1)
    broadcast_sent.pop(admin_id, None)
    await callback.message.edit_text(
        f"🗑 Рассылка удалена\n✅ Удалено: {del_success}\n❌ Не удалено: {del_failed}"
    )
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data == "cancel_broadcast_delete")
async def cancel_broadcast_delete(callback: types.CallbackQuery):
    admin_id = callback.from_user.id
    details = broadcast_details.get(admin_id)
    if details:
        summary_text = (f"✉ Рассылка завершена\n"
                        f"✅ Успешно: {len(details.get('delivered', []))}\n"
                        f"❌ Не доставлено: {len(details.get('failed', []))}")
    else:
        summary_text = "🗑 Удаление сообщения отменено"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton("Назад", callback_data="broadcast_back")]
    ])
    await callback.message.edit_text(summary_text, reply_markup=kb)
    await callback.answer()



 
def profile_button(user_id):
    return InlineKeyboardMarkup().add(
        InlineKeyboardButton("👤 Профиль", callback_data=f"user_profile_{user_id}")
    )
    
    
    

def plural_ru(n, forms):
    n = abs(n)
    if n % 10 == 1 and n % 100 != 11: return forms[0]
    elif 2 <= n % 10 <= 4 and not (12 <= n % 100 <= 14): return forms[1]
    else: return forms[2]






def parse_duration(text):
    text = text.lower().strip().replace(' ', '').replace('ё', 'е')
    logging.info(f"Парсинг времени: '{text}'")

    time_units = [
        {'patterns': ['год', 'г', 'y'], 'sec': 31557600, 'forms': ('год', 'года', 'лет')},
        {'patterns': ['мес', 'm'], 'sec': 2629800, 'forms': ('месяц', 'месяца', 'месяцев')},
        {'patterns': ['нед', 'w'], 'sec': 604800, 'forms': ('неделя', 'недели', 'недель')},
        {'patterns': ['день', 'д', 'd'], 'sec': 86400, 'forms': ('день', 'дня', 'дней')},
        {'patterns': ['час', 'ч', 'h'], 'sec': 3600, 'forms': ('час', 'часа', 'часов')},
        {'patterns': ['мин', 'м', 'm'], 'sec': 60, 'forms': ('минута', 'минуты', 'минут')},
        {'patterns': ['сек', 'с', 's'], 'sec': 1, 'forms': ('секунда', 'секунды', 'секунд')},
        {'patterns': ['навсегда', 'perm'], 'sec': None, 'forms': ('навсегда',)}
    ]

    if text in ['навсегда', 'perm', 'permanent']:
        return None, 'навсегда', 'permanent'

    match = re.match(r'^(\d+)([а-яa-z]+)$', text)
    if not match:
        return None, None, None

    num = int(match.group(1))
    unit = match.group(2)

    for u in time_units:
        for pattern in u['patterns']:
            if unit.startswith(pattern):
                if u['sec'] is None:
                    return None, 'навсегда', 'permanent'
                
                sec = u['sec'] * num
                display = f"{num} {plural_ru(num, u['forms'])}"
                db_text = f"{num}{u['patterns'][-1]}"
                return sec, display, db_text

    return None, None, None
    
    

   
   
@dp.message_handler(commands=["ban"], chat_id=SUPPORT_CHAT_ID)
async def cmd_ban(message: types.Message):
    try:
        import re
        match = re.match(
            r"/ban\s+(@?\w+)(?:\s+((?:\d+\s*[а-яa-z]+(?:\s+\d+\s*[а-яa-z]+)?)))?(?:\s+(.+))?",
            message.text,
            re.IGNORECASE
        )
        if not match:
            await message.answer("❌ Формат: /ban [ID/@username] [время] [причина]\nПример: /ban @user 2 часа спам")
            return

        target = match.group(1)
        time_arg = match.group(2)
        reason = match.group(3) if match.group(3) else "не указана"

        if time_arg:
            time_arg_clean = time_arg.replace(" ", "")
            duration, display_text, db_text = parse_duration(time_arg_clean)
            if not duration and db_text != "permanent":
                await message.answer(f"❌ Неверный формат времени: {time_arg}")
                return
        else:
            duration, display_text, db_text = None, "навсегда", "permanent"

        if target.startswith('@'):
            user = await fetch_query(
                "SELECT user_id, username, banned FROM users WHERE username = ?",
                (target[1:],),
                fetch_one=True
            )
        else:
            try:
                user = await fetch_query(
                    "SELECT user_id, username, banned FROM users WHERE user_id = ?",
                    (int(target),),
                    fetch_one=True
                )
            except ValueError:
                await message.answer("❌ Некорректный ID пользователя!")
                return

        if not user:
            await message.answer("❌ Пользователь не найден!")
            return

        if user['banned']:
            await message.answer("⚠️ Пользователь уже забанен!")
            return

        user_id = user['user_id']
        username = user.get('username', 'N/A')

        ban_until_db = None
        ban_until_display = "никогда"
        if duration:
            ban_until = datetime.now() + timedelta(seconds=duration)
            ban_until_db = ban_until.strftime('%Y-%m-%d %H:%M:%S')
            ban_until_display = ban_until.strftime('%d.%m.%Y %H:%M')

        await execute_query(
            '''UPDATE users SET 
               banned = 1, 
               ban_reason = ?, 
               ban_time = ?, 
               ban_until = ? 
               WHERE user_id = ?''',
            (reason, db_text, ban_until_db, user_id)
        )

        response_msg = "\n".join([
            f"🚫 Пользователь @{username} забанен",
            f"🆔 ID: {user_id}",
            f"⌛ Время: {display_text}",
            f"💬 Причина: {reason}",
            f"📅 Дата окончания: {ban_until_display}"
        ])
        await message.answer(response_msg, reply_markup=profile_button(user_id))

        user_message = "\n".join([
            "🚫 Ваш аккаунт заблокирован!",
            f"⌛ Срок блокировки: {display_text}",
            f"💬 Причина: {reason}",
            f"📅 Дата разблокировки: {ban_until_display}"
        ])
        appeal_kb = InlineKeyboardMarkup().add(
            InlineKeyboardButton("⚙ Подать апелляцию", callback_data=f"appeal_{user_id}")
        )
        await bot.send_message(user_id, user_message, reply_markup=appeal_kb)
    except Exception as e:
        logging.error(f"Ошибка в /ban: {e}", exc_info=True)
        await message.answer("❌ Произошла системная ошибка")




@dp.callback_query_handler(lambda c: c.data and c.data.startswith("appeal_"))
async def handle_appeal_callback(call: types.CallbackQuery, state: FSMContext):
    user_id = int(call.data.split("_")[1])
    if call.from_user.id != user_id:
        await call.answer("🚫 Эта кнопка не для вас!", show_alert=True)
        return
    
    prompt = (
        "📝 Напишите, как вы думаете, из-за чего вы могли быть заблокированы, "
        "и почему ваш аккаунт должен быть разблокирован\n\n"
        "📎 Вы можете прикрепить *любой тип контента* (фото, видео, файл, ГС и тд.)"
    )
    
    await call.message.edit_text(prompt, parse_mode="Markdown")
    await States.waiting_for_appeal.set()
    await call.answer()




@dp.message_handler(state=States.waiting_for_appeal, content_types=ContentTypes.ANY)
async def process_appeal_message(message: types.Message, state: FSMContext):
    user = message.from_user
    user_id = user.id
    sender = f"@{user.username}" if user.username else f"ID: {user_id}"
    text_part = f"\n💬 Аппеляция: {message.text}" if message.content_type == "text" else ""
    content_part = "\n💬 Аппеляция 👇" if message.content_type != "text" else ""
    
    header = (
        f"✉️ Аппеляция на разблокировку\n"
        f"👤 Пользователь: {sender}\n"
        f"🆔 ID: {user_id}{text_part}{content_part}"
    )
    
    kb = InlineKeyboardMarkup().add(
        InlineKeyboardButton("👤 Профиль", callback_data=f"user_profile_{user_id}")
    )
    
    await bot.send_message(SUPPORT_CHAT_ID, header, reply_markup=kb)
    
    if message.content_type != "text":
        if message.content_type == "photo":
            await bot.send_photo(SUPPORT_CHAT_ID, message.photo[-1].file_id, caption=message.caption or "")
        elif message.content_type == "video":
            await bot.send_video(SUPPORT_CHAT_ID, message.video.file_id, caption=message.caption or "")
        elif message.content_type == "document":
            await bot.send_document(SUPPORT_CHAT_ID, message.document.file_id, caption=message.caption or "")
        elif message.content_type == "audio":
            await bot.send_audio(SUPPORT_CHAT_ID, message.audio.file_id, caption=message.caption or "")
        elif message.content_type == "voice":
            await bot.send_voice(SUPPORT_CHAT_ID, message.voice.file_id, caption=message.caption or "")
        elif message.content_type == "video_note":
            await bot.send_video_note(SUPPORT_CHAT_ID, message.video_note.file_id)
        elif message.content_type == "animation":
            await bot.send_animation(SUPPORT_CHAT_ID, message.animation.file_id, caption=message.caption or "")
        else:
            await message.forward(SUPPORT_CHAT_ID)

    await message.answer("✅ Ваша апелляция отправлена в поддержку")
    await state.finish()




@dp.message_handler(commands=["unban"], chat_id=SUPPORT_CHAT_ID)
async def cmd_unban(message: types.Message):
    try:
        args = message.text.split(maxsplit=1)
        if len(args) < 2:
            await message.answer("❌ Формат: /unban [ID/@username]")
            return

        target = args[1].strip()
        user_id = None
        
        if target.startswith('@'):
            user = await fetch_query('SELECT user_id, banned FROM users WHERE username = ?', (target[1:],), fetch_one=True)
            if not user:
                await message.answer(f"❌ Пользователь {target} не найден!")
                return
            user_id = user["user_id"]
            if not user["banned"]:
                await message.answer(f"❌ Пользователь {target} не забанен!")
                return
        else:
            try:
                user_id = int(target)
            except ValueError:
                await message.answer("❌ Некорректный user_id!")
                return
            user = await fetch_query('SELECT banned FROM users WHERE user_id = ?', (user_id,), fetch_one=True)
            if not user:
                await message.answer(f"❌ Пользователь с ID {user_id} не найден!")
                return
            if not user["banned"]:
                await message.answer(f"❌ Пользователь с ID {user_id} не забанен!")
                return

        await execute_query('''UPDATE users SET 
                            banned = 0, 
                            ban_reason = NULL, 
                            ban_time = NULL, 
                            ban_until = NULL 
                            WHERE user_id = ?''', (user_id,))

        response_msg = f"✅ Пользователь {target} разбанен!"
        await message.answer(response_msg, reply_markup=profile_button(user_id))
        try:
            await bot.send_message(user_id, "✅ Вы разблокированы!")
        except Exception as e:
            logging.error(f"Ошибка отправки: {e}")

    except Exception as e:
        logging.error(f"Ошибка разбана: {e}")
        await message.answer("❌ Ошибка выполнения команды!")


        



@dp.message_handler(commands=["top"], chat_id=SUPPORT_CHAT_ID)
async def cmd_top(message: types.Message):
    try:
        full_message = []
        online = await fetch_query('''
            SELECT users.user_id, users.username, MAX(activity.last_activity) as last_activity 
            FROM users 
            LEFT JOIN activity ON users.user_id = activity.user_id 
            WHERE users.banned = 0 
            GROUP BY users.user_id 
            ORDER BY last_activity DESC 
            LIMIT 10
        ''')
        full_message.extend(await format_top(
            "🟢 недавнему онлайну", 
            online, 
            "last_activity", 
            formatter=format_last_activity
        ))
        
        deposits = await fetch_query('''
            SELECT users.user_id, users.username, MAX(transactions.amount) as value 
            FROM transactions 
            JOIN users ON transactions.user_id = users.user_id 
            WHERE transactions.description LIKE '%Пополнение%' AND users.banned = 0 
            GROUP BY users.user_id 
            ORDER BY value DESC 
            LIMIT 10
        ''')
        full_message.extend(await format_top(
            "💰 максимальному пополнению", 
            deposits, 
            "value", 
            suffix="₽"
        ))
        
        games_played = await fetch_query('''
            SELECT users.user_id, users.username, SUM(all_games.play_count) as value 
            FROM all_games 
            JOIN users ON all_games.user_id = users.user_id 
            WHERE users.banned = 0 
            GROUP BY users.user_id 
            ORDER BY value DESC 
            LIMIT 10
        ''')
        full_message.extend(await format_top(
            "🎮 количеству сыгранных игр", 
            games_played, 
            "value"
        ))
        
        cases_opened = await fetch_query('''
            SELECT users.user_id, users.username, COUNT(*) as value 
            FROM transactions 
            JOIN users ON transactions.user_id = users.user_id 
            WHERE transactions.description LIKE '%Покупка кейса%' AND users.banned = 0 
            GROUP BY users.user_id 
            ORDER BY value DESC 
            LIMIT 10
        ''')
        full_message.extend(await format_top(
            "📦 количеству открытых кейсов", 
            cases_opened, 
            "value"
        ))
        
        balances = await fetch_query('''
            SELECT user_id, username, balance as value 
            FROM users 
            WHERE banned = 0 
            ORDER BY value DESC 
            LIMIT 10
        ''')
        full_message.extend(await format_top(
            "💸 самому большому балансу", 
            balances, 
            "value", 
            suffix="₽"
        ))
        
        winnings = await fetch_query('''
            SELECT users.user_id, users.username, SUM(transactions.amount) as value 
            FROM transactions 
            JOIN users ON transactions.user_id = users.user_id 
            WHERE (transactions.description LIKE '%Выигрыш%' 
                OR transactions.description LIKE '%Реферальный бонус%' 
                OR transactions.description LIKE '%Промокод%') 
            AND users.banned = 0 
            GROUP BY users.user_id 
            ORDER BY value DESC 
            LIMIT 10
        ''')
        full_message.extend(await format_top(
            "🤑 количеству выигранных денег", 
            winnings, 
            "value", 
            suffix="₽"
        ))
        
        await message.answer("\n".join(full_message))
    except Exception as e:
        logging.error(f"Ошибка команды /top: {e}")
        await message.answer("⚠ Ошибка при выполнении команды /top!")

        


async def format_top(
    stat_type: str, 
    data: list, 
    field: str, 
    formatter=None,
    suffix: str = ""
):
    if not data:
        return [f"\n🏆 Топ по {stat_type}: Нет данных"]
    
    top_list = [f"\n\n🏆 Топ по {stat_type}:"]
    
    for i, entry in enumerate(data, 1):
        username = f"@{entry['username']}" if entry.get('username') else "нет username"
        value = entry.get(field, 0)
        
        if formatter:
            value = await formatter(value)
        
        line = f"{i}. {username} | ID: {entry['user_id']}"
        
        if value is not None:
            line += f" - {value}{suffix}".rstrip()
        
        top_list.append(line)
    
    return top_list

async def format_last_activity(timestamp):
    if not timestamp:
        return "никогда"
    
    now = datetime.now()
    last_activity = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
    delta = now - last_activity
    
    intervals = (
        ('г.', 31104000),
        ('мес.', 2592000),
        ('д.', 86400),
        ('ч.', 3600),
        ('мин.', 60)
    )
    
    parts = []
    seconds = delta.total_seconds()
    
    for name, count in intervals:
        value = int(seconds // count)
        if value > 0:
            parts.append(f"{value} {name}")
            seconds %= count
    
    return " - ".join(parts) + " назад" if parts else "менее минуты назад"
        
                                        
            
    
    

confirm_action_callback = CallbackData("confirm_action", "user_id", "action")

async def show_user_profile(user_id, call_or_message):
    user = await fetch_query("SELECT * FROM users WHERE user_id = ?", (user_id,), fetch_one=True)
    if not user:
        if hasattr(call_or_message, "answer"):
            await call_or_message.answer("❌ Пользователь не найден!")
        return

    stats = await fetch_query('''
        SELECT 
            (SELECT COUNT(*) FROM transactions WHERE user_id = ? AND description LIKE "%Ставка%") as games_played,
            (SELECT COUNT(*) FROM transactions WHERE user_id = ? AND description LIKE "%Выигрыш%") as games_won,
            (SELECT COUNT(*) FROM used_promocodes WHERE user_id = ?) as promo_used,
            (SELECT COALESCE(SUM(p.amount), 0) FROM used_promocodes up 
             JOIN promocodes p ON up.code = p.code WHERE up.user_id = ?) as promo_total,
            (SELECT COUNT(*) FROM transactions WHERE user_id = ? AND description LIKE "%Покупка кейса%") as cases_opened,
            (SELECT COALESCE(SUM(amount), 0) FROM transactions WHERE user_id = ? AND description LIKE "%Покупка кейса%") as cases_spent,
            (SELECT game_name FROM favorite_games WHERE user_id = ?) as favorite_game_name,
            (SELECT play_count FROM favorite_games WHERE user_id = ?) as favorite_game_count
    ''', (user_id, user_id, user_id, user_id, user_id, user_id, user_id, user_id), fetch_one=True)

    activity = await fetch_query("SELECT last_activity FROM activity WHERE user_id = ?", (user_id,), fetch_one=True)
    last_activity = activity.get("last_activity") if activity else None
    activity_status = "💻 Онлайн: нет (никогда)"

    if last_activity:
        now = datetime.now()
        last_activity_dt = datetime.strptime(last_activity, "%Y-%m-%d %H:%M:%S")
        delta = now - last_activity_dt
        minutes, seconds = divmod(delta.total_seconds(), 60)
        hours, minutes = divmod(int(minutes), 60)
        days, hours = divmod(hours, 24)
        months, days = divmod(days, 30)
        years, months = divmod(months, 12)

        time_parts = []
        if years > 0: time_parts.append(f"{int(years)} г.")
        if months > 0: time_parts.append(f"{int(months)} мес.")
        if days > 0: time_parts.append(f"{int(days)} д.")
        if hours > 0: time_parts.append(f"{int(hours)} ч.")
        if minutes > 0: time_parts.append(f"{int(minutes)} мин.")
        if not time_parts:
            time_parts.append("менее минуты")

        activity_status = f"💻 Онлайн: {'да' if delta.total_seconds() < 300 else 'нет'} ({' '.join(time_parts)} назад)"

    username = user.get("username", "нет") or "нет"
    banned = "Да" if user.get("banned") else "Нет"
    referral_used = "Да" if user.get("referral_used") else "Нет"
    withdraw_amount = await get_withdraw_amount(user_id)

    subscription = await fetch_query("SELECT start_time, end_time FROM subscriptions WHERE user_id = ? AND end_time > ?", 
                                     (user_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S")), fetch_one=True)
    subscription_status = "Нет подписки"
    if subscription:
        start_time = datetime.strptime(subscription["start_time"], "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(subscription["end_time"], "%Y-%m-%d %H:%M:%S")
        subscription_status = f"Подписка активна с {start_time.strftime('%d.%m.%Y')} по {end_time.strftime('%d.%m.%Y')}"

    private_channel_purchase = await fetch_query("SELECT * FROM private_channel_purchases WHERE user_id = ?", 
                                                 (user_id,), fetch_one=True)
    private_channel_status = "не куплен"
    if private_channel_purchase and private_channel_purchase.get("status") == "yes":
        private_channel_status = "куплен"

    favorite_game = stats.get('favorite_game_name')
    favorite_game_count = stats.get('favorite_game_count', 0)
    favorite_game_str = f"❤ Любимая игра: {favorite_game} ({favorite_game_count} раз)" if favorite_game else "❤ Любимая игра: отсутствует"
    if not user.get("lucky_chance") or user.get("lucky_chance") == "default":
        lucky_line = "✈️ Шансы в игре лаки джет: стандартные"
    else:
        lucky_line = f"✈️ Шансы в игре лаки джет: {user['lucky_chance']}% что выпадет число {user.get('lucky_number', '?')} или больше"

    text = (f"👤 Профиль пользователя:\n"
            f"🆔 ID: {user['user_id']}\n"
            f"📛 Юзернейм: @{username}\n"
            f"📅 Дата регистрации: {user.get('registration_date', 'Неизвестно')}\n"
            f"💰 Баланс: {user.get('balance', 0)}₽\n"
            f"🚫 Забанен: {banned}\n"
            f"📤 На выводе: {withdraw_amount}₽\n"
            f"🎮 Игр сыграно: {stats.get('games_played', 0)}\n"
            f"🏆 Выиграно игр: {stats.get('games_won', 0)}\n"
            f"🎲 Открыто кейсов: {stats.get('cases_opened', 0)}\n"
            f"💸 Потрачено на кейсы: {abs(stats.get('cases_spent', 0))}₽\n"
            f"🎟 Промокодов активировано: {stats.get('promo_used', 0)}\n"
            f"💸 Сумма с промокодов: {stats.get('promo_total', 0)}₽\n"
            f"{favorite_game_str}\n"
            f"{lucky_line}\n"
            f"👤 Использовал рефералку: {referral_used}\n"
            f"💎 {subscription_status}\n"
            f"🔒 Приватный канал: {private_channel_status}\n"
            f"{activity_status}")

    keyboard = InlineKeyboardMarkup()
    keyboard.row(InlineKeyboardButton("💰 Баланс", callback_data=f"edit_balance_{user_id}"),
                 InlineKeyboardButton("📩 Написать", callback_data=f"message_user_{user_id}"),
                 InlineKeyboardButton("🗑 Удалить", callback_data=f"delete_user_{user_id}"))

    if subscription:
        keyboard.row(InlineKeyboardButton("💎 Удалить подписку", callback_data=f"subscription_remove_{user_id}"),
                     InlineKeyboardButton("☘️ Изменить шансы", callback_data=f"edit_chances_{user_id}"))
    else:
        keyboard.row(InlineKeyboardButton("💎 Активировать подписку", callback_data=f"subscription_activate_{user_id}"),
                     InlineKeyboardButton("☘️ Изменить шансы", callback_data=f"edit_chances_{user_id}"))

    photo = user.get("photo")
    if photo and str(photo).strip() != "":
        try:
            if hasattr(call_or_message, "answer_photo"):
                await call_or_message.answer_photo(photo=photo, caption=text, reply_markup=keyboard)
            elif hasattr(call_or_message, "message"):
                await call_or_message.message.answer_photo(photo=photo, caption=text, reply_markup=keyboard)
            else:
                await call_or_message.bot.send_photo(chat_id=call_or_message.chat.id, photo=photo, caption=text, reply_markup=keyboard)
        except Exception:
            if hasattr(call_or_message, "edit_text"):
                await call_or_message.edit_text(text, reply_markup=keyboard)
            else:
                await call_or_message.answer(text, reply_markup=keyboard)
    else:
        try:
            if hasattr(call_or_message, "edit_text"):
                await call_or_message.edit_text(text, reply_markup=keyboard)
            else:
                await call_or_message.answer(text, reply_markup=keyboard)
        except (exceptions.MessageCantBeEdited, exceptions.MessageNotModified):
            await call_or_message.answer(text, reply_markup=keyboard)
            
            
    

    
    
    

@dp.message_handler(commands=["user"], chat_id=SUPPORT_CHAT_ID)
async def cmd_user(message: types.Message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer("❌ Формат: /user [ID/@username]")
        return
    target = args[1].strip()
    user = None
    if target.startswith("@"):
        user = await fetch_query("SELECT * FROM users WHERE username = ?", (target[1:],), fetch_one=True)
    else:
        try:
            user_id = int(target)
            user = await fetch_query("SELECT * FROM users WHERE user_id = ?", (user_id,), fetch_one=True)
        except ValueError:
            await message.answer("❌ Некорректный ID!")
            return
    if not user:
        await message.answer("❌ Пользователь не найден!")
        return
    await show_user_profile(user["user_id"], message)



@dp.callback_query_handler(lambda c: c.data.startswith("user_profile_"), state="*")
async def callback_user_profile(call: types.CallbackQuery, state: FSMContext):
    await state.finish()  
    user_id = int(call.data.split("_")[-1])
    await show_user_profile(user_id, call.message)
    await call.answer()
    
    
@dp.callback_query_handler(lambda c: c.data.startswith("edit_balance_"), state="*")
async def edit_balance(call: types.CallbackQuery, state: FSMContext):
    user_id = int(call.data.split("_")[2])
    user = await fetch_query("SELECT user_id, balance, username FROM users WHERE user_id = ?", (user_id,), fetch_one=True)
    if not user:
        await call.answer("❌ Пользователь не найден", show_alert=True)
        return
    text = (f"👤 Пользователь: @{user.get('username', 'без юзернейма')}\n"
            f"💰 Текущий баланс: {user.get('balance', 0)}₽\n\n"
            "Введите новое значение баланса:")
    keyboard = InlineKeyboardMarkup().add(InlineKeyboardButton("❌ Отменить", callback_data=f"user_profile_{user_id}"))
    try:
        await call.message.edit_text(text, reply_markup=keyboard)
    except (exceptions.MessageCantBeEdited, exceptions.MessageNotModified):
        await call.message.answer(text, reply_markup=keyboard)
    await state.update_data(target_user=user_id)
    await States.EDIT_BALANCE.set()
    await call.answer()

@dp.message_handler(state=States.EDIT_BALANCE, chat_id=SUPPORT_CHAT_ID)
async def process_balance(message: types.Message, state: FSMContext):
    data = await state.get_data()
    user_id = data["target_user"]
    parts = message.text.split(" ", 1)
    new_balance_str = parts[0]
    comment = parts[1] if len(parts) > 1 else None
    try:
        new_balance = int(new_balance_str)
        if new_balance < 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введите целое положительное число!")
        return
    await execute_query("UPDATE users SET balance = ? WHERE user_id = ?", (new_balance, user_id))
    result_text = f"✅ Баланс изменён до {new_balance}₽" + (f"\n💬 Комментарий: {comment}" if comment else "")
    keyboard = InlineKeyboardMarkup().add(InlineKeyboardButton("◀️ Назад", callback_data=f"user_profile_{user_id}"))
    try:
        await message.answer(result_text, reply_markup=keyboard)
    except (exceptions.MessageCantBeEdited, exceptions.MessageNotModified):
        await message.answer(result_text, reply_markup=keyboard)
    try:
        await bot.send_message(user_id, f"📢 Ваш баланс изменён администратором\n💰 Новый баланс: {new_balance}₽" +
                                   (f"\n💬 Комментарий: {comment}" if comment else ""))
    except exceptions.BotBlocked:
        logging.warning(f"Пользователь {user_id} заблокировал бота")
    await state.finish()






@dp.callback_query_handler(lambda c: c.data.startswith(("subscription_activate_", "subscription_remove_")), state="*")
async def confirm_subscription_action(call: types.CallbackQuery):
    action = "activate" if "activate" in call.data else "remove"
    user_id = int(call.data.split("_")[-1])
    
    user = await fetch_query("SELECT user_id, username FROM users WHERE user_id = ?", (user_id,), fetch_one=True)
    username = f"@{user['username']}" if user and user.get("username") else f"ID: {user_id}"
    
    action_text = "активировать" if action == "activate" else "удалить"
    text = f"⚠️ Вы уверены, что хотите {action_text} подписку пользователю {username}?"
    
    keyboard = InlineKeyboardMarkup()
    keyboard.row(
        InlineKeyboardButton("✅ Да", callback_data=f"confirm_subscription_{action}_{user_id}"),
        InlineKeyboardButton("❌ Нет", callback_data=f"user_profile_{user_id}")
    )
    
    try:
        await call.message.edit_text(text, reply_markup=keyboard)
    except (exceptions.MessageCantBeEdited, exceptions.MessageNotModified):
        await call.message.answer(text, reply_markup=keyboard)
    await call.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("confirm_subscription_"), state="*")
async def handle_subscription_confirmation(call: types.CallbackQuery):
    action = call.data.split("_")[2]
    user_id = int(call.data.split("_")[-1])
    
    if action == "activate":
       
        now = datetime.now()
        expiration_date = now + timedelta(days=30)
        
        db_start_time = now.strftime("%Y-%m-%d %H:%M:%S")
        db_expiration = expiration_date.strftime("%Y-%m-%d %H:%M:%S")
        
        user_start = now.strftime("%d.%m.%Y %H:%M")
        user_expiration = expiration_date.strftime("%d.%m.%Y %H:%M")

        await execute_query(
            """INSERT INTO subscriptions (user_id, start_time, end_time, is_trial) 
            VALUES (?, ?, ?, 0) 
            ON CONFLICT(user_id) DO UPDATE SET 
                start_time = excluded.start_time, 
                end_time = excluded.end_time, 
                is_trial = excluded.is_trial""",
            (user_id, db_start_time, db_expiration)
        )
        
        user_message = f"💎 Ваша подписка активирована администратором\n⏳ Срок действия: с {user_start} по {user_expiration}"
        admin_message = f"💎 Подписка активирована\n⌛ Срок: {user_start} - {user_expiration}"
        
    else:
        await execute_query("DELETE FROM subscriptions WHERE user_id = ?", (user_id,))
        user_message = "💎 Ваша подписка была удалена администратором"
        admin_message = "💎 Подписка удалена"

    try:
        await bot.send_message(user_id, user_message)
    except Exception:
        pass
    keyboard = InlineKeyboardMarkup().add(
        InlineKeyboardButton("◀️ Назад", callback_data=f"user_profile_{user_id}")
    )
    
    try:
        await call.message.edit_text(admin_message, reply_markup=keyboard)
    except (exceptions.MessageCantBeEdited, exceptions.MessageNotModified):
        await call.message.answer(admin_message, reply_markup=keyboard)
    
    await call.answer()
    
    
    
    
    
        
@dp.callback_query_handler(lambda c: c.data.startswith('delete_user_'))
async def confirm_delete_user(call: types.CallbackQuery):
    user_id = int(call.data.split('_')[-1])
    markup = InlineKeyboardMarkup()
    markup.row(
        InlineKeyboardButton("✅ Удалить", callback_data=f"confirm_delete_{user_id}"),
        InlineKeyboardButton("❌ Отмена", callback_data=f"user_profile_{user_id}")
    )
    
    text = (
        f"⚠️ Вы уверены, что хотите удалить ВСЕ данные пользователя {user_id}?\n"
        "Это действие нельзя отменить!"
    )
    
    if call.message.photo:
        await call.message.edit_caption(caption=text, reply_markup=markup)
    else:
        await call.message.edit_text(text, reply_markup=markup)
    await call.answer()





@dp.callback_query_handler(lambda c: c.data.startswith('confirm_delete_'))
async def execute_delete_user(call: types.CallbackQuery):
    user_id = int(call.data.split('_')[-1])
    
    try:
        await delete_user_data(user_id)
        
        if call.message.photo:
            await call.message.edit_caption(f"✅ Данные пользователя {user_id} удалены!")
        else:
            await call.message.edit_text(f"✅ Данные пользователя {user_id} удалены!")
            
    except Exception as e:
        logging.error(f"Ошибка удаления: {str(e)}")
        await call.answer("⚠️ Ошибка удаления!", show_alert=True)







async def delete_user_data(user_id: int):
    deletion_order = [
        ('activity', 'user_id'),
        ('transactions', 'user_id'),
        ('withdraw_requests', 'user_id'),
        ('payment_requests', 'user_id'),
        ('used_promocodes', 'user_id'),
        ('admin_actions', 'target_user'),
        ('all_games', 'user_id'),
        ('favorite_games', 'user_id'),
        ('subscriptions', 'user_id'),
        ('private_channel_purchases', 'user_id'),
        ('users', 'user_id')
    ]

    try:
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute("PRAGMA foreign_keys = ON")
            for table, column in deletion_order:
                await db.execute(f"DELETE FROM {table} WHERE {column} = ?", (user_id,))
            await db.commit()
            
            cursor = await db.execute("SELECT 1 FROM activity WHERE user_id = ?", (user_id,))
            assert not await cursor.fetchone(), "Активность не удалена!"
            
    except Exception as e:
        logging.error(f"Ошибка удаления: {str(e)}")
        raise
            
         
            



        
@dp.callback_query_handler(lambda c: c.data.startswith("message_user_"), state="*")
async def handle_message_user(call: types.CallbackQuery, state: FSMContext):
    user_id = int(call.data.split("_")[2])
    user = await fetch_query("SELECT username, banned FROM users WHERE user_id = ?", (user_id,), fetch_one=True)
    username = user.get("username", "без юзернейма") if user else str(user_id)
    await state.update_data(target_user=user_id, original_message_id=call.message.message_id)
    keyboard = InlineKeyboardMarkup().add(InlineKeyboardButton("❌ Отменить", callback_data=f"user_profile_{user_id}"))
    try:
        await call.message.edit_text(
            f"✉️ Отправка сообщения пользователю @{username}\nНапишите текст или прикрепите любой тип контента:",
            reply_markup=keyboard
        )
    except (exceptions.MessageCantBeEdited, exceptions.MessageNotModified):
        new_msg = await call.message.answer(
            f"✉️ Отправка сообщения пользователю @{username}\nНапишите текст или прикрепите любой тип контента:",
            reply_markup=keyboard
        )
        await state.update_data(original_message_id=new_msg.message_id)
    await States.ADMIN_MESSAGE.set()
    await call.answer()


@dp.message_handler(state=States.ADMIN_MESSAGE, chat_id=SUPPORT_CHAT_ID, content_types=types.ContentTypes.ANY)
async def process_admin_message(message: types.Message, state: FSMContext):
    data = await state.get_data()
    target_user = data["target_user"]
    original_message_id = data.get("original_message_id")
    text_content = message.text or message.caption
    if original_message_id:
        try:
            await bot.delete_message(chat_id=message.chat.id, message_id=original_message_id)
        except Exception:
            pass
    try:
        ct = message.content_type
        file_id = None
        if ct in ['photo', 'video', 'audio', 'document', 'voice']:
            caption = f"📨 Сообщение от поддержки:\n\n{text_content}" if text_content else "📨 Сообщение от поддержки"
            if ct == 'photo':
                file_id = message.photo[-1].file_id
                await bot.send_photo(chat_id=target_user, photo=file_id, caption=caption)
            elif ct == 'video':
                file_id = message.video.file_id
                await bot.send_video(chat_id=target_user, video=file_id, caption=caption)
            elif ct == 'audio':
                file_id = message.audio.file_id
                await bot.send_audio(chat_id=target_user, audio=file_id, caption=caption)
            elif ct == 'document':
                file_id = message.document.file_id
                await bot.send_document(chat_id=target_user, document=file_id, caption=caption)
            elif ct == 'voice':
                file_id = message.voice.file_id
                await bot.send_voice(chat_id=target_user, voice=file_id, caption=caption)
        elif ct == 'location':
            await bot.send_location(chat_id=target_user, latitude=message.location.latitude, longitude=message.location.longitude)
        elif ct == 'contact':
            await bot.send_contact(
                chat_id=target_user,
                phone_number=message.contact.phone_number,
                first_name=message.contact.first_name
            )
        elif ct == 'sticker':
            await bot.send_sticker(chat_id=target_user, sticker=message.sticker.file_id)
        elif ct == 'animation':
            await bot.send_animation(chat_id=target_user, animation=message.animation.file_id)
        elif ct == 'text':
            await bot.send_message(chat_id=target_user, text=f"📨 Сообщение от поддержки:\n\n{text_content}")
        else:
            await message.answer("❌ Отправьте поддерживаемый тип контента")
            return
        await execute_query(
            "INSERT INTO admin_actions (admin_id, action_type, target_user, details, media_data) VALUES (?, ?, ?, ?, ?)",
            (message.from_user.id, "message", target_user, text_content, file_id)
        )
        result_text = f"✅ Сообщение отправлено пользователю\n🆔 ID: {target_user}"
        keyboard = InlineKeyboardMarkup().add(InlineKeyboardButton("◀️ Назад", callback_data=f"user_profile_{target_user}"))
        try:
            await message.answer(result_text, reply_markup=keyboard)
        except (exceptions.MessageCantBeEdited, exceptions.MessageNotModified):
            await message.answer(result_text, reply_markup=keyboard)
    except exceptions.BotBlocked:
        await message.answer("❌ Пользователь заблокировал бота")
        await execute_query("UPDATE users SET banned = 1 WHERE user_id = ?", (target_user,))
    except exceptions.ChatNotFound:
        await message.answer("❌ Пользователь не найден")
    except exceptions.RetryAfter as e:
        await message.answer(f"⚠️ Лимит сообщений. Попробуйте через {e.timeout} сек.")
    except Exception as e:
        logging.error(f"Ошибка отправки: {str(e)}")
        await message.answer("❌ Ошибка при отправке сообщения")
    await state.finish()

    

@dp.callback_query_handler(lambda c: c.data.startswith("user_profile_"), state="*")
async def cancel_editing(call: types.CallbackQuery, state: FSMContext):
    await state.finish()
    user_id = int(call.data.split("_")[-1])
    await show_user_profile(user_id, call.message)
    await call.answer()







@dp.callback_query_handler(lambda c: c.data.startswith("edit_chances_"), state="*")
async def edit_chances_handler(call: types.CallbackQuery, state: FSMContext):
    user_id = int(call.data.split("_")[-1])
    text = "☘️ Введите счастливое число для игры 🚀 Lucky Jet:"
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(InlineKeyboardButton("❌ Отменить", callback_data=f"user_profile_{user_id}"))
    user_record = await fetch_query(
        "SELECT lucky_number, lucky_chance FROM users WHERE user_id = ?",
        (user_id,), fetch_one=True
    )
    if user_record and (user_record["lucky_number"] != "default" or user_record["lucky_chance"] != "default"):
        keyboard.add(InlineKeyboardButton("↩️ Вернуть стандартные", callback_data=f"reset_lucky_{user_id}"))
    try:
        await call.message.edit_text(text, reply_markup=keyboard)
    except (exceptions.MessageCantBeEdited, exceptions.MessageNotModified):
        await call.message.answer(text, reply_markup=keyboard)
    await state.update_data(target_user=user_id)
    await state.set_state(States.EDIT_LUCKY_NUMBER.state)
    await call.answer()


@dp.message_handler(state=States.EDIT_LUCKY_NUMBER)
async def process_lucky_number(message: types.Message, state: FSMContext):
    lucky_number = message.text.strip()
    try:
        float(lucky_number)
    except ValueError:
        await message.answer("❌ Введите корректное число!")
        return
    await state.update_data(lucky_number=lucky_number)
    user_data = await state.get_data()
    user_id = user_data.get("target_user")
    keyboard = InlineKeyboardMarkup().add(
        InlineKeyboardButton("❌ Отменить", callback_data=f"user_profile_{user_id}")
    )
    await message.answer("☘️ Введите шанс выпадения этого числа в %:", reply_markup=keyboard)
    await state.set_state(States.EDIT_LUCKY_CHANCE.state)


@dp.message_handler(state=States.EDIT_LUCKY_CHANCE)
async def process_lucky_chance(message: types.Message, state: FSMContext):
    chance_str = message.text.strip()
    try:
        chance_val = float(chance_str)
        if chance_val < 0 or chance_val > 100:
            await message.answer("❌ Введите число от 0 до 100!")
            return
    except ValueError:
        await message.answer("❌ Введите корректное число!")
        return
    user_data = await state.get_data()
    user_id = user_data.get("target_user")
    lucky_number = user_data.get("lucky_number")
    await execute_query(
        "UPDATE users SET lucky_number = ?, lucky_chance = ? WHERE user_id = ?",
        (lucky_number, chance_str, user_id)
    )
    user_record = await fetch_query(
        "SELECT username, lucky_number, lucky_chance FROM users WHERE user_id = ?",
        (user_id,), fetch_one=True
    )
    username = user_record.get("username", "нет") if user_record else "нет"

    chat_text = (
        f"☘️ Шансы пользователя @{username} в игре ✈️ Lucky Jet изменены:\n"
        f"🚀 Шанс выпадения числа {lucky_number} или больше - {chance_str}%"
    )
    keyboard = InlineKeyboardMarkup(row_width=2)
    if user_record and (user_record["lucky_number"] != "default" or user_record["lucky_chance"] != "default"):
        keyboard.add(InlineKeyboardButton("↩️ Вернуть стандартные", callback_data=f"reset_lucky_{user_id}"))
    keyboard.add(InlineKeyboardButton("◀️ Назад", callback_data=f"user_profile_{user_id}"))
    if message.chat.id == SUPPORT_CHAT_ID:
        await message.answer(chat_text, reply_markup=keyboard)

    user_text = (
        f"☘️ Ваши шансы в игре ✈️ Lucky Jet изменены:\n"
        f"🚀 Шанс выпадения числа {lucky_number} или больше - {chance_str}%"
    )
    await bot.send_message(user_id, user_text)

    await state.finish()


@dp.callback_query_handler(lambda c: c.data.startswith("reset_lucky_"), state="*")
async def reset_lucky_callback(callback_query: types.CallbackQuery, state: FSMContext):
    user_id = int(callback_query.data.split("_")[-1])
    await execute_query(
        "UPDATE users SET lucky_number = 'default', lucky_chance = 'default' WHERE user_id = ?",
        (user_id,)
    )
    user_record = await fetch_query(
        "SELECT username FROM users WHERE user_id = ?",
        (user_id,), fetch_one=True
    )
    username = user_record.get("username", "нет") if user_record else "нет"

    chat_text = f"☘️ Шансы пользователя @{username} в игре ✈️ Lucky Jet сброшены до стандартных"
    keyboard = InlineKeyboardMarkup().add(
        InlineKeyboardButton("◀️ Назад", callback_data=f"user_profile_{user_id}")
    )
    if callback_query.message.chat.id == SUPPORT_CHAT_ID:
        await callback_query.message.edit_text(chat_text, reply_markup=keyboard)

    user_text = "☘️ Ваши шансы в игре ✈️ Lucky Jet сброшены до стандартных"
    await bot.send_message(user_id, user_text)

    await callback_query.answer()

    
    
    

async def add_admin_action(admin_id: int, action: str, details: str = ""):
    await execute_query(
        '''INSERT INTO admin_actions 
        (admin_id, action, details, timestamp) 
        VALUES (?, ?, ?, CURRENT_TIMESTAMP)''',
        (admin_id, action, details)
    )




restart_count = 0


async def handle_error_and_restart(exception: Exception):
    global restart_count
    
    if isinstance(exception, exceptions.RetryAfter):
        logging.warning(f"RetryAfter error detected. Waiting for {exception.timeout} seconds.")
        await asyncio.sleep(exception.timeout)
        return  

    if restart_count < 1:
        error_message = f"Произошла ошибка: {str(exception)}\n\nПерезапускаю бот..."
        await bot.send_message(SUPPORT_CHAT_ID, error_message)
        logging.error(f"Ошибка: {exception}. Перезапускаю бот...")
        restart_count += 1
        os.execl(sys.executable, sys.executable, *sys.argv)
    else:
        error_message = f"Произошла ошибка: {str(exception)}. Перезапуски ограничены"
        await bot.send_message(SUPPORT_CHAT_ID, error_message)
        logging.error(f"Ошибка: {exception}. Превышено максимальное количество перезапусков")
        
        







            
@dp.message_handler(commands=["info"], chat_id=SUPPORT_CHAT_ID)
async def cmd_info(message: types.Message):
    t0 = time.time()
    sent = await message.reply("🏓 Измерение пинга...")
    t1 = time.time()
    await bot.delete_message(sent.chat.id, sent.message_id)
    ping_ms = (t1 - t0) * 1000
    status = (
        "💫 Идеально" if ping_ms < 100 else
        "✅ Нормально" if ping_ms < 200 else
        "⚠️ Средне" if ping_ms < 400 else
        "❌ Плохо"
    )
    now = datetime.now()
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute("PRAGMA table_info(bot_info)")
        cols = [row[1] for row in await cur.fetchall()]
        if "first_start" not in cols:
            await db.execute("ALTER TABLE bot_info ADD COLUMN first_start TEXT")
        if "creator" not in cols:
            await db.execute("ALTER TABLE bot_info ADD COLUMN creator TEXT")
        await db.commit()
        cur = await db.execute("SELECT * FROM bot_info")
        bot_info = await cur.fetchone()
        if bot_info:
            first_start, creator = bot_info[0], bot_info[1]
            if not first_start or not creator:
                first_start = first_start or now.strftime("%Y-%m-%d %H:%M:%S")
                creator = creator or message.from_user.username
                await db.execute("UPDATE bot_info SET first_start = ?, creator = ?", (first_start, creator))
                await db.commit()
        else:
            first_start = now.strftime("%Y-%m-%d %H:%M:%S")
            creator = message.from_user.username
            await db.execute("INSERT INTO bot_info (first_start, creator) VALUES (?, ?)", (first_start, creator))
            await db.commit()
    escaped_creator = creator.replace('_', r'\_')
    start_dt = datetime.strptime(first_start, '%Y-%m-%d %H:%M:%S')
    time_passed = now - start_dt
    formatted_start = start_dt.strftime('%d.%m.%Y в %H:%M')
    info_text = f"""
🤖 Информация о боте
🚀 Первый запуск: {formatted_start} ({time_passed.days} д. назад)
👨‍💻 Создатель: @{escaped_creator}
🆔 ID админ чата: `{SUPPORT_CHAT_ID}`

🏓 Пинг: {ping_ms:.2f} мс
🖥 Статус: {status}
"""
    await message.reply(info_text, parse_mode="Markdown")
         


            
                        
                                    
                                                            
@dp.message_handler(commands=['restart'], chat_id=SUPPORT_CHAT_ID)
async def restart_bot(message: types.Message):
    global restart_count
    try:
        await message.reply("🔃 Перезапускаю бот...\n\n⚠ Сообщение может дублироваться если вы используете Termux")
        restart_count = 0  
        os.execl(sys.executable, sys.executable, *sys.argv)
    except Exception as e:
        await message.reply(f"❌ Не удалось перезагрузить бот: {str(e)}")
        logging.error(f"❌ Ошибка при попытке перезапуска: {e}")


        




@dp.message_handler(commands=['backup'], chat_id=SUPPORT_CHAT_ID)
async def cmd_backup(message: types.Message):
    status_msg = await message.reply("⏳ Создаю резервную копию базы данных...")
    if await backup_db():
        await bot.edit_message_text("🗄 Резервная копия базы данных успешно создана", message.chat.id, status_msg.message_id)
    else:
        await bot.edit_message_text("❌ Не удалось создать резервную копию базы данных", message.chat.id, status_msg.message_id)




@dp.errors_handler()
async def errors_handler(update: types.Update, exception: Exception):
    try:
        await handle_error_and_restart(exception)
    except Exception as e:
        logging.critical(f"Критическая ошибка при обработке ошибки: {e}")
    return True




if __name__ == "__main__":
    restart_count = 0
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()]
    )
    loop = asyncio.get_event_loop()
    ban_middleware = BanCheckMiddleware()
    dp.middleware.setup(ban_middleware)

    try:
        loop.run_until_complete(init_db())
        loop.create_task(ban_middleware.auto_unban_loop())  
        loop.create_task(scheduled_hourly_backup())
        loop.create_task(manage_subscriptions())
        executor.start_polling(dp, skip_updates=True, loop=loop, timeout=60, reset_webhook=True)
    except Exception as e:
        loop.run_until_complete(handle_error_and_restart(e))
