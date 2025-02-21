import psycopg2
import random
import aiohttp
import logging
import os
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes
from datetime import datetime, time, timedelta
import asyncio

# Настройка логирования
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# ID администратора (замените на ваш)
ADMIN_ID = 486000906
REQUEST_LIMIT = 60  # Лимит запросов в минуту на пользователя
REQUEST_WINDOW = 60  # Окно в секундах

# Подключение к PostgreSQL (строка подключения из Render)
DB_CONN_STRING = os.getenv('DB_CONN_STRING', 'postgresql://loli_db_user:UxaiJ1HL8xZp67mf1zzEikqFzvgH57Ch@dpg-cusbuba3esus73flt5qg-a/loli_db')

# Инициализация базы данных
def init_db():
    conn = psycopg2.connect(DB_CONN_STRING)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS books 
                 (id TEXT PRIMARY KEY, title TEXT, description TEXT, genres TEXT, cover_url TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS user_read 
                 (user_id BIGINT, book_id TEXT, rating INTEGER, PRIMARY KEY (user_id, book_id))''')
    c.execute('''CREATE TABLE IF NOT EXISTS user_favorites 
                 (user_id BIGINT, book_id TEXT, PRIMARY KEY (user_id, book_id))''')
    c.execute('''CREATE TABLE IF NOT EXISTS users 
                 (user_id BIGINT PRIMARY KEY, username TEXT, agreed INTEGER DEFAULT 0, banned_until BIGINT DEFAULT 0, ban_reason TEXT, requests INTEGER DEFAULT 0, last_request BIGINT DEFAULT 0)''')
    c.execute('''CREATE TABLE IF NOT EXISTS search_history 
                 (user_id BIGINT, query TEXT, timestamp BIGINT)''')
    conn.commit()
    conn.close()
    logger.info("База данных PostgreSQL инициализирована")

# Главное меню
def main_menu(user_id):
    keyboard = [
        [InlineKeyboardButton("📚 Поиск по жанру", callback_data='search_genre'),
         InlineKeyboardButton("🔍 Поиск книги", callback_data='search_title')],
        [InlineKeyboardButton("📖 Добавить в прочитанное", callback_data='add_read'),
         InlineKeyboardButton("❤️ Добавить в избранное", callback_data='add_favorite')],
        [InlineKeyboardButton("📜 Мои прочитанные", callback_data='show_read'),
         InlineKeyboardButton("⭐ Мои избранные", callback_data='show_favorites')],
        [InlineKeyboardButton("✍️ Поиск по автору", callback_data='search_author')]
    ]
    if user_id == ADMIN_ID:
        keyboard.append([InlineKeyboardButton("🔧 Админ-панель", callback_data='admin_panel')])
    return InlineKeyboardMarkup(keyboard)

# Преобразование рейтинга в звёздочки
def rating_to_stars(rating):
    if rating is None:
        return "☆☆☆☆☆"
    return "★" * rating + "☆" * (5 - rating)

# Проверка лимита запросов
def check_rate_limit(user_id):
    conn = psycopg2.connect(DB_CONN_STRING)
    c = conn.cursor()
    c.execute("SELECT requests, last_request FROM users WHERE user_id = %s", (user_id,))
    user = c.fetchone()
    current_time = int(time.time())
    
    if not user or current_time - user[1] > REQUEST_WINDOW:
        c.execute("INSERT INTO users (user_id, requests, last_request) VALUES (%s, 1, %s) ON CONFLICT (user_id) DO UPDATE SET requests = 1, last_request = %s", 
                  (user_id, current_time, current_time))
        conn.commit()
        conn.close()
        return True
    
    requests, last_request = user
    if requests < REQUEST_LIMIT:
        c.execute("UPDATE users SET requests = requests + 1 WHERE user_id = %s", (user_id,))
        conn.commit()
        conn.close()
        return True
    logger.warning(f"Пользователь {user_id} превысил лимит запросов")
    conn.close()
    return False

# Проверка согласия и бана
async def check_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if isinstance(update, Update) and update.callback_query:
        user_id = update.callback_query.from_user.id
        message = update.callback_query.message
    else:
        user_id = update.message.from_user.id
        message = update.message
    
    if not check_rate_limit(user_id):
        await message.reply_text("🚫 Вы превысили лимит запросов (60 в минуту). Подождите немного!", parse_mode=ParseMode.MARKDOWN)
        return False
    
    conn = psycopg2.connect(DB_CONN_STRING)
    c = conn.cursor()
    c.execute("SELECT agreed, banned_until, ban_reason FROM users WHERE user_id = %s", (user_id,))
    user = c.fetchone()
    conn.close()
    
    if not user:  # Новый пользователь
        return True
    if user[1] and user[1] > int(time.time()):  # Пользователь забанен
        await message.reply_text(f"🚫 Вы заблокированы до {datetime.fromtimestamp(user[1]).strftime('%Y-%m-%d %H:%M:%S')}.\n*Причина:* {user[2]}", parse_mode=ParseMode.MARKDOWN)
        return False
    if user[0] == 1:  # Уже согласен
        return True
    return False

# Команда /start
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    username = update.message.from_user.username
    
    conn = psycopg2.connect(DB_CONN_STRING)
    c = conn.cursor()
    c.execute("INSERT INTO users (user_id, username) VALUES (%s, %s) ON CONFLICT (user_id) DO NOTHING", (user_id, username))
    c.execute("SELECT agreed FROM users WHERE user_id = %s", (user_id,))
    agreed = c.fetchone()[0]
    conn.commit()
    conn.close()
    
    if not agreed:
        keyboard = [
            [InlineKeyboardButton("✅ Согласен", callback_data='agree_policy'),
             InlineKeyboardButton("❌ Отказ", callback_data='refuse_policy')]
        ]
        await update.message.reply_text("📜 *Пожалуйста, согласитесь с политикой использования бота*\n(запрашивается только один раз):", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
    else:
        await update.message.reply_text("📚 *Добро пожаловать в бот для книг!*\nЯ использую Open Library для поиска. Выберите действие:", reply_markup=main_menu(user_id), parse_mode=ParseMode.MARKDOWN)

# Поиск книги через Open Library API
async def search_book_by_title_or_genre(query, is_genre=False, author=None):
    async with aiohttp.ClientSession() as session:
        if is_genre:
            url = f"https://openlibrary.org/subjects/{query.lower().replace(' ', '_')}.json?limit=1&sort=random"
        elif author:
            url = f"https://openlibrary.org/search.json?author={query.replace(' ', '+')}&limit=1"
        else:
            url = f"https://openlibrary.org/search.json?q={query.replace(' ', '+')}&limit=1"
        async with session.get(url) as response:
            if response.status != 200:
                logger.error(f"Ошибка API: {response.status}")
                return None
            data = await response.json()
            works = data.get('works') if is_genre else data.get('docs')
            if not works:
                return None
            
            work = works[0]
            book_id = work['key'].split('/')[-1] if is_genre else work['key'].split('/')[-1]
            title = work.get('title', 'Нет названия')
            
            detail_url = f"https://openlibrary.org/works/{book_id}.json"
            async with session.get(detail_url) as detail_response:
                if detail_response.status != 200:
                    logger.error(f"Ошибка получения деталей книги: {detail_response.status}")
                    return None
                detail_data = await detail_response.json()
                description = detail_data.get('description', 'Нет описания') if isinstance(detail_data.get('description'), str) else 'Нет описания'
                genres = ','.join(work.get('subject', ['Нет жанров']))
                cover_id = work.get('cover_id') if is_genre else work.get('cover_i')
                cover_url = f"https://covers.openlibrary.org/b/id/{cover_id}-L.jpg" if cover_id else "https://via.placeholder.com/150"
                
                return {'id': book_id, 'title': title, 'description': description, 'genres': genres, 'cover_url': cover_url}
    return None

# Кэширование книги
def cache_book(book):
    conn = psycopg2.connect(DB_CONN_STRING)
    c = conn.cursor()
    c.execute("INSERT INTO books (id, title, description, genres, cover_url) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING",
              (book['id'], book['title'], book['description'], book['genres'], book['cover_url']))
    conn.commit()
    conn.close()

# Резервное копирование базы (опционально, для PostgreSQL не требуется локально)
async def backup_database(context: ContextTypes.DEFAULT_TYPE):
    logger.info("Бэкап для PostgreSQL не требуется, данные сохраняются автоматически")

# Сброс базы данных
def reset_database(user_id=None):
    conn = psycopg2.connect(DB_CONN_STRING)
    c = conn.cursor()
    if user_id:
        c.execute("DELETE FROM user_read WHERE user_id = %s", (user_id,))
        c.execute("DELETE FROM user_favorites WHERE user_id = %s", (user_id,))
        c.execute("DELETE FROM users WHERE user_id = %s", (user_id,))
        c.execute("DELETE FROM search_history WHERE user_id = %s", (user_id,))
    else:
        c.execute("TRUNCATE TABLE books, user_read, user_favorites, users, search_history RESTART IDENTITY")
    conn.commit()
    conn.close()

# Обработка кнопок
async def button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    logger.info(f"Обработка callback: {query.data} для пользователя {user_id}")
    
    if query.data in ['agree_policy', 'refuse_policy']:
        if query.data == 'agree_policy':
            conn = psycopg2.connect(DB_CONN_STRING)
            c = conn.cursor()
            c.execute("UPDATE users SET agreed = 1 WHERE user_id = %s", (user_id,))
            conn.commit()
            conn.close()
            logger.info(f"Пользователь {user_id} согласился с политикой")
            await query.message.reply_text("✅ *Спасибо за согласие!*\nТеперь вы можете использовать бот.", reply_markup=main_menu(user_id), parse_mode=ParseMode.MARKDOWN)
        elif query.data == 'refuse_policy':
            logger.info(f"Пользователь {user_id} отказался от политики")
            await query.message.reply_text("❌ *Вы отказались от политики.*\nИспользование бота невозможно.")
        return

    if not await check_user(update, context):
        return
    
    try:
        if query.data == 'search_genre':
            await query.message.reply_text("📚 Укажи жанр (например, *Фэнтези*):", parse_mode=ParseMode.MARKDOWN)
            context.user_data['state'] = 'search_genre'
        elif query.data == 'search_title':
            await query.message.reply_text("🔍 Укажи название книги:", parse_mode=ParseMode.MARKDOWN)
            context.user_data['state'] = 'search_title'
        elif query.data == 'search_author':
            await query.message.reply_text("✍️ Укажи имя автора:", parse_mode=ParseMode.MARKDOWN)
            context.user_data['state'] = 'search_author'
        elif query.data == 'add_read':
            await query.message.reply_text("📖 Укажи название книги для добавления в прочитанное:", parse_mode=ParseMode.MARKDOWN)
            context.user_data['state'] = 'add_read'
        elif query.data == 'add_favorite':
            await query.message.reply_text("❤️ Укажи название книги для добавления в избранное:", parse_mode=ParseMode.MARKDOWN)
            context.user_data['state'] = 'add_favorite'
        elif query.data == 'show_read':
            await show_read(query, context, page=1)
        elif query.data == 'show_favorites':
            await show_favorites(query, context, page=1)
        elif query.data.startswith('page_read_'):
            page = int(query.data.split('_')[2])
            await show_read(query, context, page)
        elif query.data.startswith('page_favorites_'):
            page = int(query.data.split('_')[2])
            await show_favorites(query, context, page)
        elif query.data == 'add_found_to_read':
            book = context.user_data.get('last_found_book')
            if book:
                conn = psycopg2.connect(DB_CONN_STRING)
                c = conn.cursor()
                c.execute("INSERT INTO user_read (user_id, book_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", (user_id, book['id']))
                conn.commit()
                conn.close()
                await query.message.reply_text(f"📖 Книга *{book['title']}* добавлена в прочитанное.\nПопробуйте */search*!", reply_markup=main_menu(user_id), parse_mode=ParseMode.MARKDOWN)
        elif query.data == 'add_found_to_favorite':
            book = context.user_data.get('last_found_book')
            if book:
                conn = psycopg2.connect(DB_CONN_STRING)
                c = conn.cursor()
                c.execute("INSERT INTO user_favorites (user_id, book_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", (user_id, book['id']))
                conn.commit()
                conn.close()
                await query.message.reply_text(f"❤️ Книга *{book['title']}* добавлена в избранное.\nПопробуйте */search*!", reply_markup=main_menu(user_id), parse_mode=ParseMode.MARKDOWN)
        elif query.data.startswith('list_action_'):
            action, list_type = query.data.split('_')[2], query.data.split('_')[3]
            context.user_data['list_action'] = action
            context.user_data['list_type'] = list_type
            await query.message.reply_text("🔢 Укажи номер книги из списка (1, 2, 3...) или её название:", parse_mode=ParseMode.MARKDOWN)
            context.user_data['state'] = 'list_action_select'
        elif query.data.startswith('rate_'):
            _, book_id, rating = query.data.split('_')
            rating = int(rating)
            conn = psycopg2.connect(DB_CONN_STRING)
            c = conn.cursor()
            c.execute("UPDATE user_read SET rating = %s WHERE user_id = %s AND book_id = %s", (rating, user_id, book_id))
            if c.rowcount == 0:
                c.execute("INSERT INTO user_read (user_id, book_id, rating) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", (user_id, book_id, rating))
            conn.commit()
            conn.close()
            await query.message.reply_text(f"⭐ Оценка {rating}★ сохранена.", reply_markup=main_menu(user_id), parse_mode=ParseMode.MARKDOWN)
        elif query.data == 'main_menu':
            await query.message.reply_text("🔙 *Возвращаемся в главное меню:*", reply_markup=main_menu(user_id), parse_mode=ParseMode.MARKDOWN)
        elif query.data == 'admin_panel':
            if user_id == ADMIN_ID:
                keyboard = [
                    [InlineKeyboardButton("✉️ Рассылка", callback_data='admin_broadcast'),
                     InlineKeyboardButton("🚫 Заблокировать", callback_data='admin_ban')],
                    [InlineKeyboardButton("✅ Разблокировать", callback_data='admin_unban'),
                     InlineKeyboardButton("📊 Статистика", callback_data='admin_stats')],
                    [InlineKeyboardButton("📜 Логи", callback_data='admin_logs'),
                     InlineKeyboardButton("🔄 Восстановить бэкап", callback_data='admin_restore')],
                    [InlineKeyboardButton("🗑️ Сброс базы", callback_data='admin_reset_all'),
                     InlineKeyboardButton("👤 Сброс пользователя", callback_data='admin_reset_user')],
                    [InlineKeyboardButton("🔙 Главное меню", callback_data='main_menu')]
                ]
                await query.message.reply_text("🔧 *Админ-панель:*", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
        elif query.data == 'admin_broadcast':
            await query.message.reply_text("✉️ Введите сообщение для рассылки:", parse_mode=ParseMode.MARKDOWN)
            context.user_data['state'] = 'admin_broadcast_message'
        elif query.data == 'admin_ban':
            await query.message.reply_text("🚫 Введите ID пользователя для блокировки:", parse_mode=ParseMode.MARKDOWN)
            context.user_data['state'] = 'admin_ban_id'
        elif query.data == 'admin_unban':
            await query.message.reply_text("✅ Введите ID пользователя для разблокировки:", parse_mode=ParseMode.MARKDOWN)
            context.user_data['state'] = 'admin_unban_id'
        elif query.data == 'admin_stats':
            conn = psycopg2.connect(DB_CONN_STRING)
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM users")
            user_count = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM books")
            book_count = c.fetchone()[0]
            c.execute("SELECT AVG(rating) FROM user_read WHERE rating IS NOT NULL")
            avg_rating = c.fetchone()[0]
            conn.close()
            avg_rating = f"{avg_rating:.2f}★" if avg_rating else "Нет оценок"
            await query.message.reply_text(f"📊 *Статистика:*\n- Пользователей: {user_count}\n- Книг в базе: {book_count}\n- Средний рейтинг: {avg_rating}", reply_markup=main_menu(user_id), parse_mode=ParseMode.MARKDOWN)
        elif query.data == 'admin_logs':
            conn = psycopg2.connect(DB_CONN_STRING)
            c = conn.cursor()
            c.execute("SELECT user_id FROM users ORDER BY user_id LIMIT 5")
            users = c.fetchall()
            conn.close()
            log_text = "📜 *Последние действия пользователей:*\n"
            for uid in users:
                log_text += f"- Пользователь {uid[0]}\n"
            await query.message.reply_text(log_text, reply_markup=main_menu(user_id), parse_mode=ParseMode.MARKDOWN)
        elif query.data == 'admin_restore':
            await query.message.reply_text("🔄 *Восстановление бэкапа не поддерживается для PostgreSQL напрямую. Используйте дамп базы через Render.*", reply_markup=main_menu(user_id), parse_mode=ParseMode.MARKDOWN)
        elif query.data == 'admin_reset_all':
            reset_database()
            await query.message.reply_text("🗑️ *База данных полностью сброшена.*", reply_markup=main_menu(user_id), parse_mode=ParseMode.MARKDOWN)
        elif query.data == 'admin_reset_user':
            await query.message.reply_text("👤 Введите ID пользователя для сброса данных:", parse_mode=ParseMode.MARKDOWN)
            context.user_data['state'] = 'admin_reset_user_id'
        elif query.data == 'select_book_read':
            await query.message.reply_text("🔢 Укажи номер книги из списка прочитанного (1, 2, 3...):", parse_mode=ParseMode.MARKDOWN)
            context.user_data['state'] = 'select_book_read'
        elif query.data == 'select_book_favorite':
            await query.message.reply_text("🔢 Укажи номер книги из списка избранного (1, 2, 3...):", parse_mode=ParseMode.MARKDOWN)
            context.user_data['state'] = 'select_book_favorite'
        elif query.data == 'back_to_select_read':
            await show_read(query, context, page=1)
        elif query.data == 'back_to_select_favorite':
            await show_favorites(query, context, page=1)
        elif query.data == 'edit_book':
            await query.message.reply_text("📝 Укажи номер книги для редактирования:", parse_mode=ParseMode.MARKDOWN)
            context.user_data['state'] = 'edit_book_select'
        elif query.data == 'export_read':
            conn = psycopg2.connect(DB_CONN_STRING)
            c = conn.cursor()
            c.execute("SELECT b.title, ur.rating FROM user_read ur JOIN books b ON ur.book_id = b.id WHERE ur.user_id = %s", (user_id,))
            books = c.fetchall()
            conn.close()
            if books:
                export_text = "📖 *Ваши прочитанные книги:*\n"
                for i, (title, rating) in enumerate(books, 1):
                    export_text += f"{i}. {title} - {rating_to_stars(rating)}\n"
                with open(f'read_export_{user_id}.txt', 'w', encoding='utf-8') as f:
                    f.write(export_text.replace('*', ''))
                await context.bot.send_document(chat_id=user_id, document=open(f'read_export_{user_id}.txt', 'rb'), filename=f"read_export_{user_id}.txt")
                os.remove(f'read_export_{user_id}.txt')
            else:
                await query.message.reply_text("📖 *Список прочитанного пуст.*", reply_markup=main_menu(user_id), parse_mode=ParseMode.MARKDOWN)
        elif query.data == 'export_favorites':
            conn = psycopg2.connect(DB_CONN_STRING)
            c = conn.cursor()
            c.execute("SELECT b.title FROM user_favorites uf JOIN books b ON uf.book_id = b.id WHERE uf.user_id = %s", (user_id,))
            books = c.fetchall()
            conn.close()
            if books:
                export_text = "⭐ *Ваши избранные книги:*\n"
                for i, (title,) in enumerate(books, 1):
                    export_text += f"{i}. {title}\n"
                with open(f'favorites_export_{user_id}.txt', 'w', encoding='utf-8') as f:
                    f.write(export_text.replace('*', ''))
                await context.bot.send_document(chat_id=user_id, document=open(f'favorites_export_{user_id}.txt', 'rb'), filename=f"favorites_export_{user_id}.txt")
                os.remove(f'favorites_export_{user_id}.txt')
            else:
                await query.message.reply_text("⭐ *Список избранного пуст.*", reply_markup=main_menu(user_id), parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"Ошибка в button: {e}")
        await query.message.reply_text("⚠️ *Произошла ошибка, попробуйте позже.*", reply_markup=main_menu(user_id), parse_mode=ParseMode.MARKDOWN)

# Обработка текстовых сообщений
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    state = context.user_data.get('state')
    user_id = update.message.from_user.id
    logger.info(f"Получено сообщение: {text} в состоянии {state} от {user_id}")
    
    if not await check_user(update, context):
        return
    
    try:
        if state == 'search_genre':
            msg = await update.message.reply_text("⏳ *Поиск книги...*", parse_mode=ParseMode.MARKDOWN)
            await asyncio.sleep(1)
            book = await search_book_by_title_or_genre(text, is_genre=True)
            if book:
                cache_book(book)
                context.user_data['last_found_book'] = book
                conn = psycopg2.connect(DB_CONN_STRING)
                c = conn.cursor()
                c.execute("INSERT INTO search_history (user_id, query, timestamp) VALUES (%s, %s, %s)", (user_id, text, int(time.time())))
                conn.commit()
                conn.close()
                keyboard = [
                    [InlineKeyboardButton("📖 Добавить в прочитанное", callback_data='add_found_to_read'),
                     InlineKeyboardButton("❤️ Добавить в избранное", callback_data='add_found_to_favorite')],
                    [InlineKeyboardButton("🔙 Главное меню", callback_data='main_menu')]
                ]
                await update.message.reply_photo(
                    photo=book['cover_url'],
                    caption=f"**{book['title']}**\n\n_{book['description']}_\n\n*Жанры:* {book['genres']}",
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                await update.message.reply_text("📚 *Книга не найдена.*\nПопробуйте */search*!", reply_markup=main_menu(user_id), parse_mode=ParseMode.MARKDOWN)
            await msg.delete()
        
        elif state == 'search_title':
            msg = await update.message.reply_text("⏳ *Поиск книги...*", parse_mode=ParseMode.MARKDOWN)
            await asyncio.sleep(1)
            book = await search_book_by_title_or_genre(text)
            if book:
                cache_book(book)
                context.user_data['last_found_book'] = book
                conn = psycopg2.connect(DB_CONN_STRING)
                c = conn.cursor()
                c.execute("INSERT INTO search_history (user_id, query, timestamp) VALUES (%s, %s, %s)", (user_id, text, int(time.time())))
                conn.commit()
                conn.close()
                keyboard = [
                    [InlineKeyboardButton("📖 Добавить в прочитанное", callback_data='add_found_to_read'),
                     InlineKeyboardButton("❤️ Добавить в избранное", callback_data='add_found_to_favorite')],
                    [InlineKeyboardButton("🔙 Главное меню", callback_data='main_menu')]
                ]
                await update.message.reply_photo(
                    photo=book['cover_url'],
                    caption=f"**{book['title']}**\n\n_{book['description']}_\n\n*Жанры:* {book['genres']}",
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                await update.message.reply_text("📚 *Книга не найдена.*\nУкажи описание:", parse_mode=ParseMode.MARKDOWN)
                context.user_data['manual_title'] = text
                context.user_data['manual_list'] = 'title'
                context.user_data['state'] = 'manual_description'
            await msg.delete()
        
        elif state == 'search_author':
            msg = await update.message.reply_text("⏳ *Поиск книги...*", parse_mode=ParseMode.MARKDOWN)
            await asyncio.sleep(1)
            book = await search_book_by_title_or_genre(text, author=True)
            if book:
                cache_book(book)
                context.user_data['last_found_book'] = book
                conn = psycopg2.connect(DB_CONN_STRING)
                c = conn.cursor()
                c.execute("INSERT INTO search_history (user_id, query, timestamp) VALUES (%s, %s, %s)", (user_id, text, int(time.time())))
                conn.commit()
                conn.close()
                keyboard = [
                    [InlineKeyboardButton("📖 Добавить в прочитанное", callback_data='add_found_to_read'),
                     InlineKeyboardButton("❤️ Добавить в избранное", callback_data='add_found_to_favorite')],
                    [InlineKeyboardButton("🔙 Главное меню", callback_data='main_menu')]
                ]
                await update.message.reply_photo(
                    photo=book['cover_url'],
                    caption=f"**{book['title']}**\n\n_{book['description']}_\n\n*Жанры:* {book['genres']}",
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                await update.message.reply_text("📚 *Книга не найдена.*\nПопробуйте */search*!", reply_markup=main_menu(user_id), parse_mode=ParseMode.MARKDOWN)
            await msg.delete()
        
        elif state == 'add_read':
            msg = await update.message.reply_text("⏳ *Добавление книги...*", parse_mode=ParseMode.MARKDOWN)
            await asyncio.sleep(1)
            conn = psycopg2.connect(DB_CONN_STRING)
            c = conn.cursor()
            c.execute("SELECT id FROM books WHERE title ILIKE %s", (f'%{text}%',))
            book = c.fetchone()
            if book:
                c.execute("INSERT INTO user_read (user_id, book_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", (user_id, book[0]))
                conn.commit()
                await update.message.reply_text(f"📖 Книга *{text}* добавлена в прочитанное.\nПопробуйте */read*!", reply_markup=main_menu(user_id), parse_mode=ParseMode.MARKDOWN)
            else:
                book = await search_book_by_title_or_genre(text)
                if book:
                    cache_book(book)
                    c.execute("INSERT INTO user_read (user_id, book_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", (user_id, book['id']))
                    conn.commit()
                    await update.message.reply_text(f"📖 Книга *{text}* добавлена в прочитанное.\nПопробуйте */read*!", reply_markup=main_menu(user_id), parse_mode=ParseMode.MARKDOWN)
                else:
                    context.user_data['manual_title'] = text
                    context.user_data['manual_list'] = 'read'
                    await update.message.reply_text("📚 *Книга не найдена.*\nУкажи описание:", parse_mode=ParseMode.MARKDOWN)
                    context.user_data['state'] = 'manual_description'
            conn.close()
            await msg.delete()
        
        elif state == 'add_favorite':
            msg = await update.message.reply_text("⏳ *Добавление книги...*", parse_mode=ParseMode.MARKDOWN)
            await asyncio.sleep(1)
            conn = psycopg2.connect(DB_CONN_STRING)
            c = conn.cursor()
            c.execute("SELECT id FROM books WHERE title ILIKE %s", (f'%{text}%',))
            book = c.fetchone()
            if book:
                c.execute("INSERT INTO user_favorites (user_id, book_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", (user_id, book[0]))
                conn.commit()
                await update.message.reply_text(f"❤️ Книга *{text}* добавлена в избранное.\nПопробуйте */favorites*!", reply_markup=main_menu(user_id), parse_mode=ParseMode.MARKDOWN)
            else:
                book = await search_book_by_title_or_genre(text)
                if book:
                    cache_book(book)
                    c.execute("INSERT INTO user_favorites (user_id, book_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", (user_id, book['id']))
                    conn.commit()
                    await update.message.reply_text(f"❤️ Книга *{text}* добавлена в избранное.\nПопробуйте */favorites*!", reply_markup=main_menu(user_id), parse_mode=ParseMode.MARKDOWN)
                else:
                    context.user_data['manual_title'] = text
                    context.user_data['manual_list'] = 'favorite'
                    await update.message.reply_text("📚 *Книга не найдена.*\nУкажи описание:", parse_mode=ParseMode.MARKDOWN)
                    context.user_data['state'] = 'manual_description'
            conn.close()
            await msg.delete()
        
        elif state == 'manual_description':
            context.user_data['manual_description'] = text
            await update.message.reply_text("📷 Прикрепи фото обложки (или отправь 'нет', если нет фото):", parse_mode=ParseMode.MARKDOWN)
            context.user_data['state'] = 'manual_cover'
        
        elif state == 'manual_cover':
            msg = await update.message.reply_text("⏳ *Сохранение книги...*", parse_mode=ParseMode.MARKDOWN)
            await asyncio.sleep(1)
            title = context.user_data['manual_title']
            description = context.user_data['manual_description']
            list_type = context.user_data['manual_list']
            if update.message.photo:
                cover_url = update.message.photo[-1].file_id
            elif text.lower() == 'нет':
                cover_url = "https://via.placeholder.com/150"
            else:
                await update.message.reply_text("📷 *Пожалуйста, прикрепи фото или напиши 'нет'.*", reply_markup=main_menu(user_id), parse_mode=ParseMode.MARKDOWN)
                await msg.delete()
                return
            
            book_id = f"manual_{user_id}_{int(time.time())}"
            book = {'id': book_id, 'title': title, 'description': description, 'genres': 'Не указаны', 'cover_url': cover_url}
            cache_book(book)
            conn = psycopg2.connect(DB_CONN_STRING)
            c = conn.cursor()
            if list_type == 'read':
                c.execute("INSERT INTO user_read (user_id, book_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", (user_id, book_id))
            else:
                c.execute("INSERT INTO user_favorites (user_id, book_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", (user_id, book_id))
            conn.commit()
            conn.close()
            await update.message.reply_text(f"📚 Книга *{title}* добавлена в {list_type == 'read' and 'прочитанное' or 'избранное'}.\nПопробуйте */{'read' if list_type == 'read' else 'favorites'}*!", reply_markup=main_menu(user_id), parse_mode=ParseMode.MARKDOWN)
            await msg.delete()
            context.user_data['state'] = None
        
        elif state == 'list_action_select':
            msg = await update.message.reply_text("⏳ *Обработка...*", parse_mode=ParseMode.MARKDOWN)
            await asyncio.sleep(1)
            action = context.user_data['list_action']
            list_type = context.user_data['list_type']
            conn = psycopg2.connect(DB_CONN_STRING)
            c = conn.cursor()
            if list_type == 'read':
                c.execute("SELECT b.id, b.title FROM user_read ur JOIN books b ON ur.book_id = b.id WHERE ur.user_id = %s", (user_id,))
            else:
                c.execute("SELECT b.id, b.title FROM user_favorites uf JOIN books b ON uf.book_id = b.id WHERE uf.user_id = %s", (user_id,))
            books = c.fetchall()
            conn.close()
            
            try:
                index = int(text) - 1
                if 0 <= index < len(books):
                    book_id = books[index][0]
                else:
                    raise ValueError
            except ValueError:
                book_id = next((b[0] for b in books if text.lower() in b[1].lower()), None)
                if not book_id:
                    await update.message.reply_text("📚 *Книга не найдена в списке.*", reply_markup=main_menu(user_id), parse_mode=ParseMode.MARKDOWN)
                    await msg.delete()
                    return
            
            conn = psycopg2.connect(DB_CONN_STRING)
            c = conn.cursor()
            if action == 'rate':
                keyboard = [[InlineKeyboardButton(f"{i}★", callback_data=f'rate_{book_id}_{i}') for i in range(1, 6)]]
                await update.message.reply_text("⭐ *Выбери оценку:*", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
            elif action == 'delete':
                if list_type == 'read':
                    c.execute("DELETE FROM user_read WHERE user_id = %s AND book_id = %s", (user_id, book_id))
                else:
                    c.execute("DELETE FROM user_favorites WHERE user_id = %s AND book_id = %s", (user_id, book_id))
                conn.commit()
                await update.message.reply_text(f"🗑️ Книга удалена из {list_type == 'read' and 'прочитанного' or 'избранного'}.", reply_markup=main_menu(user_id), parse_mode=ParseMode.MARKDOWN)
            elif action == 'move':
                if list_type == 'read':
                    c.execute("INSERT INTO user_favorites (user_id, book_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", (user_id, book_id))
                else:
                    c.execute("INSERT INTO user_read (user_id, book_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", (user_id, book_id))
                conn.commit()
                await update.message.reply_text(f"➡️ Книга добавлена в {list_type == 'read' and 'избранное' or 'прочитанное'}.", reply_markup=main_menu(user_id), parse_mode=ParseMode.MARKDOWN)
            conn.close()
            await msg.delete()
            context.user_data['state'] = None
        
        elif state == 'select_book_read':
            msg = await update.message.reply_text("⏳ *Поиск книги...*", parse_mode=ParseMode.MARKDOWN)
            await asyncio.sleep(1)
            conn = psycopg2.connect(DB_CONN_STRING)
            c = conn.cursor()
            c.execute("SELECT b.id, b.title, b.description, b.genres, b.cover_url, ur.rating FROM user_read ur JOIN books b ON ur.book_id = b.id WHERE ur.user_id = %s", (user_id,))
            books = c.fetchall()
            conn.close()
            
            try:
                index = int(text) - 1
                if 0 <= index < len(books):
                    book_id, title, description, genres, cover_url, rating = books[index]
                    keyboard = [
                        [InlineKeyboardButton("❤️ Добавить в избранное", callback_data='list_action_move_read'),
                         InlineKeyboardButton("⭐ Оценить", callback_data='list_action_rate_read')],
                        [InlineKeyboardButton("✏️ Редактировать", callback_data=f'edit_book_{book_id}'),
                         InlineKeyboardButton("🔍 Выбрать другую", callback_data='back_to_select_read')],
                        [InlineKeyboardButton("🔙 Главное меню", callback_data='main_menu')]
                    ]
                    await update.message.reply_photo(
                        photo=cover_url,
                        caption=f"**{title}**\n\n_{description}_\n\n*Жанры:* {genres}\n*Оценка:* {rating_to_stars(rating)}",
                        reply_markup=InlineKeyboardMarkup(keyboard),
                        parse_mode=ParseMode.MARKDOWN
                    )
                else:
                    await update.message.reply_text("❌ *Неверный номер книги.*", reply_markup=main_menu(user_id), parse_mode=ParseMode.MARKDOWN)
            except ValueError:
                await update.message.reply_text("❌ *Введите корректный номер книги.*", reply_markup=main_menu(user_id), parse_mode=ParseMode.MARKDOWN)
            await msg.delete()
            context.user_data['state'] = None
        
        elif state == 'select_book_favorite':
            msg = await update.message.reply_text("⏳ *Поиск книги...*", parse_mode=ParseMode.MARKDOWN)
            await asyncio.sleep(1)
            conn = psycopg2.connect(DB_CONN_STRING)
            c = conn.cursor()
            c.execute("SELECT b.id, b.title, b.description, b.genres, b.cover_url FROM user_favorites uf JOIN books b ON uf.book_id = b.id WHERE uf.user_id = %s", (user_id,))
            books = c.fetchall()
            conn.close()
            
            try:
                index = int(text) - 1
                if 0 <= index < len(books):
                    book_id, title, description, genres, cover_url = books[index]
                    conn = psycopg2.connect(DB_CONN_STRING)
                    c = conn.cursor()
                    c.execute("SELECT rating FROM user_read WHERE user_id = %s AND book_id = %s", (user_id, book_id))
                    rating = c.fetchone()
                    conn.close()
                    rating = rating[0] if rating else None
                    keyboard = [
                        [InlineKeyboardButton("🗑️ Удалить из избранного", callback_data='list_action_delete_favorite'),
                         InlineKeyboardButton("⭐ Оценить", callback_data='list_action_rate_favorite')],
                        [InlineKeyboardButton("✏️ Редактировать", callback_data=f'edit_book_{book_id}'),
                         InlineKeyboardButton("🔍 Выбрать другую", callback_data='back_to_select_favorite')],
                        [InlineKeyboardButton("🔙 Главное меню", callback_data='main_menu')]
                    ]
                    await update.message.reply_photo(
                        photo=cover_url,
                        caption=f"**{title}**\n\n_{description}_\n\n*Жанры:* {genres}\n*Оценка:* {rating_to_stars(rating)}",
                        reply_markup=InlineKeyboardMarkup(keyboard),
                        parse_mode=ParseMode.MARKDOWN
                    )
                else:
                    await update.message.reply_text("❌ *Неверный номер книги.*", reply_markup=main_menu(user_id), parse_mode=ParseMode.MARKDOWN)
            except ValueError:
                await update.message.reply_text("❌ *Введите корректный номер книги.*", reply_markup=main_menu(user_id), parse_mode=ParseMode.MARKDOWN)
            await msg.delete()
            context.user_data['state'] = None
        
        elif state == 'edit_book_select':
            msg = await update.message.reply_text("⏳ *Поиск книги...*", parse_mode=ParseMode.MARKDOWN)
            await asyncio.sleep(1)
            conn = psycopg2.connect(DB_CONN_STRING)
            c = conn.cursor()
            c.execute("SELECT id, title FROM books WHERE id LIKE 'manual_%'", ())
            books = c.fetchall()
            conn.close()
            
            try:
                index = int(text) - 1
                if 0 <= index < len(books):
                    book_id = books[index][0]
                    context.user_data['edit_book_id'] = book_id
                    await update.message.reply_text("📝 Укажи новое описание (или 'без изменений' для сохранения текущего):", parse_mode=ParseMode.MARKDOWN)
                    context.user_data['state'] = 'edit_book_description'
                else:
                    await update.message.reply_text("❌ *Неверный номер книги.*", reply_markup=main_menu(user_id), parse_mode=ParseMode.MARKDOWN)
            except ValueError:
                await update.message.reply_text("❌ *Введите корректный номер книги.*", reply_markup=main_menu(user_id), parse_mode=ParseMode.MARKDOWN)
            await msg.delete()
        
        elif state == 'edit_book_description':
            context.user_data['edit_description'] = text if text.lower() != 'без изменений' else None
            await update.message.reply_text("📷 Прикрепи новое фото обложки (или отправь 'без изменений' для сохранения текущего):", parse_mode=ParseMode.MARKDOWN)
            context.user_data['state'] = 'edit_book_cover'
        
        elif state == 'edit_book_cover':
            msg = await update.message.reply_text("⏳ *Обновление книги...*", parse_mode=ParseMode.MARKDOWN)
            await asyncio.sleep(1)
            book_id = context.user_data['edit_book_id']
            new_description = context.user_data['edit_description']
            if update.message.photo:
                new_cover_url = update.message.photo[-1].file_id
            elif text.lower() == 'без изменений':
                new_cover_url = None
            else:
                await update.message.reply_text("📷 *Пожалуйста, прикрепи фото или напиши 'без изменений'.*", reply_markup=main_menu(user_id), parse_mode=ParseMode.MARKDOWN)
                await msg.delete()
                return
            
            conn = psycopg2.connect(DB_CONN_STRING)
            c = conn.cursor()
            if new_description:
                c.execute("UPDATE books SET description = %s WHERE id = %s", (new_description, book_id))
            if new_cover_url:
                c.execute("UPDATE books SET cover_url = %s WHERE id = %s", (new_cover_url, book_id))
            conn.commit()
            conn.close()
            await update.message.reply_text("📝 *Книга обновлена!*", reply_markup=main_menu(user_id), parse_mode=ParseMode.MARKDOWN)
            await msg.delete()
            context.user_data['state'] = None
        
        elif state == 'admin_broadcast_message' and user_id == ADMIN_ID:
            msg = await update.message.reply_text("⏳ *Отправка рассылки...*", parse_mode=ParseMode.MARKDOWN)
            await asyncio.sleep(1)
            conn = psycopg2.connect(DB_CONN_STRING)
            c = conn.cursor()
            c.execute("SELECT user_id FROM users WHERE agreed = 1 AND (banned_until IS NULL OR banned_until < %s)", (int(time.time()),))
            users = c.fetchall()
            conn.close()
            for uid in users:
                try:
                    await context.bot.send_message(chat_id=uid[0], text=text, parse_mode=ParseMode.MARKDOWN)
                except Exception as e:
                    logger.error(f"Ошибка отправки сообщения пользователю {uid[0]}: {e}")
            await update.message.reply_text("✉️ *Рассылка завершена.*", reply_markup=main_menu(user_id), parse_mode=ParseMode.MARKDOWN)
            await msg.delete()
            context.user_data['state'] = None
        
        elif state == 'admin_ban_id' and user_id == ADMIN_ID:
            try:
                ban_user_id = int(text)
                context.user_data['ban_user_id'] = ban_user_id
                await update.message.reply_text("⏳ Укажи срок блокировки в днях (например, 7):", parse_mode=ParseMode.MARKDOWN)
                context.user_data['state'] = 'admin_ban_duration'
            except ValueError:
                await update.message.reply_text("❌ *Введите корректный ID пользователя.*", reply_markup=main_menu(user_id), parse_mode=ParseMode.MARKDOWN)
        
        elif state == 'admin_ban_duration' and user_id == ADMIN_ID:
            try:
                duration = int(text)
                context.user_data['ban_duration'] = duration
                await update.message.reply_text("📝 Укажи причину блокировки:", parse_mode=ParseMode.MARKDOWN)
                context.user_data['state'] = 'admin_ban_reason'
            except ValueError:
                await update.message.reply_text("❌ *Введите корректное число дней.*", reply_markup=main_menu(user_id), parse_mode=ParseMode.MARKDOWN)
        
        elif state == 'admin_ban_reason' and user_id == ADMIN_ID:
            ban_user_id = context.user_data['ban_user_id']
            duration = context.user_data['ban_duration']
            reason = text
            ban_until = int(time.time()) + duration * 86400
            conn = psycopg2.connect(DB_CONN_STRING)
            c = conn.cursor()
            c.execute("UPDATE users SET banned_until = %s, ban_reason = %s WHERE user_id = %s", (ban_until, reason, ban_user_id))
            conn.commit()
            conn.close()
            await update.message.reply_text(f"🚫 Пользователь {ban_user_id} заблокирован до {datetime.fromtimestamp(ban_until).strftime('%Y-%m-%d %H:%M:%S')} по причине: *{reason}*", reply_markup=main_menu(user_id), parse_mode=ParseMode.MARKDOWN)
            await context.bot.send_message(chat_id=ban_user_id, text=f"🚫 *Вы заблокированы до {datetime.fromtimestamp(ban_until).strftime('%Y-%m-%d %H:%M:%S')}*\n*Причина:* {reason}", parse_mode=ParseMode.MARKDOWN)
            context.user_data['state'] = None
        
        elif state == 'admin_unban_id' and user_id == ADMIN_ID:
            try:
                unban_user_id = int(text)
                conn = psycopg2.connect(DB_CONN_STRING)
                c = conn.cursor()
                c.execute("UPDATE users SET banned_until = 0, ban_reason = NULL WHERE user_id = %s", (unban_user_id,))
                conn.commit()
                conn.close()
                await update.message.reply_text(f"✅ Пользователь {unban_user_id} разблокирован.", reply_markup=main_menu(user_id), parse_mode=ParseMode.MARKDOWN)
                await context.bot.send_message(chat_id=unban_user_id, text="✅ *Вы были разблокированы!*", parse_mode=ParseMode.MARKDOWN)
                context.user_data['state'] = None
            except ValueError:
                await update.message.reply_text("❌ *Введите корректный ID пользователя.*", reply_markup=main_menu(user_id), parse_mode=ParseMode.MARKDOWN)
        
        elif state == 'admin_reset_user_id' and user_id == ADMIN_ID:
            try:
                reset_user_id = int(text)
                reset_database(reset_user_id)
                await update.message.reply_text(f"🗑️ Данные пользователя {reset_user_id} сброшены.", reply_markup=main_menu(user_id), parse_mode=ParseMode.MARKDOWN)
                context.user_data['state'] = None
            except ValueError:
                await update.message.reply_text("❌ *Введите корректный ID пользователя.*", reply_markup=main_menu(user_id), parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"Ошибка в handle_message: {e}")
        await update.message.reply_text("⚠️ *Произошла ошибка, попробуйте позже.*", reply_markup=main_menu(user_id), parse_mode=ParseMode.MARKDOWN)

# Быстрые команды
async def read_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await check_user(update, context):
        await show_read(update.callback_query or update.message, context, page=1)

async def favorites_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await check_user(update, context):
        await show_favorites(update.callback_query or update.message, context, page=1)

async def search_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await check_user(update, context):
        await update.message.reply_text("🔍 *Укажи название книги для поиска:*", parse_mode=ParseMode.MARKDOWN)
        context.user_data['state'] = 'search_title'

# Пагинация списков
async def show_read(query, context, page):
    user_id = query.from_user.id if query.from_user else query.message.from_user.id
    logger.info(f"Показ списка прочитанного для {user_id}, страница {page}")
    conn = psycopg2.connect(DB_CONN_STRING)
    c = conn.cursor()
    c.execute("SELECT b.id, b.title, ur.rating FROM user_read ur JOIN books b ON ur.book_id = b.id WHERE ur.user_id = %s", (user_id,))
    books = c.fetchall()
    conn.close()
    
    items_per_page = 10
    total_pages = (len(books) + items_per_page - 1) // items_per_page
    start_idx = (page - 1) * items_per_page
    end_idx = start_idx + items_per_page
    
    if books:
        list_text = f"📖 *Список прочитанного (страница {page}/{total_pages}):*\n"
        for i, (book_id, title, rating) in enumerate(books[start_idx:end_idx], start_idx + 1):
            list_text += f"{i}. {title} - {rating_to_stars(rating)}\n"
        keyboard = [
            [InlineKeyboardButton("⭐ Оценить", callback_data='list_action_rate_read'),
             InlineKeyboardButton("❤️ Добавить в избранное", callback_data='list_action_move_read')],
            [InlineKeyboardButton("🗑️ Удалить", callback_data='list_action_delete_read'),
             InlineKeyboardButton("🔍 Выбрать книгу", callback_data='select_book_read')],
            [InlineKeyboardButton("📥 Экспорт", callback_data='export_read')]
        ]
        if page > 1:
            keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data=f'page_read_{page-1}')])
        if page < total_pages:
            keyboard[-1].append(InlineKeyboardButton("➡️ Вперёд", callback_data=f'page_read_{page+1}'))
        keyboard.append([InlineKeyboardButton("🔙 Главное меню", callback_data='main_menu')])
        await (query.message.reply_text if query.from_user else query.edit_message_text)(list_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
    else:
        await (query.message.reply_text if query.from_user else query.edit_message_text)("📖 *Список прочитанного пуст.*", reply_markup=main_menu(user_id), parse_mode=ParseMode.MARKDOWN)

async def show_favorites(query, context, page):
    user_id = query.from_user.id if query.from_user else query.message.from_user.id
    logger.info(f"Показ списка избранного для {user_id}, страница {page}")
    conn = psycopg2.connect(DB_CONN_STRING)
    c = conn.cursor()
    c.execute("SELECT b.id, b.title FROM user_favorites uf JOIN books b ON uf.book_id = b.id WHERE uf.user_id = %s", (user_id,))
    books = c.fetchall()
    conn.close()
    
    items_per_page = 10
    total_pages = (len(books) + items_per_page - 1) // items_per_page
    start_idx = (page - 1) * items_per_page
    end_idx = start_idx + items_per_page
    
    if books:
        list_text = f"⭐ *Список избранного (страница {page}/{total_pages}):*\n"
        for i, (book_id, title) in enumerate(books[start_idx:end_idx], start_idx + 1):
            conn = psycopg2.connect(DB_CONN_STRING)
            c = conn.cursor()
            c.execute("SELECT rating FROM user_read WHERE user_id = %s AND book_id = %s", (user_id, book_id))
            rating = c.fetchone()
            conn.close()
            rating = rating[0] if rating else None
            list_text += f"{i}. {title} - {rating_to_stars(rating)}\n"
        keyboard = [
            [InlineKeyboardButton("⭐ Оценить", callback_data='list_action_rate_favorite'),
             InlineKeyboardButton("📖 Добавить в прочитанное", callback_data='list_action_move_favorite')],
            [InlineKeyboardButton("🗑️ Удалить", callback_data='list_action_delete_favorite'),
             InlineKeyboardButton("🔍 Выбрать книгу", callback_data='select_book_favorite')],
            [InlineKeyboardButton("📥 Экспорт", callback_data='export_favorites')]
        ]
        if page > 1:
            keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data=f'page_favorites_{page-1}')])
        if page < total_pages:
            keyboard[-1].append(InlineKeyboardButton("➡️ Вперёд", callback_data=f'page_favorites_{page+1}'))
        keyboard.append([InlineKeyboardButton("🔙 Главное меню", callback_data='main_menu')])
        await (query.message.reply_text if query.from_user else query.edit_message_text)(list_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
    else:
        await (query.message.reply_text if query.from_user else query.edit_message_text)("⭐ *Список избранного пуст.*", reply_markup=main_menu(user_id), parse_mode=ParseMode.MARKDOWN)

# Ежедневная рекомендация (с учётом часового пояса UTC+3)
async def daily_recommendation(context: ContextTypes.DEFAULT_TYPE):
    conn = psycopg2.connect(DB_CONN_STRING)
    c = conn.cursor()
    c.execute("SELECT user_id FROM users WHERE agreed = 1 AND (banned_until IS NULL OR banned_until < %s)", (int(time.time()),))
    users = c.fetchall()
    conn.close()
    
    for user_id in users:
        user_id = user_id[0]
        conn = psycopg2.connect(DB_CONN_STRING)
        c = conn.cursor()
        c.execute("SELECT genres FROM books b JOIN user_favorites uf ON b.id = uf.book_id WHERE uf.user_id = %s", (user_id,))
        genres = [g[0] for g in c.fetchall()]
        conn.close()
        
        if genres:
            random_genre = random.choice(genres.split(','))
            book = await search_book_by_title_or_genre(random_genre, is_genre=True)
            if book:
                cache_book(book)
                await context.bot.send_photo(
                    chat_id=user_id,
                    photo=book['cover_url'],
                    caption=f"📚 *Ежедневная рекомендация:*\n**{book['title']}**\n\n_{book['description']}_\n\n*Жанры:* {book['genres']}",
                    parse_mode=ParseMode.MARKDOWN
                )

def main():
    init_db()
    application = Application.builder().token(os.getenv('TELEGRAM_BOT_TOKEN', '8173510242:AAH0x9rsdU5Fv3aRJhlZ1zF_mdlSTFffHos')).build()
    
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("read", read_command))
    application.add_handler(CommandHandler("favorites", favorites_command))
    application.add_handler(CommandHandler("search", search_command))
    application.add_handler(CallbackQueryHandler(button))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    application.add_handler(MessageHandler(filters.PHOTO | filters.TEXT, handle_message))
    
    # Установка времени для Москвы (UTC+3)
    moscow_time = time(hour=9, tzinfo=tzoffset(10800))  # 9 утра по Москве
    application.job_queue.run_daily(daily_recommendation, moscow_time)
    application.job_queue.run_daily(backup_database, time(hour=0, tzinfo=tzoffset(10800)))  # Ежедневный бэкап в полночь
    
    application.run_polling()

# Временная зона для Москвы
from datetime import tzinfo
class tzoffset(tzinfo):
    def __init__(self, offset):
        self._offset = timedelta(seconds=offset)
    def utcoffset(self, dt): return self._offset
    def tzname(self, dt): return "UTC+3"
    def dst(self, dt): return timedelta(0)

if __name__ == '__main__':
    main()
