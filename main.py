import sqlite3
import random
import aiohttp
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes
from datetime import datetime, time, timedelta
import os
import asyncio

# Настройка логирования
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# ID администратора (замените на ваш)
ADMIN_ID = 123456789

# Инициализация базы данных
def init_db():
    conn = sqlite3.connect('books.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS books 
                 (id TEXT PRIMARY KEY, title TEXT, description TEXT, genres TEXT, cover_url TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS user_read 
                 (user_id INTEGER, book_id TEXT, rating INTEGER, PRIMARY KEY (user_id, book_id))''')
    c.execute('''CREATE TABLE IF NOT EXISTS user_favorites 
                 (user_id INTEGER, book_id TEXT, PRIMARY KEY (user_id, book_id))''')
    c.execute('''CREATE TABLE IF NOT EXISTS users 
                 (user_id INTEGER PRIMARY KEY, username TEXT, agreed INTEGER DEFAULT 0, banned_until INTEGER DEFAULT 0, ban_reason TEXT)''')
    conn.commit()
    conn.close()

# Главное меню
def main_menu(user_id):
    keyboard = [
        [InlineKeyboardButton("Поиск по жанру", callback_data='search_genre'),
         InlineKeyboardButton("Поиск книги", callback_data='search_title')],
        [InlineKeyboardButton("Добавить в прочитанное", callback_data='add_read'),
         InlineKeyboardButton("Добавить в избранное", callback_data='add_favorite')],
        [InlineKeyboardButton("Мои прочитанные", callback_data='show_read'),
         InlineKeyboardButton("Мои избранные", callback_data='show_favorites')]
    ]
    if user_id == ADMIN_ID:
        keyboard.append([InlineKeyboardButton("Админ-панель", callback_data='admin_panel')])
    return InlineKeyboardMarkup(keyboard)

# Преобразование рейтинга в звёздочки
def rating_to_stars(rating):
    if rating is None:
        return "☆☆☆☆☆"
    return "★" * rating + "☆" * (5 - rating)

# Проверка согласия и бана
async def check_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    with sqlite3.connect('books.db') as conn:
        c = conn.cursor()
        c.execute("SELECT agreed, banned_until, ban_reason FROM users WHERE user_id = ?", (user_id,))
        user = c.fetchone()
    
    if not user or not user[0]:  # Не согласен или новый пользователь
        return False
    if user[1] and user[1] > int(time.time()):  # Пользователь забанен
        await update.message.reply_text(f"Вы заблокированы до {datetime.fromtimestamp(user[1]).strftime('%Y-%m-%d %H:%M:%S')}.\nПричина: {user[2]}")
        return False
    return True

# Команда /start
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    username = update.message.from_user.username
    
    with sqlite3.connect('books.db') as conn:
        c = conn.cursor()
        c.execute("INSERT OR IGNORE INTO users (user_id, username) VALUES (?, ?)", (user_id, username))
        c.execute("SELECT agreed FROM users WHERE user_id = ?", (user_id,))
        agreed = c.fetchone()[0]
    
    if not agreed:
        keyboard = [
            [InlineKeyboardButton("Согласен", callback_data='agree_policy'),
             InlineKeyboardButton("Отказ", callback_data='refuse_policy')]
        ]
        await update.message.reply_text("Пожалуйста, согласитесь с политикой использования бота:", reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await update.message.reply_text("Добро пожаловать в бот для книг! Я использую Open Library.", reply_markup=main_menu(user_id))

# Поиск книги через Open Library API
async def search_book_by_title_or_genre(query, is_genre=False):
    async with aiohttp.ClientSession() as session:
        if is_genre:
            url = f"https://openlibrary.org/subjects/{query.lower().replace(' ', '_')}.json?limit=1&sort=random"
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
    with sqlite3.connect('books.db') as conn:
        c = conn.cursor()
        c.execute("INSERT OR IGNORE INTO books (id, title, description, genres, cover_url) VALUES (?, ?, ?, ?, ?)",
                  (book['id'], book['title'], book['description'], book['genres'], book['cover_url']))
        conn.commit()

# Обработка кнопок
async def button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    logger.info(f"Обработка callback: {query.data} для пользователя {user_id}")
    
    if not await check_user(query, context):
        return
    
    if query.data == 'search_genre':
        await query.message.reply_text("Укажи жанр (например, 'Фэнтези'):")
        context.user_data['state'] = 'search_genre'
    elif query.data == 'search_title':
        await query.message.reply_text("Укажи название книги:")
        context.user_data['state'] = 'search_title'
    elif query.data == 'add_read':
        await query.message.reply_text("Укажи название книги для добавления в прочитанное:")
        context.user_data['state'] = 'add_read'
    elif query.data == 'add_favorite':
        await query.message.reply_text("Укажи название книги для добавления в избранное:")
        context.user_data['state'] = 'add_favorite'
    elif query.data == 'show_read':
        await show_read(query, context)
    elif query.data == 'show_favorites':
        await show_favorites(query, context)
    elif query.data == 'add_found_to_read':
        book = context.user_data.get('last_found_book')
        if book:
            with sqlite3.connect('books.db') as conn:
                c = conn.cursor()
                c.execute("INSERT INTO user_read (user_id, book_id) VALUES (?, ?)", (user_id, book['id']))
                conn.commit()
            await query.message.reply_text(f"Книга '{book['title']}' добавлена в прочитанное.", reply_markup=main_menu(user_id))
    elif query.data == 'add_found_to_favorite':
        book = context.user_data.get('last_found_book')
        if book:
            with sqlite3.connect('books.db') as conn:
                c = conn.cursor()
                c.execute("INSERT INTO user_favorites (user_id, book_id) VALUES (?, ?)", (user_id, book['id']))
                conn.commit()
            await query.message.reply_text(f"Книга '{book['title']}' добавлена в избранное.", reply_markup=main_menu(user_id))
    elif query.data.startswith('list_action_'):
        action, list_type = query.data.split('_')[2], query.data.split('_')[3]
        context.user_data['list_action'] = action
        context.user_data['list_type'] = list_type
        await query.message.reply_text("Укажи номер книги из списка (1, 2, 3...) или её название:")
        context.user_data['state'] = 'list_action_select'
    elif query.data.startswith('rate_'):
        _, book_id, rating = query.data.split('_')
        rating = int(rating)
        with sqlite3.connect('books.db') as conn:
            c = conn.cursor()
            c.execute("UPDATE user_read SET rating = ? WHERE user_id = ? AND book_id = ?", (rating, user_id, book_id))
            if c.rowcount == 0:
                c.execute("INSERT INTO user_read (user_id, book_id, rating) VALUES (?, ?, ?)", (user_id, book_id, rating))
            conn.commit()
        await query.message.reply_text(f"Оценка {rating}★ сохранена.", reply_markup=main_menu(user_id))
    elif query.data == 'main_menu':
        await query.message.reply_text("Возвращаемся в главное меню.", reply_markup=main_menu(user_id))
    elif query.data == 'agree_policy':
        with sqlite3.connect('books.db') as conn:
            c = conn.cursor()
            c.execute("UPDATE users SET agreed = 1 WHERE user_id = ?", (user_id,))
            conn.commit()
        await query.message.reply_text("Спасибо за согласие! Теперь вы можете использовать бот.", reply_markup=main_menu(user_id))
    elif query.data == 'refuse_policy':
        await query.message.reply_text("Вы отказались от политики. Без согласия использовать бот невозможно.", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("Согласен", callback_data='agree_policy'),
             InlineKeyboardButton("Отказ", callback_data='refuse_policy')]
        ]))
    elif query.data == 'admin_panel':
        if user_id == ADMIN_ID:
            keyboard = [
                [InlineKeyboardButton("Рассылка", callback_data='admin_broadcast'),
                 InlineKeyboardButton("Заблокировать пользователя", callback_data='admin_ban')],
                [InlineKeyboardButton("Статистика", callback_data='admin_stats'),
                 InlineKeyboardButton("Главное меню", callback_data='main_menu')]
            ]
            await query.message.reply_text("Админ-панель:", reply_markup=InlineKeyboardMarkup(keyboard))
    elif query.data == 'admin_broadcast':
        await query.message.reply_text("Введите сообщение для рассылки:")
        context.user_data['state'] = 'admin_broadcast_message'
    elif query.data == 'admin_ban':
        await query.message.reply_text("Введите ID пользователя для блокировки:")
        context.user_data['state'] = 'admin_ban_id'
    elif query.data == 'admin_stats':
        with sqlite3.connect('books.db') as conn:
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM users")
            user_count = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM books")
            book_count = c.fetchone()[0]
        await query.message.reply_text(f"Статистика:\nПользователей: {user_count}\nКниг в базе: {book_count}", reply_markup=main_menu(user_id))

# Обработка текстовых сообщений
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    state = context.user_data.get('state')
    user_id = update.message.from_user.id
    logger.info(f"Получено сообщение: {text} в состоянии {state} от {user_id}")
    
    if not await check_user(update, context):
        return
    
    if state == 'search_genre':
        msg = await update.message.reply_text("⏳ Поиск книги...")
        await asyncio.sleep(1)
        book = await search_book_by_title_or_genre(text, is_genre=True)
        if book:
            cache_book(book)
            context.user_data['last_found_book'] = book
            keyboard = [
                [InlineKeyboardButton("Добавить в прочитанное", callback_data='add_found_to_read'),
                 InlineKeyboardButton("Добавить в избранное", callback_data='add_found_to_favorite')],
                [InlineKeyboardButton("Главное меню", callback_data='main_menu')]
            ]
            await update.message.reply_photo(
                photo=book['cover_url'],
                caption=f"**{book['title']}**\n\n{book['description']}\n\nЖанры: {book['genres']}",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        else:
            await update.message.reply_text("Книга не найдена.", reply_markup=main_menu(user_id))
        await msg.delete()
    
    elif state == 'search_title':
        msg = await update.message.reply_text("⏳ Поиск книги...")
        await asyncio.sleep(1)
        book = await search_book_by_title_or_genre(text)
        if book:
            cache_book(book)
            context.user_data['last_found_book'] = book
            keyboard = [
                [InlineKeyboardButton("Добавить в прочитанное", callback_data='add_found_to_read'),
                 InlineKeyboardButton("Добавить в избранное", callback_data='add_found_to_favorite')],
                [InlineKeyboardButton("Главное меню", callback_data='main_menu')]
            ]
            await update.message.reply_photo(
                photo=book['cover_url'],
                caption=f"**{book['title']}**\n\n{book['description']}\n\nЖанры: {book['genres']}",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        else:
            await update.message.reply_text("Книга не найдена.", reply_markup=main_menu(user_id))
        await msg.delete()
    
    elif state == 'add_read':
        msg = await update.message.reply_text("⏳ Добавление книги...")
        await asyncio.sleep(1)
        with sqlite3.connect('books.db') as conn:
            c = conn.cursor()
            c.execute("SELECT id FROM books WHERE title LIKE ?", (f'%{text}%',))
            book = c.fetchone()
            if book:
                c.execute("INSERT INTO user_read (user_id, book_id) VALUES (?, ?)", (user_id, book[0]))
                conn.commit()
                await update.message.reply_text(f"Книга '{text}' добавлена в прочитанное.", reply_markup=main_menu(user_id))
            else:
                book = await search_book_by_title_or_genre(text)
                if book:
                    cache_book(book)
                    c.execute("INSERT INTO user_read (user_id, book_id) VALUES (?, ?)", (user_id, book['id']))
                    conn.commit()
                    await update.message.reply_text(f"Книга '{text}' добавлена в прочитанное.", reply_markup=main_menu(user_id))
                else:
                    context.user_data['manual_title'] = text
                    context.user_data['manual_list'] = 'read'
                    await update.message.reply_text("Книга не найдена. Укажи описание:")
                    context.user_data['state'] = 'manual_description'
        await msg.delete()
    
    elif state == 'add_favorite':
        msg = await update.message.reply_text("⏳ Добавление книги...")
        await asyncio.sleep(1)
        with sqlite3.connect('books.db') as conn:
            c = conn.cursor()
            c.execute("SELECT id FROM books WHERE title LIKE ?", (f'%{text}%',))
            book = c.fetchone()
            if book:
                c.execute("INSERT INTO user_favorites (user_id, book_id) VALUES (?, ?)", (user_id, book[0]))
                conn.commit()
                await update.message.reply_text(f"Книга '{text}' добавлена в избранное.", reply_markup=main_menu(user_id))
            else:
                book = await search_book_by_title_or_genre(text)
                if book:
                    cache_book(book)
                    c.execute("INSERT INTO user_favorites (user_id, book_id) VALUES (?, ?)", (user_id, book['id']))
                    conn.commit()
                    await update.message.reply_text(f"Книга '{text}' добавлена в избранное.", reply_markup=main_menu(user_id))
                else:
                    context.user_data['manual_title'] = text
                    context.user_data['manual_list'] = 'favorite'
                    await update.message.reply_text("Книга не найдена. Укажи описание:")
                    context.user_data['state'] = 'manual_description'
        await msg.delete()
    
    elif state == 'manual_description':
        context.user_data['manual_description'] = text
        await update.message.reply_text("Прикрепи фото обложки (или отправь 'нет', если нет фото):")
        context.user_data['state'] = 'manual_cover'
    
    elif state == 'manual_cover':
        msg = await update.message.reply_text("⏳ Сохранение книги...")
        await asyncio.sleep(1)
        title = context.user_data['manual_title']
        description = context.user_data['manual_description']
        list_type = context.user_data['manual_list']
        if update.message.photo:
            cover_url = update.message.photo[-1].file_id
        elif text.lower() == 'нет':
            cover_url = "https://via.placeholder.com/150"
        else:
            await update.message.reply_text("Пожалуйста, прикрепи фото или напиши 'нет'.", reply_markup=main_menu(user_id))
            await msg.delete()
            return
        
        book_id = f"manual_{user_id}_{int(time.time())}"
        book = {'id': book_id, 'title': title, 'description': description, 'genres': 'Не указаны', 'cover_url': cover_url}
        cache_book(book)
        with sqlite3.connect('books.db') as conn:
            c = conn.cursor()
            if list_type == 'read':
                c.execute("INSERT INTO user_read (user_id, book_id) VALUES (?, ?)", (user_id, book_id))
            else:
                c.execute("INSERT INTO user_favorites (user_id, book_id) VALUES (?, ?)", (user_id, book_id))
            conn.commit()
        await update.message.reply_text(f"Книга '{title}' добавлена в {list_type == 'read' and 'прочитанное' or 'избранное'}.", reply_markup=main_menu(user_id))
        await msg.delete()
        context.user_data['state'] = None
    
    elif state == 'list_action_select':
        msg = await update.message.reply_text("⏳ Обработка...")
        await asyncio.sleep(1)
        action = context.user_data['list_action']
        list_type = context.user_data['list_type']
        with sqlite3.connect('books.db') as conn:
            c = conn.cursor()
            if list_type == 'read':
                c.execute("SELECT b.id, b.title FROM user_read ur JOIN books b ON ur.book_id = b.id WHERE ur.user_id = ?", (user_id,))
            else:
                c.execute("SELECT b.id, b.title FROM user_favorites uf JOIN books b ON uf.book_id = b.id WHERE uf.user_id = ?", (user_id,))
            books = c.fetchall()
        
        try:
            index = int(text) - 1
            if 0 <= index < len(books):
                book_id = books[index][0]
            else:
                raise ValueError
        except ValueError:
            book_id = next((b[0] for b in books if text.lower() in b[1].lower()), None)
            if not book_id:
                await update.message.reply_text("Книга не найдена в списке.", reply_markup=main_menu(user_id))
                await msg.delete()
                return
        
        if action == 'rate':
            keyboard = [[InlineKeyboardButton(f"{i}★", callback_data=f'rate_{book_id}_{i}') for i in range(1, 6)]]
            await update.message.reply_text("Выбери оценку:", reply_markup=InlineKeyboardMarkup(keyboard))
        elif action == 'delete':
            with sqlite3.connect('books.db') as conn:
                c = conn.cursor()
                if list_type == 'read':
                    c.execute("DELETE FROM user_read WHERE user_id = ? AND book_id = ?", (user_id, book_id))
                else:
                    c.execute("DELETE FROM user_favorites WHERE user_id = ? AND book_id = ?", (user_id, book_id))
                conn.commit()
            await update.message.reply_text(f"Книга удалена из {list_type == 'read' and 'прочитанного' or 'избранного'}.", reply_markup=main_menu(user_id))
        elif action == 'move':
            with sqlite3.connect('books.db') as conn:
                c = conn.cursor()
                if list_type == 'read':
                    c.execute("DELETE FROM user_read WHERE user_id = ? AND book_id = ?", (user_id, book_id))
                    c.execute("INSERT INTO user_favorites (user_id, book_id) VALUES (?, ?)", (user_id, book_id))
                else:
                    c.execute("DELETE FROM user_favorites WHERE user_id = ? AND book_id = ?", (user_id, book_id))
                    c.execute("INSERT INTO user_read (user_id, book_id) VALUES (?, ?)", (user_id, book_id))
                conn.commit()
            await update.message.reply_text(f"Книга перемещена в {list_type == 'read' and 'избранное' or 'прочитанное'}.", reply_markup=main_menu(user_id))
        await msg.delete()
        context.user_data['state'] = None
    
    elif state == 'admin_broadcast_message' and user_id == ADMIN_ID:
        msg = await update.message.reply_text("⏳ Отправка рассылки...")
        await asyncio.sleep(1)
        with sqlite3.connect('books.db') as conn:
            c = conn.cursor()
            c.execute("SELECT user_id FROM users WHERE agreed = 1 AND (banned_until IS NULL OR banned_until < ?)", (int(time.time()),))
            users = c.fetchall()
        for uid in users:
            try:
                await context.bot.send_message(chat_id=uid[0], text=text)
            except Exception as e:
                logger.error(f"Ошибка отправки сообщения пользователю {uid[0]}: {e}")
        await update.message.reply_text("Рассылка завершена.", reply_markup=main_menu(user_id))
        await msg.delete()
        context.user_data['state'] = None
    
    elif state == 'admin_ban_id' and user_id == ADMIN_ID:
        try:
            ban_user_id = int(text)
            context.user_data['ban_user_id'] = ban_user_id
            await update.message.reply_text("Укажи срок блокировки в днях (например, 7):")
            context.user_data['state'] = 'admin_ban_duration'
        except ValueError:
            await update.message.reply_text("Введите корректный ID пользователя.", reply_markup=main_menu(user_id))
    
    elif state == 'admin_ban_duration' and user_id == ADMIN_ID:
        try:
            duration = int(text)
            context.user_data['ban_duration'] = duration
            await update.message.reply_text("Укажи причину блокировки:")
            context.user_data['state'] = 'admin_ban_reason'
        except ValueError:
            await update.message.reply_text("Введите корректное число дней.", reply_markup=main_menu(user_id))
    
    elif state == 'admin_ban_reason' and user_id == ADMIN_ID:
        ban_user_id = context.user_data['ban_user_id']
        duration = context.user_data['ban_duration']
        reason = text
        ban_until = int(time.time()) + duration * 86400
        with sqlite3.connect('books.db') as conn:
            c = conn.cursor()
            c.execute("UPDATE users SET banned_until = ?, ban_reason = ? WHERE user_id = ?", (ban_until, reason, ban_user_id))
            conn.commit()
        await update.message.reply_text(f"Пользователь {ban_user_id} заблокирован до {datetime.fromtimestamp(ban_until).strftime('%Y-%m-%d %H:%M:%S')} по причине: {reason}", reply_markup=main_menu(user_id))
        await context.bot.send_message(chat_id=ban_user_id, text=f"Вы заблокированы до {datetime.fromtimestamp(ban_until).strftime('%Y-%m-%d %H:%M:%S')}.\nПричина: {reason}")
        context.user_data['state'] = None

# Показать прочитанное
async def show_read(query, context):
    user_id = query.from_user.id
    logger.info(f"Показ списка прочитанного для {user_id}")
    with sqlite3.connect('books.db') as conn:
        c = conn.cursor()
        c.execute("SELECT b.id, b.title, ur.rating FROM user_read ur JOIN books b ON ur.book_id = b.id WHERE ur.user_id = ?", (user_id,))
        books = c.fetchall()
    
    if books:
        list_text = "Список прочитанного:\n"
        for i, (book_id, title, rating) in enumerate(books, 1):
            list_text += f"{i}. {title} - {rating_to_stars(rating)}\n"
        keyboard = [
            [InlineKeyboardButton("Оценить", callback_data='list_action_rate_read'),
             InlineKeyboardButton("Добавить в избранное", callback_data='list_action_move_read')],
            [InlineKeyboardButton("Удалить", callback_data='list_action_delete_read'),
             InlineKeyboardButton("Главное меню", callback_data='main_menu')]
        ]
        await query.message.reply_text(list_text, reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await query.message.reply_text("Список прочитанного пуст.", reply_markup=main_menu(user_id))

# Показать избранное
async def show_favorites(query, context):
    user_id = query.from_user.id
    logger.info(f"Показ списка избранного для {user_id}")
    with sqlite3.connect('books.db') as conn:
        c = conn.cursor()
        c.execute("SELECT b.id, b.title FROM user_favorites uf JOIN books b ON uf.book_id = b.id WHERE uf.user_id = ?", (user_id,))
        books = c.fetchall()
    
    if books:
        list_text = "Список избранного:\n"
        for i, (book_id, title) in enumerate(books, 1):
            c.execute("SELECT rating FROM user_read WHERE user_id = ? AND book_id = ?", (user_id, book_id))
            rating = c.fetchone()
            rating = rating[0] if rating else None
            list_text += f"{i}. {title} - {rating_to_stars(rating)}\n"
        keyboard = [
            [InlineKeyboardButton("Оценить", callback_data='list_action_rate_favorite'),
             InlineKeyboardButton("Добавить в прочитанное", callback_data='list_action_move_favorite')],
            [InlineKeyboardButton("Удалить", callback_data='list_action_delete_favorite'),
             InlineKeyboardButton("Главное меню", callback_data='main_menu')]
        ]
        await query.message.reply_text(list_text, reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await query.message.reply_text("Список избранного пуст.", reply_markup=main_menu(user_id))

# Ежедневная рекомендация (с учётом часового пояса UTC+3)
async def daily_recommendation(context: ContextTypes.DEFAULT_TYPE):
    with sqlite3.connect('books.db') as conn:
        c = conn.cursor()
        c.execute("SELECT user_id FROM users WHERE agreed = 1 AND (banned_until IS NULL OR banned_until < ?)", (int(time.time()),))
        users = c.fetchall()
    
    for user_id in users:
        user_id = user_id[0]
        with sqlite3.connect('books.db') as conn:
            c = conn.cursor()
            c.execute("SELECT genres FROM books b JOIN user_favorites uf ON b.id = uf.book_id WHERE uf.user_id = ?", (user_id,))
            genres = [g[0] for g in c.fetchall()]
        
        if genres:
            random_genre = random.choice(genres.split(','))
            book = await search_book_by_title_or_genre(random_genre, is_genre=True)
            if book:
                cache_book(book)
                await context.bot.send_photo(
                    chat_id=user_id,
                    photo=book['cover_url'],
                    caption=f"Ежедневная рекомендация:\n**{book['title']}**\n\n{book['description']}\n\nЖанры: {book['genres']}"
                )

def main():
    init_db()
    application = Application.builder().token(os.getenv('TELEGRAM_BOT_TOKEN', '8173510242:AAH0x9rsdU5Fv3aRJhlZ1zF_mdlSTFffHos')).build()
    
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(button))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    application.add_handler(MessageHandler(filters.PHOTO | filters.TEXT, handle_message))
    
    # Установка времени для Москвы (UTC+3)
    moscow_time = time(hour=9, tzinfo=tzoffset(None, 10800))  # 9 утра по Москве
    application.job_queue.run_daily(daily_recommendation, moscow_time)
    
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
