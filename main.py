import sqlite3
import random
import aiohttp
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes
from datetime import datetime, time
import os
import json

# Инициализация базы данных
def init_db():
    conn = sqlite3.connect('books.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS books 
                 (id TEXT PRIMARY KEY, title TEXT, description TEXT, genres TEXT, cover_url TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS user_read 
                 (user_id INTEGER, book_id TEXT, rating INTEGER)''')
    c.execute('''CREATE TABLE IF NOT EXISTS user_favorites 
                 (user_id INTEGER, book_id TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS users 
                 (user_id INTEGER PRIMARY KEY, username TEXT)''')
    conn.commit()
    conn.close()

# Главное меню с кнопками
def main_menu():
    keyboard = [
        [InlineKeyboardButton("Поиск по жанру", callback_data='search_genre')],
        [InlineKeyboardButton("Добавить в прочитанное", callback_data='add_read'),
         InlineKeyboardButton("Добавить в избранное", callback_data='add_favorite')],
        [InlineKeyboardButton("Оценить книгу", callback_data='rate_book')],
        [InlineKeyboardButton("Мои прочитанные", callback_data='show_read'),
         InlineKeyboardButton("Мои избранные", callback_data='show_favorites')]
    ]
    return InlineKeyboardMarkup(keyboard)

# Команда /start
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    username = update.message.from_user.username
    
    conn = sqlite3.connect('books.db')
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO users (user_id, username) VALUES (?, ?)", (user_id, username))
    conn.commit()
    conn.close()
    
    await update.message.reply_text("Добро пожаловать в бот для книг! Я использую Open Library для поиска книг по всему миру.", reply_markup=main_menu())

# Поиск книги через Open Library API
async def search_book_by_genre(genre):
    async with aiohttp.ClientSession() as session:
        url = f"https://openlibrary.org/subjects/{genre.lower().replace(' ', '_')}.json?limit=1&sort=random"
        async with session.get(url) as response:
            if response.status != 200:
                return None
            data = await response.json()
            if not data.get('works'):
                return None
            
            work = data['works'][0]
            book_id = work['key'].split('/')[-1]  # Например, "OL123W"
            title = work.get('title', 'Нет названия')
            
            # Получаем детальную информацию
            detail_url = f"https://openlibrary.org/works/{book_id}.json"
            async with session.get(detail_url) as detail_response:
                if detail_response.status != 200:
                    return None
                detail_data = await detail_response.json()
                description = detail_data.get('description', 'Нет описания') if isinstance(detail_data.get('description'), str) else 'Нет описания'
                genres = ','.join(work.get('subject', ['Нет жанров']))
                cover_id = work.get('cover_id')
                cover_url = f"https://covers.openlibrary.org/b/id/{cover_id}-L.jpg" if cover_id else "https://via.placeholder.com/150"
                
                return {'id': book_id, 'title': title, 'description': description, 'genres': genres, 'cover_url': cover_url}
    return None

# Кэширование книги в локальной базе
def cache_book(book):
    conn = sqlite3.connect('books.db')
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO books (id, title, description, genres, cover_url) VALUES (?, ?, ?, ?, ?)",
              (book['id'], book['title'], book['description'], book['genres'], book['cover_url']))
    conn.commit()
    conn.close()

# Обработка кнопок
async def button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    if query.data == 'search_genre':
        await query.edit_message_text("Укажи жанр (например, 'Фэнтези'):")
        context.user_data['state'] = 'search_genre'
    elif query.data == 'add_read':
        await query.edit_message_text("Укажи название книги для добавления в прочитанное:")
        context.user_data['state'] = 'add_read'
    elif query.data == 'add_favorite':
        await query.edit_message_text("Укажи название книги для добавления в избранное:")
        context.user_data['state'] = 'add_favorite'
    elif query.data == 'rate_book':
        await query.edit_message_text("Укажи название книги и оценку (например, 'Гарри Поттер 5'):")
        context.user_data['state'] = 'rate_book'
    elif query.data == 'show_read':
        await show_read(query, context)
    elif query.data == 'show_favorites':
        await show_favorites(query, context)

# Обработка текстовых сообщений
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    state = context.user_data.get('state')
    user_id = update.message.from_user.id
    
    conn = sqlite3.connect('books.db')
    c = conn.cursor()
    
    if state == 'search_genre':
        book = await search_book_by_genre(text)
        if book:
            cache_book(book)
            await update.message.reply_photo(
                photo=book['cover_url'],
                caption=f"**{book['title']}**\n\n{book['description']}\n\nЖанры: {book['genres']}",
                reply_markup=main_menu()
            )
        else:
            await update.message.reply_text("Книга не найдена.", reply_markup=main_menu())
    
    elif state == 'add_read':
        c.execute("SELECT id FROM books WHERE title LIKE ?", (f'%{text}%',))
        book = c.fetchone()
        if book:
            c.execute("INSERT INTO user_read (user_id, book_id) VALUES (?, ?)", (user_id, book[0]))
            conn.commit()
            await update.message.reply_text(f"Книга '{text}' добавлена в прочитанное.", reply_markup=main_menu())
        else:
            await update.message.reply_text("Книга не найдена в базе. Попробуй поискать её по жанру сначала.", reply_markup=main_menu())
    
    elif state == 'add_favorite':
        c.execute("SELECT id FROM books WHERE title LIKE ?", (f'%{text}%',))
        book = c.fetchone()
        if book:
            c.execute("INSERT INTO user_favorites (user_id, book_id) VALUES (?, ?)", (user_id, book[0]))
            conn.commit()
            await update.message.reply_text(f"Книга '{text}' добавлена в избранное.", reply_markup=main_menu())
        else:
            await update.message.reply_text("Книга не найдена в базе. Попробуй поискать её по жанру сначала.", reply_markup=main_menu())
    
    elif state == 'rate_book':
        try:
            title, rating = text.rsplit(' ', 1)
            rating = int(rating)
            c.execute("SELECT id FROM books WHERE title LIKE ?", (f'%{title}%',))
            book = c.fetchone()
            if book and 1 <= rating <= 5:
                c.execute("UPDATE user_read SET rating = ? WHERE user_id = ? AND book_id = ?", (rating, user_id, book[0]))
                conn.commit()
                await update.message.reply_text(f"Оценка {rating} для '{title}' сохранена.", reply_markup=main_menu())
            else:
                await update.message.reply_text("Книга не найдена или оценка неверная (1-5).", reply_markup=main_menu())
        except:
            await update.message.reply_text("Формат: 'Название Оценка'.", reply_markup=main_menu())
    
    context.user_data['state'] = None
    conn.close()

# Показать прочитанное
async def show_read(query, context):
    user_id = query.from_user.id
    conn = sqlite3.connect('books.db')
    c = conn.cursor()
    c.execute("SELECT b.title, b.description, b.genres, b.cover_url, ur.rating FROM user_read ur JOIN books b ON ur.book_id = b.id WHERE ur.user_id = ?", (user_id,))
    books = c.fetchall()
    conn.close()
    
    if books:
        for book in books:
            await query.message.reply_photo(
                photo=book[3],
                caption=f"**{book[0]}**\n\n{book[1]}\n\nЖанры: {book[2]}\nОценка: {book[4] or 'Не указана'}"
            )
        await query.edit_message_text("Список выше.", reply_markup=main_menu())
    else:
        await query.edit_message_text("Список прочитанного пуст.", reply_markup=main_menu())

# Показать избранное
async def show_favorites(query, context):
    user_id = query.from_user.id
    conn = sqlite3.connect('books.db')
    c = conn.cursor()
    c.execute("SELECT b.title, b.description, b.genres, b.cover_url FROM user_favorites uf JOIN books b ON uf.book_id = b.id WHERE uf.user_id = ?", (user_id,))
    books = c.fetchall()
    conn.close()
    
    if books:
        for book in books:
            await query.message.reply_photo(
                photo=book[3],
                caption=f"**{book[0]}**\n\n{book[1]}\n\nЖанры: {book[2]}"
            )
        await query.edit_message_text("Список выше.", reply_markup=main_menu())
    else:
        await query.edit_message_text("Список избранного пуст.", reply_markup=main_menu())

# Ежедневная рекомендация
async def daily_recommendation(context: ContextTypes.DEFAULT_TYPE):
    conn = sqlite3.connect('books.db')
    c = conn.cursor()
    c.execute("SELECT user_id FROM users")
    users = c.fetchall()
    
    for user_id in users:
        user_id = user_id[0]
        c.execute("SELECT genres FROM books b JOIN user_favorites uf ON b.id = uf.book_id WHERE uf.user_id = ?", (user_id,))
        genres = [g[0] for g in c.fetchall()]
        
        if genres:
            random_genre = random.choice(genres.split(','))
            book = await search_book_by_genre(random_genre)
            if book:
                cache_book(book)
                await context.bot.send_photo(
                    chat_id=user_id,
                    photo=book['cover_url'],
                    caption=f"Ежедневная рекомендация:\n**{book['title']}**\n\n{book['description']}\n\nЖанры: {book['genres']}"
                )
    conn.close()

def main():
    init_db()
    
    application = Application.builder().token(os.getenv('TELEGRAM_BOT_TOKEN', '8173510242:AAH0x9rsdU5Fv3aRJhlZ1zF_mdlSTFffHos')).build()
    
    # Обработчики
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(button))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    # Ежедневная рекомендация в 9 утра
    application.job_queue.run_daily(daily_recommendation, time=time(hour=9))
    
    application.run_polling()

if __name__ == '__main__':
    main()
