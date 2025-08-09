import asyncio
import logging
from datetime import datetime
import os
import json

import gspread
from google.oauth2.service_account import Credentials
from aiogram import Bot, Dispatcher, F, types
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove

# ==== НАСТРОЙКИ ====
TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
SPREADSHEET_ID = os.getenv("SHEET_ID")

CASHBACK_RATE = 0.03         # 3% начисление
MAX_REDEEM_RATIO = 0.5       # списывать можно до 50% покупки
# ====================

logging.basicConfig(level=logging.INFO)
bot = Bot(
    token=TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher()

def open_sheets():
    sa_info = json.loads(os.getenv("GSERVICE_JSON"))
    creds = Credentials.from_service_account_info(sa_info, scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ])
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)
    try:
        ws_clients = sh.worksheet("Clients")
    except gspread.WorksheetNotFound:
        ws_clients = sh.add_worksheet(title="Clients", rows=1000, cols=10)
        ws_clients.append_row(["ID","ФИО","Телефон","Баланс_тиыны","Дата регистрации"])
    try:
        ws_ops = sh.worksheet("Operations")
    except gspread.WorksheetNotFound:
        ws_ops = sh.add_worksheet(title="Operations", rows=2000, cols=12)
        ws_ops.append_row(["Дата","Тип","UserID","ФИО","Телефон",
                           "Сумма_покупки_тиыны","Сумма_кешбека_тиыны",
                           "Баланс_до_тиыны","Баланс_после_тиыны","Комментарий"])
    return sh, ws_clients, ws_ops

sh, ws_clients, ws_ops = open_sheets()

def parse_amount_to_cents(text: str):
    try:
        return int(round(float(text.replace(",", ".")) * 100))
    except:
        return None

def cents_to_str(cents: int) -> str:
    return f"{cents/100:.2f}"

def get_client_row(user_id: int):
    data = ws_clients.get_all_values()
    for idx, row in enumerate(data[1:], start=2):
        if str(row[0]).strip() == str(user_id):
            return idx, row
    return None, None

def ensure_client(user_id: int, fio: str=None, phone: str=None):
    row_idx, row = get_client_row(user_id)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if row_idx:
        if fio is not None: ws_clients.update_cell(row_idx, 2, fio)
        if phone is not None: ws_clients.update_cell(row_idx, 3, phone)
        return row_idx
    else:
        ws_clients.append_row([str(user_id), fio or "", phone or "", "0", now])
        return ws_clients.row_count

def get_balance_cents(user_id: int) -> int:
    row_idx, row = get_client_row(user_id)
    if not row_idx: return 0
    try: return int(row[3])
    except: return 0

def set_balance_cents(user_id: int, cents: int):
    row_idx, _ = get_client_row(user_id)
    if not row_idx:
        ensure_client(user_id)
        row_idx, _ = get_client_row(user_id)
    ws_clients.update_cell(row_idx, 4, str(cents))

def add_balance_cents(user_id: int, cents: int):
    set_balance_cents(user_id, get_balance_cents(user_id) + cents)

def log_operation(op_type, user_id, purchase_c=None, cashback_c=None, before=None, after=None, comment=""):
    row_idx, row = get_client_row(user_id)
    fio = row[1] if row else ""
    phone = row[2] if row else ""
    ws_ops.append_row([
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        op_type, str(user_id), fio, phone,
        str(purchase_c or 0), str(cashback_c or 0),
        str(before or ""), str(after or ""), comment
    ])

def kb_main(is_admin: bool):
    rows = [
        [KeyboardButton(text="📊 Проверить баланс")],
        [KeyboardButton(text="💳 Списать кешбек")],
        [KeyboardButton(text="📖 Как использовать кешбек")],
        [KeyboardButton(text="ℹ О магазине")]
    ]
    if is_admin:
        rows += [[KeyboardButton(text="➕ Начислить кешбек")],
                 [KeyboardButton(text="🔄 Изменить баланс")],
                 [KeyboardButton(text="📋 Список клиентов")]]
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)

def kb_phone():
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="📱 Отправить номер", request_contact=True)]],
                               resize_keyboard=True)

class Reg(StatesGroup):
    fio = State()
    phone = State()

@dp.message(Command("start"))
async def start_cmd(message: types.Message, state: FSMContext):
    row_idx, row = get_client_row(message.from_user.id)
    is_admin = (message.from_user.id == ADMIN_ID)
    if not row_idx or not row[1] or not row[2]:
        await state.set_state(Reg.fio)
        await message.answer(
            "💫 Добро пожаловать в <b>Home Queen Astana</b>!\n\n"
            "Чтобы начислять кешбек, укажите, пожалуйста, <b>ФИО</b>.",
            reply_markup=ReplyKeyboardRemove()
        )
        return
    await message.answer("✨ Рады видеть вас снова! Выберите действие:",
                         reply_markup=kb_main(is_admin))

@dp.message(Reg.fio)
async def reg_fio(message: types.Message, state: FSMContext):
    fio = message.text.strip()
    if len(fio) < 2:
        await message.answer("Пожалуйста, введите корректное ФИО.")
        return
    await state.update_data(fio=fio)
    ensure_client(message.from_user.id, fio=fio)
    await state.set_state(Reg.phone)
    await message.answer("Отлично! Теперь отправьте, пожалуйста, <b>номер телефона</b> кнопкой ниже.",
                         reply_markup=kb_phone())

@dp.message(Reg.phone, F.contact)
async def reg_phone_contact(message: types.Message, state: FSMContext):
    phone = message.contact.phone_number
    fio = (await state.get_data()).get("fio", "")
    ensure_client(message.from_user.id, fio=fio, phone=phone)
    await state.clear()
    await message.answer("✅ Регистрация завершена!",
                         reply_markup=kb_main(message.from_user.id == ADMIN_ID))

@dp.message(Reg.phone)
async def reg_phone_text(message: types.Message, state: FSMContext):
    phone = message.text.strip()
    fio = (await state.get_data()).get("fio", "")
    ensure_client(message.from_user.id, fio=fio, phone=phone)
    await state.clear()
    await message.answer("✅ Регистрация завершена!",
                         reply_markup=kb_main(message.from_user.id == ADMIN_ID))

@dp.message(F.text == "📊 Проверить баланс")
async def show_balance(message: types.Message):
    ensure_client(message.from_user.id)
    bal = get_balance_cents(message.from_user.id)
    await message.answer(f"💳 Ваш баланс кешбека: <b>{cents_to_str(bal)} ₸</b>")

@dp.message(F.text == "📖 Как использовать кешбек")
async def how_to(message: types.Message):
    await message.answer(
        "📖 <b>Как использовать кешбек</b>\n\n"
        "1️⃣ Делайте покупки в <b>Home Queen Astana</b>.\n"
        "2️⃣ Получайте <b>3%</b> от суммы в виде кешбека.\n"
        "3️⃣ При следующей покупке можно оплатить кешбеком <b>до 50%</b> от чека.\n"
        "4️⃣ Остальное оплачивается наличными или картой.\n\n"
        "✨ Чем больше покупаете — тем больше экономите!"
    )

@dp.message(F.text == "ℹ О магазине")
async def about_shop(message: types.Message):
    await message.answer("🏠 <b>Home Queen Astana</b>\nСтильные товары для вашего дома ✨\n📱 Instagram: @home_queen_astana")

@dp.message(F.text == "💳 Списать кешбек")
async def use_cashback_start(message: types.Message):
    await message.answer("Введите <b>сумму покупки</b> и <b>сколько кешбека списать</b> через пробел.\nПример: <code>10000 2000</code>",
                         reply_markup=ReplyKeyboardRemove())

@dp.message()
async def router(message: types.Message):
    txt = message.text.strip()

    if txt == "➕ Начислить кешбек":
        if message.from_user.id != ADMIN_ID:
            await message.answer("❌ Нет доступа."); return
        await message.answer("Введите: <code>user_id сумма_покупки</code>\nПример: <code>123456789 10000</code>",
                             reply_markup=ReplyKeyboardRemove()); return

    if txt == "🔄 Изменить баланс":
        if message.from_user.id != ADMIN_ID:
            await message.answer("❌ Нет доступа."); return
        await message.answer("Введите: <code>user_id новый_баланс</code> (в тенге)\nПример: <code>123456789 2500</code>",
                             reply_markup=ReplyKeyboardRemove()); return

    if txt == "📋 Список клиентов":
        if message.from_user.id != ADMIN_ID:
            await message.answer("❌ Нет доступа."); return
        count = max(0, len(ws_clients.get_all_values()) - 1)
        await message.answer(f"👥 Клиентов в базе: <b>{count}</b>"); return

    parts = txt.split()

    # Клиент: списание "<purchase> <use>"
    if len(parts) == 2:
        purchase_c = parse_amount_to_cents(parts[0])
        use_c = parse_amount_to_cents(parts[1])
        if purchase_c and use_c:
            uid = message.from_user.id
            ensure_client(uid)
            before = get_balance_cents(uid)
            max_allowed = int(round(purchase_c * MAX_REDEEM_RATIO))
            if use_c > before:
                await message.answer(f"⚠ Недостаточно кешбека. Баланс: <b>{cents_to_str(before)} ₸</b>",
                                     reply_markup=kb_main(uid == ADMIN_ID)); return
            if use_c > max_allowed:
                await message.answer(f"⚠ Можно списать максимум <b>{cents_to_str(max_allowed)} ₸</b> для этой покупки.",
                                     reply_markup=kb_main(uid == ADMIN_ID)); return
            set_balance_cents(uid, before - use_c)
            after = before - use_c
            log_operation("SPEND", uid, purchase_c, use_c, before, after, "Client spend")
            await message.answer(f"✅ Списано <b>{cents_to_str(use_c)} ₸</b>.\nНовый баланс: <b>{cents_to_str(after)} ₸</b>",
                                 reply_markup=kb_main(uid == ADMIN_ID)); return

    # Админ: начисление "user_id сумма_покупки"
    if message.from_user.id == ADMIN_ID and len(parts) == 2 and parts[0].isdigit():
        uid = int(parts[0]); purchase_c = parse_amount_to_cents(parts[1])
        if purchase_c is not None:
            ensure_client(uid)
            before = get_balance_cents(uid)
            cashback_c = int(round(purchase_c * CASHBACK_RATE))
            add_balance_cents(uid, cashback_c)
            after = before + cashback_c
            log_operation("ADD_3_PERCENT", uid, purchase_c, cashback_c, before, after, "Admin add 3%")
            await message.answer(f"✅ Начислено <b>{cents_to_str(cashback_c)} ₸</b> пользователю {uid} "
                                 f"(3% от {cents_to_str(purchase_c)} ₸).\nБаланс: <b>{cents_to_str(after)} ₸</b>",
                                 reply_markup=kb_main(True)); return

    # Админ: установка баланса "user_id новый_баланс"
    if message.from_user.id == ADMIN_ID and len(parts) == 2 and parts[0].isdigit():
        uid = int(parts[0]); new_bal_c = parse_amount_to_cents(parts[1])
        if new_bal_c is not None:
            ensure_client(uid)
            before = get_balance_cents(uid)
            set_balance_cents(uid, new_bal_c)
            log_operation("SET_BALANCE", uid, None, None, before, new_bal_c, "Admin set balance")
            await message.answer(f"🔄 Баланс пользователя {uid} установлен в <b>{cents_to_str(new_bal_c)} ₸</b>.",
                                 reply_markup=kb_main(True)); return

    await message.answer("Выберите действие:", reply_markup=kb_main(message.from_user.id == ADMIN_ID))

async def main():
    logging.info("Bot started.")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
