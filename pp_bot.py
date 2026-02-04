import os
import re
import uuid
import html
import logging
from datetime import datetime, timezone, timedelta
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from dotenv import load_dotenv

from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import Image

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaPhoto,
    InputMediaVideo,
    InputMediaDocument,
    InputFile,
)
from telegram.error import Forbidden, BadRequest, TimedOut
from telegram.constants import ChatType
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)
from telegram.request import HTTPXRequest

from pp_states import *

from pp_excel import (
    ensure_workbook,
    add_order,
    add_items,
    generate_order_id,
    update_order_fields,
    update_order_payment,
    update_order_status,
    update_delivery,
    get_order_user_id,
    get_order_assignment,
    get_order_bundle,
    mark_order_forwarded,
    get_trader_profile,
    upsert_trader_profile,
    list_orders,
    list_orders_for_trader,
    compute_admin_financials,
    compute_revenue_breakdown,
    get_setting,
    set_setting,
    append_legal_log,
    list_traders,
    set_trader_enabled,
    is_trader_enabled,
    list_legal_log,
    month_key_utc,
    upsert_trader_subscription,
    get_trader_subscription,
    list_trader_subscriptions,
)

from pp_security import parse_admin_ids


load_dotenv()

BOT_TOKEN = (os.getenv("PP_BOT_TOKEN") or "").strip()
TEAM_CHAT_ID_RAW = (os.getenv("PARTS_TEAM_CHAT_ID") or "").strip()
TEAM_CHAT_ID = int(TEAM_CHAT_ID_RAW) if TEAM_CHAT_ID_RAW.lstrip("-").isdigit() else None

# ✅ مجموعة التجار (لازم البوت يكون عضو فيها)
TRADERS_GROUP_ID_RAW = (os.getenv("PP_TRADERS_GROUP_ID") or "").strip()
TRADERS_GROUP_ID = int(TRADERS_GROUP_ID_RAW) if TRADERS_GROUP_ID_RAW.lstrip("-").isdigit() else None

ADMIN_IDS = parse_admin_ids()
# ===== Backup/Restore (Render-friendly) =====
BACKUP_CHAT_ID_RAW = (os.getenv("PP_BACKUP_CHAT_ID") or "").strip()
PP_BACKUP_CHAT_ID = int(BACKUP_CHAT_ID_RAW) if BACKUP_CHAT_ID_RAW.lstrip("-").isdigit() else None
PP_BACKUP_EVERY_HOURS = int((os.getenv("PP_BACKUP_EVERY_HOURS") or "6").strip() or "6")
PP_BACKUP_MIN_SECONDS = int((os.getenv("PP_BACKUP_MIN_SECONDS") or "600").strip() or "600")

# كلمة مرور الاسترجاع (اختياري). إذا فاضية = بدون كلمة مرور.
PP_RESTORE_PASS = (os.getenv("PP_RESTORE_PASS") or "").strip()
PP_RESTORE_OK_MINUTES = int((os.getenv("PP_RESTORE_OK_MINUTES") or "10").strip() or "10")

PP_BOT_USERNAME = (os.getenv('PP_BOT_USERNAME') or 'ppartsbot').strip().lstrip('@')
PP_BOT_DEEPLINK = f"https://t.me/{PP_BOT_USERNAME}?start=1"

async def _is_trader_group_member(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> bool:
    """
    True فقط إذا كان المستخدم عضو/ادمن/منشئ داخل مجموعة التجار.
    لازم البوت يكون عضو (ويفضل Admin) في مجموعة التجار.
    """
    if not TRADERS_GROUP_ID:
        return False
    try:
        m = await context.bot.get_chat_member(chat_id=TRADERS_GROUP_ID, user_id=int(user_id))
        st = (getattr(m, "status", None) or "").lower()
        return st in ("member", "administrator", "creator")
    except Exception:
        return False

async def _notify_invoice_error(context, order_id: str, stage: str, err: Exception):
    msg = (
        "⚠️ فشل نظام الفواتير الداخلية\n\n"
        f"🧾 رقم الطلب: {order_id}\n"
        f"📍 المرحلة: {stage}\n"
        f"🛑 الخطأ:\n{err}"
    )
    for aid in ADMIN_IDS:
        try:
            await context.bot.send_message(chat_id=aid, text=msg)
        except Exception:
            pass

def _is_maintenance_mode() -> bool:
    try:
        v = (get_setting("maintenance_mode", "off") or "").strip().lower()
        return v in ("on", "1", "yes", "true")
    except Exception:
        return False

# ===== Platform Fee Free Mode (settings) =====
PLATFORM_FEE_FREE_KEY = "platform_fee_free"  # 1/0 in settings sheet

def _is_platform_fee_free_mode() -> bool:
    try:
        v = str(get_setting(PLATFORM_FEE_FREE_KEY) or "").strip().lower()
    except Exception:
        v = ""
    return v in ("1", "true", "yes", "on", "enable", "enabled")

def _set_platform_fee_free_mode(enable: bool) -> None:
    try:
        set_setting(PLATFORM_FEE_FREE_KEY, "1" if enable else "0")
    except Exception:
        pass

def _maintenance_block_text() -> str:
    return (
        "🟧 <b>تنبيه صيانة</b>\n"
        "المنصة حاليا في وضع الصيانة المؤقتة.\n"
        "⛔ تم ايقاف استقبال الطلبات الجديدة وتقديم عروض السعر مؤقتا.\n"
        "يرجى المحاولة لاحقا."
    )

def _trader_is_disabled(tid: int) -> bool:
    try:
        return not bool(is_trader_enabled(int(tid)))
    except Exception as e:
        # ✅ Fail-closed: إذا فشلنا نقرأ الحالة، نعتبره موقوف (أمان للمبيعات)
        try:
            log.exception("TRADER_ENABLE_CHECK_FAILED tid=%s", tid)
        except Exception:
            pass
        return True

async def _deny_disabled_trader_q(q, reason: str = "حساب التاجر موقوف"):
    # 1) تنبيه سريع (Alert)
    try:
        await _alert(q, f"⛔ {reason}")
    except Exception:
        try:
            await q.answer(f"⛔ {reason}", show_alert=True)
        except Exception:
            pass

    # 2) رسالة خاصة واضحة للتاجر + زر مراسلة الإدارة
    try:
        uid = int(getattr(q, "from_user", None).id or 0)
    except Exception:
        uid = 0

    if not uid:
        return

    try:
        bot = q.get_bot()
    except Exception:
        bot = None

    if not bot:
        return

    try:
        await bot.send_message(
            chat_id=uid,
            text=(
                f"{_user_name(q)}\n"
                "⛔ حسابك موقوف مؤقتًا.\n\n"
                "هذا الزر غير متاح لك الآن.\n"
                "راجع لوحة التاجر لمعرفة الحالة، أو تواصل مع المنصة عبر الزر بالأسفل."
            ),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📩 اتصل بالمنصة", callback_data="pp_support_open")],
            ]),
            disable_web_page_preview=True,
        )
    except Exception:
        pass


def _trader_disabled_msg() -> str:
    return "⛔ حسابك موقوف مؤقتًا. راجع لوحة التاجر لمعرفة الحالة، أو تواصل مع الإدارة عبر زر (مراسلة الإدارة)."


def _bot_username(context: ContextTypes.DEFAULT_TYPE = None) -> str:
    # اسم المنصة للروابط (deep-link). يعتمد على PP_BOT_USERNAME من env
    try:
        return (PP_BOT_USERNAME or '').strip().lstrip('@') or 'ppartsbot'
    except Exception:
        return 'ppartsbot'

# ===== UI helpers =====
def _money(v) -> str:
    try:
        s = str(v or "").strip()
        s = re.sub(r"[^0-9.]+", "", s)
        if not s:
            return ""
        f = float(s)
        if f.is_integer():
            return f"{int(f):,} ر.س"
        return f"{f:,.2f} ر.س"
    except Exception:
        return str(v or "").strip()

def _trader_label(uid: int, fallback_name: str = "") -> str:
    try:
        tp = get_trader_profile(int(uid or 0)) or {}
    except Exception:
        tp = {}
    dn = (tp.get("display_name") or "").strip()
    cn = (tp.get("company_name") or "").strip()
    if not dn:
        dn = (fallback_name or "").strip() or "التاجر"
    if cn:
        return f"{dn} ({cn})"
    return dn

def _trade_payment_block(tp: dict) -> str:
    bank = (tp.get("bank_name") or "").strip()
    iban = (tp.get("iban") or "").strip()
    stc = (tp.get("stc_pay") or "").strip()

    if not bank and not iban and not stc:
        return "غير مضافة بعد"

    parts = []
    if bank:
        parts.append(f"🏦 البنك: {bank}")
    if iban:
        parts.append(f"💳 IBAN: {iban}")
    if stc:
        parts.append(f"📱 STC Pay: {stc}")
    return "\n".join(parts)

async def ui_close_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await _alert(q, "")
    try:
        await q.message.delete()
    except Exception:
        # fallback: لو ما قدر يحذف، نخليه يعدّل
        try:
            await q.message.edit_text("✅ تم الإغلاق")
        except Exception:
            pass

# Manual payment info (required)
PP_BANK_NAME = (os.getenv("PP_BANK_NAME") or "").strip()
PP_BENEFICIARY = (os.getenv("PP_BENEFICIARY") or "").strip()
PP_IBAN = (os.getenv("PP_IBAN") or "").strip()
PP_STC_PAY = (os.getenv("PP_STC_PAY") or "").strip()
# optional
PP_PAY_LINK_URL = (os.getenv("PP_PAY_LINK_URL") or "").strip()

PP_SUPPORT_LABEL = (os.getenv("PP_SUPPORT_LABEL") or "الإدارة").strip()
PP_TRADER_LABEL  = (os.getenv("PP_TRADER_LABEL")  or "التاجر").strip()


MAX_ITEMS = 30

# ===== منصة الدعم المباشر (أمر سلاش فقط) =====
# خمول: 10 دقائق / حد أقصى: 60 دقيقة
SUPPORT_IDLE_SECONDS = 10 * 60
SUPPORT_MAX_SECONDS  = 60 * 60
STAGE_SUPPORT_ADMIN_REPLY = "pp_support_admin_reply"

STAGE_ADMIN_TRADER_MSG = "pp_admin_trader_msg"

VIN_RE = re.compile(r"^[A-HJ-NPR-Z0-9]{17}$")  # 17 chars, excludes I O Q

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO
)
log = logging.getLogger("PP")


# ===== Event Logger (Audit / Trace) =====
def log_event(order_id, event, **kwargs):
    """
    يسجل الاحداث المهمة (تغيير حالة، دفع، شحن، الخ)
    بدون التأثير على منطق البوت او ايقافه عند الخطأ
    """
    try:
        log.info(
            "EVENT %s | order=%s | %s",
            event,
            order_id,
            kwargs,
        )
    except Exception:
        pass

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def set_stage(context: ContextTypes.DEFAULT_TYPE, user_id: int, stage: str):
    ud = context.user_data.setdefault(user_id, {})
    ud[ACTION_KEY] = ACTION_PAID_PARTS
    ud[STAGE_KEY] = stage

def get_ud(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> dict:
    return context.user_data.setdefault(user_id, {})

def reset_flow(context: ContextTypes.DEFAULT_TYPE, user_id: int):
    context.user_data.setdefault(user_id, {}).clear()

def _support_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔒 إنهاء", callback_data="pp_support_close")]])

def _support_is_open(ud: dict) -> bool:
    return bool(ud.get("support_open"))

def _support_touch(ud: dict):
    now = utc_now_iso()
    ud["support_last_at_utc"] = now
    ud.setdefault("support_started_at_utc", now)

def _support_should_close_by_time(ud: dict) -> bool:
    """True if support chat should auto-close (idle or max duration)."""
    if not _support_is_open(ud):
        return False
    try:
        now = datetime.now(timezone.utc)
        started = datetime.fromisoformat((ud.get("support_started_at_utc") or "").replace("Z", "+00:00"))
        last = datetime.fromisoformat((ud.get("support_last_at_utc") or ud.get("support_started_at_utc") or "").replace("Z", "+00:00"))
        if (now - last).total_seconds() >= SUPPORT_IDLE_SECONDS:
            return True
        if (now - started).total_seconds() >= SUPPORT_MAX_SECONDS:
            return True
        return False
    except Exception:
        # إذا تعذر قراءة التوقيتات نقفل احتياطياً
        return True

async def _support_close(update_or_q, context: ContextTypes.DEFAULT_TYPE, user_id: int, reason: str = ""):
    ud = get_ud(context, user_id)
    ud.pop("support_open", None)
    ud.pop("support_started_at_utc", None)
    ud.pop("support_last_at_utc", None)
    try:
        txt = "✅ تم إغلاق قناة التواصل مع الإدارة"
        if reason:
            txt += f"\n{reason}"
        if hasattr(update_or_q, "callback_query") and update_or_q.callback_query:
            q = update_or_q.callback_query
            try:
                await q.answer("تم الإغلاق")
            except Exception:
                pass
            try:
                await q.message.reply_text(txt)
            except Exception:
                pass
        elif hasattr(update_or_q, "message") and update_or_q.message:
            await update_or_q.message.reply_text(txt)
        else:
            await context.bot.send_message(chat_id=user_id, text=txt)
    except Exception:
        pass

def price_for_count(c: int) -> int:
    """رسوم المنصة حسب عدد القطع (غير الاستهلاكية)."""

    # ✅ عرض مجاني للمنصة: رسوم المنصة = 0
    if _is_platform_fee_free_mode():
        return 0

    if c <= 0:
        return 0

    return 25 if c <= 5 else 39

# ===== مساعدات تنسيق رسائل الإدارة (بدون تشوه بصري) =====
STAGE_ADMIN_SEND_PAYLINK = "admin_send_paylink"

def _trim_caption(s: str, limit: int = 950) -> str:
    s = (s or "").strip()
    if len(s) <= limit:
        return s
    return s[: max(0, limit-1)].rstrip() + "…"

def _build_admin_order_caption(order_id: str, ud: dict, order: dict, title: str, extra_lines=None) -> str:
    extra_lines = extra_lines or []
    user_name = (ud.get("user_name") or order.get("user_name") or "").strip()
    car = (ud.get("car_name") or order.get("car_name") or "").strip()
    model = (ud.get("car_model") or order.get("car_model") or "").strip()
    vin = (ud.get("vin") or order.get("vin") or "").strip()
    fee = ud.get("price_sar", order.get("price_sar", ""))
    ship_method = (ud.get("ship_method") or order.get("ship_method") or "").strip()
    delivery_details = (ud.get("delivery_details") or order.get("delivery_details") or "").strip()

    # ✅ إضافة: قراءة ملاحظات العميل (fallback: ud -> order)
    notes = (ud.get("notes") or order.get("notes") or "").strip()

    parts = []
    try:
        b = get_order_bundle(order_id)
        items = b.get("items", []) or []
        for i, it in enumerate(items, start=1):
            nm = (it.get("name") or "").strip()
            pn = (it.get("part_no") or it.get("item_part_no") or "").strip()
            if not nm:
                continue
            parts.append(f"{i}- {nm}" + (f" ({pn})" if pn else ""))
            if len(parts) >= 6:
                break
    except Exception:
        parts = []
    parts_txt = "\n".join(parts) if parts else "—"

    lines = [title, f"🧾 رقم الطلب: {order_id}"]
    if user_name:
        lines.append(f"👤 العميل: {user_name}")
    if car or model:
        lines.append(f"🚗 السيارة: {(car + ' ' + model).strip()}")
    if vin:
        lines.append(f"🔎 VIN: {vin}")
    if str(fee).strip() not in ("", "0", "0.0"):
        lines.append(f"💰 رسوم المنصة: {fee} ريال")

    # ✅ إضافة: إظهار الملاحظات في رسالة المجموعة
    if notes:
        lines += ["", "📝 ملاحظات العميل:", notes]

    lines.extend(extra_lines)
    lines += ["", "🧩 القطع:", parts_txt]

    if ship_method or delivery_details:
        lines += ["", "📦 طريقة التسليم:"]
        if ship_method:
            lines.append(ship_method)
        if delivery_details:
            lines += ["", "📍 تفاصيل التسليم:", delivery_details]

    return _trim_caption("\n".join(lines))

# ✅ MUST be defined BEFORE _is_consumable_part()
_CONSUMABLE_KEYWORDS = [
    # Arabic
    "زيت", "زيوت", "فلتر", "فلاتر", "سيفون",
    "بوجي", "بواجي", "شمعة اشعال", "شمعات اشعال",
    "سير", "سيور",
    "سائل", "سوائل",
    "فحمات", "فحمات اشعال", "فحمات إشعال",
    "صرة", "صره", "صوفة", "صوفه", "جاسكيت",
    # English
    "oil", "filter", "filters", "spark plug", "spark plugs", "plug", "plugs",
    "belt", "belts",
    "fluid", "fluids", "coolant",
    "gasket",
    "brake pad", "brake pads", "pads",
]


def _is_consumable_part(name: str) -> bool:
    s = (name or "").strip().lower()
    if not s:
        return False
    # توحيد بسيط
    s = re.sub(r"\s+", " ", s)
    # بحث احتوائي (يشمل مفرد/جمع وتنوعات بسيطة)
    return any(k in s for k in _CONSUMABLE_KEYWORDS)


def _platform_fee_for_items(items: list[dict]) -> tuple[int, int, int]:
    """Returns (fee_sar, non_consumable_count, consumable_count)."""
    if not items:
        return 0, 0, 0
    c_cons = 0
    c_non = 0
    for it in items:
        nm = (it.get("name") or "").strip()
        if _is_consumable_part(nm):
            c_cons += 1
        else:
            c_non += 1
    fee = 0 if (c_non == 0 and c_cons > 0) else price_for_count(c_non)
    return fee, c_non, c_cons

def main_menu_kb():
    # تم تعطيل زر طلب جديد لمنع التداخل. بدء الطلب يكون بكتابة pp فقط.
    return None

def more_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ انهاء وارسال للدفع", callback_data="pp_more_no")],
        [InlineKeyboardButton("✖️ الغاء الطلب", callback_data="pp_cancel")],
    ])

def photo_prompt_kb():
    # زر انهاء يظهر دائما حتى لو العميل ما رفع صورة
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ انهاء وارسال للدفع", callback_data="pp_more_no")],
        [InlineKeyboardButton("✖️ الغاء الطلب", callback_data="pp_cancel")],
    ])

def partno_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⏭️ تخطي", callback_data="pp_partno_skip")],
        [InlineKeyboardButton("✖️ الغاء الطلب", callback_data="pp_cancel")],
    ])

def prepay_notes_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⏭️ تخطي والانتقال للدفع", callback_data="pp_prepay_notes_skip")],
        [InlineKeyboardButton("✖️ الغاء الطلب", callback_data="pp_cancel")],
    ])

def build_order_preview(ud: dict) -> str:
    # مهم: لازم يكون عندك import html أعلى الملف
    # import html

    def esc(x) -> str:
        # يمنع اختفاء الرقم 0
        return html.escape("" if x is None else str(x), quote=False)

    order_id = esc(ud.get("order_id", ""))
    uname = esc(ud.get("user_name", ""))
    car = esc(ud.get("car_name", ""))
    model = esc(ud.get("car_model", ""))
    vin = esc(ud.get("vin", ""))
    notes_raw = _norm(ud.get("notes", ""))
    notes = esc(notes_raw)

    items = ud.get("items", []) or []

    # رسوم المنصة (تظهر 0 دائمًا لو ما فيه رسوم)
    price = ud.get("price_sar", 0)
    if price is None or str(price).strip() == "":
        price = 0

    lines = []

    lines.append(f"🧾 <b>معاينة الطلب</b> <i>#{order_id}</i>")
    lines.append(f"👤 <b>العميل</b>: <i>{uname}</i>")
    lines.append(f"🚗 <b>السيارة</b>: <i>{car}</i>")
    lines.append(f"📌 <b>الموديل</b>: <i>{model}</i>")
    lines.append(f"🔎 <b>VIN</b>: <i>{vin}</i>")
    lines.append(f"📝 <b>الملاحظات</b>: <i>{notes if notes else 'لا يوجد'}</i>")
    lines.append("")

    lines.append(f"🧩 <b>القطع المطلوبة</b> <i>({len(items)})</i>:")
    for i, it in enumerate(items, start=1):
        nm = esc((it.get("name") or "").strip())
        pn = esc((it.get("part_no") or "").strip())
        if nm:
            if pn:
                lines.append(f"  🔹 <b>{i}</b>- <i>{nm}</i> <b>رقم</b>: <code>{pn}</code>")
            else:
                lines.append(f"  🔹 <b>{i}</b>- <i>{nm}</i>")

    lines.append("")
    lines.append(f"💰 <b>رسوم المنصة</b>: <i>{esc(price)} ريال</i>")

    if str(price) == "0":
        lines.append("✅ <i>لا توجد رسوم منصة على هذا الطلب</i>")

    return "\n".join(lines)

def pay_method_kb():
    rows = [
        [InlineKeyboardButton("🏦 تحويل بنكي", callback_data="pp_pay_bank")],
        [InlineKeyboardButton("📱 STC Pay", callback_data="pp_pay_stc")],
        [InlineKeyboardButton("🔗 رابط دفع سريع", callback_data="pp_pay_link")],
        [InlineKeyboardButton("✖️  الغاء الطلب", callback_data="pp_cancel")],
    ]
    return InlineKeyboardMarkup(rows)

# === Structured Quote Engine (Trader Private Wizard) ===

def trader_quote_start_kb(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🚀 بدء بناء عرض السعر", callback_data=f"ppq_begin|{order_id}")],
    ])


# --- UI helper: make single-column buttons look consistently wide ---
_WIDE_FILL = "\u2800"  # braille blank (renders as a visible width placeholder)

def _wide_btn_label(s: str, target: int = 22) -> str:
    s = "" if s is None else str(s)
    # Pad with braille blanks to make rows feel equally wide in Telegram.
    pad = max(0, int(target) - len(s))
    return s + (_WIDE_FILL * pad)

def trader_quote_type_kb(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(_wide_btn_label("✔️ وكالة"), callback_data=f"ppq_type|{order_id}|agency")],
        [InlineKeyboardButton(_wide_btn_label("✔️ وكلاء محليين"), callback_data=f"ppq_type|{order_id}|local_dealers")],
        [InlineKeyboardButton(_wide_btn_label("✔️ تجاري"), callback_data=f"ppq_type|{order_id}|aftermarket")],
        [InlineKeyboardButton(_wide_btn_label("✔️ مختلط"), callback_data=f"ppq_type|{order_id}|mixed")],
    ])

def trader_quote_shipping_method_kb(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(_wide_btn_label("🚚 محلي"), callback_data=f"ppq_ship|{order_id}|local")],
        [InlineKeyboardButton(_wide_btn_label("✈️ دولي"), callback_data=f"ppq_ship|{order_id}|intl")],
    ])

def trader_quote_shipping_included_kb(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(_wide_btn_label("✅ السعر يشمل الشحن"), callback_data=f"ppq_shipinc|{order_id}|yes")],
        [InlineKeyboardButton(_wide_btn_label("❌ الشحن غير مشمول"), callback_data=f"ppq_shipinc|{order_id}|no")],
    ])

def trader_quote_eta_kb(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(_wide_btn_label("⏱ 1-2 يوم"), callback_data=f"ppq_eta|{order_id}|1-2")],
        [InlineKeyboardButton(_wide_btn_label("⏱ 3-5 ايام"), callback_data=f"ppq_eta|{order_id}|3-5")],
        [InlineKeyboardButton(_wide_btn_label("⏱ 7-14 يوم"), callback_data=f"ppq_eta|{order_id}|7-14")],
        [InlineKeyboardButton(_wide_btn_label("✍️ مدة اخرى"), callback_data=f"ppq_eta|{order_id}|custom")],
    ])

def trader_quote_availability_kb(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(_wide_btn_label("⏱ 1-2 يوم"), callback_data=f"ppq_avail|{order_id}|1-2")],
        [InlineKeyboardButton(_wide_btn_label("⏱ 3-5 ايام"), callback_data=f"ppq_avail|{order_id}|3-5")],
        [InlineKeyboardButton(_wide_btn_label("⏱ 7-14 يوم"), callback_data=f"ppq_avail|{order_id}|7-14")],
        [InlineKeyboardButton(_wide_btn_label("✍️ مدة اخرى"), callback_data=f"ppq_avail|{order_id}|custom")],
    ])

def _ppq_type_label(v: str) -> str:
    return {
        "agency": "وكالة",
        "local_dealers": "وكلاء محليين",
        "aftermarket": "تجاري",
        "mixed": "مختلط",
    }.get(v, "غير محدد")

def _ppq_ship_label(v: str) -> str:
    return {"local": "محلي", "intl": "دولي"}.get(v, "غير محدد")

def build_legal_shipping_block(method: str, fee_sar: str, eta: str, included: str) -> str:
    # صيغة موحدة قانونيا يعاد استخدامها (بدون اسم شركة الشحن)
    inc = "مشمولة" if included == "yes" else "غير مشمولة"
    fee_txt = str(fee_sar or "").strip()
    if not fee_txt:
        fee_txt = "0" if included == "yes" else "25"
    return (
        "🚚 الشحن:\n"
        f"طريقة الشحن: {_ppq_ship_label(method)}\n"
        f"مدة الشحن: {eta}\n"
        f"تكلفة الشحن: {inc}\n"
        f"قيمة الشحن: {fee_txt} ر.س"
    )

def build_official_quote_text(order_id: str, goods_amount_sar: str, parts_type: str, ship_block: str, availability: str) -> str:
    return (
        "💰 عرض سعر رسمي\n"
        f"رقم الطلب: {order_id}\n\n"
        f"مبلغ القطع: {goods_amount_sar} ريال\n\n"
        "🔧 نوع القطع:\n"
        f"✔️ {_ppq_type_label(parts_type)}\n\n"
        f"{ship_block}\n\n"
        f"⏳ مدة التجهيز: {availability}\n\n"
        "يرجى مراجعة العرض ثم اختيار القرار من الازرار بالاسفل في حالة قبول العرض سيتم فتح قناة اتصال داخلي بين التاجر والعميل"
    )

def quote_client_kb(order_id: str, trader_id: int) -> InlineKeyboardMarkup:
    tid = int(trader_id or 0)
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "✅ أقبل العرض",
                callback_data=f"pp_quote_ok|{order_id}|{tid}"
            ),
        ],
        [
            InlineKeyboardButton(
                "❌ أرفض العرض",
                callback_data=f"pp_quote_no|{order_id}|{tid}"
            ),
        ],
    ])

def trader_status_kb(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🟡 جاري تجهيز الطلب", callback_data=f"pp_trader_status|prep|{order_id}")],
        [InlineKeyboardButton("🟢 الطلب جاهز للشحن", callback_data=f"pp_trader_status|ready|{order_id}")],
        [InlineKeyboardButton("🚚 تم شحن الطلب", callback_data=f"pp_trader_status|shipped|{order_id}")],
        [InlineKeyboardButton("✅ تم تسليم الطلب للعميل بنجاح", callback_data=f"pp_trader_status|delivered|{order_id}")],
        [InlineKeyboardButton("💬 مراسلة العميل داخل المنصة", callback_data=f"pp_chat_open|{order_id}")],
    ])

def pay_goods_method_kb(order_id: str) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("🏦 تحويل بنكي", callback_data=f"pp_goods_pay_bank|{order_id}")],
        [InlineKeyboardButton("📱 STC Pay", callback_data=f"pp_goods_pay_stc|{order_id}")],
        [InlineKeyboardButton("🔗 رابط الدفع", callback_data=f"pp_goods_pay_link|{order_id}")],
        [InlineKeyboardButton("💬 مراسلة التاجر", callback_data=f"pp_chat_trader|{order_id}")],
    ]
    return InlineKeyboardMarkup(rows)

def team_goods_confirm_kb(order_id: str):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ تأكيد استلام قيمة القطع", callback_data=f"pp_team_goods_confirm|{order_id}")]
    ])

def trader_goods_receipt_kb(order_id: str, user_id: int) -> InlineKeyboardMarkup:
    # للتاجر: تأكيد استلام قيمة القطع + مراسلة العميل (بعد الدفع)
    uid = int(user_id or 0)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ تأكيد استلام قيمة القطع", callback_data=f"pp_team_goods_confirm|{order_id}")],
        [InlineKeyboardButton("💬 مراسلة العميل داخل المنصة", callback_data=f"pp_trader_reply|{order_id}|{uid}")],
        [InlineKeyboardButton("✖️ إغلاق", callback_data="pp_ui_close")],
    ])

def admin_free_order_kb(order_id: str, client_id: int) -> InlineKeyboardMarkup:
    oid = (order_id or "").strip()
    uid = int(client_id or 0)

    rows = []
    if oid and uid:
        # ✅ نفس نظام رد الإدارة الموجود عندك
        rows.append([InlineKeyboardButton("💬 مراسلة العميل", callback_data=f"pp_admin_reply|{oid}|{uid}")])

    if oid:
        rows.append([InlineKeyboardButton("⛔ إلغاء الطلب", callback_data=f"pp_admin_cancel|{oid}")])

    rows.append([InlineKeyboardButton("✖️ إغلاق", callback_data="pp_ui_close")])
    return InlineKeyboardMarkup(rows)

def bank_info_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 نسخ IBAN", callback_data="pp_copy_iban")],
        [InlineKeyboardButton("❌  الغاء الطلب", callback_data="pp_cancel")],
    ])

def stc_info_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 نسخ رقم STC Pay", callback_data="pp_copy_stc")],
        [InlineKeyboardButton("❌  الغاء الطلب", callback_data="pp_cancel")],
    ])

def delivery_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🚚 شحن", callback_data="pp_delivery_ship")],
        [InlineKeyboardButton("📍 استلام من الموقع", callback_data="pp_delivery_pickup")],
    ])

def track_kb(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔎 مراسلة المنصة", callback_data=f"pp_track|{order_id}")],
    ])

def admin_reply_kb(order_id: str, user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✍️ رد كالإدارة", callback_data=f"pp_admin_reply|{order_id}|{user_id}")],
    ])

def admin_reply_done_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ انهاء الرد", callback_data="pp_admin_reply_done")],
    ])

from io import BytesIO
try:
    from reportlab.lib.pagesizes import A4
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    from reportlab.pdfgen import canvas
    REPORTLAB_AVAILABLE = True
except Exception:
    REPORTLAB_AVAILABLE = False


async def send_platform_invoice_pdf(
    context: ContextTypes.DEFAULT_TYPE,
    order_id: str,
    kind: str = "preliminary",
    tracking_number: str = "",
    admin_only: bool = False,
):
    # فاتورة المنصة: رسوم المنصة فقط + كل بيانات العميل/الطلب
    return await send_invoice_pdf(
        context=context,
        order_id=order_id,
        kind=kind,
        tracking_number=tracking_number,
        admin_only=admin_only,
        invoice_for="platform",
    )


async def send_trader_invoice_pdf(
    context: ContextTypes.DEFAULT_TYPE,
    order_id: str,
    kind: str = "preliminary",
    tracking_number: str = "",
    admin_only: bool = False,
):
    # فاتورة التاجر: قيمة القطع + الشحن فقط (بدون رسوم المنصة)
    return await send_invoice_pdf(
        context=context,
        order_id=order_id,
        kind=kind,
        tracking_number=tracking_number,
        admin_only=admin_only,
        invoice_for="trader",
    )
    
    
async def send_invoice_pdf(
    context: ContextTypes.DEFAULT_TYPE,
    order_id: str,
    kind: str = "preliminary",
    tracking_number: str = "",
    admin_only: bool = False,
    invoice_for: str = "platform",   # "platform" or "trader"
    debug: bool = False,
):
    """
    Compact one-page Arabic invoice (Platform/Trader) with:
    ✅ اسم المنصة داخل الصف الملون: منصة قطع غيار PARTS / فاتورة داخلية (عند admin_only)
    ✅ تقسيم البيانات: (معلومات العميل) / (معلومات السيارة) / (تفاصيل الشحن)
    ✅ KV عربي مثل تفاصيل القطع: المعرّف يمين والمعلومة يساره (عمودين واضحين)
    ✅ جدول القطع RTL: # أقصى اليمين + أعمدة منفصلة (اسم القطعة / رقم القطعة)
    ✅ ختم مدفوع احترافي ثابت أسفل الصفحة (مرة واحدة) + "الخدمات المساندة GO" تحته
    ✅ العلامة المائية خلف المحتوى ومرفوعة للأعلى وتظهر (بدون ما تغطيها خلفيات بيضاء)
    ✅ رسوم الشحن ثابتة 25 ريال
    ✅ ألوان مختلفة (المنصة أزرق / التاجر أخضر)
    """

    # ✅ tempfile
    try:
        import tempfile
    except Exception as e:
        await _notify_invoice_error(context, order_id, "تهيئة (tempfile)", e)
        return

    import os, html, uuid, re
    from datetime import datetime, timezone, timedelta

    # --- Arabic RTL + shaping ---
    try:
        import arabic_reshaper
        from bidi.algorithm import get_display

        def _ar(s: str) -> str:
            s = "" if s is None else str(s)
            if not s:
                return s
            try:
                return get_display(arabic_reshaper.reshape(s))
            except Exception:
                return s
    except Exception:
        def _ar(s: str) -> str:
            return "" if s is None else str(s)

    # ✅ reportlab imports
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.units import cm
        from reportlab.lib import colors
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Spacer, Paragraph, Image as RLImage
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.enums import TA_RIGHT, TA_CENTER, TA_LEFT
        from reportlab.pdfbase import pdfmetrics
        from reportlab.pdfbase.ttfonts import TTFont
    except Exception as e:
        await _notify_invoice_error(context, order_id, "استيراد مكتبات PDF (reportlab)", e)
        return

    # 1) اقرأ الطلب
    try:
        b = get_order_bundle(order_id)
        order = b.get("order", {}) or {}
        items = b.get("items", []) or []
    except Exception as e:
        await _notify_invoice_error(context, order_id, "قراءة بيانات الطلب من الإكسل", e)
        return

    invoice_for_norm = (str(invoice_for or "platform").strip().lower())
    if invoice_for_norm not in ("platform", "trader"):
        invoice_for_norm = "platform"

    # ---------------- Helpers ----------------
    def _s(x: object) -> str:
        return ("" if x is None else str(x)).strip()

    def _to_float(x: object) -> float:
        try:
            return float(str(x or 0).replace(",", "").strip() or 0)
        except Exception:
            return 0.0

    def _money_safe(x: object, fb: str = "0") -> str:
        try:
            s = _money(x)
            s = (s or "").strip()
            return s if s else fb
        except Exception:
            return fb

    def _pay_status_ar(x: object) -> str:
        v = _s(x).strip().lower()
        if not v:
            return ""
        mp = {
            "paid": "مدفوع",
            "confirmed": "مؤكد",
            "success": "ناجح",
            "successful": "ناجح",
            "done": "مكتمل",
            "ok": "مؤكد",
            "pending": "بانتظار الدفع",
            "payment_pending": "بانتظار الدفع",
            "awaiting_confirm": "بانتظار التحقق",
            "awaiting_confirmation": "بانتظار التحقق",
            "unpaid": "غير مدفوع",
            "failed": "فشل",
            "canceled": "ملغي",
            "cancelled": "ملغي",
        }
        # عربي جاهز؟
        if any(ch in v for ch in "ابتثجحخدذرزسشصضطظعغفقكلمنهوي"):
            return _s(x)
        return mp.get(v, _s(x))

    def _extract_phone(txt: str) -> str:
        t = _s(txt)
        if not t:
            return ""
        m = re.search(r'(\+?9665\d{8}|9665\d{8}|05\d{8})', t)
        return m.group(1) if m else ""

    def _cell_clip(s: str, max_chars: int = 120) -> str:
        s = _s(s)
        s = re.sub(r"\s+", " ", s).strip()
        if len(s) <= max_chars:
            return s
        return s[: max(0, max_chars - 1)].rstrip() + "…"

    # ✅ (02) تنسيق المبلغ: رقم + ﷼ (بدل ر.س/س.ر)
    def _money_tail(x: object, fb: str = "0") -> str:
        s = _money_safe(x, fb=fb)
        s = _s(s)
        s = re.sub(r'^\s*(ر\.?\s*س|ر\.س|SAR|SR|s\.r|s\.r\.?)\s*', '', s, flags=re.I)
        s = re.sub(r'\s*(ر\.?\s*س|ر\.س|SAR|SR|s\.r|s\.r\.?)\s*$', '', s, flags=re.I)
        s = s.strip() or fb
        return f"{s} ﷼"

    # ---------------- IDs / dates ----------------
    client_id = int(order.get("user_id") or 0) if _s(order.get("user_id")).isdigit() else 0
    trader_id = int(order.get("accepted_trader_id") or 0) if _s(order.get("accepted_trader_id")).isdigit() else 0

    now_dt = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=3)))
    inv_date = now_dt.strftime("%Y-%m-%d")
    inv_time = now_dt.strftime("%H:%M")

    kind_norm = (kind or "preliminary").strip().lower()
    if kind_norm not in ("preliminary", "shipping"):
        kind_norm = "preliminary"

    def _get_existing_inv():
        if kind_norm == "preliminary":
            return _s(order.get("invoice_pre_no"))
        if kind_norm == "shipping":
            return _s(order.get("invoice_ship_no"))
        return ""

    inv_no = _get_existing_inv()
    if not inv_no:
        inv_no = f"PP-{order_id}-{kind_norm.upper()}-{now_dt.strftime('%Y%m%d')}-{uuid.uuid4().hex[:4].upper()}"

    # خزّن رقم الفاتورة (مرة واحدة)
    try:
        if kind_norm == "preliminary" and not _s(order.get("invoice_pre_no")):
            update_order_fields(order_id, {"invoice_pre_no": inv_no})
        if kind_norm == "shipping" and not _s(order.get("invoice_ship_no")):
            update_order_fields(order_id, {"invoice_ship_no": inv_no})
    except Exception:
        pass

    # ---------------- Anti-duplicate send (Excel flags) ----------------
    if invoice_for_norm == "trader":
        sent_flag_field = "invoice_trader_pre_pdf_sent" if kind_norm == "preliminary" else "invoice_trader_ship_pdf_sent"
        legacy_flag = "invoice_trader_pdf_sent"
    else:
        sent_flag_field = "invoice_platform_pre_pdf_sent" if kind_norm == "preliminary" else "invoice_platform_ship_pdf_sent"
        legacy_flag = "invoice_platform_pdf_sent"

    def _is_yes(v) -> bool:
        return _s(v).strip().lower() in {"1", "yes", "true", "sent", "done"}

    if (_is_yes(order.get(sent_flag_field)) or _is_yes(order.get(legacy_flag))) and (not debug):
        return

    # ---------------- Data ----------------
    client_name = _s(order.get("user_name")) or "—"

    client_phone = _s(order.get("ship_phone") or order.get("pickup_phone"))
    if not client_phone:
        client_phone = _extract_phone(_s(order.get("delivery_details")))
    if not client_phone:
        client_phone = _extract_phone(_s(order.get("address_text")))
    if not client_phone:
        client_phone = _extract_phone(_s(order.get("full_address")))
    if not client_phone:
        client_phone = _extract_phone(_s(order.get("address")))
    if not client_phone:
        client_phone = "—"

    car_name = _s(order.get("car_name")) or "—"
    car_model = _s(order.get("car_model")) or "—"
    vin = _s(order.get("vin")) or "—"

    trader_name = _s(order.get("accepted_trader_name") or order.get("quoted_trader_name"))
    if not trader_name and trader_id:
        try:
            tp = get_trader_profile(int(trader_id)) or {}
            trader_name = _s(tp.get("display_name")) or _s(tp.get("company_name"))
        except Exception:
            trader_name = ""
    trader_name = trader_name or "—"

    ship_method = _s(order.get("delivery_type") or order.get("ship_method") or order.get("delivery_choice")) or "—"
    ship_city = _s(order.get("ship_city") or order.get("pickup_city"))
    ship_district = _s(order.get("ship_district"))
    ship_short = _s(order.get("ship_short_address"))
    delivery_blob = _s(order.get("delivery_details") or order.get("address_text") or order.get("full_address") or order.get("address"))
    delivery_details = _s(delivery_blob)

    raw_platform_fee = order.get("price_sar")
    raw_goods_amount = order.get("goods_amount_sar")

    # ✅ رسوم الشحن: قيمة متغيرة من الاكسل (shipping_fee_sar)
    # - إذا الشحن مشمول => الافتراضي 0
    # - إذا غير مشمول ولم تُحدد قيمة => الافتراضي 25
    ship_included = str(order.get("ship_included") or "").strip().lower()
    raw_shipping_fee = order.get("shipping_fee_sar")
    if raw_shipping_fee is None or str(raw_shipping_fee).strip() == "":
        raw_shipping_fee = 0 if ship_included in ("yes", "true", "1", "included") else 25

    # ✅ رسوم المنصة: إذا لم تُسجل قبل الطباعة نحسبها من القطع ونحفظها (مرة واحدة)
    try:
        if invoice_for_norm != "trader":
            pf = raw_platform_fee
            pf_f = _to_float(pf) if pf not in (None, "") else 0.0
            if pf_f <= 0:
                auto_fee = _platform_fee_for_items(items)
                if auto_fee and _to_float(auto_fee) > 0:
                    raw_platform_fee = auto_fee
                    try:
                        update_order_fields(order_id, {"price_sar": auto_fee})
                    except Exception:
                        pass
    except Exception:
        pass

    platform_fee = _money_safe(raw_platform_fee or 0, fb="0")
    goods_amount = _money_safe(raw_goods_amount or 0, fb="0")

    if invoice_for_norm == "trader":
        pay_method = _s(order.get("goods_payment_method")) or _s(order.get("payment_method"))
        pay_status_raw = _s(order.get("goods_payment_status")) or _s(order.get("payment_status"))
        pay_status = _pay_status_ar(pay_status_raw)

        gt_val = _to_float(raw_goods_amount) + _to_float(raw_shipping_fee)
        _ = _money_safe(gt_val, fb=goods_amount if goods_amount != "0" else "0")  # لا نغيّر المنطق

        inv_title = "فاتورة تاجر - داخلية - قطع + شحن"
    else:
        pay_method = _s(order.get("payment_method")) or _s(order.get("goods_payment_method"))
        pay_status_raw = _s(order.get("payment_status")) or _s(order.get("goods_payment_status"))
        pay_status = _pay_status_ar(pay_status_raw)

        inv_title = "فاتورة داخلية"

    # ✅ ثابت: مؤكد (لا نعرض جاري التحقق)
    pay_status = "مؤكد"

    if kind_norm == "shipping":
        inv_title = "فاتورة شحن" if invoice_for_norm == "trader" else "فاتورة شحن - منصة"

    # ✅ صيغة العنوان داخل الشريط الملون
    platform_bar = "منصة قطع غيار PPARTS"
    if admin_only:
        platform_bar = platform_bar + " / فاتورة داخلية"

    # --------------- temp pdf ---------------
    tmpdir = tempfile.gettempdir()
    pdf_path = os.path.join(tmpdir, f"pp_invoice_{order_id}_{kind_norm}_{uuid.uuid4().hex[:6]}.pdf")

    # --------------- Arabic font ---------------
    font_name = "Helvetica"
    chosen = ""
    try:
        base_dir = os.path.dirname(__file__)
        amiri_path = os.path.join(base_dir, "Amiri-Regular.ttf")
        noto_path = os.path.join(base_dir, "NotoNaskhArabic-Regular.ttf")

        if os.path.exists(amiri_path):
            chosen = amiri_path
        elif os.path.exists(noto_path):
            chosen = noto_path
        else:
            dejavu = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
            if os.path.exists(dejavu):
                chosen = dejavu

        if chosen:
            font_name = "PP_AR"
            try:
                pdfmetrics.getFont(font_name)
            except Exception:
                pdfmetrics.registerFont(TTFont(font_name, chosen))
    except Exception:
        font_name = "Helvetica"

    stamp_font = font_name
    try:
        if chosen:
            stamp_font = "PP_AR_STAMP"
            try:
                pdfmetrics.getFont(stamp_font)
            except Exception:
                pdfmetrics.registerFont(TTFont(stamp_font, chosen))
    except Exception:
        stamp_font = font_name

    # --------------- Colors (per invoice type) ---------------
    C_BORDER = colors.HexColor("#CBD5E1")
    C_TEXT = colors.HexColor("#0B1220")

    if invoice_for_norm == "trader":
        # أخضر (تدرج/تنويع + شفافية)
        C_DARK    = colors.HexColor("#065F46")
        C_DARK_2  = colors.HexColor("#0B7A57")
        BADGE_BG  = colors.HexColor("#E9FFF6")
        SEC_HDR   = colors.HexColor("#0F3D2E")
        SEC_HDR_2 = colors.HexColor("#145A43")
        STAMP     = colors.HexColor("#16A34A")
        ROW_TINT1 = "#ECFDF5"
        ROW_TINT2 = "#E6FFFA"
    else:
        # أزرق (تدرج/تنويع + شفافية)
        C_DARK    = colors.HexColor("#0B3A6E")
        C_DARK_2  = colors.HexColor("#145AA0")
        BADGE_BG  = colors.HexColor("#EAF2FF")
        SEC_HDR   = colors.HexColor("#0A2E57")
        SEC_HDR_2 = colors.HexColor("#123E6D")
        STAMP     = colors.HexColor("#2563EB")
        ROW_TINT1 = "#EFF6FF"
        ROW_TINT2 = "#E8F1FF"

    # ✅ (01) ألوان شفافة (تظهر العلامة المائية)
    def _with_alpha(c, a: float):
        try:
            return colors.Color(c.red, c.green, c.blue, alpha=max(0.0, min(1.0, float(a))))
        except Exception:
            return c

    def _hexA(hx: str, a: float):
        try:
            c = colors.HexColor(hx)
            return colors.Color(c.red, c.green, c.blue, alpha=max(0.0, min(1.0, float(a))))
        except Exception:
            return colors.HexColor(hx)

    # --------------- Styles (tight to keep 1 page) ---------------
    styles = getSampleStyleSheet()

    kv_label = ParagraphStyle(
        "kv_label",
        parent=styles["Normal"],
        alignment=TA_RIGHT,
        fontSize=8.6,
        leading=10.2,
        fontName=font_name,
        textColor=C_TEXT
    )

    kv_value = ParagraphStyle(
        "kv_value",
        parent=styles["Normal"],
        alignment=TA_RIGHT,
        fontSize=8.6,
        leading=10.2,
        fontName=font_name,
        textColor=C_TEXT
    )

    center  = ParagraphStyle("center", parent=styles["Normal"], alignment=TA_CENTER, fontSize=11.4, leading=12.6, fontName=font_name)
    tiny_c  = ParagraphStyle("tiny_c", parent=styles["Normal"], alignment=TA_CENTER, fontSize=8.8, leading=10.2, fontName=font_name)

    doc = SimpleDocTemplate(
        pdf_path,
        pagesize=A4,
        rightMargin=0.85 * cm,
        leftMargin=0.85 * cm,
        topMargin=0.65 * cm,
        bottomMargin=0.75 * cm,
        title=inv_title,
        author="PP Platform",
    )

    def P(txt: str, st):
        return Paragraph(_ar(txt), st)

    full_w = A4[0] - doc.leftMargin - doc.rightMargin
    story = []

    # -------- Logo path --------
    logo_path = ""
    try:
        p1 = os.path.join(os.path.dirname(__file__), "pparts.jpg")
        if os.path.exists(p1):
            logo_path = p1
        elif os.path.exists("pparts.jpg"):
            logo_path = "pparts.jpg"
    except Exception:
        logo_path = ""

    # ===== Header: Bigger Logo centered =====
    logo_cell = ""
    try:
        if logo_path and os.path.exists(logo_path):
            img = RLImage(logo_path)
            img.drawHeight = 3.00 * cm
            img.drawWidth = 3.00 * cm
            logo_cell = img
    except Exception:
        logo_cell = ""

    header_tbl = Table([[logo_cell if logo_cell else P("PPARTS", center)]], colWidths=[full_w])
    header_tbl.setStyle(TableStyle([
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 1),
    ]))
    story.append(header_tbl)
    story.append(Spacer(1, 2))

    title_bar = Table([[
        P(f"<b>{platform_bar}</b>    |    <b>{inv_title}</b>",
          ParagraphStyle("tbar", parent=center, textColor=colors.white, fontSize=10.6, leading=12.0, fontName=font_name))
    ]], colWidths=[full_w])
    title_bar.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), C_DARK),
        ("BOX", (0, 0), (-1, -1), 0.0, colors.white),
        ("LINEBELOW", (0, 0), (-1, 0), 1.6, _with_alpha(C_DARK_2, 0.95)),  # تنويع/تدرج بصري
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    story.append(title_bar)
    story.append(Spacer(1, 3))

    badges = Table([[
        P(f"رقم الفاتورة: <b>{inv_no}</b>", tiny_c),
        P(f"رقم الطلب: <b>{order_id}</b>", tiny_c),
        P(f"{inv_date}  {inv_time} (KSA)", tiny_c),
    ]], colWidths=[0.40 * full_w, 0.30 * full_w, 0.30 * full_w])
    badges.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), _with_alpha(BADGE_BG, 0.58)),  # شفافية لإظهار العلامة المائية
        ("BOX", (0, 0), (-1, -1), 0.6, C_BORDER),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))
    story.append(badges)
    story.append(Spacer(1, 3))

    def section_kv(title: str, rows: list):
        hdr = Table([[
            P(f"<b>{title}</b>",
              ParagraphStyle("sh", parent=kv_label, fontSize=9.1, leading=10.6,
                             textColor=colors.white, fontName=font_name))
        ]], colWidths=[full_w])
        hdr.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), _with_alpha(SEC_HDR, 0.92)),
            ("LINEBELOW", (0, 0), (-1, 0), 1.1, _with_alpha(SEC_HDR_2, 0.92)),  # تنويع/تدرج بصري
            ("BOX", (0, 0), (-1, -1), 0.6, C_BORDER),
            ("LEFTPADDING", (0, 0), (-1, -1), 6),
            ("RIGHTPADDING", (0, 0), (-1, -1), 6),
            ("TOPPADDING", (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ]))
        story.append(hdr)

        data = []
        for k, v in rows:
            data.append([
                P(html.escape(str(v)), kv_value),
                P("", kv_value),
                P(f"<b>{html.escape(str(k))}</b>", kv_label),
            ])

        t = Table(data, colWidths=[0.64 * full_w, 0.03 * full_w, 0.33 * full_w])
        t.setStyle(TableStyle([
            ("BOX", (0, 0), (-1, -1), 0.6, C_BORDER),
            ("INNERGRID", (0, 0), (-1, -1), 0.25, C_BORDER),
            ("LEFTPADDING", (0, 0), (-1, -1), 4),
            ("RIGHTPADDING", (0, 0), (-1, -1), 4),
            ("TOPPADDING", (0, 0), (-1, -1), 2),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LINEBEFORE", (1, 0), (1, -1), 0, colors.white),
            ("LINEAFTER",  (1, 0), (1, -1), 0, colors.white),
            ("ROWBACKGROUNDS", (0, 0), (-1, -1), [_hexA("#FFFFFF", 0.00), _hexA("#FFFFFF", 0.00)]),
        ]))
        story.append(t)
        story.append(Spacer(1, 3))

    rows_client = [("اسم العميل", client_name), ("رقم الجوال", client_phone)]
    pm = _s(order.get("goods_payment_method")) or _s(order.get("payment_method")) or ""
    if pm:
        rows_client.append(("طريقة الدفع", pm))
    rows_client.append(("حالة الدفع", "مؤكد"))
    section_kv("معلومات العميل", rows_client)

    rows_car = [
        ("اسم السيارة", car_name),
        ("الموديل", car_model),
        ("رقم الهيكل VIN", vin),
    ]
    if invoice_for_norm == "trader":
        rows_car.append(("اسم التاجر", trader_name))
    section_kv("معلومات السيارة", rows_car)

    rows_ship = [("نوع التسليم", ship_method)]
    if ship_city:
        rows_ship.append(("المدينة", ship_city))
    if ship_district:
        rows_ship.append(("الحي", ship_district))
    if ship_short:
        rows_ship.append(("العنوان المختصر", ship_short))
    if delivery_details:
        rows_ship.append(("تفاصيل العنوان", _cell_clip(delivery_details, 140)))
    if kind_norm == "shipping":
        rows_ship.append(("رقم التتبع", (tracking_number or order.get("shipping_tracking") or "—")))
    section_kv("تفاصيل الشحن", rows_ship)

    sec_parts = Table([[P("<b>تفاصيل القطع</b>",
                          ParagraphStyle("sh2", parent=kv_label, fontSize=9.0, leading=10.5,
                                         textColor=colors.white, fontName=font_name))]],
                      colWidths=[full_w])
    sec_parts.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), _with_alpha(SEC_HDR, 0.92)),
        ("LINEBELOW", (0, 0), (-1, 0), 1.1, _with_alpha(SEC_HDR_2, 0.92)),
        ("BOX", (0, 0), (-1, -1), 0.6, C_BORDER),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]))
    story.append(sec_parts)

    parts_cell_r = ParagraphStyle("parts_cell_r", parent=styles["Normal"], alignment=TA_RIGHT, fontSize=8.2, leading=9.6, fontName=font_name)
    parts_cell_num = ParagraphStyle("parts_cell_num", parent=styles["Normal"], alignment=TA_RIGHT, fontSize=8.2, leading=9.6, fontName=font_name)

    parts_rows = [[
        P("<b>رقم القطعة</b>", ParagraphStyle("ph1", parent=parts_cell_r, textColor=colors.white)),
        P("<b>اسم القطعة</b>", ParagraphStyle("ph2", parent=parts_cell_r, textColor=colors.white)),
        P("<b>#</b>", ParagraphStyle("ph3", parent=parts_cell_r, textColor=colors.white)),
    ]]

    if items:
        for i, it in enumerate(items, start=1):
            nm = _cell_clip(it.get("name") or it.get("item_name") or "—", 60) or "—"
            pn = _cell_clip(it.get("part_no") or it.get("item_part_no") or it.get("number") or "—", 40) or "—"
            parts_rows.append([
                Paragraph(_ar(html.escape(pn)), parts_cell_r),
                Paragraph(_ar(html.escape(nm)), parts_cell_r),
                Paragraph(_ar(str(i)), parts_cell_num),
            ])
    else:
        parts_rows.append([
            Paragraph(_ar("—"), parts_cell_r),
            Paragraph(_ar("—"), parts_cell_r),
            Paragraph(_ar("1"), parts_cell_num),
        ])

    col_w = [0.34 * full_w, 0.58 * full_w, 0.08 * full_w]
    row_h = 0.62 * cm
    parts_tbl = Table(parts_rows, colWidths=col_w, rowHeights=[row_h] * len(parts_rows), repeatRows=1)
    parts_tbl.setStyle(TableStyle([
        ("BOX", (0, 0), (-1, -1), 0.7, C_BORDER),
        ("INNERGRID", (0, 0), (-1, -1), 0.25, C_BORDER),
        ("BACKGROUND", (0, 0), (-1, 0), _with_alpha(C_DARK, 0.92)),
        ("LINEBELOW", (0, 0), (-1, 0), 1.2, _with_alpha(C_DARK_2, 0.95)),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING", (0, 0), (-1, -1), 3),
        ("RIGHTPADDING", (0, 0), (-1, -1), 3),
        ("TOPPADDING", (0, 0), (-1, -1), 1),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 1),
        ("RIGHTPADDING", (2, 0), (2, -1), 1.5),
        ("LEFTPADDING", (2, 0), (2, -1), 1.5),
        # تظليل صفوف خفيف شفاف لإظهار العلامة المائية + وضوح أعلى
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [_hexA(ROW_TINT1, 0.18), _hexA(ROW_TINT2, 0.12)]),
    ]))
    story.append(parts_tbl)
    story.append(Spacer(1, 3))

    # ===== Financial Summary Header =====
    sec_fin = Table([[P("<b>الملخص المالي</b>",
                        ParagraphStyle("sh3", parent=kv_label, fontSize=9.0, leading=10.5,
                                       textColor=colors.white, fontName=font_name))]],
                    colWidths=[full_w])
    sec_fin.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), _with_alpha(SEC_HDR, 0.92)),
        ("LINEBELOW", (0, 0), (-1, 0), 1.1, _with_alpha(SEC_HDR_2, 0.92)),
        ("BOX", (0, 0), (-1, -1), 0.6, C_BORDER),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]))
    story.append(sec_fin)

    # ===== Financial Summary (احترافي + محاذاة يمين + ألوان شفافة) =====
    fin_lbl_w = ParagraphStyle("fin_lbl_w", parent=tiny_c, alignment=TA_RIGHT,
                               fontSize=9.0, leading=10.2, fontName=font_name, textColor=colors.white)
    fin_lbl_d = ParagraphStyle("fin_lbl_d", parent=tiny_c, alignment=TA_RIGHT,
                               fontSize=9.0, leading=10.2, fontName=font_name, textColor=C_TEXT)

    fin_amt_w = ParagraphStyle("fin_amt_w", parent=tiny_c, alignment=TA_RIGHT,
                               fontSize=10.0, leading=11.0, fontName=font_name, textColor=colors.white)
    fin_amt_d = ParagraphStyle("fin_amt_d", parent=tiny_c, alignment=TA_RIGHT,
                               fontSize=10.0, leading=11.0, fontName=font_name, textColor=C_TEXT)

    if invoice_for_norm == "trader":
        gt_val = _to_float(raw_goods_amount) + _to_float(raw_shipping_fee)

        # ✅ خلفيات شفافة + تنويع ألوان متناسق
        BG_TOTAL = _with_alpha(C_DARK, 0.82)
        BG_SHIP  = _hexA("#DFF7EA", 0.18)
        BG_PARTS = _hexA("#D8F0FF", 0.18)

        money_box = Table([
            [
                P("<b>الإجمالي</b>", fin_lbl_w),
                P("<b>رسوم الشحن</b>", fin_lbl_d),
                P("<b>قيمة القطع</b>", fin_lbl_d),
            ],
            [
                Paragraph(_money_tail(gt_val, fb="0"), fin_amt_w),
                Paragraph(_money_tail(raw_shipping_fee, fb="0"), fin_amt_d),
                Paragraph(_money_tail(raw_goods_amount, fb="0"), fin_amt_d),
            ],
        ], colWidths=[0.34 * full_w, 0.33 * full_w, 0.33 * full_w])

        money_box.setStyle(TableStyle([
            ("BOX", (0, 0), (-1, -1), 0.8, C_BORDER),
            ("INNERGRID", (0, 0), (-1, -1), 0.35, C_BORDER),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("ALIGN", (0, 0), (-1, -1), "RIGHT"),
            ("LEFTPADDING", (0, 0), (-1, -1), 7),
            ("RIGHTPADDING", (0, 0), (-1, -1), 7),
            ("TOPPADDING", (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 5),

            ("BACKGROUND", (0, 0), (0, 1), BG_TOTAL),
            ("BACKGROUND", (1, 0), (1, 1), BG_SHIP),
            ("BACKGROUND", (2, 0), (2, 1), BG_PARTS),

            ("LINEABOVE", (0, 1), (-1, 1), 0.6, C_BORDER),
        ]))
        story.append(money_box)

    else:
        # منصة: بدون رسوم شحن نهائياً (لا تظهر ولا تُحسب)
        BG_TOTAL = _with_alpha(C_DARK, 0.82)
        BG_FEE   = _hexA("#D7E7FF", 0.18)

        platform_total_val = _to_float(raw_platform_fee)

        one_box = Table([
            [P("<b>الإجمالي</b>", fin_lbl_w), P("<b>رسوم المنصة</b>", fin_lbl_d)],
            [
                Paragraph(_money_tail(platform_total_val, fb="0"), fin_amt_w),
                Paragraph(_money_tail(raw_platform_fee, fb="0"), fin_amt_d),
            ],
        ], colWidths=[0.45 * full_w, 0.55 * full_w])

        one_box.setStyle(TableStyle([
            ("BOX", (0, 0), (-1, -1), 0.8, C_BORDER),
            ("INNERGRID", (0, 0), (-1, -1), 0.35, C_BORDER),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("ALIGN", (0, 0), (-1, -1), "RIGHT"),
            ("LEFTPADDING", (0, 0), (-1, -1), 7),
            ("RIGHTPADDING", (0, 0), (-1, -1), 7),
            ("TOPPADDING", (0, 0), (-1, -1), 6),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 6),

            ("BACKGROUND", (0, 0), (0, 1), BG_TOTAL),
            ("BACKGROUND", (1, 0), (1, 1), BG_FEE),

            ("LINEABOVE", (0, 1), (-1, 1), 0.6, C_BORDER),
        ]))
        story.append(one_box)

    story.append(Spacer(1, 2))

    footer_email = "p200126p@hotmail.com"
    rights_line = "/ الخدمات المساندة GO ومنصة PP"

    def _draw_extras(canvas, _doc, *, draw_stamp: bool):
        canvas.saveState()

        # Watermark (أوضح + مرفوع للأعلى)
        try:
            if logo_path and os.path.exists(logo_path):
                from reportlab.lib.utils import ImageReader
                img = ImageReader(logo_path)
                page_w, page_h = A4

                wm_w = 17.2 * cm
                wm_h = 17.2 * cm
                x = (page_w - wm_w) / 2.0
                y = (page_h - wm_h) / 2.0 + (3.2 * cm)

                try:
                    canvas.setFillAlpha(0.16)
                except Exception:
                    pass

                canvas.drawImage(
                    img, x, y,
                    width=wm_w, height=wm_h,
                    mask='auto',
                    preserveAspectRatio=True,
                    anchor='c'
                )

                try:
                    canvas.setFillAlpha(1)
                except Exception:
                    pass
        except Exception:
            pass

        # Footer line
        canvas.setStrokeColor(C_BORDER)
        canvas.setLineWidth(0.55)
        canvas.line(doc.leftMargin, 0.92 * cm, A4[0] - doc.rightMargin, 0.92 * cm)

        canvas.setFillColor(C_TEXT)
        try:
            canvas.setFont(font_name, 7.6)
        except Exception:
            canvas.setFont("Helvetica", 7.6)

        canvas.drawString(doc.leftMargin, 0.60 * cm, _ar(rights_line))
        canvas.drawRightString(A4[0] - doc.rightMargin, 0.60 * cm, _ar(footer_email))

        # ✅ ختم مدفوع دائري: (3 أسطر بتباعد موزون + خيار إطارين)
        if draw_stamp:
            if invoice_for_norm == "trader":
                stamp_cx = doc.leftMargin + (0.34 * full_w) / 2.0
            else:
                stamp_cx = doc.leftMargin + (0.45 * full_w) / 2.0

            stamp_cy = 2.55 * cm

            # تكبير بسيط لإراحة النصوص
            r = 1.22 * cm

            # دائرة تعبئة + إطارين (شكل أنظف)
            try:
                # تعبئة
                canvas.setFillColor(STAMP)
                canvas.setStrokeColor(STAMP)
                canvas.setLineWidth(1.2)
                canvas.circle(stamp_cx, stamp_cy, r, stroke=1, fill=1)

                # إطار خارجي
                canvas.setStrokeColor(colors.white)
                canvas.setLineWidth(1.15)
                canvas.circle(stamp_cx, stamp_cy, r - (0.06 * cm), stroke=1, fill=0)

                # إطار داخلي خفيف
                canvas.setStrokeColor(_with_alpha(colors.white, 0.65))
                canvas.setLineWidth(0.9)
                canvas.circle(stamp_cx, stamp_cy, r - (0.18 * cm), stroke=1, fill=0)
            except Exception:
                pass

            # النصوص داخل الختم (3 أسطر) — تباعد موزون
            try:
                canvas.setFillColor(colors.white)

                # 1) مدفوع (أكبر)
                try:
                    canvas.setFont(stamp_font, 13.2)
                except Exception:
                    canvas.setFont("Helvetica-Bold", 13.2)
                canvas.drawCentredString(stamp_cx, stamp_cy + 0.42 * cm, _ar("مدفوع"))

                # 2) منصة قطع الغيار PP
                try:
                    canvas.setFont(stamp_font, 6.5)
                except Exception:
                    canvas.setFont("Helvetica", 6.5)
                canvas.drawCentredString(stamp_cx, stamp_cy + 0.04 * cm, _ar("منصة قطع الغيار PP"))

                # 3) الخدمات المساندة GO
                try:
                    canvas.setFont(stamp_font, 6.4)
                except Exception:
                    canvas.setFont("Helvetica", 6.4)
                canvas.drawCentredString(stamp_cx, stamp_cy - 0.34 * cm, _ar("الخدمات المساندة GO"))
            except Exception:
                pass

        canvas.restoreState()

    def _on_first(canvas, _doc):
        _draw_extras(canvas, _doc, draw_stamp=True)

    def _on_later(canvas, _doc):
        _draw_extras(canvas, _doc, draw_stamp=False)

    # Build PDF
    try:
        doc.build(story, onFirstPage=_on_first, onLaterPages=_on_later)
    except Exception as e:
        await _notify_invoice_error(context, order_id, f"إنشاء PDF ({kind_norm})", e)
        try:
            os.remove(pdf_path)
        except Exception:
            pass
        return

    # Send PDF
    caption = f"📄 {inv_title}\nرقم الطلب: {order_id}\nرقم الفاتورة: {inv_no}"
    filename = f"PP_Invoice_{inv_no}.pdf"

    targets = []
    if admin_only:
        for aid in ADMIN_IDS:
            try:
                targets.append(int(aid))
            except Exception:
                pass
    else:
        if client_id:
            targets.append(int(client_id))
        for aid in ADMIN_IDS:
            try:
                targets.append(int(aid))
            except Exception:
                pass

    targets = [x for i, x in enumerate(targets) if x and x not in targets[:i]]

    failed = []
    sent_any = False

    for cid in targets:
        try:
            with open(pdf_path, "rb") as f:
                await context.bot.send_document(
                    chat_id=cid,
                    document=f,
                    filename=filename,
                    caption=caption,
                    disable_content_type_detection=False,
                )
            sent_any = True
        except Exception as e:
            failed.append((cid, str(e)))

    if failed:
        lines = []
        for cid, err in failed[:8]:
            lines.append(f"- chat_id={cid}: {err}")
        more = f"\n(+{len(failed)-8} أخطاء أخرى)" if len(failed) > 8 else ""
        await _notify_invoice_error(
            context,
            order_id,
            f"إرسال PDF ({kind_norm}){' - لم يُرسل لأي جهة' if not sent_any else ''}",
            "\n".join(lines) + more
        )
    else:
        try:
            update_order_fields(order_id, {sent_flag_field: "yes", legacy_flag: "yes"})
        except Exception:
            pass

    try:
        os.remove(pdf_path)
    except Exception:
        pass


def client_trader_chat_kb(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💬 مراسلة التاجر", callback_data=f"pp_chat_trader|{order_id}")],
    ])

def client_trader_chat_done_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ انهاء المراسلة", callback_data="pp_chat_trader_done")],
    ])

def trader_reply_kb(order_id: str, user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💬 رد على العميل", callback_data=f"pp_trader_reply|{order_id}|{user_id}")],
    ])

def trader_reply_done_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ انهاء الرد", callback_data="pp_trader_reply_done")],
    ])

def team_group_kb(order_id: str, bot_username: str | None = None) -> InlineKeyboardMarkup:
    """Keyboard used inside TEAM group for the initial order post.

    Requirement: only allow starting a quote from the group.
    All quote details are collected in private to avoid clutter and to keep finance/details private.
    """
    # افضل تجربة: زر URL يفتح الخاص مباشرة بدون ما يبحث التاجر عن البوت.
    if bot_username:
        deeplink = f"https://t.me/{bot_username}?start=ppq_{order_id}"
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("💰 تقديم عرض سعر ➜", url=deeplink)],
        ])

    # fallback (لو لم يتوفر اسم البوت)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💰 تقديم عرض سعر ➜", callback_data=f"pp_team_quote|{order_id}")],
    ])


def _norm(s: str) -> str:
    return (s or "").strip()

def _user_name(update_or_q) -> str:
    try:
        u = update_or_q.effective_user if hasattr(update_or_q, "effective_user") else update_or_q.from_user
        return (u.full_name or "عميلنا").strip()
    except Exception:
        return "عميلنا"

def _looks_like_vin(s: str) -> bool:
    s = _norm(s).replace(" ", "").upper()
    return bool(VIN_RE.match(s))

def _sanitize_delivery_details(details: str, hide_phone: bool = True) -> str:
    d = (details or "").strip()
    if not hide_phone:
        return d
    # remove any line that contains phone/contact
    lines = []
    for ln in d.splitlines():
        if "رقم الاتصال" in ln or "الجوال" in ln or "الهاتف" in ln:
            continue
        lines.append(ln)
    return "\n".join(lines).strip()

def _save_order_once(ud: dict):
    if ud.get("order_saved"):
        return
    add_order({
        "order_id": ud.get("order_id",""),
        "user_id": ud.get("user_id",0),
        "user_name": ud.get("user_name",""),
        "car_name": ud.get("car_name",""),
        "car_model": ud.get("car_model",""),
        "vin": ud.get("vin",""),
        "notes": ud.get("notes",""),
        "items_count": len(ud.get("items",[])),
        "price_sar": ud.get("price_sar",0),
        "status": "payment_pending",
        "payment_method": ud.get("payment_method",""),
        "payment_status": "pending",
        "receipt_file_id": "",
        "payment_confirmed_at_utc": "",
        "delivery_choice": "",
        "delivery_details": "",
        "created_at_utc": ud.get("created_at_utc", utc_now_iso()),
    })
    add_items(ud.get("order_id",""), _items_for_excel(ud.get("items",[])))
    ud["order_saved"] = True


def _items_for_excel(items: list[dict]) -> list[dict]:
    out = []
    for it in items or []:
        name = it.get("name","")
        part_no = it.get("part_no","") or ""  # ✅ جديد
        photo = it.get("photo_file_id","") or it.get("file_id","") or ""
        out.append({
            "name": name,
            "part_no": part_no,  # ✅ جديد (يروح لعمود item_part_no)
            "photo_file_id": photo,
            "created_at_utc": it.get("created_at_utc", utc_now_iso()),
        })
    return out

def _pay_method_ar(method: str) -> str:
    m = (method or "").strip().lower()
    return {
        "bank_transfer": "🏦 تحويل بنكي",
        "stc_pay": "📱 STC Pay",
        "pay_link": "🔗 رابط دفع",
        "free": "🆓 مجاني",
    }.get(m, method or "—")


async def send_trader_subscription_invoice_pdf(
    context: ContextTypes.DEFAULT_TYPE,
    trader_id: int,
    month: str,
    amount_sar: int = 99,
):
    """Generate and send a simple 1-page PDF invoice for trader subscription."""
    try:
        from reportlab.pdfgen import canvas as rl_canvas
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.units import mm
        from reportlab.lib import colors
    except Exception:
        return

    # Arabic shaping (best effort)
    try:
        import arabic_reshaper
        from bidi.algorithm import get_display
        def _ar(s: str) -> str:
            s = "" if s is None else str(s)
            if not s:
                return s
            try:
                return get_display(arabic_reshaper.reshape(s))
            except Exception:
                return s
    except Exception:
        def _ar(s: str) -> str:
            return "" if s is None else str(s)

    font_name = "Helvetica"
    try:
        from reportlab.pdfbase import pdfmetrics
        from reportlab.pdfbase.ttfonts import TTFont
        if os.path.exists("Amiri-Regular.ttf"):
            pdfmetrics.registerFont(TTFont("Amiri", "Amiri-Regular.ttf"))
            font_name = "Amiri"
    except Exception:
        pass

    month = str(month or "").strip() or month_key_utc()
    amount_sar = int(float(amount_sar or 99))

    tmp = f"sub_invoice_{trader_id}_{month.replace('-', '')}.pdf"
    path = os.path.join("/tmp", tmp)

    c = rl_canvas.Canvas(path, pagesize=A4)
    w, h = A4

    c.setFillColor(colors.HexColor("#0B3D91"))
    c.rect(0, h-35*mm, w, 35*mm, fill=1, stroke=0)
    c.setFillColor(colors.white)
    c.setFont(font_name, 16)
    c.drawRightString(w-15*mm, h-18*mm, _ar("فاتورة اشتراك منصة"))
    c.setFont(font_name, 10)
    c.drawRightString(w-15*mm, h-26*mm, _ar(f"الشهر: {month}"))

    c.setFillColor(colors.whitesmoke)
    c.rect(15*mm, h-140*mm, w-30*mm, 90*mm, fill=1, stroke=0)

    c.setFillColor(colors.black)
    c.setFont(font_name, 12)
    c.drawRightString(w-20*mm, h-70*mm, _ar("البند: رسوم اشتراك منصة"))
    c.drawRightString(w-20*mm, h-85*mm, _ar(f"المبلغ: {amount_sar} ريال"))
    c.drawRightString(w-20*mm, h-100*mm, _ar(f"المرجع: SUB-{trader_id}-{month}"))

    c.setFont(font_name, 9)
    c.setFillColor(colors.gray)
    c.drawString(15*mm, 15*mm, "PP / GO - Platform Subscription Invoice")

    c.showPage()
    c.save()

    caption = f"🧾 فاتورة اشتراك منصة — {month} — {amount_sar} ريال"
    try:
        with open(path, "rb") as f:
            await context.bot.send_document(chat_id=int(trader_id), document=f, caption=caption)
    except Exception:
        pass

    for aid in ADMIN_IDS:
        try:
            with open(path, "rb") as f:
                await context.bot.send_document(chat_id=int(aid), document=f, caption=f"(نسخة) {caption} — trader_id {trader_id}")
        except Exception:
            pass

    try:
        os.remove(path)
    except Exception:
        pass


async def _send_client_payment_preview(
    context: ContextTypes.DEFAULT_TYPE,
    client_id: int,
    order_id: str,
    pay_scope: str = "platform",  # platform / goods
) -> None:
    if not client_id or not order_id:
        return

    try:
        b = get_order_bundle(order_id)
        order = b.get("order", {}) or {}
        items = b.get("items", []) or []
    except Exception:
        order = {}
        items = []

    car = (order.get("car_name") or "").strip()
    model = (order.get("car_model") or "").strip()
    vin = (order.get("vin") or "").strip()

    ship_method = (order.get("ship_method") or "").strip()
    delivery_details = (order.get("delivery_details") or "").strip()

    # بيانات الدفع حسب النطاق
    if pay_scope == "goods":
        amount = order.get("goods_amount_sar") or ""
        method = order.get("goods_payment_method") or ""
        title = "📦 تم استلام إيصال دفع قيمة القطع"
        status_line = "⏳ بانتظار التحقق ثم الشحن"
    else:
        amount = order.get("price_sar") or ""
        method = order.get("payment_method") or ""
        title = "🧾 تم ارسال إيصال دفع رسوم المنصة"
        status_line = "⏳ جارٍ التحقق من الدفع"

    amt_txt = f"{amount} ريال" if str(amount).strip() not in ("", "0", "0.0") else "—"
    method_txt = _pay_method_ar(str(method))

    # القطع: داخل الرسالة + عريض (بدون صندوق)
    parts_lines = []
    for i, it in enumerate(items, start=1):
        nm = (it.get("name") or "").strip()
        pn = (it.get("part_no") or it.get("item_part_no") or "").strip()
        if not nm:
            continue

        if pn:
            parts_lines.append(f"• <b>{html.escape(nm)}</b>  —  <i>{html.escape(pn)}</i>")
        else:
            parts_lines.append(f"• <b>{html.escape(nm)}</b>")

        if len(parts_lines) >= 14:
            break

    parts_txt = "\n".join(parts_lines) if parts_lines else "• —"

    msg = (
        f"✅ <b>{html.escape(title)}</b>\n\n"
        f"🧾 <b>رقم الطلب</b>: <code>{html.escape(order_id)}</code>\n"
        f"💰 <b>المبلغ</b>: <b>{html.escape(amt_txt)}</b>\n"
        f"💳 <b>طريقة الدفع</b>: <b>{html.escape(method_txt)}</b>\n"
        f"📌 <b>الحالة</b>: <i>{html.escape(status_line)}</i>\n"
        "\n"
        "🚗 <b>بيانات السيارة</b>\n"
        + (f"• <b>{html.escape((car + ' ' + model).strip())}</b>\n" if (car or model) else "• —\n")
        + (f"• VIN: <code>{html.escape(vin)}</code>\n" if vin else "")
        + "\n"
        "🧩 <b>القطع المطلوبة</b>\n"
        + parts_txt
    )

    # ✅ العنوان فقط داخل صندوق
    if ship_method or delivery_details:
        msg += "\n\n📦 <b>طريقة التسليم</b>\n"
        if ship_method:
            msg += f"• <b>{html.escape(ship_method)}</b>\n"
        if delivery_details:
            msg += "\n📍 <b>تفاصيل التسليم</b>\n"
            msg += f"<pre>{html.escape(delivery_details)}</pre>"

    # ✅ زر المراسلة الصحيح حسب المرحلة:
    # - عند رسوم المنصة: مراسلة الإدارة
    # - عند قيمة القطع: مراسلة التاجر
    kb = track_kb(order_id) if pay_scope != "goods" else client_trader_chat_kb(order_id)

    try:
        await context.bot.send_message(
            chat_id=client_id,
            text=msg,
            parse_mode="HTML",
            reply_markup=kb,
            disable_web_page_preview=True,
        )
    except Exception:
        pass
    

async def _alert(q, text: str | None = None, force: bool = False):
    """
    force=True  → Popup
    force=False → Toast (بدون إزعاج) إذا فيه نص
    """
    try:
        if text is None or str(text).strip() == "":
            await q.answer()
            return
        await q.answer(text=str(text), show_alert=bool(force))
    except Exception:
        # لا نكسر تدفق الزر لو تيليجرام رجع خطأ (مثلاً query قديم)
        pass

        return False
    

async def _deny_disabled_trader_msg(update: Update, reason: str = "حساب التاجر موقوف"):
    try:
        if update and update.message:
            await update.message.reply_text(f"⛔ {reason}")
    except Exception:
        pass


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    log.exception("Unhandled error: %s", context.error)

    try:
        # إذا الخطأ جاء من CallbackQuery → تنبيه مربع فقط
        if isinstance(update, Update) and update.callback_query:
            await update.callback_query.answer(
                "⚠️ حدث خطأ غير متوقع\nيرجى المحاولة مرة أخرى",
                show_alert=True
            )
            return

        # ❌ لا نرسل أي رسالة نصية للشات
    except Exception:
        pass


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_workbook()

    # Deep-link args
    try:
        args = getattr(context, "args", []) or []
    except Exception:
        args = []

    # =========================
    # (1) ppq_ : لوحة عرض السعر للتاجر
    # =========================
    if args and isinstance(args[0], str) and args[0].startswith("ppq_"):
        order_id = args[0][4:].strip()

        td = context.user_data.setdefault(update.effective_user.id, {})
        td["quote_order_id"] = order_id
        td["quote_step"] = "start"
        set_stage(context, update.effective_user.id, STAGE_TRADER_SET_QUOTE)

        # ملخص سريع للطلب
        order_snapshot = ""
        try:
            b = get_order_bundle(order_id)
            order = b.get("order", {}) or {}
            items = b.get("items", []) or []

            # طريقة الشحن + المدينة
            ship_method = (order.get("ship_method") or order.get("shipping_method") or "").strip()
            delivery_details = (order.get("delivery_details") or order.get("address") or "").strip()

            city = (order.get("city") or "").strip()
            if not city and delivery_details:
                try:
                    for ln in delivery_details.splitlines():
                        ln2 = (ln or "").strip()
                        if ln2.startswith("المدينة"):
                            if ":" in ln2:
                                city = ln2.split(":", 1)[1].strip()
                            elif "-" in ln2:
                                city = ln2.split("-", 1)[1].strip()
                            break
                except Exception:
                    pass

            lines = []
            for i, it in enumerate(items, start=1):
                nm = (it.get("name") or "").strip()
                if nm:
                    lines.append(f"{i}- {nm}")

            parts_txt = "\n".join(lines) if lines else "لا يوجد"

            order_snapshot = (
                "📌 ملخص الطلب\n"
                f"رقم الطلب: {order_id}\n"
                f"السيارة: {order.get('car_name','')}\n"
                f"الموديل: {order.get('car_model','')}\n"
                f"VIN: {order.get('vin','')}\n"
                f"طريقة الشحن: {ship_method or 'غير محدد'}\n"
                f"المدينة: {city or 'غير محددة'}\n"
                f"الملاحظات: {order.get('notes','') or 'لا يوجد'}\n\n"
                f"القطع:\n{parts_txt}"
            )
        except Exception:
            order_snapshot = f"رقم الطلب: {order_id}"

        await update.message.reply_text(
            f"{_user_name(update)}\n"
            "✨ اهلا بك في لوحة عرض السعر\n\n"
            "هذه الخطوات مصممة لتبني عرض منسق واحترافي\n\n"
            f"{order_snapshot}\n\n"
            "اضغط زر البدء بالاسفل ثم اتبع الخطوات خطوة بخطوة",
            reply_markup=trader_quote_start_kb(order_id),
            parse_mode="HTML",
        )
        return

    # =========================
    # (2) ppopen_ : فتح لوحة الطلب
    # =========================
    if args and isinstance(args[0], str) and args[0].startswith("ppopen_"):
        order_id = args[0][7:].strip()

        try:
            b = get_order_bundle(order_id)
            order = b.get("order", {}) or {}
        except Exception:
            order = {}

        try:
            acc = int(order.get("accepted_trader_id") or 0)
        except Exception:
            acc = 0

        qs = str(order.get("quote_status") or "").strip().lower()
        locked = str(order.get("quote_locked") or "").strip().lower() == "yes"

        if locked or qs == "accepted":
            tid = acc
        else:
            try:
                qid = int(order.get("quoted_trader_id") or 0)
            except Exception:
                qid = 0
            tid = acc or qid

        if not tid:
            await update.message.reply_text("🔒 لم يتم إسناد الطلب لتاجر بعد")
            return

        actor_id = update.effective_user.id

        accepted_name = (order.get("accepted_trader_name") or order.get("quoted_trader_name") or "").strip()
        if not accepted_name:
            try:
                tp = get_trader_profile(int(tid)) or {}
                accepted_name = (tp.get("display_name") or "").strip()
            except Exception:
                accepted_name = ""

        who = accepted_name or "التاجر المستلم"

        if tid != actor_id and actor_id not in ADMIN_IDS:
            await update.message.reply_text(
                "🔒 هذه اللوحة مخصصة لتاجر محدد\n"
                f"🧾 رقم الطلب: {order_id}\n"
                f"👤 التاجر: {who}\n\n"
                "✅ إذا كنت أنت التاجر المستلم افتح المنصة من نفس الحساب الذي استلم الطلب"
            )
            return

        try:
            if acc and actor_id == acc:
                notified = str(order.get("accepted_trader_notified") or "").strip().lower() == "yes"
                if not notified:
                    await context.bot.send_message(
                        chat_id=acc,
                        text=(
                            "✅ تم قبول عرض السعر من العميل\n"
                            f"🧾 رقم الطلب: {order_id}\n"
                            "🧰 ابدأ تجهيز الطلب ثم حدّث الحالة من لوحة التحكم"
                        ),
                        reply_markup=trader_status_kb(order_id),
                        disable_web_page_preview=True,
                    )
                    try:
                        update_order_fields(order_id, {"accepted_trader_notified": "yes"})
                    except Exception:
                        pass
        except Exception:
            pass

        await update.message.reply_text(
            f"🧰 لوحة التحكم للطلب\n"
            f"🧾 رقم الطلب: {order_id}\n"
            f"👤 التاجر: {who}",
            reply_markup=trader_status_kb(order_id),
            disable_web_page_preview=True,
        )
        return

    # =========================
    # (3) Start normal
    # =========================
    name = _user_name(update)
    await update.message.reply_text(
        f"<i>اهلا {name}</i>\n\n"
        "<b>✨ مرحبا بك في PP</b>\n\n"
        "<i>"
        "تجربة احترافية صممت بعناية للبحث الدقيق عن قطع سيارتك\n"
        "وتقديم تسعيرة واضحة وموثوقة قبل اتخاذ القرار\n"
        "</i>\n\n"
        "<b>🔍 ماذا يميز هذه الخدمة؟</b>\n"
        "<i>"
        "تحليل دقيق لبيانات سيارتك\n"
        "تحقق كامل من التوافق والتوفر\n"
        "وتسعيرة مبنية على واقع السوق بكل شفافية\n"
        "</i>\n\n"
        "<b>📋 للبدء نحتاج فقط:</b>\n"
        "• <i>اسم السيارة</i>\n"
        "• <i>الموديل (سنة من 4 ارقام)</i>\n"
        "• <i>رقم الهيكل VIN من 17 خانة</i>\n\n"
        "<b>🤝 هدفنا</b>\n"
        "<i>"
        "ان تصل الى القطعة والتسعيرة من خلال شركات السيارات او وكلاء محليين / عالميين باسرع وقت وتلقي عروض مختلفة\n"
        "</i>\n\n"
        "<b>⬇️ لبدء طلب جديد ارسل كلمة pp فقط</b>",
        parse_mode="HTML",
        reply_markup=main_menu_kb(),
    )
    

async def chatid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    await update.message.reply_text(f"Chat ID: {chat.id}\nType: {chat.type}")
    
    
async def support_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/منصة و /help : يفتح قناة تواصل مباشر مع الإدارة داخل الخاص فقط."""
    chat = update.effective_chat
    user_id = update.effective_user.id

    if chat.type != ChatType.PRIVATE:
        try:
            await update.message.reply_text("ℹ️ هذا الأمر يعمل في الخاص فقط")
        except Exception:
            pass
        return

    ud = get_ud(context, user_id)

    # إذا كان المستخدم داخل مراحل طلب/عملية، لا نفتح منصة حتى لا تتداخل المدخلات
    try:
        cur_stage = ud.get(STAGE_KEY)
    except Exception:
        cur_stage = None
    if cur_stage and cur_stage != STAGE_NONE:
        try:
            await update.message.reply_text(
                "⚠️ أنت الآن داخل خطوة/عملية. أكملها أو الغِها ثم أعد كتابة /منصة\n"
                "(حتى لا تختلط رسائل الطلب برسائل الإدارة)",
                disable_web_page_preview=True,
            )
        except Exception:
            pass
        return

    ud["support_open"] = True
    _support_touch(ud)

    try:
        await update.message.reply_text(
            "✅ تم فتح قناة تواصل مباشر مع الإدارة\n"
            "اكتب رسالتك الآن (استفسار/شكوى/ملاحظة)…",
            reply_markup=_support_kb(),
        )
    except Exception:
        pass
    

async def support_open_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await _alert(q, "")
    user_id = q.from_user.id

    # زر الدعم لازم يكون في الخاص (سياسة المنصة: لا رسائل داخل المجموعات)
    if q.message and q.message.chat and q.message.chat.type != ChatType.PRIVATE:
        try:
            # نرسل للمستخدم بالخاص فقط بدون أي رد داخل المجموعة
            await context.bot.send_message(
                chat_id=user_id,
                text=(
                    "✅ لفتح قناة التواصل مع الإدارة اكتب: منصة\n"
                    "أو اضغط زر (فتح المنصة) بالأسفل."
                ),
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📩 فتح المنصة", callback_data="pp_support_open")]
                ]),
                disable_web_page_preview=True,
            )
        except Exception:
            pass
        return

    ud = get_ud(context, user_id)

    # ✅ استثناء مهم: التاجر الموقوف يسمح له بفتح منصة حتى لو داخل خطوة/عملية
    # الهدف: زر "مراسلة الإدارة" لا يُقفل بسبب STAGE حتى لا يُحرم الموقوف من التواصل
    try:
        if _trader_is_disabled(int(user_id or 0)):
            ud["support_open"] = True
            _support_touch(ud)
            try:
                await q.message.reply_text(
                    "✅ تم فتح قناة تواصل مباشر مع الإدارة\n"
                    "اكتب رسالتك الآن (استفسار/شكوى/ملاحظة)…",
                    reply_markup=_support_kb(),
                    disable_web_page_preview=True,
                )
            except Exception:
                pass
            return
    except Exception:
        pass

    # نفس شرط support_cmd: لا نفتح منصة لو داخل عملية
    try:
        cur_stage = ud.get(STAGE_KEY)
    except Exception:
        cur_stage = None
    if cur_stage and cur_stage != STAGE_NONE:
        try:
            await q.message.reply_text(
                "⚠️ أنت الآن داخل خطوة/عملية. أكملها أو الغِها ثم أعد فتح منصة\n"
                "(حتى لا تختلط رسائل الطلب برسائل الإدارة)",
                disable_web_page_preview=True,
            )
        except Exception:
            pass
        return

    ud["support_open"] = True
    _support_touch(ud)

    try:
        await q.message.reply_text(
            "✅ تم فتح قناة تواصل مباشر مع الإدارة\n"
            "اكتب رسالتك الآن (استفسار/شكوى/ملاحظة)…",
            reply_markup=_support_kb(),
        )
    except Exception:
        pass


async def support_close_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    user_id = q.from_user.id
    await _support_close(update, context, user_id)


async def support_admin_reply_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """زر عند الإدارة لبدء الرد على مستخدم داخل /منصة."""
    q = update.callback_query
    await q.answer()
    admin_id = q.from_user.id
    if admin_id not in ADMIN_IDS:
        return

    data = (q.data or "").split("|")
    if len(data) < 2:
        return
    try:
        target_uid = int(data[1] or 0)
    except Exception:
        target_uid = 0
    if not target_uid:
        return

    ud = get_ud(context, admin_id)
    ud[STAGE_KEY] = STAGE_SUPPORT_ADMIN_REPLY
    ud["support_reply_to_uid"] = target_uid
    try:
        await q.message.reply_text(
            f"✉️ اكتب رد الإدارة الآن (سيصل للمستخدم {target_uid})",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✅ إنهاء", callback_data="pp_support_admin_done")]]),
        )
    except Exception:
        pass


async def support_admin_done_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer("تم")
    admin_id = q.from_user.id
    if admin_id not in ADMIN_IDS:
        return
    ud = get_ud(context, admin_id)
    if ud.get(STAGE_KEY) == STAGE_SUPPORT_ADMIN_REPLY:
        ud[STAGE_KEY] = STAGE_NONE
    ud.pop("support_reply_to_uid", None)
    try:
        await q.message.reply_text("✅ تم إنهاء وضع الرد")
    except Exception:
        pass


async def begin_flow(update_or_q, context: ContextTypes.DEFAULT_TYPE):
    user = update_or_q.effective_user if hasattr(update_or_q, "effective_user") else update_or_q.from_user
    user_id = user.id

    # 🔧 وضع الصيانة
    if _is_maintenance_mode() and user_id not in ADMIN_IDS:
        try:
            if hasattr(update_or_q, "message") and update_or_q.message:
                await update_or_q.message.reply_text(
                    _maintenance_block_text(),
                    parse_mode="HTML",
                    disable_web_page_preview=True
                )
            else:
                await update_or_q.edit_message_text(
                    _maintenance_block_text(),
                    parse_mode="HTML",
                    disable_web_page_preview=True
                )
        except Exception:
            pass
        return
    # ✅ إذا كانت قناة /منصة مفتوحة: نغلقها تلقائياً عند بدء أي طلب
    try:
        ud0 = get_ud(context, user_id)
        if _support_is_open(ud0):
            await _support_close(update_or_q, context, user_id, reason="(تم إغلاقها لأنك بدأت طلباً جديداً)")
    except Exception:
        pass

    # ✅ بدء الطلب فعلياً
    reset_flow(context, user_id)
    ud = get_ud(context, user_id)
    ud["order_id"] = generate_order_id("PP")
    ud["user_id"] = user_id
    ud["user_name"] = user.full_name or ""
    ud["items"] = []
    ud["notes"] = ""
    ud["created_at_utc"] = utc_now_iso()

    set_stage(context, user_id, STAGE_ASK_CAR)

    try:
        text = (
            f"{ud['user_name']}\n"
            "اكتب اسم الشركة واسم السيارة بشكل واضح كما يظهر بالاستمارة\n"
            "مثال: شيري اريزو 8 او تويوتا كامري"
        )
        if hasattr(update_or_q, "message") and update_or_q.message:
            await update_or_q.message.reply_text(text)
        else:
            await update_or_q.message.reply_text(text)
    except Exception:
        pass


async def cancel_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await _alert(q, f"تم الالغاء يا {_user_name(q)}")
    user_id = q.from_user.id
    reset_flow(context, user_id)
    await q.message.reply_text("تم الغاء العملية\للبداء بطلب قطع غيار  ارسل كلمة pp فقط")

async def skip_notes_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await _alert(q, f"تم يا {_user_name(q)}")
    user_id = q.from_user.id
    ud = get_ud(context, user_id)

    if ud.get(STAGE_KEY) != STAGE_ASK_NOTES:
        await q.message.reply_text(f"{_user_name(q)}\nلا يوجد ملاحظات حاليا")
        return

    ud["notes"] = ""
    set_stage(context, user_id, STAGE_ASK_ITEM_NAME)
    await q.message.reply_text(
        f"{_user_name(q)}\n"
        "اكتب اسم القطعة المطلوبة بدقة\n"
        "واذكر رقم القطعة ان توفر لرفع دقة الطلب"
)


async def prepay_notes_skip_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await _alert(q, "")
    user_id = q.from_user.id
    ud = get_ud(context, user_id)

    if ud.get(STAGE_KEY) not in (STAGE_PREPAY_NOTES, STAGE_PREPAY_NOTES_TEXT):
        return

    # ✅ تثبيت الملاحظات في الطلب قبل الانتقال (حتى تظهر بالمجموعة دائمًا)
    # لا نحفظ قيمة فاضية حتى لا نمسح ملاحظة موجودة سابقًا بالطلب
    try:
        order_id = (ud.get("order_id") or "").strip()
        notes = (ud.get("notes") or "").strip()
        if order_id and notes:
            update_order_fields(order_id, {"notes": notes})
    except Exception:
        pass

    # بعد الملاحظات -> ننتقل للتسليم (العنوان) ثم بعدها الدفع
    set_stage(context, user_id, STAGE_AWAIT_DELIVERY)
    await q.message.reply_text(
        f"{_user_name(q)}\nاختر طريقة التسليم",
        reply_markup=delivery_kb(),
        disable_web_page_preview=True,
    )


async def more_yes_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await _alert(q, "")
    user_id = q.from_user.id
    ud = get_ud(context, user_id)

    items = ud.get("items", []) or []
    if len(items) >= MAX_ITEMS:
        try:
            await context.bot.send_message(
                chat_id=q.message.chat_id,
                text=f"{_user_name(q)}\nتم الوصول للحد الاقصى {MAX_ITEMS} قطعة"
            )
        except Exception:
            pass
        return

    set_stage(context, user_id, STAGE_ASK_ITEM_NAME)
    next_no = len(items) + 1
    try:
        await context.bot.send_message(
            chat_id=q.message.chat_id,
            text=f"{_user_name(q)}\nاكتب اسم القطعة رقم {next_no}"
        )
    except Exception:
        pass

async def more_no_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await _alert(q, "")
    user_id = q.from_user.id
    ud = get_ud(context, user_id)

    items = ud.get("items", []) or []
    if not items:
        await q.message.reply_text(f"{_user_name(q)}\nلا يوجد قطع مضافة اكتب اسم القطعة اولا")
        set_stage(context, user_id, STAGE_ASK_ITEM_NAME)
        return

    # رسوم المنصة: حسب عدد القطع غير الاستهلاكية (الاستهلاكي مجاني بالكامل)
    fee, non_cnt, cons_cnt = _platform_fee_for_items(items)
    ud["price_sar"] = fee
    ud["non_consumable_count"] = non_cnt
    ud["consumable_count"] = cons_cnt

    # حفظ الطلب (مرة واحدة) قبل الانتقال للخطوات التالية
    try:
        _save_order_once(ud)
    except Exception:
        pass

    order_id = (ud.get("order_id") or "").strip()
    if order_id:
        try:
            update_order_fields(order_id, {
                "price_sar": fee,
                "non_consumable_count": non_cnt,
                "consumable_count": cons_cnt,
            })
        except Exception:
            pass

    # معاينة أولية (اختياري)
    try:
        await q.message.reply_text(build_order_preview(ud), parse_mode="HTML", disable_web_page_preview=True)
    except Exception:
        pass

    # ✅ رجّع مرحلة الملاحظات (بدل ما تختفي)
    set_stage(context, user_id, STAGE_PREPAY_NOTES)
    await q.message.reply_text(
        f"{_user_name(q)}\nاذا لديك ملاحظة ارسلها الان او اختر تخطي للانتقال لاختيار طريقة التسليم",
        reply_markup=prepay_notes_kb(),
        disable_web_page_preview=True,
    )
    return


async def partno_skip_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await _alert(q, "")
    user_id = q.from_user.id
    ud = get_ud(context, user_id)

    if ud.get(STAGE_KEY) != STAGE_ASK_ITEM_PARTNO:
        return

    pending_name = _norm(ud.get("pending_item_name", ""))
    if not pending_name:
        set_stage(context, user_id, STAGE_ASK_ITEM_NAME)
        await q.message.reply_text(f"{_user_name(q)}\nاكتب اسم القطعة اولا")
        return

    ud.setdefault("items", []).append({
        "name": pending_name,
        "part_no": "",
        "photo_file_id": "",
        "created_at_utc": utc_now_iso(),
    })
    ud.pop("pending_item_name", None)
    ud["pending_item_idx"] = len(ud["items"]) - 1

    set_stage(context, user_id, STAGE_ASK_ITEM_PHOTO)
    item_no = len(ud["items"])
    await q.message.reply_text(
        f"{_user_name(q)}\nتمت اضافة القطعة رقم {item_no}\nارسل صورة الان (اختياري) او اكتب اسم القطعة التالية مباشرة",
        reply_markup=photo_prompt_kb(),
    )


async def skip_photo_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await _alert(q, "")
    user_id = q.from_user.id
    ud = get_ud(context, user_id)

    if ud.get(STAGE_KEY) != STAGE_ASK_ITEM_PHOTO:
        await q.message.reply_text(f"{_user_name(q)}\nلا يوجد طلب صورة حاليا")
        return

    items = ud.get("items", []) or []
    idx = ud.get("pending_item_idx")
    try:
        item_no = int(idx) + 1 if isinstance(idx, int) else len(items)
    except Exception:
        item_no = len(items)

    ud.pop("pending_item_idx", None)
    ud.pop("pending_item_name", None)

    set_stage(context, user_id, STAGE_CONFIRM_MORE)
    await q.message.reply_text(
        f"{_user_name(q)}\n"
        f"تم تخطي صورة القطعة رقم {item_no}\n"
        f"عدد القطع الحالي: {len(items)}\n\n"
        "يمكنك الان كتابة اسم قطعة جديدة مباشرة\n"
        "او اختيار انهاء وارسال للدفع",
        reply_markup=more_kb(),
    )
    

async def copy_iban_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await _alert(q, f"تم تجهيز الايبان يا {_user_name(q)}")
    await q.message.reply_text(f"IBAN:\n`{PP_IBAN}`", parse_mode="Markdown")
    

async def copy_beneficiary_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await _alert(q, f"تم تجهيز اسم المستفيد يا {_user_name(q)}")
    await q.message.reply_text(f"اسم المستفيد:\n`{PP_BENEFICIARY}`", parse_mode="Markdown")


async def copy_stc_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await _alert(q, f"تم تجهيز رقم STC Pay يا {_user_name(q)}")
    await q.message.reply_text(f"رقم STC Pay:\n`{PP_STC_PAY}`", parse_mode="Markdown")


async def pay_bank_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await _alert(q, f"تمام يا {_user_name(q)}")
    user_id = q.from_user.id
    ud = get_ud(context, user_id)

    stage = ud.get(STAGE_KEY)
    if stage == STAGE_TRADER_SUB_AWAIT_PAY_METHOD:
        month = str(ud.get("sub_month") or month_key_utc()).strip()
        amount = int(float(ud.get("sub_amount_sar") or 99))
        ud["sub_payment_method"] = "bank_transfer"
        set_stage(context, user_id, STAGE_TRADER_SUB_AWAIT_RECEIPT)
        try:
            upsert_trader_subscription(user_id, month, {
                "amount_sar": amount,
                "payment_method": "bank_transfer",
                "payment_status": "pending",
            })
        except Exception:
            pass

        await q.message.reply_text(
            f"🤍 اهلا {_user_name(q)}\n\n"
            f"💳 <b>طريقة الدفع: تحويل بنكي</b>\n\n"
            f"المبلغ المطلوب <b>{amount} ريال</b> مقابل <b>رسوم اشتراك المنصة</b> لشهر {month}\n\n"
            f"🏦 <b>المستفيد</b>:\n<i>{PP_BENEFICIARY}</i>\n\n"\
            f"IBAN:\n<code>{PP_IBAN}</code>\n\n"\

            f"🧾 <b>رقم المرجع</b>:\n<code>SUB-{user_id}-{month}</code>\n\n"
            "📸 بعد الدفع أرسل <b>صورة/ملف الإيصال</b> هنا مباشرة (الايصال الزامي)\n",
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
        return

    ud["payment_method"] = "bank_transfer"
    set_stage(context, user_id, STAGE_AWAIT_RECEIPT)

    _save_order_once(ud)
    update_order_fields(ud["order_id"], {"payment_method": "bank_transfer", "payment_status": "pending"})

    await q.message.reply_text(
        f"🤍 اهلا { _user_name(q) }\n\n"
        "💳 <b>طريقة الدفع: تحويل بنكي</b>\n\n"
        f"المبلغ المطلوب <b>{ud.get('price_sar', 0)} ريال</b> هو مقابل خدمة احترافية تشمل\n"
        "البحث الدقيق عن القطع المطلوبة حسب بيانات سيارتك\n"
        "والتحقق من التوافق والتوفر وإصدار تسعيرة واضحة قبل تنفيذ الطلب\n\n"
        "هدفنا ان تصل الى القطعة والتسعيرة من خلال شركات السيارات او وكلاء محليين / عالميين باسرع وقت وتلقي عروض مختلفة\n"
        f"🏦 <b>المستفيد</b>:\n<i>{PP_BENEFICIARY}</i>\n\n"
        f"IBAN:\n<code>{PP_IBAN}</code>\n\n"
        f"🧾 <b>رقم المرجع</b>:\n<code>{ud.get('order_id','')}</code>\n\n"
        "📸 بعد التحويل يرجى ارسال <b>صورة ايصال الدفع</b> هنا مباشرة\n"
        "لاستكمال الطلب (الايصال الزامي)\n\n"
        "✨ سعداء بخدمتك وملتزمون بتقديم تجربة موثوقة وواضحة",
        parse_mode="HTML",
        reply_markup=bank_info_kb()
    )


async def pay_stc_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await _alert(q, f"تمام يا {_user_name(q)}")
    user_id = q.from_user.id
    ud = get_ud(context, user_id)

    stage = ud.get(STAGE_KEY)
    if stage == STAGE_TRADER_SUB_AWAIT_PAY_METHOD:
        month = str(ud.get("sub_month") or month_key_utc()).strip()
        amount = int(float(ud.get("sub_amount_sar") or 99))
        ud["sub_payment_method"] = "stc_pay"
        set_stage(context, user_id, STAGE_TRADER_SUB_AWAIT_RECEIPT)
        try:
            upsert_trader_subscription(user_id, month, {
                "amount_sar": amount,
                "payment_method": "stc_pay",
                "payment_status": "pending",
            })
        except Exception:
            pass

        await q.message.reply_text(
            f"🤍 اهلا {_user_name(q)}\n\n"
            f"💳 <b>طريقة الدفع: STC Pay</b>\n\n"
            f"المبلغ المطلوب <b>{amount} ريال</b> مقابل <b>رسوم اشتراك المنصة</b> لشهر {month}\n\n"
            f"📱 <b>رقم STC Pay</b>:\n<code>{PP_STC_PAY}</code>\n\n"\

            f"🧾 <b>رقم المرجع</b>:\n<code>SUB-{user_id}-{month}</code>\n\n"
            "📸 بعد الدفع أرسل <b>صورة/ملف الإيصال</b> هنا مباشرة (الايصال الزامي)\n",
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
        return

    ud["payment_method"] = "stc_pay"
    set_stage(context, user_id, STAGE_AWAIT_RECEIPT)

    _save_order_once(ud)
    update_order_fields(ud["order_id"], {"payment_method": "stc_pay", "payment_status": "pending"})

    await q.message.reply_text(
        f"🤍 اهلا { _user_name(q) }\n\n"
        "💳 <b>طريقة الدفع: STC Pay</b>\n\n"
        f"المبلغ المطلوب <b>{ud.get('price_sar', 0)} ريال</b> هو مقابل خدمة احترافية تشمل\n"
        "البحث الدقيق عن القطع المطلوبة حسب بيانات سيارتك\n"
        "والتحقق من التوافق والتوفر وإصدار تسعيرة واضحة قبل تنفيذ الطلب\n\n"
        "نحرص ان تكمل العملية وانت مطمئن تماما 🤝\n\n"
        f"📱 <b>رقم STC Pay</b>:\n<code>{PP_STC_PAY}</code>\n\n"
        f"🧾 <b>رقم المرجع</b>:\n<code>{ud.get('order_id','')}</code>\n\n"
        "📸 بعد التحويل يرجى ارسال <b>صورة ايصال الدفع</b> هنا مباشرة\n"
        "لاستكمال الطلب (الايصال الزامي)\n\n"
        "✨ سعداء بخدمتك وملتزمون بتقديم تجربة موثوقة وواضحة",
        parse_mode="HTML",
        reply_markup=stc_info_kb()
)

async def pay_link_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await _alert(q, "")
    user_id = q.from_user.id
    ud = get_ud(context, user_id)

    stage = ud.get(STAGE_KEY)
    if stage == STAGE_TRADER_SUB_AWAIT_PAY_METHOD:
        month = str(ud.get("sub_month") or month_key_utc()).strip()
        amount = int(float(ud.get("sub_amount_sar") or 99))
        ud["sub_payment_method"] = "pay_link"
        set_stage(context, user_id, STAGE_TRADER_SUB_AWAIT_RECEIPT)
        try:
            upsert_trader_subscription(user_id, month, {
                "amount_sar": amount,
                "payment_method": "pay_link",
                "payment_status": "pending",
            })
        except Exception:
            pass

        if PP_PAY_LINK_URL:
            await q.message.reply_text(
                f"🔗 <b>رابط دفع الاشتراك</b>\n\n{html.escape(PP_PAY_LINK_URL)}\n\n"
                f"المرجع: <code>SUB-{user_id}-{month}</code>\n"
                "بعد الدفع أرسل صورة/ملف الإيصال هنا (الايصال الزامي)",
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
            return

        try:
            cap = (
                "🔗 <b>طلب رابط دفع (اشتراك تاجر)</b>\n"
                f"👤 التاجر: <b>{html.escape(str(q.from_user.full_name or q.from_user.first_name or ''))}</b>\n"
                f"🆔 trader_id: <code>{user_id}</code>\n"
                f"📅 الشهر: <b>{html.escape(month)}</b>\n"
                f"💰 المبلغ: <b>{amount}</b> ريال\n\n"
                "الصق رابط الدفع وارسله للتاجر."
            )
            for aid in ADMIN_IDS:
                try:
                    await context.bot.send_message(chat_id=aid, text=cap, parse_mode="HTML", disable_web_page_preview=True)
                except Exception:
                    pass
        except Exception:
            pass

        await q.message.reply_text(
            f"{_user_name(q)}\n"
            "✅ تم تسجيل طلب رابط الدفع للاشتراك\n"
            "سيتم تزويدك بالرابط قريبًا\n"
            "بعد الدفع أرسل الإيصال هنا",
            disable_web_page_preview=True,
        )
        return

    ud["payment_method"] = "pay_link"
    set_stage(context, user_id, STAGE_AWAIT_RECEIPT)

    # حفظ الطلب مرة واحدة
    try:
        _save_order_once(ud)
    except Exception:
        pass

    order_id = (ud.get("order_id") or "").strip()
    if not order_id:
        await q.message.reply_text(f"{_user_name(q)}\n🟥 تعذر تحديد رقم الطلب")
        return

    try:
        update_order_fields(order_id, {
            "payment_method": "pay_link",
            "payment_status": "pending",
        })
    except Exception:
        pass

    # ✅ في حال وجود رابط ثابت
    if PP_PAY_LINK_URL:
        await q.message.reply_text(
            "طريقة الدفع: رابط دفع سريع\n\n"
            f"{PP_PAY_LINK_URL}\n\n"
            f"المرجع: {order_id}\n"
            "بعد الدفع ارسل صورة ايصال الدفع هنا (الايصال الزامي)",
            disable_web_page_preview=True,
        )
        return

    # ❗ بدون رابط ثابت → طلب يدوي من الإدارة
    try:
        # جلب نسخة الطلب للمعاينة
        try:
            b = get_order_bundle(order_id)
            order = b.get("order", {}) or {}
        except Exception:
            order = {}

        cap = _build_admin_order_caption(
            order_id,
            ud,
            order,
            "🔗 طلب رابط دفع يدوي (رسوم المنصة)",
            extra_lines=[
                "المطلوب: اضغط الزر ثم الصق رابط الدفع ليتم إرساله للعميل"
            ],
        )

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(
                "📩 إرسال رابط الدفع للعميل",
                callback_data=f"pp_admin_paylink|{order_id}|{user_id}"
            )],
        ])

        for aid in ADMIN_IDS:
            try:
                await context.bot.send_message(
                    chat_id=aid,
                    text=cap,
                    reply_markup=kb,
                    disable_web_page_preview=True,
                )
            except Exception:
                pass

    except Exception:
        pass

    await q.message.reply_text(
        f"{_user_name(q)}\n"
        "✅ تم تسجيل طلب الدفع بالرابط\n"
        "سيتم تزويدك برابط الدفع قريبًا داخل المنصة\n"
        "بعد السداد أرسل صورة الإيصال هنا",
        disable_web_page_preview=True,
    )

async def admin_paylink_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """الإدارة تضغط زر (إرسال رابط الدفع) ثم تلصق الرابط ليتم إرساله للعميل."""
    q = update.callback_query
    await _alert(q, "")

    data = (q.data or "").strip()
    parts = data.split("|")
    if len(parts) < 3:
        return

    order_id = (parts[1] or "").strip()
    try:
        client_id = int(parts[2] or 0)
    except Exception:
        client_id = 0

    actor_id = q.from_user.id
    if actor_id not in ADMIN_IDS:
        await _alert(q, "⛔ غير مصرح")
        return

    if not order_id or not client_id:
        await _alert(q, "تعذر تحديد الطلب/العميل")
        return

    ud = get_ud(context, actor_id)
    ud["paylink_order_id"] = order_id
    ud["paylink_client_id"] = client_id
    set_stage(context, actor_id, STAGE_ADMIN_SEND_PAYLINK)

    await q.message.reply_text(
        f"{_user_name(q)}\n🟦 ارسل الآن رابط الدفع (نص فقط)\n🧾 رقم الطلب: {order_id}",
        disable_web_page_preview=True,
    )

async def quote_ok_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await _alert(q, "")

    data = (q.data or "").strip()
    parts = data.split("|")
    order_id = parts[1].strip() if len(parts) >= 2 else ""

    cb_tid = 0
    if len(parts) >= 3:
        try:
            cb_tid = int(parts[2] or 0)
        except Exception:
            cb_tid = 0

    if not order_id:
        return

    b = get_order_bundle(order_id)
    order = b.get("order", {}) or {}

    gps = str(order.get("goods_payment_status") or "").strip().lower()
    ost = str(order.get("order_status") or "").strip().lower()
    locked_now = str(order.get("quote_locked") or "").strip().lower() == "yes"

    # ✅ بعد الدفع/القفل: ممنوع قبول جديد
    if gps in ("awaiting_confirm", "confirmed") or locked_now or ost in ("closed", "delivered"):
        await q.message.reply_text(f"{_user_name(q)}\n🔒 الطلب مغلق/مدفوع ولا يمكن قبول عروض جديدة")
        return

    if not cb_tid:
        await q.message.reply_text(
            f"{_user_name(q)}\n"
            "⚠️ هذا زر قديم ولا يحتوي هوية التاجر.\n"
            "افتح آخر رسالة عرض سعر ثم اضغط زر القبول منها."
        )
        return

    tid = int(cb_tid or 0)
    if tid <= 0:
        await q.message.reply_text(f"{_user_name(q)}\nلا يوجد تاجر مرسل عرض سعر لهذا الطلب")
        return

    prev_tid = 0
    try:
        prev_tid = int(order.get("accepted_trader_id") or 0)
    except Exception:
        prev_tid = 0

    # ✅ اسم التاجر الجديد (لإشعار العميل فقط)
    try:
        tprof = get_trader_profile(tid) or {}
    except Exception:
        tprof = {}
    tname = (tprof.get("display_name") or "").strip() or (order.get("quoted_trader_name") or "").strip() or "التاجر"
    tcompany = (tprof.get("company_name") or "").strip()
    tlabel = tname + (f" ({tcompany})" if tcompany else "")

    # ✅ اسم التاجر السابق (لإشعار العميل فقط)
    prev_label = ""
    if prev_tid:
        try:
            pp = get_trader_profile(int(prev_tid)) or {}
            pn = (pp.get("display_name") or "").strip() or (order.get("accepted_trader_name") or "").strip() or "التاجر"
            pc = (pp.get("company_name") or "").strip()
            prev_label = pn + (f" ({pc})" if pc else "")
        except Exception:
            prev_label = (order.get("accepted_trader_name") or "").strip() or "التاجر"

    # ✅ عدول تلقائي: يكفي وجود تاجر مقبول سابقًا مختلف عن الحالي
    switched = bool(prev_tid and prev_tid != tid)

    # ✅ مدينة الشحن فقط للتاجر (بدون رقم/تفاصيل)
    ship_city = (order.get("ship_city") or "").strip()
    city_line = f"\n🏙️ مدينة التسليم: {ship_city}" if ship_city else ""

    update_order_fields(order_id, {
        "quote_status": "accepted",
        "accepted_trader_id": tid,
        "accepted_trader_name": tname,
        "accepted_at_utc": utc_now_iso(),
    })

    # إشعار التاجر الجديد (خاص فقط) — بدون رقم العميل
    try:
        await context.bot.send_message(
            chat_id=tid,
            text=(
                "✅ تم قبول عرض السعر من العميل\n"
                f"🧾 رقم الطلب: {order_id}"
                f"{city_line}\n"
                "سيتم تزويدك بإشعار عند إرسال إثبات الدفع.\n\n"
                "🧰 ابدأ تجهيز الطلب ثم حدّث الحالة من لوحة التحكم\n\n"
                "⚠️ ملاحظة: معلومات التواصل/العنوان الكامل لا تُعرض قبل الدفع."
            ),
            reply_markup=trader_status_kb(order_id),
            disable_web_page_preview=True,
        )
    except Exception:
        pass

    # ✅ منع التداخل: ثبت أنه تم إشعار التاجر بالقبول بالفعل
    try:
        update_order_fields(order_id, {"accepted_trader_notified": "yes"})
    except Exception:
        pass

    # إشعار التاجر السابق عند العدول (خاص فقط) — بدون ذكر التاجر الجديد + زر عرض جديد
    if switched:
        try:
            await context.bot.send_message(
                chat_id=prev_tid,
                text=(
                    "ℹ️ تم إلغاء موافقة العميل على عرضك\n"
                    f"🧾 رقم الطلب: {order_id}"
                    f"{city_line}\n\n"
                    "وصل للعميل عرض أفضل وتم اختيار عرض آخر.\n"
                    "نعتذر لك، ويمكنك تقديم عرض جديد إذا رغبت (طالما لم يتم الدفع).\n\n"
                    "⚠️ تنبيه: لا يتم عرض رقم العميل قبل الدفع."
                ),
                reply_markup=trader_quote_start_kb(order_id),
                disable_web_page_preview=True,
            )
        except Exception:
            pass

    # إشعار العميل (خاص فقط)
    try:
        msg = (
            f"{_user_name(q)}\n"
            f"✅ تم قبول عرض السعر\n"
            f"🧾 رقم الطلب: {order_id}\n"
            f"👤 التاجر: {tlabel}\n\n"
            "📌 يمكنك العدول واختيار عرض آخر طالما لم يتم الدفع.\n"
            "عند الدفع سيتم قفل الطلب تلقائيًا ومنع العروض الجديدة."
        )
        if switched and prev_label:
            msg += f"\n\nℹ️ تم إلغاء الموافقة السابقة تلقائيًا عن: {prev_label}"
        await q.message.reply_text(msg, disable_web_page_preview=True)
    except Exception:
        pass


async def quote_no_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await _alert(q, "")

    data = (q.data or "").strip()
    parts = data.split("|")

    order_id = (parts[1] or "").strip() if len(parts) >= 2 else ""
    btn_tid = 0
    if len(parts) >= 3:
        try:
            btn_tid = int(parts[2] or 0)
        except Exception:
            btn_tid = 0

    if not order_id:
        return

    try:
        b = get_order_bundle(order_id)
        order = b.get("order", {}) or {}
        _ = b.get("items", []) or []
    except Exception:
        order = {}

    if not order:
        try:
            await q.message.reply_text(f"{_user_name(q)}\nتعذر قراءة بيانات الطلب")
        except Exception:
            pass
        return

    gps = str(order.get("goods_payment_status") or "").strip().lower()
    ost = str(order.get("order_status") or "").strip().lower()
    locked_now = str(order.get("quote_locked") or "").strip().lower() == "yes"
    if gps in ("awaiting_confirm", "confirmed") or ost in ("closed", "delivered") or locked_now:
        try:
            await q.message.reply_text(f"{_user_name(q)}\n🔒 لا يمكن رفض العرض بعد الدفع/قفل الطلب")
        except Exception:
            pass
        return

    # ✅ حماية: لو الزر قديم ولا يحمل هوية التاجر لا نرسل إشعار لتاجر خاطئ
    tid = int(btn_tid or 0)
    if not tid:
        try:
            await q.message.reply_text(
                f"{_user_name(q)}\n"
                "⚠️ هذا زر قديم ولا يحتوي هوية التاجر.\n"
                "افتح آخر رسالة عرض سعر ثم اضغط زر (غير موافق) منها."
            )
        except Exception:
            pass
        return

    # ✅ مدينة التسليم فقط للتاجر (بدون رقم/تفاصيل)
    ship_city = (order.get("ship_city") or "").strip()
    city_line = f"\n🏙️ مدينة التسليم: {ship_city}" if ship_city else ""

    # ✅ تسجيل الرفض وفتح الباب لعروض أخرى
    try:
        update_order_fields(order_id, {
            "quote_status": "rejected",
            "accepted_trader_id": "",
            "accepted_trader_name": "",
            "quoted_trader_id": "",
            "quoted_trader_name": "",
            "quote_locked": "no",
            "last_group_broadcast_at_utc": utc_now_iso(),
        })
    except Exception:
        pass

    # ✅ إشعار التاجر صاحب العرض فقط (بدون ذكر أي تاجر آخر، وبدون رقم العميل) + زر عرض جديد
    try:
        await context.bot.send_message(
            chat_id=tid,
            text=(
                "❌ لم يوافق العميل على عرض السعر\n"
                f"🧾 رقم الطلب: {order_id}"
                f"{city_line}\n\n"
                "يمكنك تقديم عرض جديد إذا رغبت (طالما لم يتم الدفع).\n"
                "⚠️ تنبيه: معلومات التواصل/العنوان الكامل لا تُعرض قبل الدفع."
            ),
            reply_markup=trader_quote_start_kb(order_id),
            disable_web_page_preview=True,
        )
    except Exception:
        pass

    # ✅ إشعار العميل
    try:
        await q.message.reply_text(
            f"{_user_name(q)}\n"
            "تم تسجيل عدم الموافقة.\n"
            "يمكنك اختيار عرض آخر من العروض المتاحة.",
            disable_web_page_preview=True,
        )
    except Exception:
        pass

    # ✅ لا يوجد أي إرسال للمجموعة نهائيًا
    await _alert(q, "تم")


async def ppq_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    data = q.data or ""
    parts = data.split("|")
    action = parts[0]

    user_id = q.from_user.id
    name = _user_name(q)
    td = context.user_data.setdefault(user_id, {})

    # ✅ كل خطوات عرض السعر تكون بالخاص فقط
    if q.message and q.message.chat and q.message.chat.type != "private":
        await _alert(q, "افتح الخاص لاكمال عرض السعر")
        return

    # ✅ حارس واحد فقط: منع التاجر الموقوف من (بناء/تعديل/إرسال) عروض السعر
    if _trader_is_disabled(user_id):
        await _deny_disabled_trader_q(q, "لا يمكنك تقديم أو تعديل عروض السعر لأن حسابك موقوف")
        return

    if action == "ppq_begin":
        if len(parts) < 2:
            return
        order_id = parts[1]

        # منع بناء عرض سعر اذا الطلب مقفول / ملغي / بعد سداد قيمة القطع
        try:
            ob = get_order_bundle(order_id)
            oo = ob.get("order", {}) or {}
        except Exception:
            oo = {}

        order_status = str(oo.get("order_status") or "").strip().lower()
        quote_locked = str(oo.get("quote_locked") or "").strip().lower()
        goods_pay_status = str(oo.get("goods_payment_status") or "").strip().lower()

        # ✅ امنع عرض السعر لو الطلب مقفل/ملغي
        if (
            quote_locked in ("1", "true", "yes", "on")
            or order_status in ("closed", "delivered", "canceled", "cancelled", "ملغي")
            or goods_pay_status in ("awaiting_confirm", "confirmed")
        ):
            await _alert(q, "🔒 الطلب منتهي/مغلق ولا يقبل عروض جديدة")
            return

        td["quote_order_id"] = order_id
        td["quote_step"] = "amount"
        set_stage(context, user_id, STAGE_TRADER_SET_QUOTE)
        await q.message.reply_text(f"{name}\nاكتب مبلغ القطع بالريال (ارقام فقط)")
        return

    # كل الاكشنات التالية تتطلب order_id
    if len(parts) < 2:
        return
    order_id = parts[1]
    td["quote_order_id"] = order_id

    if action == "ppq_type":
        if len(parts) < 3:
            return
        td["quote_parts_type"] = parts[2]
        td["quote_step"] = "shipping_method"
        try:
            await q.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        await q.message.reply_text(f"{name}\nاختر طريقة الشحن", reply_markup=trader_quote_shipping_method_kb(order_id))
        return

    if action == "ppq_ship":
        if len(parts) < 3:
            return
        td["quote_ship_method"] = parts[2]
        # بدل اسم شركة الشحن: نسأل هل الشحن مشمول ثم (عند عدم الشمول) نطلب قيمة الشحن
        td.pop("quote_ship_carrier", None)
        td.pop("quote_shipping_fee", None)
        td["quote_step"] = "shipinc"
        try:
            await q.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        await q.message.reply_text(
            f"{name}\n🟦 هل السعر يشمل الشحن؟",
            parse_mode="Markdown",
            reply_markup=trader_quote_shipping_included_kb(order_id),
            disable_web_page_preview=True,
        )
        return

    if action == "ppq_shipinc":
        if len(parts) < 3:
            return
        v_inc = parts[2]
        td["quote_ship_included"] = v_inc
        if v_inc == "yes":
            # مشمولة -> قيمة الشحن = 0 ثم ننتقل لمدة التجهيز
            td["quote_shipping_fee"] = "0"
            td["quote_step"] = "availability"
            try:
                await q.message.edit_reply_markup(reply_markup=None)
            except Exception:
                pass
            await q.message.reply_text(f"{name}\nحدد مدة التجهيز", reply_markup=trader_quote_availability_kb(order_id))
            return

        # غير مشمولة -> اطلب قيمة الشحن بالأرقام
        td["quote_step"] = "shipping_fee"
        try:
            await q.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        await q.message.reply_text(
            f"{name}\nاكتب قيمة الشحن بالريال (ارقام فقط)\nمثال: 25 أو 40.5",
            disable_web_page_preview=True,
        )
        return

    if action == "ppq_eta":
        if len(parts) < 3:
            return
        v = parts[2]
        if v == "custom":
            td["quote_step"] = "eta_custom"
            try:
                await q.message.edit_reply_markup(reply_markup=None)
            except Exception:
                pass
            await q.message.reply_text(f"{name}\nاكتب مدة الشحن مثلا 2-3 ايام")
            return

        td["quote_ship_eta"] = v
        td["quote_step"] = "done"
        try:
            await q.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        await finalize_quote_send(context, user_id, q.message, order_id)
        return

    if action == "ppq_avail":
        if len(parts) < 3:
            return
        v = parts[2]
        if v == "custom":
            td["quote_step"] = "avail_custom"
            try:
                await q.message.edit_reply_markup(reply_markup=None)
            except Exception:
                pass
            await q.message.reply_text(f"{name}\nاكتب مدة التجهيز مثلا 5 ايام")
            return

        td["quote_availability"] = v
        td["quote_step"] = "eta"
        try:
            await q.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        await q.message.reply_text(f"{name}\nحدد مدة الشحن", reply_markup=trader_quote_eta_kb(order_id))
        return


async def finalize_quote_send(context: ContextTypes.DEFAULT_TYPE, trader_id: int, message, order_id: str):
    td = context.user_data.setdefault(int(trader_id or 0), {})

    # 🔧 وضع الصيانة: منع ارسال عروض جديدة (لغير الادمن)
    if _is_maintenance_mode() and int(trader_id or 0) not in ADMIN_IDS:
        try:
            await message.reply_text(
                f"{_user_name(message)}\n🟧 المنصة في وضع الصيانة حاليا\nتم ايقاف ارسال عروض السعر مؤقتا"
            )
        except Exception:
            pass
        return

    # ✅ حماية: لا تسمح بإرسال عرض إذا الطلب مقفول/مقبول/مدفوع
    try:
        b0 = get_order_bundle(order_id)
        o0 = b0.get("order", {}) or {}
    except Exception:
        o0 = {}

    try:
        accepted_tid0 = int(o0.get("accepted_trader_id") or 0)
    except Exception:
        accepted_tid0 = 0

    locked0 = str(o0.get("quote_locked") or "").strip().lower() == "yes"
    qst0 = str(o0.get("quote_status") or "").strip().lower()
    gps0 = str(o0.get("goods_payment_status") or "").strip().lower()
    ost0 = str(o0.get("order_status") or "").strip().lower()

    if locked0 or gps0 in ("awaiting_confirm", "confirmed") or ost0 in ("closed", "delivered"):
        try:
            tname = (message.from_user.full_name or "").strip() if message and message.from_user else ""
            await message.reply_text(
                f"{_user_name(message)}\n"
                "⛔ هذا الطلب مقفول ولا يقبل عروض جديدة.\n"
                f"رقم الطلب: {order_id}"
            )
        except Exception:
            pass
        return

    goods_amount = str(td.get("quote_goods_amount") or "").strip()
    parts_type = str(td.get("quote_parts_type") or "").strip()
    ship_method = str(td.get("quote_ship_method") or "").strip()
    ship_inc = str(td.get("quote_ship_included") or "").strip() or "no"
    fee_sar = str(td.get("quote_shipping_fee") or "").strip()
    if not fee_sar:
        fee_sar = "0" if ship_inc == "yes" else "25"
    ship_eta = str(td.get("quote_ship_eta") or "").strip() or "غير محدد"
    availability = str(td.get("quote_availability") or "").strip() or ship_eta

    if not goods_amount or not parts_type or not ship_method:
        try:
            await message.reply_text(f"{_user_name(message)}\nنقص في بيانات العرض اعد المحاولة من زر البدء")
        except Exception:
            pass
        return

    ship_block = build_legal_shipping_block(ship_method, fee_sar, ship_eta, ship_inc)
    official = build_official_quote_text(order_id, goods_amount, parts_type, ship_block, availability)

    # ✅ بيانات التاجر من لوحة التاجر (الاسم + الشركة)
    trader_profile = {}
    try:
        trader_profile = get_trader_profile(int(trader_id or 0)) or {}
    except Exception:
        trader_profile = {}

    trader_display = (trader_profile.get("display_name") or "").strip()
    if not trader_display:
        trader_display = (message.from_user.full_name or "").strip() if message and message.from_user else "تاجر"

    trader_company = (trader_profile.get("company_name") or "").strip()

    trader_header = f"👤 التاجر: {trader_display}"
    if trader_company:
        trader_header += f"\n🏢 المتجر: {trader_company}"

    # ✅ نقل اسم التاجر: بعد مدة التجهيز وقبل "يرجى مراجعة العرض"
    official_with_trader = official
    try:
        anchor = "يرجى مراجعة العرض"
        if anchor in official_with_trader:
            official_with_trader = official_with_trader.replace(
                anchor,
                f"{trader_header}\n\n{anchor}",
                1
            )
        else:
            anchor2 = "يرجى مراجعة"
            if anchor2 in official_with_trader:
                official_with_trader = official_with_trader.replace(
                    anchor2,
                    f"{trader_header}\n\n{anchor2}",
                    1
                )
            else:
                # fallback: لو تغير النص داخل build_official_quote_text
                official_with_trader = official_with_trader.rstrip() + "\n\n" + trader_header
    except Exception:
        official_with_trader = official

    # ✅ حفظ: ثبّت quoted_trader_id = trader_id (مو message.from_user)
    fields_to_update = {
        "goods_amount_sar": goods_amount,
        "parts_type": _ppq_type_label(parts_type),
        "ship_method": _ppq_ship_label(ship_method),
        "shipping_fee_sar": fee_sar,
        "ship_eta": ship_eta,
        "ship_included": "مشمولة" if ship_inc == "yes" else "غير مشمولة",
        "availability_days": availability,
        "quoted_trader_id": int(trader_id or 0),
        "quoted_trader_name": trader_display,
    # ✅ لا تفك القفل هنا أبداً
    # "quote_locked": "no",
    }

    # ✅ إذا الطلب كان accepted سابقاً: لا تكسرها بإرجاعه quoted/sent
    if str(o0.get("quote_status") or "").strip().lower() != "accepted":
        fields_to_update["quote_status"] = "sent"
        fields_to_update["order_status"] = "quoted"

    update_order_fields(order_id, fields_to_update)

    # ✅ ارسال للعميل + كيبورد يحمل trader_id
    client_id = 0
    try:
        b = get_order_bundle(order_id)
        order = b.get("order", {}) or {}
        client_id = int(order.get("user_id") or 0)
    except Exception:
        client_id = 0

    if client_id:
        try:
            await context.bot.send_message(
                chat_id=client_id,
                text=f"عزيزي العميل\n{official_with_trader}",
                reply_markup=quote_client_kb(order_id, int(trader_id or 0)),
                disable_web_page_preview=True,
            )
        except Exception:
            pass

    # ✅ نسخة للتاجر (مخصصة): اسم العميل + حذف سطر الأزرار + جملة منطقية للتاجر
    trader_copy = official_with_trader
    try:
        client_real_name = (o0.get("user_name") or "").strip() or "غير محدد"
        trader_copy = f"👤 اسم العميل: {client_real_name}\n\n" + trader_copy

        old_line = "يرجى مراجعة العرض ثم اختيار القرار من الازرار بالاسفل"
        if old_line in trader_copy:
            trader_copy = trader_copy.replace(
                old_line,
                "في حال قبول عرضك من العميل سيتم إرسال لوحة الطلب لك على الخاص",
                1
            )
    except Exception:
        pass

    try:
        await message.reply_text(f"{_user_name(message)}\nتم ارسال عرض السعر للعميل\n\n{trader_copy}")
    except Exception:
        pass

    td["quote_step"] = "done"
    set_stage(context, int(trader_id or 0), STAGE_NONE)


async def trader_status_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await _alert(q, "")
    data = q.data or ""
    try:
        _, st, order_id = data.split("|", 2)
    except Exception:
        return

    order_id = (order_id or "").strip()
    if not order_id:
        return

    # ✅ ترجمة الزر (st) إلى حالة داخلية + اسم عرض عربي
    st_norm = (st or "").strip().lower()
    _st_map = {
        "prep": ("preparing", "جاري تجهيز الطلب"),
        "ready": ("ready_to_ship", "جاهز للشحن"),
        "shipped": ("shipped", "تم الشحن"),
        "delivered": ("delivered", "تم الاستلام"),
        "closed": ("closed", "مغلق"),
    }
    new_status, display_status = _st_map.get(st_norm, (st_norm, ""))

    if not new_status:
        return
    if not display_status:
        # fallback بسيط (بدون تغيير منطق العمل)
        display_status = str(new_status)

    b = get_order_bundle(order_id)
    order = b.get("order", {}) or {}

    accepted_tid = int(order.get("accepted_trader_id") or 0)
    actor_id = q.from_user.id
    actor_name = (q.from_user.first_name or q.from_user.full_name or "").strip() or "عزيزي التاجر"

    # ✅ منع التاجر المعطّل من تحديث حالات الطلب
    if actor_id not in ADMIN_IDS and _trader_is_disabled(actor_id):
        await _deny_disabled_trader_q(q, "لا يمكنك تحديث حالة الطلب لأن حسابك موقوف")
        return

    accepted_name = (order.get("accepted_trader_name") or order.get("quoted_trader_name") or "").strip()
    if not accepted_name and accepted_tid:
        try:
            tp = get_trader_profile(int(accepted_tid)) or {}
            accepted_name = (tp.get("display_name") or "").strip() or (tp.get("company_name") or "").strip()
        except Exception:
            accepted_name = ""
    accepted_name = accepted_name or "تاجر آخر"

    # ✅ سماح للتاجر المقبول فقط او الادمن
    if actor_id not in ADMIN_IDS and actor_id != accepted_tid:
        try:
            await context.bot.send_message(
                chat_id=actor_id,
                text=(
                    f"{actor_name}\n"
                    "🔒 هذا الزر غير متاح لك حاليًا\n\n"
                    f"رقم الطلب: {order_id}\n"
                    f"تم إسناد الطلب إلى: {accepted_name}\n"
                    "لذلك تم تجميد أزرار تحديث الحالة عن حسابك.\n\n"
                    "إذا وصلك طلب جديد على حسابك ستظهر لك الأزرار بشكل طبيعي."
                ),
                disable_web_page_preview=True,
            )
        except Exception:
            pass

        await _alert(q, "تم إرسال تنبيه لك بالخاص")
        return

    # # ✅ شرط الفاتورة قبل (جاهز للشحن) فقط
    if new_status == "ready_to_ship":
        inv_file = (str(order.get("seller_invoice_file_id") or order.get("shop_invoice_file_id") or "")).strip()
        if not inv_file:
            # مهم جداً: تفعيل وضع انتظار رفع فاتورة التاجر حتى يلتقطها media_router
            ud2 = get_ud(context, actor_id)
            ud2["tsu_kind"] = "seller_invoice"   # لازم تكون seller_invoice (مو shop_invoice)
            ud2["tsu_order_id"] = order_id
            set_stage(context, actor_id, STAGE_TRADER_STATUS_UPDATE)

            await q.message.reply_text(
                f"{_user_name(q)}\n"
                "🧾 قبل تحديث الحالة الى (جاهز للشحن) يجب رفع *فاتورة المتجر الرسمية* (PDF أو صورة)\n"
                f"رقم الطلب: {order_id}\n\n"
                "ارسل الفاتورة الآن هنا بالخاص.",
                parse_mode="Markdown",
                disable_web_page_preview=True,
            )
            await _alert(q, "ارسل فاتورة المتجر بالخاص")
            return

    fields: dict = {"order_status": new_status}

    if new_status == "shipped":
        fields["shipped_at_utc"] = utc_now_iso()
        # ⏳ ثبّت مؤقت المراسلة 7 أيام (أول مرة)
        try:
            if not (order.get("chat_expires_at_utc") or "").strip():
                expires = datetime.now(timezone.utc) + timedelta(days=7)
                fields["chat_expires_at_utc"] = expires.isoformat()
        except Exception:
            pass

    if new_status in ("delivered", "closed"):
        fields["closed_at_utc"] = utc_now_iso()

    update_order_fields(order_id, fields)

    try:
        log_event(
            order_id,
            "status_updated",
            actor_role="trader" if actor_id == accepted_tid else "admin",
            actor_id=actor_id,
            actor_name=_user_name(q),
            payload={"order_status": new_status},
        )
    except Exception:
        pass

    # اشعار العميل (خاص فقط)
    client_id = 0
    try:
        client_id = int(order.get("user_id") or 0)
    except Exception:
        client_id = 0

    if client_id:
        try:
            await context.bot.send_message(
                chat_id=client_id,
                text=f"📦 تحديث حالة الطلب رقم {order_id}\nالحالة: {display_status}",
                reply_markup=client_trader_chat_kb(order_id) if _assigned_trader_id(order_id) else None,
            )
        except Exception:
            pass

    # نسخة للادمن (خاص)
    for aid in ADMIN_IDS:
        try:
            await context.bot.send_message(
                chat_id=aid,
                text=(
                    "📌 تحديث حالة من التاجر\n"
                    f"رقم الطلب: {order_id}\n"
                    f"الحالة: {display_status}\n"
                    f"التاجر: {_user_name(q)}"
                ),
            )
        except Exception:
            pass

    # اشعار للتاجر نفسه
    try:
        await context.bot.send_message(
            chat_id=actor_id,
            text=(
                f"{actor_name}\n"
                "✅ تم إرسال تحديث الحالة بنجاح\n"
                f"رقم الطلب: {order_id}\n"
                f"الحالة: {display_status}\n\n"
                "تم إشعار العميل والإدارة بهذا التحديث."
            ),
            disable_web_page_preview=True,
        )
    except Exception:
        pass

    await _alert(q, "تم التحديث")


async def _open_chat_session(context: ContextTypes.DEFAULT_TYPE, order_id: str, client_id: int, trader_id: int):
    if not (client_id and trader_id):
        return
    try:
        await context.bot.send_message(chat_id=client_id, text=f"💬 تم فتح المراسلة الداخلية للطلب {order_id}\nارسل رسالتك هنا وسيتم تمريرها للطرف الاخر")
    except Exception:
        pass
    try:
        await context.bot.send_message(chat_id=trader_id, text=f"💬 تم فتح المراسلة الداخلية للطلب {order_id}\nارسل رسالتك هنا وسيتم تمريرها للعميل")
    except Exception:
        pass
    context.bot_data.setdefault("pp_chat_sessions", {})[str(client_id)] = {"order_id": order_id, "peer_id": trader_id, "role": "client"}
    context.bot_data.setdefault("pp_chat_sessions", {})[str(trader_id)] = {"order_id": order_id, "peer_id": client_id, "role": "trader"}

# ==============================
# ✅ نظام مراسلة محكم (مختصر)
# Admin ↔ Client  |  Admin ↔ Trader
# ==============================

STAGE_ADMIN_CHAT = "pp_admin_chat"
STAGE_TRADER_CHAT_ADMIN = "pp_trader_chat_admin"

def admin_contact_kb(order_id: str) -> InlineKeyboardMarkup:
    # زر مراسلة العميل + التاجر (من رسالة الإدارة)
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("💬 مراسلة العميل", callback_data=f"pp_admin_chat_client|{order_id}"),
            InlineKeyboardButton("🧑‍🔧 مراسلة التاجر", callback_data=f"pp_admin_chat_trader|{order_id}"),
        ],
        [InlineKeyboardButton("✖️ إنهاء", callback_data="pp_admin_chat_done")],
    ])

def trader_chat_admin_kb(order_id: str, admin_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✍️ رد للإدارة", callback_data=f"pp_trader_chat_admin|{order_id}|{admin_id}")],
        [InlineKeyboardButton("✖️ إنهاء", callback_data="pp_trader_chat_admin_done")],
    ])


async def admin_chat_client_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await _alert(q, "")
    actor_id = q.from_user.id
    if actor_id not in ADMIN_IDS:
        await _alert(q, "غير مصرح")
        return

    data = (q.data or "").strip()
    parts = data.split("|", 1)
    if len(parts) != 2:
        return
    order_id = (parts[1] or "").strip()
    if not order_id:
        return

    uid = get_order_user_id(order_id)
    if not uid:
        await _alert(q, "لا يوجد عميل مرتبط بالطلب")
        return

    ud = get_ud(context, actor_id)
    ud["admin_chat_order_id"] = order_id
    ud["admin_chat_peer_id"] = int(uid)
    ud["admin_chat_role"] = "client"
    set_stage(context, actor_id, STAGE_ADMIN_CHAT)

    await q.message.reply_text(
        f"{_user_name(q)}\n🟦 مراسلة العميل\n🧾 رقم الطلب: {order_id}\nاكتب رسالتك الآن وسيتم إرسالها للعميل.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✖️ إنهاء", callback_data="pp_admin_chat_done")]]),
        disable_web_page_preview=True,
    )


async def admin_chat_trader_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await _alert(q, "")
    actor_id = q.from_user.id
    if actor_id not in ADMIN_IDS:
        await _alert(q, "غير مصرح")
        return

    data = (q.data or "").strip()
    parts = data.split("|", 1)
    if len(parts) != 2:
        return
    order_id = (parts[1] or "").strip()
    if not order_id:
        return

    tid = _assigned_trader_id(order_id)
    if not tid:
        await _alert(q, "لا يوجد تاجر مُسنَد للطلب حتى الآن")
        return

    ud = get_ud(context, actor_id)
    ud["admin_chat_order_id"] = order_id
    ud["admin_chat_peer_id"] = int(tid)
    ud["admin_chat_role"] = "trader"
    set_stage(context, actor_id, STAGE_ADMIN_CHAT)

    await q.message.reply_text(
        f"{_user_name(q)}\n🟨 مراسلة التاجر\n🧾 رقم الطلب: {order_id}\nاكتب رسالتك الآن وسيتم إرسالها للتاجر.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✖️ إنهاء", callback_data="pp_admin_chat_done")]]),
        disable_web_page_preview=True,
    )


async def admin_chat_done_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await _alert(q, "")
    actor_id = q.from_user.id
    if actor_id not in ADMIN_IDS:
        return
    ud = get_ud(context, actor_id)
    ud.pop("admin_chat_order_id", None)
    ud.pop("admin_chat_peer_id", None)
    ud.pop("admin_chat_role", None)
    set_stage(context, actor_id, STAGE_NONE)
    try:
        await q.message.reply_text("تم إنهاء وضع المراسلة.")
    except Exception:
        pass


async def trader_chat_admin_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await _alert(q, "")
    actor_id = q.from_user.id

    data = (q.data or "").strip()
    parts = data.split("|")
    if len(parts) != 3:
        return
    _, order_id, admin_id = parts
    order_id = (order_id or "").strip()
    try:
        admin_id = int(admin_id)
    except Exception:
        admin_id = 0

    if not order_id or not admin_id:
        await _alert(q, "بيانات غير صحيحة")
        return

    ud = get_ud(context, actor_id)
    ud["trader_chat_order_id"] = order_id
    ud["trader_chat_admin_id"] = admin_id
    set_stage(context, actor_id, STAGE_TRADER_CHAT_ADMIN)

    await q.message.reply_text(
        f"{_user_name(q)}\n🟨 رد للإدارة\n🧾 رقم الطلب: {order_id}\nاكتب ردك الآن وسيصل للإدارة.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✖️ إنهاء", callback_data="pp_trader_chat_admin_done")]]),
        disable_web_page_preview=True,
    )

async def trader_chat_admin_done_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await _alert(q, "")
    actor_id = q.from_user.id
    ud = get_ud(context, actor_id)
    ud.pop("trader_chat_order_id", None)
    ud.pop("trader_chat_admin_id", None)
    set_stage(context, actor_id, STAGE_NONE)
    try:
        await q.message.reply_text("تم إنهاء وضع الرد للإدارة.")
    except Exception:
        pass


async def chat_open_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await _alert(q, "")
    data = (q.data or "").strip()

    order_id = data.split("|", 1)[1] if "|" in data else ""
    order_id = (order_id or "").strip()
    if not order_id:
        return

    actor_id = q.from_user.id
    actor_name = (q.from_user.full_name or "").strip()
    actor_first = (q.from_user.first_name or actor_name or "").strip()

    def _to_int(v):
        try:
            return int(v)
        except Exception:
            try:
                return int(float(str(v).strip()))
            except Exception:
                return 0

    # جلب الطلب
    try:
        b = get_order_bundle(order_id)
        order = b.get("order", {}) or {}
        items = b.get("items", []) or []
    except Exception:
        order = {}
        items = []

    client_id = _to_int(order.get("user_id"))
    trader_id = _to_int(order.get("accepted_trader_id"))

    if not client_id:
        await _alert(q, "لا يوجد عميل مرتبط بالطلب")
        return

    # السماح فقط للتاجر المقبول أو الادمن
    if actor_id not in ADMIN_IDS and actor_id != trader_id:
        intruder = actor_first or actor_name or "التاجر"
        await _alert(
            q,
            f"🔒 {intruder}\n"
            "هذا الزر مخصص للتاجر المستلم فقط.\n"
            "تم إيقاف المراسلة لبقية التجار بعد قبول العرض."
        )
        return

    # ============================
    # ⏳ مؤقت المراسلة 7 أيام
    # ============================
    # - إذا وجدنا chat_expires_at_utc نلتزم به.
    # - إذا لم يوجد: نثبته أول مرة بعد (تأكيد الدفع/الشحن/التسليم/الإغلاق).
    if actor_id not in ADMIN_IDS:
        now_utc = datetime.now(timezone.utc)

        expires_raw = (order.get("chat_expires_at_utc") or "").strip()
        expires_dt = None
        if expires_raw:
            try:
                expires_dt = datetime.fromisoformat(expires_raw.replace("Z", "+00:00")).astimezone(timezone.utc)
            except Exception:
                expires_dt = None

        if not expires_dt:
            st = str(order.get("order_status") or "").strip().lower()
            gps = str(order.get("goods_payment_status") or "").strip().lower()

            base_raw = (
                (order.get("shipped_at_utc") or "").strip()
                or (order.get("delivered_at_utc") or "").strip()
                or (order.get("closed_at_utc") or "").strip()
                or (order.get("goods_payment_confirmed_at_utc") or "").strip()
                or (order.get("delivered_at") or "").strip()
                or (order.get("closed_at") or "").strip()
            )
            base_dt = None
            if base_raw:
                try:
                    base_dt = datetime.fromisoformat(base_raw.replace("Z", "+00:00")).astimezone(timezone.utc)
                except Exception:
                    base_dt = None

            if not base_dt and (gps == "confirmed" or st in ("shipped", "delivered", "closed")):
                base_dt = now_utc

            if base_dt:
                expires_dt = base_dt + timedelta(days=7)
                try:
                    update_order_fields(order_id, {"chat_expires_at_utc": expires_dt.isoformat()})
                except Exception:
                    pass

        if expires_dt and now_utc > expires_dt:
            await _alert(q, "🔒 انتهت مدة المتابعة/الاسترجاع (7 أيام) لهذا الطلب")
            return

    # تفعيل وضع الرد (Relay)
    td = context.user_data.setdefault(actor_id, {})
    td["trader_reply_user_id"] = client_id
    td["trader_reply_order_id"] = order_id
    set_stage(context, actor_id, STAGE_TRADER_REPLY)

    # اسم التاجر
    tp = get_trader_profile(actor_id) or {}
    tname = (tp.get("display_name") or "").strip() or actor_first or actor_name or "التاجر"
    tco = (tp.get("company_name") or "").strip()
    tline = f"👤 <b>{html.escape(tname)}</b>" + (f" • 🏢 <b>{html.escape(tco)}</b>" if tco else "")

    # ملخص الطلب
    car = (order.get("car_name") or "").strip()
    model = (order.get("car_model") or "").strip()
    amt = _money(order.get("goods_amount_sar") or "")
    parts_lines = []
    for i, it in enumerate(items, start=1):
        nm = (it.get("name") or "").strip()
        pn = (it.get("part_no") or it.get("item_part_no") or "").strip()
        if nm and pn:
            parts_lines.append(f"{i}- {nm} (رقم: {pn})")
        elif nm:
            parts_lines.append(f"{i}- {nm}")
    parts_txt = "\n".join(parts_lines) if parts_lines else "—"

    msg = (
        "🟦 <b>تم فتح قناة المراسلة مع العميل</b>\n"
        f"{tline}\n"
        f"🧾 رقم الطلب: <b>{html.escape(order_id)}</b>\n"
        + (f"🚗 السيارة: <b>{html.escape((car + ' ' + model).strip())}</b>\n" if (car or model) else "")
        + (f"💰 مبلغ الطلب: <b>{html.escape(amt)}</b>\n" if amt else "")
        + "\n"
        + "🧩 <b>ملخص القطع</b>\n"
        + f"<pre>{html.escape(parts_txt)}</pre>\n"
        + "✍️ اكتب رسالتك الآن وسيتم إرسالها للعميل عبر المنصة."
    )

    await context.bot.send_message(
        chat_id=actor_id,
        text=msg,
        parse_mode="HTML",
        disable_web_page_preview=True,
    )


async def goods_pay_bank_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await _alert(q, f"تمام يا {_user_name(q)}")
    user_id = q.from_user.id
    order_id = (q.data or "").split("|", 1)[1] if "|" in (q.data or "") else ""
    if not order_id:
        return

    # اجلب الطلب
    try:
        b = get_order_bundle(order_id)
        order = b.get("order", {}) or {}
    except Exception:
        order = {}

    amt = order.get("goods_amount_sar")

    # ✅ التاجر المقبول
    tid = 0
    try:
        tid = int(order.get("accepted_trader_id") or 0)
    except Exception:
        tid = 0

    tp = {}
    if tid:
        try:
            tp = get_trader_profile(int(tid)) or {}
        except Exception:
            tp = {}

    t_bank = (tp.get("bank_name") or "").strip()
    t_iban = (tp.get("iban") or "").strip()
    # اسم المستفيد: الشركة ثم اسم التاجر ثم الافتراضي
    t_benef = (tp.get("company_name") or "").strip() or (tp.get("display_name") or "").strip() or ""

    # لو بيانات التاجر ناقصة: نرجع للمنصة مع تنبيه واضح
    beneficiary = t_benef if t_benef else (PP_BENEFICIARY or "—")
    iban = t_iban if t_iban else (PP_IBAN or "—")
    bank_line = f"🏦 <b>البنك</b>:\n<i>{html.escape(t_bank)}</i>\n\n" if t_bank else ""

    ud = get_ud(context, user_id)
    ud["goods_order_id"] = order_id

    try:
        update_order_fields(order_id, {
            "goods_payment_method": "bank_transfer",
            "goods_payment_status": "awaiting_receipt",
        })
    except Exception:
        pass

    set_stage(context, user_id, STAGE_AWAIT_GOODS_RECEIPT)

    warn = ""
    if tid and (not t_iban):
        warn = "\n⚠️ <b>تنبيه</b>: بيانات تحويل التاجر غير مكتملة، تم عرض بيانات المنصة مؤقتًا.\n"

    await q.message.reply_text(
        f"🤍 اهلا { _user_name(q) }\n\n"
        "💳 <b>دفع قيمة البضاعة: تحويل بنكي</b>\n\n"
        f"المبلغ المطلوب <b>{amt} ريال</b> هو قيمة القطع لتجهيز الطلب قبل الشحن\n\n"
        f"{bank_line}"
        f"🏦 <b>المستفيد</b>:\n<i>{html.escape(beneficiary)}</i>\n\n"
        f"IBAN:\n<code>{html.escape(iban)}</code>\n\n"
        f"🧾 <b>رقم المرجع</b>:\n<code>{html.escape(order_id)}</code>\n\n"
        f"{warn}"
        "📸 بعد التحويل يرجى ارسال <b>صورة ايصال الدفع</b> هنا مباشرة\n"
        "لاستكمال تجهيز الطلب (الايصال الزامي)",
        parse_mode="HTML",
        reply_markup=bank_info_kb(),
    )


async def goods_pay_stc_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await _alert(q, f"تمام يا {_user_name(q)}")
    user_id = q.from_user.id
    order_id = (q.data or "").split("|", 1)[1] if "|" in (q.data or "") else ""
    if not order_id:
        return

    b = get_order_bundle(order_id)
    order = b.get("order", {}) or {}
    amt = order.get("goods_amount_sar")

    # ✅ نحدد التاجر المرتبط بالطلب (المقبول)
    try:
        tid = int(order.get("accepted_trader_id") or 0)
    except Exception:
        tid = 0

    stc_number = ""
    if tid:
        try:
            tp = get_trader_profile(int(tid)) or {}
            stc_number = (tp.get("stc_pay") or "").strip()
        except Exception:
            stc_number = ""

    # fallback على رقم المنصة إذا التاجر ما حط رقم
    if not stc_number:
        stc_number = (PP_STC_PAY or "").strip()

    ud = get_ud(context, user_id)
    ud["goods_order_id"] = order_id

    update_order_fields(order_id, {"goods_payment_method": "stc_pay", "goods_payment_status": "awaiting_receipt"})
    set_stage(context, user_id, STAGE_AWAIT_GOODS_RECEIPT)

    await q.message.reply_text(
        f"🤍 اهلا { _user_name(q) }\n\n"
        "💳 <b>دفع قيمة البضاعة: STC Pay</b>\n\n"
        f"المبلغ المطلوب <b>{amt} ريال</b> هو قيمة القطع لتجهيز الطلب قبل الشحن\n\n"
        f"رقم STC Pay:\n<code>{html.escape(str(stc_number))}</code>\n\n"
        f"🧾 <b>رقم المرجع</b>:\n<code>{html.escape(str(order_id))}</code>\n\n"
        "📸 بعد التحويل يرجى ارسال <b>صورة ايصال الدفع</b> هنا مباشرة\n"
        "لاستكمال تجهيز الطلب (الايصال الزامي)",
        parse_mode="HTML",
        reply_markup=stc_info_kb()
    )


async def goods_pay_link_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await _alert(q, "")
    actor_id = q.from_user.id

    data = (q.data or "").strip()
    parts = data.split("|", 1)
    order_id = parts[1].strip() if len(parts) >= 2 else ""
    if not order_id:
        return

    # اجلب الطلب
    try:
        b = get_order_bundle(order_id)
        order = b.get("order", {}) or {}
    except Exception:
        order = {}

    amt = order.get("goods_amount_sar")
    if amt in (None, "", 0, "0"):
        await _alert(q, "لا يوجد مبلغ قيمة بضاعة مسجل لهذا الطلب")
        return

    # اربط مرحلة ايصال قيمة القطع عند العميل
    ud = get_ud(context, actor_id)
    ud["goods_order_id"] = order_id
    set_stage(context, actor_id, STAGE_AWAIT_GOODS_RECEIPT)

    # خزّن طريقة الدفع
    try:
        update_order_fields(order_id, {
            "goods_payment_method": "pay_link",
            "goods_payment_status": "awaiting_receipt",
        })
    except Exception:
        pass

    # رابط ثابت
    if PP_PAY_LINK_URL:
        try:
            await q.message.reply_text(
                "💳 <b>دفع قيمة القطع عبر رابط</b>\n\n"
                f"🔗 {html.escape(PP_PAY_LINK_URL)}\n\n"
                f"💰 <b>المبلغ</b>: {html.escape(str(amt))} ريال\n"
                f"🧾 <b>المرجع</b>: <code>{html.escape(order_id)}</code>\n\n"
                "بعد الدفع ارسل صورة ايصال الدفع هنا (الايصال الزامي)",
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
        except Exception:
            pass
        return

    # بدون رابط ثابت (بدون تكامل): تنبيه واحد واضح + لا صمت
    await _alert(q, "🔗 رابط الدفع غير متوفر حاليا\nاختر تحويل بنكي أو STC Pay")
    return

async def goods_receipt_photo_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    ud = get_ud(context, user_id)

    if ud.get(STAGE_KEY) != STAGE_AWAIT_GOODS_RECEIPT:
        return

    order_id = (ud.get("goods_order_id") or "").strip()
    if not order_id:
        await update.message.reply_text(f"{_user_name(update)}\nلا يوجد طلب مرتبط بالايصال حاليا")
        set_stage(context, user_id, STAGE_NONE)
        return

    photos = update.message.photo or []
    if not photos:
        await update.message.reply_text(f"{_user_name(update)}\nالايصال الزامي ارسل صورة او PDF فقط")
        return

    file_id = photos[-1].file_id

    try:
        update_order_fields(order_id, {
            "goods_receipt_file_id": file_id,
            "goods_receipt_mime": "image/jpeg",
            "goods_payment_status": "awaiting_confirm",
        })
    except Exception:
        pass

    # 🔒 قفل استقبال عروض جديدة فور ارسال الايصال
    try:
        update_order_fields(order_id, {"quote_locked": "yes"})
    except Exception:
        pass
    try:
        await _lock_team_post_keyboard(context, order_id, reason="🔒 تم إيقاف استقبال عروض السعر")
    except Exception:
        pass

    tid = _assigned_trader_id(order_id)

    # ✅ بعد دفع قيمة القطع: نرسل للتاجر العنوان كامل (بدون رقم الهاتف) + زر مراسلة العميل
    try:
        b_addr = get_order_bundle(order_id) or {}
        o_addr = b_addr.get("order", {}) or {}
    except Exception:
        o_addr = {}

    ship_city = (o_addr.get("ship_city") or o_addr.get("pickup_city") or "").strip()
    ship_district = (o_addr.get("ship_district") or "").strip()
    ship_short = (o_addr.get("ship_short_address") or "").strip()
    ship_method = (o_addr.get("delivery_type") or o_addr.get("ship_method") or o_addr.get("delivery_choice") or "").strip()
    delivery_details = (o_addr.get("delivery_details") or "").strip()

    addr_lines = []
    if ship_method:
        addr_lines.append(f"🚚 نوع التسليم: {ship_method}")
    if ship_city:
        addr_lines.append(f"🏙 المدينة: {ship_city}")
    if ship_district:
        addr_lines.append(f"📍 الحي: {ship_district}")
    if ship_short:
        addr_lines.append(f"🧭 العنوان المختصر: {ship_short}")
    if delivery_details:
        # لا نرسل رقم الجوال هنا (يبقى سري) — لكن نرسل بقية تفاصيل العنوان
        safe_details = re.sub(r"(\+?9665\d{8}|9665\d{8}|05\d{8})", "*********", delivery_details)
        addr_lines.append(f"📝 تفاصيل العنوان: {safe_details}")

    addr_block = "\n".join(addr_lines) if addr_lines else "—"

    caption = (
        f"🧾 ايصال قيمة القطع\n"
        f"رقم الطلب: {order_id}\n"
        f"العميل: {ud.get('user_name','')} ({user_id})\n"
        f"{addr_block}\n"
        f"الخطوة التالية: تاكيد الاستلام"
    )

    try:
        await notify_admins_goods_receipt(context, ud, file_id, mime="image/jpeg")
    except Exception:
        pass

    if tid:
        try:
            await context.bot.send_photo(
                chat_id=tid,
                photo=file_id,
                caption=caption,
                reply_markup=trader_goods_receipt_kb(order_id, user_id),
            )
        except Forbidden:
            for aid in ADMIN_IDS:
                try:
                    await context.bot.send_message(
                        chat_id=aid,
                        text=(
                            "⛔ تعذر ارسال ايصال القطع للتاجر (403 Forbidden)\n"
                            f"رقم الطلب: {order_id}\n"
                            f"التاجر: {tid}"
                        ),
                    )
                except Exception:
                    pass
        except BadRequest:
            for aid in ADMIN_IDS:
                try:
                    await context.bot.send_message(
                        chat_id=aid,
                        text=(
                            "⛔ تعذر ارسال ايصال القطع (BadRequest)\n"
                            f"رقم الطلب: {order_id}\n"
                            "file_id غير صالح"
                        ),
                    )
                except Exception:
                    pass
        except Exception:
            pass

    try:
        await _send_client_payment_preview(context, user_id, order_id, pay_scope="goods")
    except Exception:
        pass

    set_stage(context, user_id, STAGE_DONE)
    await update.message.reply_text(f"{_user_name(update)}\nتم استلام ايصال قيمة القطع وسيتم التحقق قبل الشحن")


async def goods_receipt_document_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    ud = get_ud(context, user_id)

    if ud.get(STAGE_KEY) != STAGE_AWAIT_GOODS_RECEIPT:
        return

    order_id = ud.get("goods_order_id", "")
    if not order_id:
        await update.message.reply_text(f"{_user_name(update)}\nلا يوجد طلب مرتبط بالايصال حاليا")
        set_stage(context, user_id, STAGE_NONE)
        return

    doc = update.message.document
    if not doc:
        await update.message.reply_text(f"{_user_name(update)}\nالايصال الزامي ارسل صورة او PDF فقط")
        return

    mime = (doc.mime_type or "").lower()
    fname = (doc.file_name or "").lower()
    is_ok = mime.startswith("image/") or mime.startswith("application/pdf") or fname.endswith((".jpg", ".jpeg", ".png", ".webp", ".pdf"))

    if not is_ok:
        await update.message.reply_text(f"{_user_name(update)}\nالايصال الزامي ارسل صورة او PDF فقط")
        return

    file_id = doc.file_id

    try:
        update_order_fields(order_id, {
            "goods_receipt_file_id": file_id,
            "goods_receipt_mime": mime,
            "goods_payment_status": "awaiting_confirm",
        })
    except Exception:
        pass

    # 🔒 قفل استقبال عروض جديدة فور ارسال الايصال
    try:
        update_order_fields(order_id, {"quote_locked": "yes"})
    except Exception:
        pass
    try:
        await _lock_team_post_keyboard(context, order_id, reason="🔒 تم إيقاف استقبال عروض السعر")
    except Exception:
        pass

    tid = _assigned_trader_id(order_id)

    caption = (
        f"🧾 ايصال قيمة القطع\n"
        f"رقم الطلب: {order_id}\n"
        f"العميل: {ud.get('user_name','')} ({user_id})\n"
        f"الخطوة التالية: تاكيد الاستلام"
    )

    try:
        await notify_admins_goods_receipt(context, ud, file_id, mime=mime)
    except Exception:
        pass

    if tid:
        try:
            await context.bot.send_document(
                chat_id=tid,
                document=file_id,
                caption=caption,
                reply_markup=team_goods_confirm_kb(order_id),
            )
        except Exception:
            pass

    try:
        await _send_client_payment_preview(context, user_id, order_id, pay_scope="goods")
    except Exception:
        pass

    set_stage(context, user_id, STAGE_DONE)
    await update.message.reply_text(f"{_user_name(update)}\nتم استلام ايصال قيمة القطع وسيتم التحقق قبل الشحن")


def _extract_city_from_delivery(details: str) -> str:
    if not details:
        return ""
    m = re.search(r"المدينة\s*:\s*([^\n\r]+)", details)
    if m:
        return (m.group(1) or "").strip()
    return ""

def _delivery_brief(order: dict, ud: dict) -> str:
    ship_method = (str(order.get("ship_method") or "")).strip() or (str(ud.get("ship_method") or "")).strip()
    ship_city = (str(order.get("ship_city") or "")).strip() or (str(ud.get("ship_city") or "")).strip()
    pickup_city = (str(order.get("pickup_city") or "")).strip() or (str(ud.get("pickup_city") or "")).strip()
    pickup_loc = (str(order.get("pickup_location") or "")).strip() or (str(ud.get("pickup_location") or "")).strip()

    d_choice = (str(order.get("delivery_choice") or ud.get("delivery_choice") or "")).strip().lower()
    d_details = (str(order.get("delivery_details") or ud.get("delivery_details") or "")).strip()

    if not ship_method:
        if d_choice == "ship" or "شحن" in d_details:
            ship_method = "شحن"
        elif d_choice == "pickup" or "استلام" in d_details:
            ship_method = "استلام من الموقع"

    if ship_method and not ship_city and ("شحن" in ship_method or d_choice == "ship"):
        ship_city = _extract_city_from_delivery(d_details)

    if ship_method and ("استلام" in ship_method or d_choice == "pickup"):
        if not pickup_city and d_details:
            m = re.search(r"مدينة\s*الاستلام\s*:\s*([^\n\r]+)", d_details)
            if m:
                pickup_city = (m.group(1) or "").strip()
        if not pickup_city:
            pickup_city = _extract_city_from_delivery(d_details)

    if not ship_method and not ship_city and not pickup_city:
        return "<i>غير محدد بعد</i>"

    parts = []
    if ship_method:
        parts.append(f"<b>طريقة التسليم</b>: <i>{html.escape(ship_method)}</i>")
    if ship_city:
        parts.append(f"<b>مدينة التسليم</b>: <i>{html.escape(ship_city)}</i>")
    if pickup_city:
        parts.append(f"<b>مدينة الاستلام</b>: <i>{html.escape(pickup_city)}</i>")
    if pickup_loc and ship_method and "استلام" in ship_method:
        parts.append(f"<b>موقع الاستلام</b>: <i>{html.escape(pickup_loc)}</i>")
    return "\n".join(parts)


async def notify_team(context: ContextTypes.DEFAULT_TYPE, ud: dict):
    if not TEAM_CHAT_ID:
        return

    order_id = (ud.get("order_id") or "").strip()
    if not order_id:
        return

    try:
        b = get_order_bundle(order_id)
        order = b.get("order", {}) or {}
        bundle_items = b.get("items", []) or []
    except Exception:
        order = {}
        bundle_items = []

    # ✅ ملاحظات العميل (fallback)
    notes = _norm(ud.get("notes") or order.get("notes") or "")
    notes_html = f"<i>{html.escape(notes)}</i>" if notes else "<i>—</i>"

    # القطع: من ud أو من bundle
    items = ud.get("items") or bundle_items or []

    # بيانات أساسية
    user_name = (ud.get("user_name") or order.get("user_name") or "").strip()
    user_id = ud.get("user_id") or order.get("user_id") or ""
    car_name = (ud.get("car_name") or order.get("car_name") or "").strip()
    car_model = (ud.get("car_model") or order.get("car_model") or "").strip()
    vin = (ud.get("vin") or order.get("vin") or "").strip()

    # مبالغ
    fee = ud.get("price_sar") or order.get("price_sar") or ""
    goods_amount = order.get("goods_amount_sar") or ""
    ship_fee = order.get("shipping_fee_sar") or ""

    fee_txt = f"{fee} ريال" if str(fee).strip() not in ("", "0", "0.0") else "—"
    goods_txt = f"{goods_amount} ريال" if str(goods_amount).strip() not in ("", "0", "0.0") else "—"
    ship_txt = f"{ship_fee} ريال" if str(ship_fee).strip() not in ("", "0", "0.0") else "—"

    # ✅ بلوك التسليم (كما هو)
    delivery_block = _delivery_brief(order, ud) or "<i>—</i>"

    # ✅ عرض القطع (فخم + مختصر)
    items_lines = []
    media_count = 0
    shown = 0
    for i, it in enumerate(items, start=1):
        nm = (it.get("name") or "").strip()
        if not nm:
            continue

        pn = (it.get("part_no") or it.get("item_part_no") or "").strip()
        has_media = bool(it.get("photo_file_id") or it.get("file_id"))
        if has_media:
            media_count += 1

        # أيقونة حسب وجود صورة
        badge = "🖼️" if has_media else "📄"
        pn_txt = f" <code>{html.escape(pn)}</code>" if pn else ""
        tail = " <i>(بدون صورة)</i>" if not has_media else ""

        items_lines.append(f"{badge} <b>{shown+1}.</b> {html.escape(nm)}{pn_txt}{tail}")
        shown += 1

        if shown >= 10:
            break

    parts_html = "\n".join(items_lines) if items_lines else "<i>—</i>"
    if len(items) > 10:
        parts_html += f"\n<i>✨ قطع إضافية: {len(items) - 10}</i>"

    # شارات سريعة
    car_txt = html.escape((car_name + " " + car_model).strip()) if (car_name or car_model) else "—"
    uname_txt = html.escape(user_name) if user_name else "—"
    uid_txt = html.escape(str(user_id)) if str(user_id).strip() else "—"

    # ✅ رسالة فخمة (بدون خطوط)
    txt = (
        "🚀 <b>طلب قطع غيار جديد</b> ✨\n"
        f"🧾 <b>رقم الطلب</b>: <code>{html.escape(order_id)}</code>\n\n"

        f"👤 <b>العميل</b>: <b>{uname_txt}</b> <code>({uid_txt})</code>\n"
        f"🚗 <b>السيارة</b>: <b>{car_txt}</b>\n"
        + (f"🔎 <b>VIN</b>: <code>{html.escape(vin)}</code>\n" if vin else "")
        + "\n"

        "📝 <b>ملاحظات العميل</b> 🧠\n"
        f"{notes_html}\n\n"

        "📦 <b>التسليم</b> 🏷️\n"
        f"{delivery_block}\n\n"

        f"🧩 <b>القطع المطلوبة</b> 🛠️  <b>({len(items)})</b>\n"
        f"📸 <b>عدد الصور</b>: <b>{media_count}</b>\n"
        f"{parts_html}\n\n"
    )

    team_msg_id = None
    try:
        sent = await context.bot.send_message(
            chat_id=TEAM_CHAT_ID,
            text=txt,
            parse_mode="HTML",
            reply_markup=team_group_kb(order_id, context.bot.username),
            disable_web_page_preview=True,
        )
        team_msg_id = getattr(sent, "message_id", None)
        if team_msg_id:
            try:
                update_order_fields(order_id, {"team_message_id": team_msg_id})
            except Exception:
                pass
    except Exception:
        return

    # ✅ إرسال الوسائط كرد (Album)
    media: list = []
    for i, it in enumerate(items, start=1):
        fid = it.get("photo_file_id") or it.get("file_id") or ""
        if not fid:
            continue

        nm = (it.get("name") or "").strip()
        pn = (it.get("part_no") or it.get("item_part_no") or "").strip()
        caption = f"🧩 قطعة {i}: {nm}" if nm else f"🧩 قطعة {i}"
        if pn:
            caption += f" ({pn})"

        mt = (it.get("media_type") or "photo").strip().lower()
        if mt in ("video", "video_note"):
            media.append(InputMediaVideo(media=fid, caption=caption))
        elif mt in ("document", "audio", "voice"):
            media.append(InputMediaDocument(media=fid, caption=caption))
        else:
            media.append(InputMediaPhoto(media=fid, caption=caption))

    if not media:
        return

    for chunk_start in range(0, len(media), 10):
        chunk = media[chunk_start:chunk_start + 10]
        try:
            await context.bot.send_media_group(
                chat_id=TEAM_CHAT_ID,
                media=chunk,
                reply_to_message_id=team_msg_id,
            )
        except Exception:
            pass
                
def _parse_item_name_partno(raw: str) -> tuple[str, str]:
    """
    Accept formats:
    - "فلتر زيت | 26300-2J000"
    - "فلتر زيت رقم 26300-2J000"
    - "فلتر زيت #26300-2J000"
    Returns (name, part_no).
    """
    s = (raw or "").strip()
    if not s:
        return "", ""
    # normalize separators
    if "|" in s:
        a, b = s.split("|", 1)
        return a.strip(), b.strip()
    m = re.search(r"(.*?)(?:\s*(?:رقم|#)\s*)([A-Za-z0-9\-_/\.]+)\s*$", s)
    if m:
        return (m.group(1) or "").strip(), (m.group(2) or "").strip()
    # try last token as part number if it has digits and letters or dashes and is long enough
    toks = s.split()
    if len(toks) >= 2:
        last = toks[-1].strip()
        if re.search(r"\d", last) and (len(last) >= 5) and re.fullmatch(r"[A-Za-z0-9\-_/\.]+", last):
            name = " ".join(toks[:-1]).strip()
            return name, last
    return s, ""

def _mask_phone_in_delivery(details: str) -> str:
    """Hide phone number line in delivery details."""
    if not details:
        return details or ""
    out_lines = []
    for ln in str(details).splitlines():
        if ln.strip().startswith("رقم الاتصال"):
            out_lines.append("رقم الاتصال: مخفي")
        else:
            out_lines.append(ln)
    return "\n".join(out_lines).strip()


# =========================
# Jobs: إعادة نشر الطلبات بدون عروض + تنبيه 24 ساعة
# =========================

def _parse_utc_iso(s: str):
    s = (s or "").strip()
    if not s:
        return None
    try:
        # Accept: 2026-02-01T00:00:00Z or without Z
        if s.endswith("Z"):
            s = s[:-1]
        return datetime.fromisoformat(s)
    except Exception:
        return None

def _dt_utc_now():
    return datetime.utcnow()

async def _rebroadcast_noquote_orders_job(context: ContextTypes.DEFAULT_TYPE):
    try:
        orders = list_orders() or []
    except Exception:
        orders = []

    now = _dt_utc_now()
    one_hour = timedelta(hours=1)
    one_day = timedelta(hours=24)

    admin_need_list = []

    for o in orders:
        try:
            order_id = str(o.get("order_id") or "").strip()
        except Exception:
            order_id = ""
        if not order_id:
            continue

        # فقط الطلبات التي تم إرسالها لمجموعة التجار
        fwd = str(o.get("forwarded_to_team_at_utc") or "").strip()
        if not fwd:
            continue

        # استثناء الطلبات المقفلة/المكتملة
        ost = str(o.get("order_status") or "").strip().lower()
        if ost in ("closed", "delivered"):
            continue

        # لو يوجد أي عرض (حتى لو آخر عرض مرفوض) لا نعتبره "بدون عروض"
        try:
            qtid = int(o.get("quoted_trader_id") or 0)
        except Exception:
            qtid = 0
        qs = str(o.get("quote_status") or "").strip().lower()

        if qtid > 0 or qs in ("sent", "accepted"):
            continue

        base_ts = _parse_utc_iso(fwd) or _parse_utc_iso(str(o.get("created_at_utc") or "")) or None
        if not base_ts:
            continue

        # ---- إعادة النشر بعد ساعة ----
        if now - base_ts >= one_hour:
            last_b = _parse_utc_iso(str(o.get("last_group_broadcast_at_utc") or "")) or None
            if not last_b or (now - last_b) >= one_hour:
                # ارسال نفس الطلب للمجموعة كتذكير
                try:
                    b = get_order_bundle(order_id)
                    order = b.get("order", {}) or {}
                    items = b.get("items", []) or []
                except Exception:
                    order, items = {}, []

                ud_payload = {
                    "order_id": str(order_id),
                    "user_id": int(order.get("user_id") or 0),
                    "user_name": str(order.get("user_name") or ""),
                    "car_name": str(order.get("car_name") or ""),
                    "car_model": str(order.get("car_model") or ""),
                    "vin": str(order.get("vin") or ""),
                    "notes": str(order.get("notes") or ""),
                    "price_sar": float(order.get("price_sar") or 0),
                    "items": items,
                    "_reminder": True,
                }
                try:
                    await notify_team(context, ud_payload)
                except Exception:
                    pass

                try:
                    update_order_fields(order_id, {"last_group_broadcast_at_utc": utc_now_iso()})
                except Exception:
                    pass

                # اشعار العميل بشكل احترافي (مرة كل 24 ساعة فقط)
                client_id = 0
                try:
                    client_id = int(order.get("user_id") or 0)
                except Exception:
                    client_id = 0

                if client_id:
                    last_ping = _parse_utc_iso(str(o.get("last_noquote_user_ping_at_utc") or "")) or None
                    if (not last_ping) or (now - last_ping) >= one_day:
                        try:
                            await context.bot.send_message(
                                chat_id=client_id,
                                text=(
                                    "🔎 تحديث حالة الطلب\n"
                                    f"🧾 رقم الطلب: {order_id}\n\n"
                                    "ما زال الطلب قيد البحث عن أفضل العروض من التجار.\n"
                                    "بمجرد وصول أي عرض سيصلك إشعار فورًا.\n\n"
                                    "🛟 للتواصل مع الإدارة اكتب: منصة"
                                ),
                                reply_markup=track_kb(order_id),
                                disable_web_page_preview=True,
                            )
                        except Exception:
                            pass
                        try:
                            update_order_fields(order_id, {"last_noquote_user_ping_at_utc": utc_now_iso()})
                        except Exception:
                            pass

        # ---- تنبيه الأدمن بعد 24 ساعة ----
        if now - base_ts >= one_day:
            last_admin = _parse_utc_iso(str(o.get("admin_noquote_24h_sent_at_utc") or "")) or None
            if (not last_admin) or (now - last_admin) >= one_day:
                admin_need_list.append(order_id)
                try:
                    update_order_fields(order_id, {"admin_noquote_24h_sent_at_utc": utc_now_iso()})
                except Exception:
                    pass

    if admin_need_list:
        admin_need_list = list(dict.fromkeys(admin_need_list))[:60]
        text = (
            "⏰ <b>تنبيه إداري</b>\n"
            "طلبات مضى عليها 24 ساعة بدون عروض: \n\n"
            + "\n".join([f"• <code>{html.escape(oid)}</code>" for oid in admin_need_list])
        )
        for aid in ADMIN_IDS:
            try:
                await context.bot.send_message(
                    chat_id=int(aid),
                    text=text,
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                )
            except Exception:
                pass

async def notify_admins_goods_receipt(context: ContextTypes.DEFAULT_TYPE, ud: dict, file_id: str, mime: str = ""):
    """Send goods payment receipt to admins only + internal PP invoice PDF + summary."""
    if not ADMIN_IDS or not file_id:
        return

    order_id = ud.get("goods_order_id") or ud.get("order_id") or ""
    user_name = ud.get("user_name", "")
    user_id = ud.get("user_id") or ud.get("client_id") or ""

    # اجلب بيانات الطلب للتفاصيل (مبلغ/تاجر)
    trader_name = ""
    goods_amt = ""
    try:
        b = get_order_bundle(order_id)
        o = b.get("order", {}) or {}
        goods_amt = str(o.get("goods_amount_sar") or "").strip()
        trader_name = (o.get("accepted_trader_name") or o.get("quoted_trader_name") or "").strip()
        if not trader_name:
            tid = int(o.get("accepted_trader_id") or 0) if str(o.get("accepted_trader_id") or "").isdigit() else 0
            if tid:
                tp = get_trader_profile(int(tid)) or {}
                trader_name = (tp.get("display_name") or "").strip() or (tp.get("company_name") or "").strip()
    except Exception:
        pass
    trader_name = trader_name or "—"

    # 1) ملخص نصي
    summary = (
        "🧾 <b>تم استلام إيصال قيمة القطع</b>\n"
        f"<b>رقم الطلب</b>: <code>{order_id}</code>\n"
        f"<b>العميل</b>: <i>{html.escape(str(user_name))}</i> {f'(<code>{user_id}</code>)' if user_id else ''}\n"
        f"<b>التاجر</b>: <i>{html.escape(str(trader_name))}</i>\n"
        f"<b>المبلغ</b>: <b>{html.escape(str(goods_amt or '—'))}</b>\n"
        "<b>الحالة</b>: بانتظار تأكيد الاستلام"
    )

    # 2) إرسال الإيصال (صورة أو PDF)
    for aid in ADMIN_IDS:
        try:
            await context.bot.send_message(chat_id=aid, text=summary, parse_mode="HTML")
        except Exception:
            pass

        try:
            m = (mime or "").lower()
            is_img = m.startswith("image/") or m.endswith(("jpg", "jpeg", "png", "webp"))
            is_pdf = m.startswith("application/pdf") or m.endswith("pdf")

            if is_img:
                await context.bot.send_photo(
                    chat_id=aid,
                    photo=file_id,
                    caption="🧾 إيصال قيمة القطع (نسخة للإدارة)",
                )
            else:
                await context.bot.send_document(
                    chat_id=aid,
                    document=file_id,
                    caption="🧾 إيصال قيمة القطع (نسخة للإدارة)",
                )
        except Exception:
            pass

    # 3) إرسال فاتورة منصة داخلية PDF (توثيق داخلي) للإدارة فقط
    try:
        if order_id:
            await send_invoice_pdf(context, order_id, kind="preliminary", admin_only=True)
    except Exception:
        pass


def admin_forward_kb(order_id: str, client_id: int = 0) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("📤 ارسال الطلب للتاجر", callback_data=f"pp_admin_forward|{order_id}")],
    ]

    if client_id:
        rows.append(
            [InlineKeyboardButton("💬 مراسلة العميل", callback_data=f"pp_admin_reply|{order_id}|{client_id}")]
        )

    rows.append(
        [InlineKeyboardButton("⛔ الغاء الطلب", callback_data=f"pp_admin_cancel|{order_id}")]
    )

    return InlineKeyboardMarkup(rows)

async def notify_admins_receipt(
    context: ContextTypes.DEFAULT_TYPE,
    ud: dict,
    receipt_file_id: str,
    receipt_is_photo: bool = True,
    client_id: int = 0,
) -> None:
    """
    اشعار الإيصال للإدارة برسالة واحدة فقط (بدون تشوه بصري):
    - نفس المعاينة (build_order_preview)
    - تفاصيل التسليم داخل صندوق <pre>
    - الإيصال مدمج مع الرسالة (كـ Photo أو Document)
    - أزرار: ارسال للتاجر + مراسلة العميل + الغاء
    """
    if not ADMIN_IDS:
        return

    order_id = (ud.get("order_id") or "").strip()
    if not order_id:
        return

    # جلب نسخة الطلب من الاكسل (للتأكد من البيانات)
    try:
        b = get_order_bundle(order_id)
        order = b.get("order", {}) or {}
    except Exception:
        order = {}

    preview_html = build_order_preview(ud)

    ship_method = (ud.get("ship_method") or order.get("ship_method") or "").strip()
    delivery_details = (ud.get("delivery_details") or order.get("delivery_details") or "").strip()
    fee = ud.get("price_sar", order.get("price_sar", ""))

    fee_txt = ""
    try:
        if str(fee).strip() not in ("", "0", "0.0"):
            fee_txt = f"\n💰 <b>رسوم المنصة</b>: <b>{html.escape(str(fee), quote=False)}</b> ريال"
    except Exception:
        fee_txt = ""

    details_block = ""
    if ship_method or delivery_details:
        safe_method = html.escape(ship_method, quote=False) if ship_method else ""
        safe_details = html.escape(delivery_details or "", quote=False)
        details_block = (
            "\n\n📦 <b>طريقة التسليم</b>: "
            + (f"<b>{safe_method}</b>" if safe_method else "—")
            + "\n<b>تفاصيل التسليم</b>:\n"
            + f"<pre>{safe_details or '—'}</pre>"
        )

    msg_html = (
        "💳 <b>إيصال دفع جديد</b>\n"
        f"🧾 <b>رقم الطلب</b>: <code>{html.escape(order_id, quote=False)}</code>"
        f"{fee_txt}\n\n"
        f"{preview_html}"
        f"{details_block}\n\n"
        "⬇️ <b>اعتماد الطلب:</b> اضغط (ارسال الطلب للتاجر) أو (الغاء الطلب)"
    )

    # قصّ بسيط عشان لا تتجاوز حدود caption
    def _trim(s: str, limit: int = 950) -> str:
        s = (s or "").strip()
        return s if len(s) <= limit else (s[: max(0, limit - 1)].rstrip() + "…")

    msg_html = _trim(msg_html, 950)

    # ✅ هنا المهم: تمرير client_id لكيبورد الادمن لإظهار زر مراسلة العميل
    kb = admin_forward_kb(order_id, int(client_id or 0))

    # fallback: نص عادي بدون HTML إذا فشل parse
    def _plain_fallback(html_text: str) -> str:
        # نحولها لنص بسيط (بدون ما نحتاج imports إضافية)
        t = html_text or ""
        for tag in ("<b>", "</b>", "<i>", "</i>", "<code>", "</code>", "<pre>", "</pre>"):
            t = t.replace(tag, "")
        return t

    for aid in ADMIN_IDS:
        try:
            if receipt_file_id:
                if receipt_is_photo:
                    await context.bot.send_photo(
                        chat_id=aid,
                        photo=receipt_file_id,
                        caption=msg_html,
                        parse_mode="HTML",
                        reply_markup=kb,
                    )
                else:
                    await context.bot.send_document(
                        chat_id=aid,
                        document=receipt_file_id,
                        caption=msg_html,
                        parse_mode="HTML",
                        reply_markup=kb,
                    )
            else:
                await context.bot.send_message(
                    chat_id=aid,
                    text=msg_html,
                    parse_mode="HTML",
                    reply_markup=kb,
                    disable_web_page_preview=True,
                )

        except Exception:
            # ✅ لا نسكت: نرسل fallback نصي بدون parse_mode (عشان ما يضيع الإيصال)
            try:
                plain = _trim(_plain_fallback(msg_html), 3500)
                if receipt_file_id:
                    # لو الإيصال موجود، نعيده بدون parse_mode وبدون HTML caption
                    if receipt_is_photo:
                        await context.bot.send_photo(
                            chat_id=aid,
                            photo=receipt_file_id,
                            caption=plain,
                            reply_markup=kb,
                        )
                    else:
                        await context.bot.send_document(
                            chat_id=aid,
                            document=receipt_file_id,
                            caption=plain,
                            reply_markup=kb,
                        )
                else:
                    await context.bot.send_message(
                        chat_id=aid,
                        text=plain,
                        reply_markup=kb,
                        disable_web_page_preview=True,
                    )
            except Exception:
                pass


async def notify_admins_free_order(
    context: ContextTypes.DEFAULT_TYPE,
    ud: dict,
    client_id: int = 0,
) -> None:
    """اشعار الإدارة بطلب مجاني (رسوم المنصة=0) برسالة واحدة: معاينة + مراسلة العميل + الغاء الطلب."""
    if not ADMIN_IDS:
        return

    order_id = (ud.get("order_id") or "").strip()
    if not order_id:
        return

    preview_html = build_order_preview(ud)
    cname = html.escape((ud.get("user_name") or "").strip())

    summary = (
        "🆓 <b>طلب مجاني (رسوم المنصة = 0)</b>\n"
        + (f"👤 العميل: <b>{cname}</b>\n" if cname else "")
        + f"{preview_html}"
    )

    kb = admin_free_order_kb(order_id, int(client_id or ud.get("user_id") or 0))

    for aid in (ADMIN_IDS or []):
        try:
            await context.bot.send_message(
                chat_id=int(aid),
                text=summary,
                parse_mode="HTML",
                reply_markup=kb,
                disable_web_page_preview=True,
            )
        except Exception:
            pass

    # (اختياري) فاتورة منصة داخلية للإدارة فقط برسوم 0
    try:
        await send_invoice_pdf(
            context,
            order_id,
            kind="preliminary",
            admin_only=True,
            invoice_for="platform",
        )
    except Exception:
        pass

async def admin_forward_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await _alert(q, "")
    actor_id = q.from_user.id

    # صلاحية الادمن
    if actor_id not in ADMIN_IDS:
        await _alert(q, "غير مصرح")
        return

    data = q.data or ""
    try:
        _, order_id = data.split("|", 1)
    except Exception:
        await _alert(q, "بيانات غير صحيحة")
        return

    order_id = (order_id or "").strip()
    if not order_id:
        await _alert(q, "رقم طلب غير صحيح")
        return

    if not TEAM_CHAT_ID:
        await _alert(q, "لم يتم ضبط مجموعة التاجر")
        return

    # تحميل الطلب من الاكسل
    try:
        bundle = get_order_bundle(order_id)
        order = bundle.get("order", {}) or {}
        items = bundle.get("items", []) or []
    except Exception:
        await _alert(q, "تعذر قراءة بيانات الطلب")
        return

    if not order:
        await _alert(q, "لم يتم العثور على الطلب")
        return

    # منع التكرار
    if str(order.get("forwarded_to_team_at_utc") or "").strip():
        await _alert(q, "تم ارسال الطلب مسبقا")
        return

    ud_payload = {
        "order_id": str(order_id),
        "user_id": int(order.get("user_id") or 0),
        "user_name": str(order.get("user_name") or ""),
        "car_name": str(order.get("car_name") or ""),
        "car_model": str(order.get("car_model") or ""),
        "vin": str(order.get("vin") or ""),
        "notes": str(order.get("notes") or ""),
        "payment_method": str(order.get("payment_method") or ""),
        "price_sar": float(order.get("price_sar") or 0),
        "items": items,
    }

    # ارسال للمجموعة
    await notify_team(context, ud_payload)

    # ✅ اشعار العميل انه تم التحقق وتم اسناد طلبه للمنصة
    client_id = 0
    try:
        client_id = int(order.get("user_id") or 0)
    except Exception:
        client_id = 0

    if client_id:
        try:
            await context.bot.send_message(
                chat_id=client_id,
                text=(
                    "✅ تم التحقق من الدفع بنجاح\n"
                    f"🧾 رقم الطلب: {order_id}\n\n"
                    "📤 تم اسناد طلبك للمنصة وارساله لمجموعة التجار\n"
                    "ستصلك عروض الأسعار فور توفرها\n\n"
                    "🔎 يمكنك المتابعة مع المنصة عند تاخر وصول العروض  "
                ),
                reply_markup=track_kb(order_id),
                disable_web_page_preview=True,
            )
        except Exception:
            pass

    # ✅ إرسال فاتورة المنصة (PDF) للعميل + الادمن بعد التحقق وإرسال الطلب للمجموعة
    try:
        await send_invoice_pdf(
            context=context,
            order_id=order_id,
            kind="preliminary",
            tracking_number="",
            admin_only=False,  # يرسل للعميل + الادمن (والتاجر لو موجود)
        )
    except Exception:
        pass

    # ✅ رسالة مختصرة للإدارة مع أزرار مراسلة (العميل/التاجر) + دمج (تم ارسال الطلب للتاجر) داخل نفس الاشعار
    try:
        brief = (
            "📌 إشعار إداري\n"
            f"🧾 رقم الطلب: {order_id}\n"
            "✅ تم ارسال الطلب لمجموعة التجار\n\n"
            "اختر جهة المراسلة:"
        )
        for aid in ADMIN_IDS:
            try:
                await context.bot.send_message(
                    chat_id=int(aid),
                    text=brief,
                    reply_markup=admin_contact_kb(order_id),
                    disable_web_page_preview=True,
                )
            except Exception:
                pass
    except Exception:
        pass

    # تمييز الطلب انه تم تمريره بواسطة الادمن
    try:
        mark_order_forwarded(
            order_id,
            admin_id=actor_id,
            admin_name=_user_name(q),
            at_utc=utc_now_iso(),
        )
    except Exception:
        pass

async def receipt_photo_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    ud = get_ud(context, user_id)

    if ud.get(STAGE_KEY) != STAGE_AWAIT_RECEIPT:
        return

    order_id = (ud.get("order_id") or "").strip()
    if not order_id:
        await update.message.reply_text(f"{_user_name(update)}\nلا يوجد طلب مرتبط بالايصال حاليا")
        set_stage(context, user_id, STAGE_NONE)
        return

    photos = update.message.photo or []
    if not photos:
        await update.message.reply_text(f"{_user_name(update)}\nالايصال الزامي ارسل صورة ايصال الدفع فقط")
        return

    file_id = photos[-1].file_id

    try:
        update_order_fields(order_id, {
            "receipt_file_id": file_id,
            "payment_status": "awaiting_confirm",
        })
    except Exception:
        pass

    # ✅ محاولة الإشعار بالطريقة الرئيسية
    sent_to_admin = False
    try:
        await notify_admins_receipt(
            context,
            ud,
            receipt_file_id=file_id,
            client_id=user_id,
            receipt_is_photo=True
        )
        sent_to_admin = True
    except Exception:
        sent_to_admin = False

    # ✅ Fallback مضمون: إذا notify_admins_receipt فشل لأي سبب (مثل اختلاف توقيع admin_forward_kb)
    if (not sent_to_admin) and ADMIN_IDS:
        try:
            preview_html = build_order_preview(ud)
        except Exception:
            preview_html = f"<b>معاينة الطلب</b>\n🧾 <b>رقم الطلب</b>: <code>{html.escape(order_id)}</code>"

        msg_html = (
            "💳 <b>إيصال دفع جديد (Fallback)</b>\n"
            f"🧾 <b>رقم الطلب</b>: <code>{html.escape(order_id)}</code>\n\n"
            f"{preview_html}\n\n"
            "⬇️ <b>اعتماد الطلب:</b> اضغط (ارسال الطلب للتاجر) أو (الغاء الطلب)"
        )

        # قصّ للـ caption
        msg_html = (msg_html or "").strip()
        if len(msg_html) > 950:
            msg_html = msg_html[:949].rstrip() + "…"

        kb = admin_forward_kb(order_id)  # الكيبورد الحالي عندك (باراميتر واحد)
        for aid in ADMIN_IDS:
            try:
                await context.bot.send_photo(
                    chat_id=aid,
                    photo=file_id,
                    caption=msg_html,
                    parse_mode="HTML",
                    reply_markup=kb,
                )
            except Exception:
                pass

    # ✅ معاينة موحّدة للعميل بعد الإيصال
    try:
        await _send_client_payment_preview(context, user_id, order_id, pay_scope="platform")
    except Exception:
        pass

    set_stage(context, user_id, STAGE_DONE)
    return


async def receipt_document_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    ud = get_ud(context, user_id)

    if ud.get(STAGE_KEY) != STAGE_AWAIT_RECEIPT:
        return

    order_id = (ud.get("order_id") or "").strip()
    if not order_id:
        await update.message.reply_text(f"{_user_name(update)}\nلا يوجد طلب مرتبط بالايصال حاليا")
        set_stage(context, user_id, STAGE_NONE)
        return

    doc = update.message.document
    if not doc:
        await update.message.reply_text(f"{_user_name(update)}\nالايصال الزامي ارسل صورة او PDF فقط")
        return

    mime = (doc.mime_type or "").lower()
    fname = (doc.file_name or "").lower()
    is_ok = (
        mime.startswith("image/")
        or mime.startswith("application/pdf")
        or fname.endswith((".jpg", ".jpeg", ".png", ".webp", ".pdf"))
    )
    if not is_ok:
        await update.message.reply_text(f"{_user_name(update)}\nالايصال الزامي ارسل صورة او PDF فقط")
        return

    file_id = doc.file_id

    try:
        update_order_fields(order_id, {
            "receipt_file_id": file_id,
            "receipt_mime": mime,
            "payment_status": "awaiting_confirm",
        })
    except Exception:
        pass

    # ✅ محاولة الإشعار بالطريقة الرئيسية
    sent_to_admin = False
    try:
        await notify_admins_receipt(
            context,
            ud,
            receipt_file_id=file_id,
            client_id=user_id,
            receipt_is_photo=False
        )
        sent_to_admin = True
    except Exception:
        sent_to_admin = False

    # ✅ Fallback مضمون: إرسال مباشر للادمن (PDF/صورة كـ Document)
    if (not sent_to_admin) and ADMIN_IDS:
        try:
            preview_html = build_order_preview(ud)
        except Exception:
            preview_html = f"<b>معاينة الطلب</b>\n🧾 <b>رقم الطلب</b>: <code>{html.escape(order_id)}</code>"

        msg_html = (
            "💳 <b>إيصال دفع جديد (Fallback)</b>\n"
            f"🧾 <b>رقم الطلب</b>: <code>{html.escape(order_id)}</code>\n\n"
            f"{preview_html}\n\n"
            "⬇️ <b>اعتماد الطلب:</b> اضغط (ارسال الطلب للتاجر) أو (الغاء الطلب)"
        )

        msg_html = (msg_html or "").strip()
        if len(msg_html) > 950:
            msg_html = msg_html[:949].rstrip() + "…"

        kb = admin_forward_kb(order_id)
        for aid in ADMIN_IDS:
            try:
                await context.bot.send_document(
                    chat_id=aid,
                    document=file_id,
                    caption=msg_html,
                    parse_mode="HTML",
                    reply_markup=kb,
                )
            except Exception:
                pass

    # ✅ معاينة موحّدة للعميل بعد الإيصال
    try:
        await _send_client_payment_preview(context, user_id, order_id, pay_scope="platform")
    except Exception:
        pass

    set_stage(context, user_id, STAGE_DONE)
    return

async def delivery_ship_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await _alert(q, "")
    user_id = q.from_user.id
    ud = get_ud(context, user_id)
    ud["ship"] = {}
    set_stage(context, user_id, STAGE_ASK_SHIP_CITY)
    await q.message.reply_text(f"{_user_name(q)}\nاكتب اسم المدينة")


async def delivery_pickup_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await _alert(q, f"تمام يا {_user_name(q)}")
    user_id = q.from_user.id
    ud = get_ud(context, user_id)

    ud["delivery_choice"] = "استلام من الموقع"
    ud.setdefault("pickup", {})

    set_stage(context, user_id, STAGE_ASK_PICKUP_CITY)
    await q.message.reply_text(f"{_user_name(q)}\nاكتب مدينة الاستلام")


def team_locked_kb(order_id: str, reason: str = "🔒 الطلب مقفول") -> InlineKeyboardMarkup:
    # زر واحد فقط داخل المجموعة يوضح أن الطلب مقفول (بدون فتح الخاص)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(reason, callback_data=f"pp_team_locked|{order_id}")]
    ])

async def _lock_team_post_keyboard(context: ContextTypes.DEFAULT_TYPE, order_id: str, reason: str = "🔒 الطلب مقفول") -> None:
    """Lock the original TEAM group order post keyboard (remove quote deeplink) once accepted/locked."""
    if not TEAM_CHAT_ID:
        return
    try:
        b = get_order_bundle(order_id)
        o = b.get("order", {}) or {}
        tm = o.get("team_message_id")
    except Exception:
        tm = None

    if not (str(tm).isdigit()):
        return

    try:
        await context.bot.edit_message_reply_markup(
            chat_id=TEAM_CHAT_ID,
            message_id=int(tm),
            reply_markup=team_locked_kb(order_id, reason=reason),
        )
    except Exception:
        # ignore (message may be too old / missing rights)
        return

async def team_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    data = (q.data or "").strip()
    parts = data.split("|")
    action = parts[0].strip() if len(parts) >= 1 else ""
    order_id = parts[1].strip() if len(parts) >= 2 else ""
    if not action or not order_id:
        return

    actor_id = q.from_user.id
    actor_name = (q.from_user.full_name or "").strip()
    actor_first = (q.from_user.first_name or actor_name or "").strip()

    # ===== مكان التنفيذ =====
    in_team_group = bool(TEAM_CHAT_ID and q.message and q.message.chat_id == TEAM_CHAT_ID)
    in_private = bool(q.message and q.message.chat.type == ChatType.PRIVATE)
    if not (in_team_group or in_private):
        return

    # ===== داخل المجموعة: أزرار محددة فقط =====
    if in_team_group and action not in (
        "pp_team_quote",
        "pp_trader_open",
        "pp_team_locked",
        "pp_team_quote_locked",
    ):
        return

    # ===== اسم التاجر =====
    def _actor_label() -> str:
        try:
            tp = get_trader_profile(actor_id) or {}
        except Exception:
            tp = {}
        dn = (tp.get("display_name") or "").strip() or actor_first or actor_name or "التاجر"
        cn = (tp.get("company_name") or "").strip()
        return f"{dn} ({cn})" if cn else dn

    # ===== زر مقفول (تنبيه فقط) =====
    if action in ("pp_team_locked", "pp_team_quote_locked"):
        try:
        # لا نعتمد على parts[2] إطلاقًا
            reason_code = "locked"

            tname = (actor_first or actor_name or "").strip() or "عزيزي التاجر"

            msg = (
                f"{tname}\n"
                "🔒 هذا الطلب مقفول ولا يستقبل عروض جديدة حالياً.\n"
                "نشكر لك اهتمامك وتعاونك."
            )

            await _alert(q, msg)
        except Exception:
            # fallback آمن
            try:
                await q.answer("🔒 هذا الطلب مقفول حالياً", show_alert=True)
            except Exception:
                pass
        return

    # ==========================================================
    # 💰 تقديم عرض سعر (من المجموعة فقط)
    # ==========================================================
    if action == "pp_team_quote":
        if _is_maintenance_mode() and actor_id not in ADMIN_IDS:
            await _alert(q, "🟧 المنصة في وضع الصيانة حاليا\nتم ايقاف تقديم عروض السعر مؤقتا")
            return

        try:
            ob = get_order_bundle(order_id)
            oo = ob.get("order", {}) or {}
        except Exception:
            oo = {}

        order_status = str(oo.get("order_status") or "").lower()
        quote_locked = str(oo.get("quote_locked") or "").lower() == "yes"
        goods_pay_status = str(oo.get("goods_payment_status") or "").lower()

        accepted_tid = int(oo.get("accepted_trader_id") or 0)
        accepted_name = (oo.get("accepted_trader_name") or "").strip()

        is_final_locked = (
            order_status in ("closed", "delivered")
            or quote_locked
            or goods_pay_status in ("awaiting_confirm", "confirmed")
        )

        if is_final_locked and actor_id not in ADMIN_IDS:
            who = accepted_name or "تاجر آخر"
            await _alert(q, f"🔒 الطلب منتهي/مغلق حاليا ومعلق لدى: {who}")
            return

        # تهيئة إدخال العرض
        ad = context.user_data.setdefault(actor_id, {})
        ad["quote_order_id"] = order_id
        set_stage(context, actor_id, STAGE_TRADER_SET_QUOTE)

        # ملخص الطلب
        order_snapshot = f"رقم الطلب: {order_id}"
        try:
            b = get_order_bundle(order_id)
            order = b.get("order", {}) or {}
            items = b.get("items", []) or []

            parts_txt = "\n".join(
                f"{i}- {it.get('name','')}"
                for i, it in enumerate(items, start=1)
                if it.get("name")
            ) or "لا يوجد"

            order_snapshot = (
                "📌 ملخص الطلب\n"
                f"رقم الطلب: {order_id}\n"
                f"السيارة: {order.get('car_name','')}\n"
                f"الموديل: {order.get('car_model','')}\n"
                f"VIN: {order.get('vin','')}\n\n"
                f"القطع:\n{parts_txt}"
            )
        except Exception:
            pass

        try:
            bot_username = getattr(context.bot, "username", "") or ""
            quote_url = f"https://t.me/{bot_username}?start=ppq_{order_id}"
            open_url = f"https://t.me/{bot_username}?start=ppopen_{order_id}"

            await context.bot.send_message(
                chat_id=actor_id,
                text=(
                    f"{_user_name(q)}\n"
                    f"👤 {_actor_label()}\n"
                    "💰 تقديم عرض سعر\n\n"
                    f"{order_snapshot}\n\n"
                    "✍️ اتبع الخطوات داخل المنصة لإرسال عرض منسق."
                ),
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("💰 فتح شاشة العرض بالخاص", url=quote_url)],
                    [InlineKeyboardButton("↗️ فتح لوحة الطلب", url=open_url)],
                ]),
                disable_web_page_preview=True,
            )
            await _alert(q, "تم إرسال التعليمات بالخاص")
        except Exception:
            await _alert(q, "افتح المنصة بالخاص ثم أعد المحاولة")
        return

    # ==========================================================
    # 🧰 فتح لوحة الطلب (للتاجر المقبول فقط)
    # ==========================================================
    if action == "pp_trader_open":
        try:
            b = get_order_bundle(order_id)
            order = b.get("order", {}) or {}
        except Exception:
            order = {}

        acc = int(order.get("accepted_trader_id") or 0)
        if not acc:
            await _alert(q, "🔒 لم يتم إسناد الطلب لتاجر بعد")
            return

        accepted_name = (order.get("accepted_trader_name") or "").strip() or "التاجر المستلم"

        if acc != actor_id and actor_id not in ADMIN_IDS:
            await _alert(q, f"🔒 الطلب مخصص للتاجر: {accepted_name}")
            return

        try:
            await context.bot.send_message(
                chat_id=actor_id,
                text=f"🧰 لوحة التحكم\n🧾 رقم الطلب: {order_id}\n👤 التاجر: {accepted_name}",
                reply_markup=trader_status_kb(order_id),
                disable_web_page_preview=True,
            )
            await _alert(q, "تم إرسال لوحة الطلب بالخاص")
        except Exception:
            await _alert(q, "تعذر إرسال اللوحة")
        return

    # ==========================================================
    # 🔐 باقي الأوامر: خاص فقط
    # ==========================================================
    if not in_private:
        return

    # ===== تأكيد استلام قيمة القطع =====
    if action == "pp_team_goods_confirm":
        assigned = _assigned_trader_id(order_id)
        if assigned and actor_id not in (assigned, *ADMIN_IDS):
            await _alert(q, "غير مصرح")
            return

        b = get_order_bundle(order_id)
        order = b.get("order", {}) or {}
        if not order.get("goods_amount_sar"):
            await q.message.reply_text("لا يوجد مبلغ مسجل لهذا الطلب")
            return

        update_order_fields(order_id, {
            "goods_payment_status": "confirmed",
            "goods_payment_confirmed_at_utc": utc_now_iso(),
            "quote_locked": "yes",
            "order_status": "in_progress",   # ✅ مفتوح للتاجر
        })

        # 🔒 قفل زر المجموعة بصريًا
        try:
            await _lock_team_post_keyboard(
                context,
                order_id,
                reason="🔒 تم إيقاف استقبال عروض السعر"
            )
        except Exception:
            pass
        # ✅ إرسال فاتورة التاجر للعميل مباشرة بعد تأكيد السداد (قطع + شحن فقط)
        try:
            await send_trader_invoice_pdf(
                context=context,
                order_id=order_id,
                kind="preliminary",
                tracking_number="",
                admin_only=False,   # للعميل فقط (الادمن نسخة منفصلة)
            )
        except Exception:
            pass


        # ✅ بعد تأكيد السداد: إرسال عنوان الشحن كامل للتاجر + لوحة الطلب (بدون تكدس)
        try:
            b3 = get_order_bundle(order_id) or {}
            o3 = b3.get("order", {}) or {}
            tid3 = int(o3.get("accepted_trader_id") or 0)
            uid3 = int(o3.get("user_id") or 0)

            ship_city = (o3.get("ship_city") or o3.get("pickup_city") or "").strip()
            ship_dist = (o3.get("ship_district") or "").strip()
            ship_short = (o3.get("ship_short_address") or "").strip()
            ship_phone = (o3.get("ship_phone") or "").strip()
            delivery_details = (o3.get("delivery_details") or "").strip()

            # تجميع عنوان واضح
            addr_lines = []
            if ship_city:
                addr_lines.append(f"المدينة: {ship_city}")
            if ship_dist:
                addr_lines.append(f"الحي: {ship_dist}")
            if ship_short:
                addr_lines.append(f"العنوان المختصر: {ship_short}")
            if delivery_details:
                addr_lines.append(f"تفاصيل إضافية: {delivery_details}")
            if ship_phone:
                addr_lines.append(f"📞 رقم الجوال: {ship_phone}")

            addr_block = "\n".join(addr_lines) if addr_lines else "—"

            amt3 = _money(o3.get("goods_amount_sar") or 0)
            client_name3 = (o3.get("user_name") or "").strip() or "العميل"

            if tid3:
                await context.bot.send_message(
                    chat_id=tid3,
                    text=(
                        "✅💳 <b>تم تأكيد سداد قيمة القطع</b>\n"
                        f"🧾 <b>رقم الطلب</b>: <code>{html.escape(order_id)}</code>\n"
                        f"👤 <b>العميل</b>: <b>{html.escape(client_name3)}</b>\n"
                        f"💰 <b>المبلغ</b>: <b>{html.escape(str(amt3))}</b>\n\n"
                        "🚀 <b>يرجى البدء بتجهيز الطلب</b> الآن\n"
                        "🚚 <b>وعند الشحن</b>: حدّث الحالة + أرسل رقم التتبع\n\n"
                        "📍 <b>عنوان الشحن (تم فك السرية بعد السداد)</b>:\n"
                        f"<pre>{html.escape(addr_block)}</pre>\n\n"
                        "⬇️ <b>لوحة الطلب</b>:"
                    ),
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                    reply_markup=trader_status_kb(order_id),
                )
        except Exception:
            pass

        # ✅ نسخة للإدارة: إرسال PDF مرة واحدة (بدون تكرار رسائل)
        try:
            await send_trader_invoice_pdf(
                context=context,
                order_id=order_id,
                kind="preliminary",
                tracking_number="",
                admin_only=True,   # للإدارة فقط
            )
        except Exception:
            pass

        # ✅ إشعار العميل (مختصر + زر مراسلة التاجر)
        uid = get_order_user_id(order_id)
        if uid:
            try:
                await context.bot.send_message(
                    chat_id=uid,
                    text=(
                        "✅ <b>تم تأكيد استلام قيمة القطع بنجاح</b>\n"
                        f"🧾 <b>رقم الطلب</b>: <code>{html.escape(order_id)}</code>\n\n"
                        "🧰 الطلب الآن قيد التجهيز\n"
                        "🚚 سيتم تحديثك عند الشحن."
                    ),
                    parse_mode="HTML",
                    reply_markup=client_trader_chat_kb(order_id),
                    disable_web_page_preview=True,
                )
            except Exception:
                pass

        return

async def media_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    ud = get_ud(context, user_id)
    stage = ud.get(STAGE_KEY)

    # === تحديث حالة التاجر: رفع فاتورة التاجر (PDF/صورة) ===
    if stage == STAGE_TRADER_STATUS_UPDATE and (ud.get("tsu_kind") or "").strip() == "seller_invoice":
        order_id2 = (ud.get("tsu_order_id") or "").strip()
        if not order_id2:
            set_stage(context, user_id, STAGE_NONE)
            return

        file_id = ""
        mime = ""
        is_photo = False

        if update.message.photo:
            file_id = update.message.photo[-1].file_id
            mime = "image/jpeg"
            is_photo = True
        else:
            doc = update.message.document
            if doc:
                mime = (doc.mime_type or "").lower()
                fname = (doc.file_name or "").lower()
                if mime.startswith("application/pdf") or mime.startswith("image/") or fname.endswith(".pdf"):
                    file_id = doc.file_id

        if not file_id:
            name = _user_name(update)
            await update.message.reply_text(f"{name}\nارسل فاتورة التاجر كملف PDF او صورة فقط")
            return

        # ✅ حفظ فاتورة التاجر + تحديث الحالة
        update_order_fields(order_id2, {
            "seller_invoice_file_id": file_id,
            "seller_invoice_mime": mime,
            "seller_invoice_at": utc_now_iso(),

    # ✅ توافق خلفي: بعض أجزاء النظام تبحث عن shop_invoice_*
            "shop_invoice_file_id": file_id,
            "shop_invoice_mime": mime,
            "shop_invoice_at": utc_now_iso(),

            "order_status": "ready_to_ship",
        })

        # ✅ فاتورة منصة داخلية (للإدارة فقط) بدون إزعاج العميل
        try:
            await send_invoice_pdf(context, order_id2, kind="preliminary", admin_only=True)
        except Exception:
            pass

        try:
            b2 = get_order_bundle(order_id2)
            o2 = b2.get("order", {}) or {}
            client_id2 = int(o2.get("user_id") or 0) if str(o2.get("user_id") or "").isdigit() else 0

            # 1) إرسال فاتورة التاجر للعميل فقط
            if client_id2:
                try:
                    if is_photo:
                        await context.bot.send_photo(chat_id=client_id2, photo=file_id, caption=f"🧾 فاتورة التاجر - الطلب {order_id2}")
                    else:
                        await context.bot.send_document(chat_id=client_id2, document=file_id, caption=f"🧾 فاتورة التاجر - الطلب {order_id2}")
                except Exception:
                    pass

            # 2) نسخة فاتورة التاجر للإدارة
            for aid in ADMIN_IDS:
                try:
                    if is_photo:
                        await context.bot.send_photo(chat_id=aid, photo=file_id, caption=f"🧾 فاتورة تاجر (نسخة للادمن) - الطلب {order_id2}")
                    else:
                        await context.bot.send_document(chat_id=aid, document=file_id, caption=f"🧾 فاتورة تاجر (نسخة للادمن) - الطلب {order_id2}")
                except Exception:
                    pass

            # 3) نقل العميل لمرحلة اختيار طريقة دفع قيمة القطع
            if client_id2:
                ud2 = get_ud(context, client_id2)
                ud2["goods_order_id"] = order_id2
                set_stage(context, client_id2, STAGE_AWAIT_GOODS_PAY_METHOD)
                update_order_fields(order_id2, {"goods_payment_status": "awaiting_method"})
                await context.bot.send_message(
                    chat_id=client_id2,
                    text=(
                        f"📦 الطلب {order_id2} جاهز للشحن ✅\n"
                        "تم إرسال فاتورة التاجر\n"
                        "اختر طريقة دفع قيمة القطع لاستكمال الشحن"
                    ),
                    reply_markup=pay_goods_method_kb(order_id2),
                    disable_web_page_preview=True,
                )
        except Exception:
            pass

        ud.pop("tsu_kind", None)
        ud.pop("tsu_order_id", None)
        set_stage(context, user_id, STAGE_NONE)

        name = _user_name(update)
        await update.message.reply_text(f"{name}\nتم تسجيل الفاتورة وتحديث الحالة الى (جاهز للشحن) ✅")
        return

    # === مرحلة استلام ايصال قيمة القطع ===
    if stage == STAGE_AWAIT_GOODS_RECEIPT:
        if update.message.photo:
            return await goods_receipt_photo_handler(update, context)

        doc = update.message.document
        if doc:
            mime = (doc.mime_type or "").lower()
            fname = (doc.file_name or "").lower()
            is_pdf = mime.startswith("application/pdf") or fname.endswith(".pdf")
            is_img = mime.startswith("image/") or fname.endswith((".jpg", ".jpeg", ".png", ".webp"))
            if is_pdf or is_img:
                return await goods_receipt_document_handler(update, context)

        name = _user_name(update)
        await update.message.reply_text(f"{name}\nالايصال الزامي ارسل صورة او PDF فقط")
        return

    # === اشتراك التاجر: استلام إيصال رسوم الاشتراك ===
    if stage == STAGE_TRADER_SUB_AWAIT_RECEIPT:
        file_id = ""
        mime = ""
        is_photo = False

        if update.message.photo:
            file_id = update.message.photo[-1].file_id
            mime = "image/jpeg"
            is_photo = True
        else:
            doc = update.message.document
            if doc:
                mime = (doc.mime_type or "").lower()
                fname = (doc.file_name or "").lower()
                if mime.startswith("application/pdf") or mime.startswith("image/") or fname.endswith(".pdf"):
                    file_id = doc.file_id

        if not file_id:
            name = _user_name(update)
            await update.message.reply_text(f"{name}\nالايصال الزامي ارسل صورة او PDF فقط")
            return

        month = str(ud.get("sub_month") or month_key_utc()).strip()
        amount = int(float(ud.get("sub_amount_sar") or 99))
        pm = str(ud.get("sub_payment_method") or ud.get("payment_method") or "").strip() or "—"

        try:
            upsert_trader_subscription(user_id, month, {
                "amount_sar": amount,
                "payment_method": pm,
                "payment_status": "pending",
                "receipt_file_id": file_id,
            })
        except Exception:
            pass

        # إشعار الإدارة مع أزرار تأكيد/رفض
        kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ تأكيد الاشتراك", callback_data=f"pp_admin_sub|confirm|{user_id}|{month}"),
                InlineKeyboardButton("❌ رفض", callback_data=f"pp_admin_sub|reject|{user_id}|{month}"),
            ]
        ])

        cap = (
            "💳 <b>إيصال اشتراك تاجر</b>\n"
            f"👤 التاجر: <b>{html.escape(str(update.effective_user.full_name or update.effective_user.first_name or ''))}</b>\n"
            f"🆔 trader_id: <code>{user_id}</code>\n"
            f"📅 الشهر: <b>{html.escape(month)}</b>\n"
            f"💰 المبلغ: <b>{amount}</b> ريال\n"
            f"💳 الطريقة: <b>{html.escape(pm)}</b>\n\n"
            "⬇️ راجع الإيصال ثم أكد/ارفض:"
        )

        for aid in ADMIN_IDS:
            try:
                if is_photo:
                    await context.bot.send_photo(chat_id=aid, photo=file_id, caption=cap, parse_mode="HTML", reply_markup=kb)
                else:
                    await context.bot.send_document(chat_id=aid, document=file_id, caption=cap, parse_mode="HTML", reply_markup=kb)
            except Exception:
                try:
                    await context.bot.send_message(chat_id=aid, text=cap, parse_mode="HTML", reply_markup=kb)
                except Exception:
                    pass

        set_stage(context, user_id, STAGE_NONE)
        await update.message.reply_text(
            f"{_user_name(update)}\n✅ تم استلام الإيصال وسيتم التحقق من الإدارة قريبًا",
            disable_web_page_preview=True,
        )
        return

# === مرحلة استلام إيصال رسوم المنصة ===
    if stage == STAGE_AWAIT_RECEIPT:
        if update.message.photo:
            return await receipt_photo_handler(update, context)

        doc = update.message.document
        if doc:
            mime = (doc.mime_type or "").lower()
            fname = (doc.file_name or "").lower()
            is_pdf = mime.startswith("application/pdf") or fname.endswith(".pdf")
            is_img = mime.startswith("image/") or fname.endswith((".jpg", ".jpeg", ".png", ".webp"))
            if is_pdf or is_img:
                return await receipt_document_handler(update, context)

        name = _user_name(update)
        await update.message.reply_text(f"{name}\nالايصال الزامي ارسل صورة او PDF فقط")
        return

    # === مرحلة وسائط القطعة (اختيارية) ===
    if stage == STAGE_ASK_ITEM_PHOTO:
        items = ud.get("items", []) or []
        idx = ud.get("pending_item_idx", None)

        if idx is None or not isinstance(idx, int) or idx < 0 or idx >= len(items):
            set_stage(context, user_id, STAGE_CONFIRM_MORE)
            await update.message.reply_text(
                f"{_user_name(update)}\nلا يوجد قطعة مرتبطة بالصورة حاليا",
                reply_markup=more_kb()
            )
            return

        media_type = None
        file_id = None

        if update.message.photo:
            media_type = "photo"
            file_id = update.message.photo[-1].file_id
        elif update.message.document:
            media_type = "document"
            file_id = update.message.document.file_id
        elif update.message.video:
            media_type = "video"
            file_id = update.message.video.file_id
        elif update.message.video_note:
            media_type = "video_note"
            file_id = update.message.video_note.file_id
        elif update.message.voice:
            media_type = "voice"
            file_id = update.message.voice.file_id
        elif update.message.audio:
            media_type = "audio"
            file_id = update.message.audio.file_id

        if not file_id:
            await update.message.reply_text(
                f"{_user_name(update)}\nارسل صورة الان (اختياري) او اكتب اسم القطعة التالية مباشرة",
                reply_markup=photo_prompt_kb(),
            )
            return

        it = items[idx]
        it["media_type"] = media_type
        it["file_id"] = file_id
        it["photo_file_id"] = file_id
        it.setdefault("created_at_utc", utc_now_iso())

        ud.pop("pending_item_idx", None)
        ud.pop("pending_item_name", None)

        set_stage(context, user_id, STAGE_CONFIRM_MORE)
        await update.message.reply_text(
            f"{_user_name(update)}\n"
            f"تم حفظ صورة القطعة رقم {idx + 1}\n"
            f"عدد القطع الحالي: {len(items)}\n\n"
            "يمكنك الان كتابة اسم قطعة جديدة مباشرة\n"
            "او اختيار انهاء وارسال للدفع",
            reply_markup=more_kb(),
        )
        return

    return

def _admin_to_trader_reply_kb(admin_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💬 رد للإدارة", callback_data=f"pp_trader_reply_admin|{admin_id}")],
        [InlineKeyboardButton("🔒 إغلاق", callback_data="pp_ui_close")],
    ])

async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    chat = update.effective_chat
    text = _norm(update.message.text)
    name = _user_name(update)

    # ===== رد الإدارة داخل /منصة (بعد ضغط زر "رد") =====
    ud_admin = get_ud(context, user_id)
    if (
        chat.type == ChatType.PRIVATE
        and user_id in ADMIN_IDS
        and ud_admin.get(STAGE_KEY) == STAGE_SUPPORT_ADMIN_REPLY
    ):
        target_uid = int(ud_admin.get("support_reply_to_uid") or 0)
        msg = (update.message.text or "").strip()

        if not target_uid or not msg:
            try:
                await update.message.reply_text(f"{name}\n🟥 اكتب رد صحيح")
            except Exception:
                pass
            return

        try:
            await context.bot.send_message(
                chat_id=target_uid,
                text=f"{PP_SUPPORT_LABEL}\n{msg}",
                reply_markup=_support_kb(),  # 🔒 زر إغلاق للمستخدم
                disable_web_page_preview=True,
            )
            await update.message.reply_text("✅ تم إرسال الرد للمستخدم")
        except Exception:
            try:
                await update.message.reply_text(
                    "🟥 تعذر إرسال الرد (قد لا يكون المستخدم بدأ البوت)"
                )
            except Exception:
                pass
            return

        ud_admin[STAGE_KEY] = STAGE_NONE
        ud_admin.pop("support_reply_to_uid", None)
        return
    
        # ===== مراسلة الأدمن للتاجر من ملف التاجر =====
    ud_admin = get_ud(context, user_id)
    if (
        chat.type == ChatType.PRIVATE
        and user_id in ADMIN_IDS
        and ud_admin.get(STAGE_KEY) == STAGE_ADMIN_TRADER_MSG
    ):
        tid = int(ud_admin.get("admin_msg_to_trader_id") or 0)
        msg = (update.message.text or "").strip()
        if not tid or not msg:
            await update.message.reply_text(f"{name}\n🟥 اكتب رسالة صحيحة")
            return

        try:
            await context.bot.send_message(
                chat_id=tid,
                text=f"{PP_SUPPORT_LABEL}\n💬 رسالة من الإدارة:\n{msg}",
                reply_markup=_admin_to_trader_reply_kb(user_id),
                disable_web_page_preview=True,
            )
            await update.message.reply_text("✅ تم إرسال الرسالة للتاجر")
        except Exception:
            await update.message.reply_text("🟥 تعذر إرسال الرسالة (قد لا يكون التاجر بدأ البوت)")
            return

        ud_admin[STAGE_KEY] = STAGE_NONE
        ud_admin.pop("admin_msg_to_trader_id", None)
        return

    # ===== قناة /منصة للمستخدم (توجيه الرسائل للإدارة فقط) =====
    ud = get_ud(context, user_id)
    if chat.type == ChatType.PRIVATE and _support_is_open(ud):

        # (1) إغلاق تلقائي: خمول / حد أقصى
        if _support_should_close_by_time(ud):
            await _support_close(
                update,
                context,
                user_id,
                reason="(تم الإغلاق تلقائياً بسبب الخمول/انتهاء المدة)",
            )
            # نترك الرسالة تكمل كرسالة طبيعية
        else:
            # (2) إغلاق تلقائي إذا بدأ المستخدم أي عملية أخرى
            try:
                cur_stage = ud.get(STAGE_KEY)
            except Exception:
                cur_stage = None

            if cur_stage and cur_stage != STAGE_NONE:
                await _support_close(
                    update,
                    context,
                    user_id,
                    reason="(تم الإغلاق تلقائياً لأنك بدأت عملية أخرى)",
                )
                # نترك الرسالة تكمل كرسالة طبيعية
            else:
                # (3) توجيه الرسالة إلى الأدمن فقط
                msg = (update.message.text or "").strip()
                if msg:
                    _support_touch(ud)

                    for aid in (ADMIN_IDS or []):
                        try:
                            await context.bot.send_message(
                                chat_id=int(aid),
                                text=(
                                    "📩 رسالة عبر /منصة\n"
                                    f"👤 {name}\n"
                                    f"🆔 {user_id}\n"
                                    "────────────────\n"
                                    f"{msg}"
                                ),
                                reply_markup=InlineKeyboardMarkup(
                                    [
                                        [
                                            InlineKeyboardButton(
                                                "✉️ رد",
                                                callback_data=f"pp_support_reply|{user_id}",
                                            )
                                        ]
                                    ]
                                ),
                                disable_web_page_preview=True,
                            )
                        except Exception:
                            pass

                    # تأكيد للمستخدم + زر إغلاق
                    try:
                        await update.message.reply_text(
                            "✅ تم إرسال رسالتك للإدارة",
                            reply_markup=_support_kb(),
                        )
                    except Exception:
                        pass

                return

    # تشغيل بكلمة pp بدون سلاش (في الخاص فقط)
    if chat.type == ChatType.PRIVATE and (text or "").lower() == "pp":
        await begin_flow(update, context)
        return

    # ===== إدخال رابط الدفع (يدوي) من الإدارة =====
    ud = get_ud(context, user_id)
    if chat.type == ChatType.PRIVATE and user_id in ADMIN_IDS and ud.get(STAGE_KEY) == STAGE_ADMIN_SEND_PAYLINK:
        link = (update.message.text or "").strip()
        if not (link.startswith("http://") or link.startswith("https://")):
            await update.message.reply_text(f"{name}\n🟥 ارسل رابط صحيح يبدأ بـ https://", disable_web_page_preview=True)
            return

        order_id = (ud.get("paylink_order_id") or "").strip()
        try:
            client_id = int(ud.get("paylink_client_id") or 0)
        except Exception:
            client_id = 0

        if not order_id or not client_id:
            await update.message.reply_text(f"{name}\n🟥 تعذر تحديد الطلب/العميل، أعد المحاولة", disable_web_page_preview=True)
            set_stage(context, user_id, STAGE_NONE)
            ud.pop("paylink_order_id", None)
            ud.pop("paylink_client_id", None)
            return

        try:
            update_order_fields(order_id, {
                "payment_method": "pay_link",
                "payment_status": "awaiting_receipt",
                "payment_link": link,
                "payment_link_sent_at_utc": utc_now_iso(),
            })
        except Exception:
            pass

    # إرسال الرابط للعميل
        try:
            b = get_order_bundle(order_id)
            order = b.get("order", {}) or {}
            fee = order.get("price_sar") or ud.get("price_sar") or ""
            fee_txt = f"{fee} ريال" if str(fee).strip() not in ("", "0", "0.0") else "—"

            await context.bot.send_message(
                chat_id=client_id,
                text=(
                    f"{_user_name(update)}\n"
                    "🔗 رابط دفع رسوم المنصة\n"
                    f"🧾 رقم الطلب: {order_id}\n"
                    f"💰 الرسوم: {fee_txt}\n\n"
                    "افتح الرابط وأكمل الدفع\n"
                    "بعد الدفع أرسل إيصال الدفع هنا داخل المنصة لإكمال الإجراء"
                ),
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔗 فتح رابط الدفع", url=link)]]),
                disable_web_page_preview=True,
            )
        except Exception:
            await update.message.reply_text(f"{name}\n🟥 تعذر إرسال الرابط للعميل (قد لا يكون بدأ المنصة)", disable_web_page_preview=True)
            return

        await update.message.reply_text(
            f"{name}\n✅ تم إرسال رابط الدفع للعميل\n🧾 رقم الطلب: {order_id}",
            disable_web_page_preview=True,
        )

        set_stage(context, user_id, STAGE_NONE)
        ud.pop("paylink_order_id", None)
        ud.pop("paylink_client_id", None)
        return

    # لوحة التاجر (تاجر) - تعمل بالخاص فقط
    if chat.type == ChatType.PRIVATE and (text or "").strip() == "تاجر":
        ud0 = get_ud(context, user_id)
        stage_now = ud0.get(STAGE_KEY, STAGE_NONE)

        # ✅ استثناء: التاجر الموقوف يسمح له بفتح اللوحة حتى لو داخل مرحلة
        # (حتى يقدر يشوف حالته ويتواصل مع الإدارة)
        is_disabled = False
        try:
            is_disabled = _trader_is_disabled(int(user_id or 0))
        except Exception:
            is_disabled = False

        if stage_now != STAGE_NONE and not is_disabled:
            return

        # ✅ السماح بفتح اللوحة إذا:
        # - عضو مجموعة التجار
        # - أو أدمن
        # - أو له ملف تاجر موجود
        # - أو مسجل في شيت التجار (تم تفعيله/إيقافه من الإدارة)
        is_admin = user_id in ADMIN_IDS
        is_member = False
        try:
            is_member = await _is_trader_group_member(context, user_id)
        except Exception:
            is_member = False

        tp = {}
        try:
            tp = get_trader_profile(int(user_id or 0)) or {}
        except Exception:
            tp = {}

        is_registered_trader = False
        if not is_member and not is_admin and not tp:
            try:
                uid_s = str(int(user_id or 0))
                for t in (list_traders() or []):
                    if str(t.get("trader_id") or "").strip() == uid_s:
                        is_registered_trader = True
                        break
            except Exception:
                is_registered_trader = False

        # إذا ليس عضو ولا أدمن ولا له ملف ولا مسجل كتاجر => منع
        if not is_member and not is_admin and not tp and not is_registered_trader:
            # للأدمن نوضح السبب، لغيره تجاهل صامت (نفس منطقك)
            if is_admin:
                if not TRADERS_GROUP_ID:
                    await update.message.reply_text(f"{name}\n⚠️ PP_TRADERS_GROUP_ID غير موجود في .env")
                else:
                    await update.message.reply_text(f"{name}\n⚠️ تعذر التحقق من عضوية مجموعة التجار (تأكد البوت عضو/مشرف بالمجموعة)")
            return

        # فتح اللوحة
        set_stage(context, user_id, STAGE_NONE)
        try:
            await show_trader_panel(update, context, user_id)
        except Exception:
            await update.message.reply_text(f"{name}\nتعذر فتح لوحة التاجر حاليا")
        return

    # لوحة الادارة (pp25s) - ادمن فقط بالخاص
    if chat.type == ChatType.PRIVATE and (text or "").strip().lower() == "pp25s":
        if user_id not in ADMIN_IDS:
            await update.message.reply_text(f"{name}\nغير مصرح")
            return
        set_stage(context, user_id, STAGE_NONE)
        try:
            await show_admin_panel(update, context, user_id)
        except Exception:
            await update.message.reply_text(f"{name}\nتعذر فتح لوحة الادارة حاليا")
        return
    
    # ===== بحث طلب من لوحة الإدارة =====
    ud = get_ud(context, user_id)
    if chat.type == ChatType.PRIVATE and user_id in ADMIN_IDS and ud.get(STAGE_KEY) == STAGE_ADMIN_FIND_ORDER:
        oid = text.strip()
        try:
            ob = get_order_bundle(oid)
        except Exception:
            ob = None

        if not ob:
            await update.message.reply_text("❌ لم يتم العثور على الطلب")
            return

        o = ob.get("order", {})
        msg = (
            f"📦 <b>الطلب {oid}</b>\n"
            f"👤 العميل: {o.get('user_name','—')}\n"
            f"🧑‍💼 التاجر: {_trader_label(int(o.get('accepted_trader_id') or 0),'—')}\n"
            f"💰 قيمة القطع: {_money(o.get('goods_amount_sar'))}\n"
            f"📌 الحالة: {o.get('order_status','—')}"
        )

        set_stage(context, user_id, STAGE_NONE)
        await update.message.reply_text(
            msg,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("↩️ رجوع", callback_data="pp_admin|home")]
            ]),
            parse_mode="HTML"
        )
        return

    # (متابعة) تبقى للعميل: تفتح شاشة متابعة الطلب (بدون تغيير منطقك الحالي)
    ud = get_ud(context, user_id)
    stage = ud.get(STAGE_KEY, STAGE_NONE)


    # === ادخال بيانات لوحة التاجر ===
    if stage == STAGE_TRADER_PROFILE_EDIT:
        field = (ud.get("tprof_field") or "").strip()
        val = (text or "").strip()

        if field not in ("display_name", "company_name", "bank_name", "iban", "stc_pay"):
            set_stage(context, user_id, STAGE_NONE)
            await update.message.reply_text(f"{name}\nتعذر تحديد الحقل المراد تعديله")
            return

    # تحقق بسيط
        if field in ("display_name", "company_name", "bank_name") and len(val) < 2:
            await update.message.reply_text(f"{name}\nالنص غير واضح اعد كتابته")
            return

        if field == "iban":
            v = re.sub(r"\s+", "", val).upper()
            if len(v) < 15 or not v.startswith("SA"):
                await update.message.reply_text(f"{name}\nاكتب الايبان بصيغة صحيحة مثال SAxxxxxxxxxxxxxxxxxxxx")
                return
            val = v

        if field == "stc_pay":
            v = re.sub(r"\s+", "", val)
        # نقبل أرقام فقط (بدون تعقيد)
            if not v.isdigit() or len(v) < 6:
                await update.message.reply_text(f"{name}\nاكتب رقم STC Pay بشكل صحيح (أرقام فقط)")
                return
            val = v

        try:
            upsert_trader_profile(int(user_id), {field: val})
        except Exception:
            await update.message.reply_text(f"{name}\nتعذر حفظ البيانات حاليا")
            return

        ud.pop("tprof_field", None)
        set_stage(context, user_id, STAGE_NONE)
        await update.message.reply_text(f"{name}\nتم حفظ بياناتك ✅")
        await show_trader_panel(update, context, user_id)
        return

    # === تحديث حالة التاجر (مدخلات إلزامية) ===
    if stage == STAGE_TRADER_STATUS_UPDATE:
        kind = (ud.get("tsu_kind") or "").strip()
        order_id2 = (ud.get("tsu_order_id") or "").strip()
        if not order_id2:
            set_stage(context, user_id, STAGE_NONE)
            return

        # محاولة جلب بيانات الطلب لتفاصيل أجمل
        try:
            b2 = get_order_bundle(order_id2)
            o2 = b2.get("order", {}) or {}
        except Exception:
            o2 = {}

        client_id2 = int(o2.get("user_id") or 0) if str(o2.get("user_id") or "").isdigit() else 0

        tprof = get_trader_profile(user_id) or {}
        tname = (tprof.get("display_name") or "").strip() or (name or "").strip() or "التاجر"

        goods_amt = str(o2.get("goods_amount_sar") or o2.get("quote_goods_amount") or "").strip()
        ship_method = str(o2.get("ship_method") or "").strip()
        ship_city = str(o2.get("ship_city") or "").strip()

        if kind == "tracking":
            tracking = (text or "").strip()
            if len(tracking) < 4:
                await update.message.reply_text(
                    f"{name}\n"
                    "🟥 *رقم التتبع غير واضح*\n"
                    "اكتبه مرة أخرى بشكل صحيح (مثال: 7845123690)\n"
                    "ملاحظة: تجنب الرموز والمسافات الطويلة",
                    parse_mode="Markdown",
                    disable_web_page_preview=True,
                )
                return

            # حفظ + تحديث حالة (تم الشحن)
            update_order_fields(order_id2, {
                "order_status": "shipped",
                "shipping_tracking": tracking,
                "shipping_at": utc_now_iso(),
            })

            # ارسال فاتورة الشحن + اشعار للعميل
            try:
                await send_invoice_pdf(
                    context,
                    order_id2,                 # ✅ الصحيح
                    kind="shipping",
                    tracking_number=tracking
                )
            except Exception as e:
                await _notify_invoice_error(
                    context,
                    order_id2,                 # ✅ الصحيح
                    "فاتورة الشحن",
                    e
                )

            # إشعار العميل برسالة واضحة + زر مراسلة التاجر
            if client_id2:
                try:
                    details_lines = []
                    if ship_method:
                        details_lines.append(f"طريقة التسليم: {ship_method}")
                    if ship_city:
                        details_lines.append(f"المدينة: {ship_city}")
                    if goods_amt:
                        details_lines.append(f"قيمة القطع: {goods_amt} ر.س")

                    extra = ("\n".join(details_lines)).strip()
                    if extra:
                        extra = "\n\n" + extra

                    await context.bot.send_message(
                        chat_id=client_id2,
                        text=(
                            "🟩 *تم شحن طلبك بنجاح*\n"
                            f"رقم الطلب: *{order_id2}*\n"
                            f"رقم التتبع: *{tracking}*\n"
                            f"التاجر: *{tname}*"
                            f"{extra}\n\n"
                            "🟦 يمكنك مراسلة التاجر أو المتابعة من الزر بالأسفل."
                        ),
                        parse_mode="Markdown",
                        reply_markup=client_trader_chat_kb(order_id2),
                        disable_web_page_preview=True,
                    )
                except Exception:
                    pass

            # نسخة للادمن
            for aid in ADMIN_IDS:
                try:
                    admin_lines = [
                        "🟨 *تحديث حالة (تم الشحن)*",
                        f"الطلب: *{order_id2}*",
                        f"التاجر: *{tname}* ({user_id})",
                        f"التتبع: *{tracking}*",
                    ]
                    if goods_amt:
                        admin_lines.append(f"قيمة القطع: *{goods_amt}* ر.س")
                    if ship_method or ship_city:
                        admin_lines.append(f"التسليم: {ship_method} - {ship_city}".strip(" -"))

                    await context.bot.send_message(
                        chat_id=aid,
                        text="\n".join(admin_lines),
                        parse_mode="Markdown",
                        disable_web_page_preview=True,
                    )
                except Exception:
                    pass

            # تنظيف وضع التحديث
            ud.pop("tsu_kind", None)
            ud.pop("tsu_order_id", None)
            set_stage(context, user_id, STAGE_NONE)

            await update.message.reply_text(
                f"{name}\n"
                "🟩 تم تحديث الحالة إلى: *تم الشحن*\n"
                f"رقم الطلب: {order_id2}\n"
                f"رقم التتبع: {tracking}",
                parse_mode="Markdown",
                disable_web_page_preview=True,
            )
            return

        # اذا وصلنا هنا: انتظار فاتورة (يتم عبر media_router)
        await update.message.reply_text(
            f"{name}\n"
            "🟦 *مطلوب فاتورة التاجر*\n"
            f"رقم الطلب: {order_id2}\n\n"
            "ارسل الفاتورة كـ PDF أو صورة واضحة.\n"
            "⚠️ بدون فاتورة لن يتم اعتماد التحديث.",
            parse_mode="Markdown",
            disable_web_page_preview=True,
        )
        return

    # === ادخال عرض السعر من التاجر ===
    if stage == STAGE_TRADER_SET_QUOTE:
        # ✅ منع التاجر الموقوف من تقديم عروض
        if _trader_is_disabled(user_id):
            set_stage(context, user_id, STAGE_NONE)
            td0 = context.user_data.setdefault(user_id, {})
            td0.pop("quote_order_id", None)
            td0.pop("quote_step", None)
            await update.message.reply_text(f"{name}\n{_trader_disabled_msg()}", disable_web_page_preview=True)
            return

        td = context.user_data.setdefault(user_id, {})
        order_id = str(td.get("quote_order_id") or "")
        if not order_id:
            set_stage(context, user_id, STAGE_NONE)
            await update.message.reply_text(
                f"{name}\n"
                "🟥 لا يوجد طلب مرتبط بعرض السعر حاليا.\n"
                "ارجع لنفس الطلب واضغط زر (تقديم عرض سعر) ثم حاول مرة أخرى.",
                disable_web_page_preview=True,
            )
            return

        step = str(td.get("quote_step") or "start")

        if step == "start":
            await update.message.reply_text(
                f"{name}\n"
                "🟦 *بناء عرض السعر*\n"
                f"رقم الطلب: {order_id}\n\n"
                "اضغط زر *بدء بناء عرض السعر* ثم اتبع الخطوات بالترتيب.",
                parse_mode="Markdown",
                reply_markup=trader_quote_start_kb(order_id),
                disable_web_page_preview=True,
            )
            return

        if step == "amount":
            m_amt = re.search(r"(\d+(?:\.\d+)?)", text)
            if not m_amt:
                await update.message.reply_text(
                    f"{name}\n"
                    "🟥 *مبلغ القطع غير صحيح*\n"
                    "اكتب مبلغ القطع بالأرقام فقط.\n"
                    "مثال: 850 أو 850.50",
                    parse_mode="Markdown",
                    disable_web_page_preview=True,
                )
                return
            amount = m_amt.group(1)
            td["quote_goods_amount"] = amount
            td["quote_step"] = "type"
            await update.message.reply_text(
                f"{name}\n"
                "🟦 *نوع القطع*\n"
                f"رقم الطلب: {order_id}\n"
                f"قيمة القطع: {amount} ر.س\n\n"
                "اختر نوع القطع من الأزرار:",
                parse_mode="Markdown",
                reply_markup=trader_quote_type_kb(order_id),
                disable_web_page_preview=True,
            )
            return

        if step == "shipping_fee":
            m_fee = re.search(r"(\d+(?:\.\d+)?)", text)
            if not m_fee:
                await update.message.reply_text(
                    f"{name}\n"
                    "🟥 *قيمة الشحن غير صحيحة*\n"
                    "اكتب قيمة الشحن بالأرقام فقط.\n"
                    "مثال: 25 أو 40.5",
                    parse_mode="Markdown",
                    disable_web_page_preview=True,
                )
                return
            fee = m_fee.group(1)
            td["quote_shipping_fee"] = fee
            td["quote_step"] = "availability"
            await update.message.reply_text(
                f"{name}\n"
                "🟦 *مدة التجهيز*\n"
                f"رقم الطلب: {order_id}\n"
                f"قيمة الشحن: {fee} ر.س\n\n"
                "حدد مدة التجهيز من الأزرار:",
                parse_mode="Markdown",
                reply_markup=trader_quote_availability_kb(order_id),
                disable_web_page_preview=True,
            )
            return
        if step == "eta_custom":
            v = (text or "").strip()
            if len(v) < 2:
                await update.message.reply_text(
                    f"{name}\n"
                    "🟥 *مدة الشحن غير واضحة*\n"
                    "اكتبها بصيغة مفهومة.\n"
                    "مثال: 2-3 ايام",
                    parse_mode="Markdown",
                    disable_web_page_preview=True,
                )
                return
            td["quote_ship_eta"] = v
            td["quote_step"] = "done"
            await finalize_quote_send(context, user_id, update.message, order_id)
            return

        if step == "avail_custom":
            v = (text or "").strip()
            if len(v) < 2:
                await update.message.reply_text(
                    f"{name}\n"
                    "🟥 *مدة التجهيز غير واضحة*\n"
                    "اكتبها بصيغة مفهومة.\n"
                    "مثال: 5 ايام",
                    parse_mode="Markdown",
                    disable_web_page_preview=True,
                )
                return
            td["quote_availability"] = v
            td["quote_step"] = "eta"
            await update.message.reply_text(
                f"{name}\n"
                "🟦 *مدة الشحن*\n"
                f"رقم الطلب: {order_id}\n"
                f"مدة التجهيز: {v}\n\n"
                "حدد مدة الشحن من الأزرار:",
                parse_mode="Markdown",
                reply_markup=trader_quote_eta_kb(order_id),
                disable_web_page_preview=True,
            )
            return

        await update.message.reply_text(
            f"{name}\n"
            "🟨 *تنبيه*\n"
            "استخدم الأزرار لبناء عرض السعر خطوة بخطوة.",
            parse_mode="Markdown",
            disable_web_page_preview=True,
        )
        return

    # === مراحل دفع قيمة القطع ===
    if stage == STAGE_AWAIT_GOODS_PAY_METHOD:
        await update.message.reply_text(
            f"{name}\n🟦 اختر طريقة الدفع من الأزرار بالأسفل.",
            disable_web_page_preview=True,
        )
        return

    if stage == STAGE_AWAIT_GOODS_RECEIPT:
        await update.message.reply_text(
            f"{name}\n🟥 الإيصال إلزامي — ارسل صورة إيصال الدفع فقط.",
            disable_web_page_preview=True,
        )
        return

    # === مراسلة التاجر (بدون كشف الهوية) ===
    if stage == STAGE_CHAT_TRADER:
        order_id = ud.get("chat_trader_order_id", "")
        tid = _assigned_trader_id(order_id) if order_id else None
        if not order_id or not tid:
            set_stage(context, user_id, STAGE_NONE)
            await update.message.reply_text(
                f"{name}\n🟥 لا يوجد تاجر محدد لهذا الطلب حاليا.",
                disable_web_page_preview=True,
            )
            return

        try:
            tprof = get_trader_profile(tid) or {}
            tname = (tprof.get("display_name") or "").strip() or "التاجر"
        except Exception:
            tname = "التاجر"

        # رسالة مختصرة لتقليل التكدس البصري
        try:
            bmsg = get_order_bundle(order_id) or {}
            omsg = bmsg.get("order", {}) or {}
            cname = (omsg.get("user_name") or "").strip() or "العميل"
            amt = _money(omsg.get("goods_amount_sar") or omsg.get("price_sar") or 0)
        except Exception:
            cname = "العميل"
            amt = ""

        head = f"💬 {cname} | طلب {order_id}"
        if amt:
            head = head + f" | {amt}"

        msg = head + "\n" + (text or "")
        try:
            await context.bot.send_message(
                chat_id=tid,
                text=msg,
                parse_mode="Markdown",
                reply_markup=trader_reply_kb(order_id, user_id),
                disable_web_page_preview=True,
            )
        except Exception:
            pass

        await update.message.reply_text(
            f"{name}\n🟩 تم إرسال رسالتك للتاجر.",
            disable_web_page_preview=True,
        )
        return
    
    # === رد التاجر (يصل للعميل باسم التاجر) ===
    if stage == STAGE_TRADER_REPLY:
        td = context.user_data.setdefault(user_id, {})

        try:
            to_uid = int(td.get("trader_reply_user_id") or 0)
        except Exception:
            to_uid = 0

        order_id = str(td.get("trader_reply_order_id") or "").strip()

        if not to_uid or not order_id:
            await update.message.reply_text(f"{name}\n🟥 تعذر تحديد العميل المرتبط بهذه المراسلة")
            set_stage(context, user_id, STAGE_NONE)
            return

    # اسم التاجر من لوحة التاجر
        tprof = get_trader_profile(user_id) or {}
        tname = (tprof.get("display_name") or "").strip() or (_user_name(update) or "").strip() or "التاجر"
        tcompany = (tprof.get("company_name") or "").strip()
        tlabel = tname + (f" ({tcompany})" if tcompany else "")

    # مبلغ الطلب (إن وجد)
        amt_txt = ""
        try:
            b2 = get_order_bundle(order_id)
            o2 = b2.get("order", {}) or {}
            amt_txt = _money(o2.get("goods_amount_sar") or "")
        except Exception:
            amt_txt = ""

        body = (text or "").strip()
        if not body:
            await update.message.reply_text(f"{name}\nاكتب رسالتك ثم ارسلها")
            return

        head = f"💬 {html.escape(tlabel)} | طلب <code>{html.escape(order_id)}</code>"
        if amt_txt:
            head = head + f" | {html.escape(amt_txt)}"
        msg_to_client = head + "\n" + html.escape(body)

        try:
            await context.bot.send_message(
                chat_id=to_uid,
                text=msg_to_client,
                parse_mode="HTML",
                reply_markup=client_trader_chat_kb(order_id),
                disable_web_page_preview=True,
            )
            await update.message.reply_text(
                f"{name}\n✅ تم ارسال ردك للعميل",
                reply_markup=trader_reply_done_kb()
            )
        except Exception:
            await update.message.reply_text(f"{name}\n🟥 تعذر ارسال الرد للعميل (قد لا يكون بدأ المنصة)")
        return
    
    # === مراسلة الإدارة (إلى عميل/تاجر) — مختصر وواضح ===
    if stage == STAGE_ADMIN_CHAT:
        if user_id not in ADMIN_IDS:
            set_stage(context, user_id, STAGE_NONE)
            await update.message.reply_text(f"{name}\n⛔ غير مصرح")
            return

        order_id = str(ud.get("admin_chat_order_id") or "").strip()
        peer_id = int(ud.get("admin_chat_peer_id") or 0)
        role = str(ud.get("admin_chat_role") or "").strip()  # client / trader
        body = (text or "").strip()

        if not order_id or not peer_id or not body:
            await update.message.reply_text(f"{name}\n🟥 اكتب رسالة صحيحة.")
            return

        try:
            if role == "client":
                msg = (
                    "🟥 [من الإدارة]\n"
                    f"🧾 الطلب: <code>{html.escape(order_id)}</code>\n\n"
                    f"{html.escape(body)}"
                )
                await context.bot.send_message(
                    chat_id=peer_id,
                    text=msg,
                    parse_mode="HTML",
                    reply_markup=track_kb(order_id),
                    disable_web_page_preview=True,
                )
            else:
                # إلى التاجر
                msg = (
                    "🟨 [من الإدارة → التاجر]\n"
                    f"🧾 الطلب: <code>{html.escape(order_id)}</code>\n\n"
                    f"{html.escape(body)}"
                )
                # حفظ جلسة رد التاجر إلى نفس الإدمن
                try:
                    context.bot_data.setdefault("pp_admin_trader_sessions", {})[str(peer_id)] = {
                        "order_id": order_id,
                        "peer_admin_id": int(user_id),
                    }
                except Exception:
                    pass

                await context.bot.send_message(
                    chat_id=peer_id,
                    text=msg,
                    parse_mode="HTML",
                    reply_markup=trader_chat_admin_kb(order_id, int(user_id)),
                    disable_web_page_preview=True,
                )

            await update.message.reply_text(f"{name}\n✅ تم الإرسال.", disable_web_page_preview=True)
        except Exception:
            await update.message.reply_text(f"{name}\n🟥 تعذر الإرسال.", disable_web_page_preview=True)
        return

    # === رد التاجر للإدارة (قناة مستقلة) ===
    if stage == STAGE_TRADER_CHAT_ADMIN:
        order_id = str(ud.get("trader_chat_order_id") or "").strip()
        admin_id = int(ud.get("trader_chat_admin_id") or 0)
        body = (text or "").strip()
        if not order_id or not admin_id or not body:
            await update.message.reply_text(f"{name}\n🟥 اكتب رسالة صحيحة.")
            return

        # اسم التاجر
        try:
            tprof = get_trader_profile(user_id) or {}
            tname = (tprof.get("display_name") or "").strip() or (_user_name(update) or "").strip() or "التاجر"
        except Exception:
            tname = _user_name(update) or "التاجر"

        msg = (
            "🟨 [من التاجر → الإدارة]\n"
            f"🧾 الطلب: {order_id}\n"
            f"🧑‍🔧 التاجر: {tname} ({user_id})\n\n"
            f"{body}"
        )

        try:
            await context.bot.send_message(
                chat_id=admin_id,
                text=msg,
                disable_web_page_preview=True,
                reply_markup=admin_contact_kb(order_id),
            )
            await update.message.reply_text(f"{name}\n✅ تم إرسال ردك للإدارة.", disable_web_page_preview=True)
        except Exception:
            await update.message.reply_text(f"{name}\n🟥 تعذر إرسال ردك.", disable_web_page_preview=True)
        return

    # === رد الإدارة (يصل للعميل باسم الإدارة) ===
    if stage == STAGE_ADMIN_REPLY:
        # حماية: لا يسمح إلا للإدمن
        if user_id not in ADMIN_IDS:
            set_stage(context, user_id, STAGE_NONE)
            await update.message.reply_text(f"{name}\n⛔ غير مصرح")
            return

        ad = context.user_data.setdefault(user_id, {})

        try:
            to_uid = int(ad.get("reply_user_id") or 0)
        except Exception:
            to_uid = 0

        order_id = str(ad.get("reply_order_id") or "").strip()

        if not to_uid or not order_id:
            await update.message.reply_text(f"{name}\n🟥 تعذر تحديد العميل المرتبط بهذه المراسلة")
            set_stage(context, user_id, STAGE_NONE)
            return

        body = (text or "").strip()
        if not body:
            await update.message.reply_text(f"{name}\nاكتب رسالتك ثم ارسلها")
            return

        # مبلغ الطلب (إن وجد)
        amt_txt = ""
        try:
            b2 = get_order_bundle(order_id)
            o2 = b2.get("order", {}) or {}
            amt_txt = _money(o2.get("goods_amount_sar") or "")
        except Exception:
            amt_txt = ""

        msg_to_client = (
            "🟥 [من الإدارة]\n"
            f"🧾 الطلب: <code>{html.escape(order_id)}</code>\n\n"
            f"{html.escape(body)}"
        )

        try:
            await context.bot.send_message(
                chat_id=to_uid,
                text=msg_to_client,
                parse_mode="HTML",
                reply_markup=track_kb(order_id),
                disable_web_page_preview=True,
            )
            # يبقى في وضع الرد لين يضغط "انهاء الرد"
            await update.message.reply_text(
                f"{name}\n✅ تم إرسال رسالتك للعميل باسم {PP_SUPPORT_LABEL}",
                reply_markup=admin_reply_done_kb(),
                disable_web_page_preview=True,
            )
        except Exception:
            await update.message.reply_text(
                f"{name}\n🟥 تعذر إرسال الرسالة للعميل (قد لا يكون بدأ المنصة أو قام بحظر البوت)"
            )
        return

    # === متابعة الطلب (قناة تواصل بدون كشف الهوية) ===
    if stage == STAGE_TRACK_ORDER:
        order_id = str(ud.get("track_order_id", "") or "").strip()

            # اسم العميل الحقيقي (الأولوية: الاكسل ثم تيليجرام)
        real_name = ""
        try:
            b = get_order_bundle(order_id)
            o = b.get("order", {}) or {}
            real_name = str(o.get("user_name") or "").strip()
        except Exception:
            real_name = ""

        if not real_name:
            try:
                real_name = (
                    update.effective_user.full_name
                    or update.effective_user.first_name
                    or ""
                ).strip()
            except Exception:
                real_name = ""

        uname = ""
        try:
            uname = (update.effective_user.username or "").strip()
        except Exception:
            uname = ""

        name_line = real_name or "—"
        if uname:
            name_line = f"{name_line} @{uname}"

        msg = (
            "🟦 [من العميل → الإدارة]\n"
            f"🧾 الطلب: {order_id}\n"
            f"👤 العميل: {name_line}\n\n"
            f"{text}"
        )

        for aid in ADMIN_IDS:
            try:
                await context.bot.send_message(
                    chat_id=aid,
                    text=msg,
                    parse_mode="Markdown",
                    reply_markup=admin_reply_kb(order_id, user_id),
                    disable_web_page_preview=True,
                )
            except Exception:
                pass

        await update.message.reply_text(
            f"{name}\n🟩 تم استلام رسالتك وسيتم الرد عليك من {PP_SUPPORT_LABEL}.",
            disable_web_page_preview=True,
        )
        return

    # === ملاحظة قبل الدفع (اختيارية) ===
    if stage == STAGE_PREPAY_NOTES:
        ud["notes"] = text
        try:
            update_order_fields(ud.get("order_id",""), {"notes": text})
        except Exception:
            pass

        await update.message.reply_text(build_order_preview(ud), parse_mode="HTML", disable_web_page_preview=True)
        await update.message.reply_text(
            f"{name}\n🟩 تم حفظ الملاحظة.\nاختر تخطي للانتقال للدفع أو ارسل ملاحظة جديدة للتعديل.",
            reply_markup=prepay_notes_kb(),
            disable_web_page_preview=True,
        )
        return

    if stage == STAGE_PREPAY_NOTES_TEXT:
        ud["notes"] = text
        try:
            update_order_fields(ud.get("order_id",""), {"notes": text})
        except Exception:
            pass

        await update.message.reply_text(build_order_preview(ud), parse_mode="HTML", disable_web_page_preview=True)
        set_stage(context, user_id, STAGE_PREPAY_NOTES)
        await update.message.reply_text(
            f"{name}\n🟩 تم حفظ الملاحظة.\nاختر تخطي للانتقال للدفع أو ارسل ملاحظة جديدة للتعديل.",
            reply_markup=prepay_notes_kb(),
            disable_web_page_preview=True,
        )
        return

    # === الايصال الزامي: اي نص يرفض ===
    if stage == STAGE_AWAIT_RECEIPT:
        await update.message.reply_text(
            f"{name}\n🟥 الإيصال إلزامي — ارسل صورة إيصال الدفع فقط.",
            disable_web_page_preview=True,
        )
        return

    # === استلام من الموقع (مدينة + جوال) ===
    if stage == STAGE_ASK_PICKUP_CITY:
        if len(text) < 2:
            await update.message.reply_text(f"{name}\n🟥 اسم المدينة غير واضح اعد كتابته")
            return
        ud.setdefault("pickup", {})["city"] = text.strip()
        set_stage(context, user_id, STAGE_ASK_PICKUP_PHONE)
        await update.message.reply_text(f"{name}\n🟦 اكتب رقم الجوال للاستلام مثال 05xxxxxxxx")
        return

    if stage == STAGE_ASK_PICKUP_PHONE:
        phone = re.sub(r"\D+", "", text or "")
        # ✅ شرط موحد: يبدأ 05 وطوله 10
        if not (phone.startswith("05") and len(phone) == 10):
            await update.message.reply_text(
                f"{name}\n🟥 رقم الجوال غير صحيح\nاكتبه ارقام فقط ويبدأ بـ 05 ويكون 10 ارقام\nمثال: 05xxxxxxxx",
                disable_web_page_preview=True,
            )
            return

        pick = ud.setdefault("pickup", {})
        pick["phone"] = phone

        order_id = (ud.get("order_id") or "").strip()
        if not order_id:
            await update.message.reply_text(f"{name}\n🟥 تعذر ربط بيانات الاستلام بالطلب اعد المحاولة من البداية")
            set_stage(context, user_id, STAGE_NONE)
            return

        details = (
            f"المدينة: {pick.get('city','')}\n"
            f"رقم الجوال: {pick.get('phone','')}\n"
            "سيتم تحديد موقع الاستلام من التاجر عند جاهزية الطلب"
        )

        try:
            update_delivery(order_id, "pickup", details)
        except Exception:
            pass

        try:
            update_order_fields(order_id, {
                "ship_method": "استلام من الموقع",
                "ship_city": pick.get("city", ""),
                "delivery_details": details,
                "delivery_choice": "استلام من الموقع",
            })
        except Exception:
            pass

        ud["delivery_choice"] = "استلام من الموقع"
        ud["delivery_details"] = details
        ud["ship_method"] = "استلام من الموقع"
        ud["ship_city"] = pick.get("city", "")

        # # ✅ المجاني / المدفوع بنفس منطق الشحن
        fee = 0
        try:
            fee = int(float(ud.get("price_sar") or 0))
        except Exception:
            fee = 0
        try:
            non_cnt = int(ud.get("non_consumable_count") or 0)
        except Exception:
            non_cnt = 0
        try:
            cons_cnt = int(ud.get("consumable_count") or 0)
        except Exception:
            cons_cnt = 0

        # ✅ (1) عرض مجاني لرسوم المنصة: أي طلب رسومه 0 بسبب العرض => نتجاوز الدفع ونرسل للفريق
        if fee <= 0 and _is_platform_fee_free_mode():
            try:
                _save_order_once(ud)
            except Exception:
                pass

            try:
                update_order_fields(order_id, {
                    "price_sar": 0,
                    "payment_method": "free",
                    "payment_status": "confirmed",
                    "payment_confirmed_at_utc": utc_now_iso(),
                })
            except Exception:
                pass

            # ✅ فاتورة منصة للعميل برسوم 0
            try:
                await send_platform_invoice_pdf(context, order_id, kind="preliminary", admin_only=False)
            except Exception:
                pass

            # ✅ يذهب مباشرة للفريق (مجموعة التجار)
            try:
                await notify_team(context, ud)
            except Exception:
                pass

            # ✅ اشعار الإدارة (معاينة فقط + مراسلة العميل + الغاء)
            try:
                await notify_admins_free_order(context, ud, client_id=user_id)
            except Exception:
                pass

            try:
                safe_details = html.escape(details)
                await update.message.reply_text(
                    build_order_preview(ud)
                    + "\n\n<b>📍 تفاصيل الاستلام</b>:\n<pre>"
                    + safe_details
                    + "</pre>\n"
                    "<b>✅ تم استلام طلبك ضمن العرض المجاني وستصلك العروض قريباً</b>",
                    parse_mode="HTML",
                    reply_markup=track_kb(order_id),
                    disable_web_page_preview=True,
                )
            except Exception:
                pass

            set_stage(context, user_id, STAGE_DONE)
            return

        # ✅ (2) المجاني الاستهلاكي فقط (منطقك الحالي كما هو)
        if fee == 0 and cons_cnt > 0 and non_cnt == 0:
            try:
                _save_order_once(ud)
            except Exception:
                pass
            try:
                update_order_fields(order_id, {
                    "price_sar": 0,
                    "payment_method": "free",
                    "payment_status": "confirmed",
                    "payment_confirmed_at_utc": utc_now_iso(),
                })
            except Exception:
                pass

            # ✅ إرسال فاتورة المنصة للعميل حتى لو الرسوم = 0
            try:
                await send_platform_invoice_pdf(context, order_id, kind="preliminary", admin_only=False)
            except Exception:
                pass

            try:
                await notify_team(context, ud)
            except Exception:
                pass

            try:
                safe_details = html.escape(details)
                await update.message.reply_text(
                    build_order_preview(ud)
                    + "\n\n<b>📍 تفاصيل الاستلام</b>:\n<pre>"
                    + safe_details
                    + "</pre>\n"
                    "<b>✅ تم إرسال طلبك للمنصة مباشرة وستصلك العروض قريباً</b>",
                    parse_mode="HTML",
                    reply_markup=track_kb(order_id),
                    disable_web_page_preview=True,
                )
            except Exception:
                pass

            set_stage(context, user_id, STAGE_DONE)
            return
        
        # === الدفع (استلام من الموقع) ===
        # ✅ فرض المجاني قبل كيبورد الدفع
        if _is_platform_fee_free_mode():
            ud["price_sar"] = 0
            fee = 0

        set_stage(context, user_id, STAGE_AWAIT_PAY_METHOD)
        try:
            safe_details = html.escape(details)
            await update.message.reply_text(
                build_order_preview(ud)
                + "\n\n<b>📍 تفاصيل الاستلام</b>:\n<pre>"
                + safe_details
                + "</pre>\n\n"
                "<b>⬇️ اختر طريقة دفع رسوم المنصة</b>",
                parse_mode="HTML",
                reply_markup=pay_method_kb(),
                disable_web_page_preview=True,
            )
        except Exception:
            await update.message.reply_text(
                f"{name}\nاختر طريقة الدفع",
                reply_markup=pay_method_kb(),
                disable_web_page_preview=True,
            )
        return

        # === بيانات الشحن (مدينة -> عنوان مختصر -> جوال) ===
    if stage == STAGE_ASK_SHIP_CITY:
        if len(text) < 2:
            await update.message.reply_text(f"{name}\n🟥 اسم المدينة غير واضح اعد كتابته")
            return
        ud.setdefault("ship", {})["city"] = text.strip()
        # ✅ حذف مرحلة الحي نهائياً
        set_stage(context, user_id, STAGE_ASK_SHIP_STREET)
        await update.message.reply_text(f"{name}\n🟦 اكتب العنوان الوطني المختصر")
        return

    if stage == STAGE_ASK_SHIP_STREET:
        if len(text) < 3:
            await update.message.reply_text(f"{name}\n🟥 العنوان المختصر غير واضح اعد كتابته")
            return
        ud.setdefault("ship", {})["short"] = text.strip()
        set_stage(context, user_id, STAGE_ASK_SHIP_PHONE)
        await update.message.reply_text(f"{name}\n🟦 اكتب رقم الاتصال مثال 05xxxxxxxx")
        return

    if stage == STAGE_ASK_SHIP_PHONE:
        phone = re.sub(r"\D+", "", text or "")
        # ✅ شرط موحد: يبدأ 05 وطوله 10
        if not (phone.startswith("05") and len(phone) == 10):
            await update.message.reply_text(
                f"{name}\n🟥 رقم الجوال غير صحيح\nاكتبه ارقام فقط ويبدأ بـ 05 ويكون 10 ارقام\nمثال: 05xxxxxxxx",
                disable_web_page_preview=True,
            )
            return

        ship = ud.setdefault("ship", {})
        ship["phone"] = phone

        order_id = (ud.get("order_id") or "").strip()
        if not order_id:
            await update.message.reply_text(f"{name}\n🟥 تعذر ربط عنوان الشحن بالطلب اعد المحاولة من البداية")
            set_stage(context, user_id, STAGE_NONE)
            return

        details = (
            f"المدينة: {ship.get('city','')}\n"
            f"العنوان الوطني المختصر: {ship.get('short','')}\n"
            f"رقم الاتصال: {ship.get('phone','')}"
        )

        try:
            update_delivery(order_id, "ship", details)
        except Exception:
            pass

        try:
            update_order_fields(order_id, {
                "ship_method": "شحن",
                "ship_city": ship.get("city", ""),
                "delivery_details": details,
                "delivery_choice": "شحن",
            })
        except Exception:
            pass

        ud["delivery_choice"] = "شحن"
        ud["delivery_details"] = details
        ud["ship_method"] = "شحن"
        ud["ship_city"] = ship.get("city", "")

        # ===== حساب الرسوم =====
        fee = 0
        try:
            fee = int(float(ud.get("price_sar") or 0))
        except Exception:
            fee = 0
        try:
            non_cnt = int(ud.get("non_consumable_count") or 0)
        except Exception:
            non_cnt = 0
        try:
            cons_cnt = int(ud.get("consumable_count") or 0)
        except Exception:
            cons_cnt = 0

        # ===== (A) عرض مجاني عام لرسوم المنصة (يشمل الشحن) =====
        if _is_platform_fee_free_mode():
            ud["price_sar"] = 0
            fee = 0

            try:
                _save_order_once(ud)
            except Exception:
                pass
            try:
                update_order_fields(order_id, {
                    "price_sar": 0,
                    "payment_method": "free",
                    "payment_status": "confirmed",
                    "payment_confirmed_at_utc": utc_now_iso(),
                })
            except Exception:
                pass

            # ✅ فاتورة منصة للعميل برسوم 0
            try:
                await send_platform_invoice_pdf(context, order_id, kind="preliminary", admin_only=False)
            except Exception:
                pass

            # ✅ يرسل مباشرة للفريق
            try:
                await notify_team(context, ud)
            except Exception:
                pass

            # ✅ إشعار الإدارة (مراسلة العميل + إلغاء)
            try:
                await notify_admins_free_order(context, ud, client_id=user_id)
            except Exception:
                pass

            try:
                safe_details = html.escape(details)
                await update.message.reply_text(
                    build_order_preview(ud)
                    + "\n\n<b>📦 تفاصيل الشحن</b>:\n<pre>"
                    + safe_details
                    + "</pre>\n"
                    "<b>✅ تم استلام طلبك    وستصلك العروض قريباً</b>",
                    parse_mode="HTML",
                    reply_markup=track_kb(order_id),
                    disable_web_page_preview=True,
                )
            except Exception:
                pass

            set_stage(context, user_id, STAGE_DONE)
            return
        # ===== END (A) =====

        # ===== (B) مجاني استهلاكي فقط (المنطق السابق) =====
        if fee == 0 and cons_cnt > 0 and non_cnt == 0:
            try:
                _save_order_once(ud)
            except Exception:
               pass
            try:
                update_order_fields(order_id, {
                    "price_sar": 0,
                    "payment_method": "free",
                    "payment_status": "confirmed",
                    "payment_confirmed_at_utc": utc_now_iso(),
                })
            except Exception:
                pass

            try:
                await send_platform_invoice_pdf(context, order_id, kind="preliminary", admin_only=False)
            except Exception:
                pass

            try:
                await notify_team(context, ud)
            except Exception:
               pass

            try:
                safe_details = html.escape(details)
                await update.message.reply_text(
                    build_order_preview(ud)
                    + "\n\n<b>📦 تفاصيل الشحن</b>:\n<pre>"
                    + safe_details
                    + "</pre>\n"
                    "<b>✅ تم إرسال طلبك للمنصة مباشرة وستصلك العروض قريباً</b>",
                    parse_mode="HTML",
                    reply_markup=track_kb(order_id),
                    disable_web_page_preview=True,
                )
            except Exception:
                pass

            set_stage(context, user_id, STAGE_DONE)
            return
        # ===== END (B) =====
        # ===== (C) مدفوع =====
        set_stage(context, user_id, STAGE_AWAIT_PAY_METHOD)
        try:
            safe_details = html.escape(details)
            await update.message.reply_text(
                build_order_preview(ud)
                + "\n\n<b>📦 تفاصيل الشحن</b>:\n<pre>"
                + safe_details
                + "</pre>\n\n"
                "<b>⬇️ اختر طريقة دفع رسوم المنصة</b>",
                parse_mode="HTML",
                reply_markup=pay_method_kb(),
                disable_web_page_preview=True,
            )
        except Exception:
            await update.message.reply_text(
                f"{name}\nاختر طريقة الدفع",
                reply_markup=pay_method_kb(),
                disable_web_page_preview=True,
            )
        return

    # === بيانات السيارة ===
    if stage == STAGE_ASK_CAR:
        if len(text) < 3:
            await update.message.reply_text(f"{name}\n🟥 اسم السيارة غير واضح اعد كتابته")
            return
        ud["car_name"] = text
        set_stage(context, user_id, STAGE_ASK_MODEL)
        await update.message.reply_text(
            f"{name}\n🟦 اكتب سنة الموديل فقط (4 ارقام)\nمثال: 2023",
            disable_web_page_preview=True,
        )

        return

    if stage == STAGE_ASK_MODEL:
        s = (text or "").strip()
        if not re.fullmatch(r"(19|20)\d{2}", s):
            await update.message.reply_text(
                f"{name}\n"
                "🟥 صيغة الموديل غير صحيحة\n"
                "اكتب سنة الموديل فقط 4 ارقام\n\n"
                "مثال:\n"
                "2023",
                disable_web_page_preview=True,
            )
            return

        ud["car_model"] = s
        set_stage(context, user_id, STAGE_ASK_VIN)
        await update.message.reply_text(f"{name}\n🟦 اكتب رقم الهيكل VIN مثال LVVDC12B4RD012345")
        return

    if stage == STAGE_ASK_VIN:
        vin = text.replace(" ", "").upper()
        if not _looks_like_vin(vin):
            await update.message.reply_text(f"{name}\n🟥 رقم الهيكل غير صحيح لازم 17 خانة مثل LVVDC12B4RD012345")
            return

        ud["vin"] = vin
        set_stage(context, user_id, STAGE_ASK_ITEM_NAME)
        await update.message.reply_text(f"{name}\n🟦 اكتب اسم القطعة رقم 1 ")
        return
    
    # (مهم) لو المستخدم كتب اسم قطعة جديدة أثناء شاشة "انهاء/ارسال للدفع" (STAGE_CONFIRM_MORE)
    # اعتبره اسم القطعة التالية مباشرة بدل ما يتجاهل الرسالة
    if stage == STAGE_CONFIRM_MORE and text:
        items = ud.get("items", []) or []
        if len(items) >= MAX_ITEMS:
            await update.message.reply_text(
                f"{name}\n🟥 وصلت للحد الأقصى من القطع ({MAX_ITEMS})\nاختر انهاء وارسال للدفع",
                reply_markup=more_kb(),
            )
            return

        if len(text) < 2:
            await update.message.reply_text(f"{name}\n🟥 اسم القطعة غير واضح اعد كتابته")
            return

    # نظّف أي مؤشرات سابقة
        ud.pop("pending_item_idx", None)
        ud.pop("pending_item_name", None)

    # خزّن الاسم واطلب رقم القطعة (اختياري)
        ud["pending_item_name"] = text
        set_stage(context, user_id, STAGE_ASK_ITEM_PARTNO)
        await update.message.reply_text(
            f"{name}\n🟦 اكتب رقم القطعة (اختياري) او اختر تخطي",
            reply_markup=partno_kb(),
            disable_web_page_preview=True,
        )
        return

        # === ادخال اسم القطعة ===
    if stage == STAGE_ASK_ITEM_NAME:
        if len(text) < 2:
            await update.message.reply_text(f"{name}\n🟥 اسم القطعة غير واضح اعد كتابته")
            return

        # خزّن الاسم مؤقتا وانتقل لرقم القطعة
        ud["pending_item_name"] = text
        set_stage(context, user_id, STAGE_ASK_ITEM_PARTNO)
        await update.message.reply_text(
            f"{name}\n🟦 اكتب رقم القطعة (اختياري) او اختر تخطي",
            reply_markup=partno_kb(),
            disable_web_page_preview=True,
        )
        return

    # === ادخال رقم القطعة (اختياري) ===
    if stage == STAGE_ASK_ITEM_PARTNO:
        pending_name = _norm(ud.get("pending_item_name", ""))
        if not pending_name:
            set_stage(context, user_id, STAGE_ASK_ITEM_NAME)
            await update.message.reply_text(f"{name}\n🟥 اكتب اسم القطعة اولا")
            return

        part_no = (text or "").strip()

        ud.setdefault("items", []).append({
            "name": pending_name,
            "part_no": part_no,
            "photo_file_id": "",
            "created_at_utc": utc_now_iso(),
        })

        ud.pop("pending_item_name", None)
        ud["pending_item_idx"] = len(ud["items"]) - 1

        set_stage(context, user_id, STAGE_ASK_ITEM_PHOTO)
        item_no = len(ud["items"])
        await update.message.reply_text(
            f"{name}\nتمت اضافة القطعة رقم {item_no}\nارسل صورة الان (اختياري) او اكتب اسم القطعة التالية مباشرة",
            reply_markup=photo_prompt_kb(),
            disable_web_page_preview=True,
        )
        return

    # (اختياري لكن مهم) لو المستخدم كتب نص أثناء مرحلة الصورة: اعتبره اسم قطعة جديدة مباشرة
    if stage == STAGE_ASK_ITEM_PHOTO and text:
        # اعتبره اسم قطعة جديدة (يعني تخطى الصورة)
        ud.pop("pending_item_idx", None)
        ud.pop("pending_item_name", None)
        set_stage(context, user_id, STAGE_ASK_ITEM_NAME)

        # اعادة تمرير نفس الرسالة كاسم قطعة (بدون تكرار انتظار رسالة جديدة)
        if len(text) < 2:
            await update.message.reply_text(f"{name}\n🟥 اسم القطعة غير واضح اعد كتابته")
            return

        ud["pending_item_name"] = text
        set_stage(context, user_id, STAGE_ASK_ITEM_PARTNO)
        await update.message.reply_text(
            f"{name}\n🟦 اكتب رقم القطعة (اختياري) او اختر تخطي",
            reply_markup=partno_kb(),
            disable_web_page_preview=True,
        )
    
        # ===== رد التاجر للإدارة (رسائل ملف التاجر) =====
    ud_t = get_ud(context, user_id)
    if chat.type == ChatType.PRIVATE and ud_t.get(STAGE_KEY) == "trader_reply_admin_msg":
        admin_id = int(ud_t.get("reply_to_admin_id") or 0)
        msg = (update.message.text or "").strip()
        if not admin_id or not msg:
            await update.message.reply_text(f"{name}\n🟥 اكتب رسالة صحيحة")
            return

        try:
            await context.bot.send_message(
                chat_id=admin_id,
                text=f"💬 <b>رد من التاجر</b>\n"
                     f"🆔 التاجر: <b>{user_id}</b>\n"
                     f"👤 الاسم: <b>{html.escape(name)}</b>\n\n"
                     f"{html.escape(msg)}",
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
            await update.message.reply_text("✅ تم إرسال الرد للإدارة")
        except Exception:
            await update.message.reply_text("🟥 تعذر إرسال الرد للإدارة")
            return

        ud_t[STAGE_KEY] = STAGE_NONE
        ud_t.pop("reply_to_admin_id", None)
        return
    
        
async def admin_cancel_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await _alert(q, "")
    actor_id = q.from_user.id

    if actor_id not in ADMIN_IDS:
        await _alert(q, "غير مصرح")
        return

    data = q.data or ""
    try:
        _, order_id = data.split("|", 1)
    except Exception:
        await _alert(q, "بيانات غير صحيحة")
        return

    order_id = (order_id or "").strip()
    if not order_id:
        await _alert(q, "رقم طلب غير صحيح")
        return

    update_order_status(order_id, "cancelled")
    update_order_fields(order_id, {
        "cancelled_by_admin_id": actor_id,
        "cancelled_by_admin_name": _user_name(q),
        "cancelled_at_utc": utc_now_iso(),
    })

    # اشعار العميل
    uid = get_order_user_id(order_id)
    if uid:
        try:
            await context.bot.send_message(
                chat_id=uid,
                text="⛔ تم الغاء الطلب من قبل الادارة\n"
                     f"رقم الطلب: {order_id}"
            )
        except Exception:
            pass

    # اشعار الفريق
    if TEAM_CHAT_ID:
        try:
            await context.bot.send_message(
                chat_id=TEAM_CHAT_ID,
                text="⛔ تم الغاء الطلب من قبل الادارة\n"
                     f"رقم الطلب: {order_id}"
            )
        except Exception:
            pass

    await _alert(q, "تم الغاء الطلب")
    try:
        await q.message.reply_text(f"{_user_name(q)}\nتم الغاء الطلب #{order_id}")
    except Exception:
        pass

async def track_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await _alert(q, "")
    user_id = q.from_user.id

    data = q.data or ""
    try:
        _, order_id = data.split("|", 1)
    except Exception:
        return

    order_id = (order_id or "").strip()
    if not order_id:
        return

    ud = get_ud(context, user_id)
    ud["track_order_id"] = order_id
    set_stage(context, user_id, STAGE_TRACK_ORDER)

    await q.message.reply_text(
        f"{_user_name(q)}\nاكتب رسالتك بخصوص الطلب {order_id}\nسيتم الرد عليك من {PP_SUPPORT_LABEL}",
    )

async def admin_reply_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await _alert(q, "")
    actor_id = q.from_user.id
    if actor_id not in ADMIN_IDS:
        await _alert(q, "غير مصرح")
        return

    data = q.data or ""
    # pp_admin_reply|order_id|user_id
    parts = data.split("|")
    if len(parts) != 3:
        return
    _, order_id, uid = parts
    try:
        uid_int = int(uid)
    except Exception:
        return

    ad = context.user_data.setdefault(actor_id, {})
    ad["reply_order_id"] = order_id
    ad["reply_user_id"] = uid_int
    set_stage(context, actor_id, STAGE_ADMIN_REPLY)

    await q.message.reply_text(
        f"{_user_name(q)}\nاكتب ردك الان وسيصل للعميل باسم {PP_SUPPORT_LABEL}\nرقم الطلب: {order_id}",
        reply_markup=admin_reply_done_kb(),
    )

async def admin_reply_done_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await _alert(q, "")
    actor_id = q.from_user.id
    if actor_id not in ADMIN_IDS:
        return
    ad = context.user_data.setdefault(actor_id, {})
    ad.pop("reply_order_id", None)
    ad.pop("reply_user_id", None)
    set_stage(context, actor_id, STAGE_NONE)
    await q.message.reply_text("تم انهاء وضع الرد")

# === شات مباشر بين العميل والتاجر (Relay) ===
def _assigned_trader_id(order_id: str) -> int:
    try:
        b = get_order_bundle(order_id)
        o = b.get("order", {}) or {}
    except Exception:
        o = {}

    # ✅ الأهم: إذا فيه تاجر مقبول (accepted_trader_id) اعتبره هو المعني دائمًا
    try:
        acc = int(o.get("accepted_trader_id") or 0)
    except Exception:
        acc = 0
    if acc:
        return acc

    # fallback: آخر تاجر قدّم عرض
    try:
        qt = int(o.get("quoted_trader_id") or 0)
    except Exception:
        qt = 0
    return qt

async def chat_trader_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await _alert(q, "")
    user_id = q.from_user.id

    data = q.data or ""
    try:
        _, order_id = data.split("|", 1)
    except Exception:
        return
    order_id = (order_id or "").strip()
    if not order_id:
        return

    tid = _assigned_trader_id(order_id)
    if not tid:
        await q.message.reply_text(f"{_user_name(q)}\nلم يتم تحديد تاجر لهذا الطلب بعد")
        return

    ud = get_ud(context, user_id)
    ud["chat_trader_order_id"] = order_id
    set_stage(context, user_id, STAGE_CHAT_TRADER)

    await q.message.reply_text(
        f"{_user_name(q)}\nاكتب رسالتك للتاجر بخصوص الطلب {order_id}",
        reply_markup=client_trader_chat_done_kb(),
    )

async def chat_trader_done_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await _alert(q, "")
    user_id = q.from_user.id
    ud = get_ud(context, user_id)
    ud.pop("chat_trader_order_id", None)
    set_stage(context, user_id, STAGE_NONE)
    await q.message.reply_text(f"{_user_name(q)}\nتم انهاء المراسلة")
    

async def trader_reply_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await _alert(q, "")
    actor_id = q.from_user.id

    data = (q.data or "").strip()
    parts = data.split("|")
    if len(parts) != 3:
        return

    _, order_id, uid = parts
    order_id = (order_id or "").strip()

    try:
        uid_int = int(uid)
    except Exception:
        return

    # يسمح فقط للتاجر المسند له الطلب (او الادمن)
    assigned = _assigned_trader_id(order_id)
    if assigned and actor_id not in (assigned, *ADMIN_IDS):
        await _alert(q, "⛔ غير مصرح")
        return

    # تجهيز وضع الرد
    td = context.user_data.setdefault(actor_id, {})
    td["trader_reply_order_id"] = order_id
    td["trader_reply_user_id"] = uid_int
    set_stage(context, actor_id, STAGE_TRADER_REPLY)

    # اسم التاجر (اختياري) من لوحة التاجر
    tp = get_trader_profile(actor_id) or {}
    tname = (tp.get("display_name") or "").strip() or (q.from_user.first_name or q.from_user.full_name or "").strip() or "التاجر"
    tco = (tp.get("company_name") or "").strip()
    tline = f"👤 <b>{html.escape(tname)}</b>" + (f"  •  🏢 <b>{html.escape(tco)}</b>" if tco else "")

    # ملخص سريع
    try:
        b = get_order_bundle(order_id)
        o = b.get("order", {}) or {}
        amt = _money(o.get("goods_amount_sar") or "")
        car = (o.get("car_name") or "").strip()
        model = (o.get("car_model") or "").strip()
    except Exception:
        amt = ""
        car = ""
        model = ""

    summary = []
    if car or model:
        summary.append(f"🚗 <b>{html.escape((car + ' ' + model).strip())}</b>")
    if amt:
        summary.append(f"💰 <b>{html.escape(amt)}</b>")

    msg = (
        "🟦 <b>مراسلة العميل</b>\n"
        f"{tline}\n"
        f"🧾 رقم الطلب: <b>{html.escape(order_id)}</b>\n"
        + (("—\n" + " • ".join(summary) + "\n") if summary else "")
        + "\n"
        "✍️ اكتب ردّك الآن وسيصل للعميل داخل المنصة.\n"
        "⚠️ لا تكتب بيانات حساسة خارج سياق الطلب."
    )

    await q.message.reply_text(
        msg,
        parse_mode="HTML",
        reply_markup=trader_reply_done_kb(),
        disable_web_page_preview=True,
    )

    data = q.data or ""
    parts = data.split("|")
    if len(parts) != 3:
        await _alert(q, "🟥 بيانات الزر غير مكتملة")
        return

    _, order_id, uid = parts
    order_id = (order_id or "").strip()

    if not order_id:
        await _alert(q, "🟥 رقم الطلب غير صحيح")
        return

    try:
        uid_int = int(uid)
    except Exception:
        await _alert(q, "🟥 تعذر تحديد العميل لهذا الطلب")
        return

    # يسمح فقط للتاجر المسند له الطلب (او الادمن)
    assigned = _assigned_trader_id(order_id)
    if assigned and actor_id not in (assigned, *ADMIN_IDS):
        intruder_name = (q.from_user.first_name or q.from_user.full_name or "").strip() or "هذا التاجر"
        # اسم التاجر المخصص (إن وجد)
        accepted_name = ""
        try:
            b0 = get_order_bundle(order_id)
            o0 = b0.get("order", {}) or {}
            accepted_name = (o0.get("accepted_trader_name") or "").strip()
            if not accepted_name and assigned:
                tp0 = get_trader_profile(int(assigned)) or {}
                accepted_name = (tp0.get("display_name") or "").strip()
        except Exception:
            accepted_name = ""

        who = accepted_name or "تاجر آخر"
        await _alert(q, f"🔒 الطلب معلق\n👤 {intruder_name}\nهذا الطلب مخصص لـ: {who}")
        return

    # اسم التاجر الذي سيظهر للعميل (اختياري)
    tprof = get_trader_profile(actor_id) or {}
    tname = (tprof.get("display_name") or "").strip() or (q.from_user.first_name or q.from_user.full_name or "").strip() or "التاجر"
    tcompany = (tprof.get("company_name") or "").strip()

    # ملخص الطلب للتاجر أثناء الرد
    snap = ""
    try:
        b = get_order_bundle(order_id)
        o = b.get("order", {}) or {}
        items = b.get("items", []) or []

        parts_lines = []
        for i, it in enumerate(items, start=1):
            nm = (it.get("name") or "").strip()
            pn = (it.get("part_no") or it.get("item_part_no") or "").strip()
            if nm and pn:
                parts_lines.append(f"{i}- {nm} (رقم: {pn})")
            elif nm:
                parts_lines.append(f"{i}- {nm}")
        parts_txt = "\n".join(parts_lines) if parts_lines else "لا يوجد"

        amt = (o.get("goods_amount_sar") or "").strip()
        amt_line = f"\n💰 مبلغ العرض: {amt} ريال" if amt else ""

        car_name = (o.get("car_name") or "").strip()
        car_model = (o.get("car_model") or "").strip()
        vin = (o.get("vin") or "").strip()
        notes = (o.get("notes") or "").strip()

        snap = (
            "📦 <b>ملخص الطلب</b>\n"
            f"🧾 <b>رقم الطلب</b>: <code>{order_id}</code>\n"
            f"🚗 <b>السيارة</b>: {car_name or '—'}\n"
            f"📌 <b>الموديل/الفئة</b>: {car_model or '—'}\n"
            f"🔎 <b>VIN</b>: {vin or '—'}\n"
            f"📝 <b>ملاحظات</b>: {notes or 'لا يوجد'}"
            f"{amt_line}\n\n"
            f"🧩 <b>القطع</b>:\n{parts_txt}\n"
        )
    except Exception:
        snap = ""

    ad = context.user_data.setdefault(actor_id, {})
    ad["trader_reply_order_id"] = order_id
    ad["trader_reply_user_id"] = uid_int
    set_stage(context, actor_id, STAGE_TRADER_REPLY)

    trader_line = f"{tname}" + (f" ({tcompany})" if tcompany else "")

    await q.message.reply_text(
        (
            f"{_user_name(q)}\n"
            "🟦 <b>تم فتح وضع الرد للعميل</b>\n"
            f"👤 <b>سيظهر اسمك للعميل كالتالي</b>: {trader_line}\n"
            f"🧾 <b>رقم الطلب</b>: <code>{order_id}</code>\n"
            f"{snap}\n\n"
            "✍️ <b>اكتب ردك الآن</b>\n"
            "• اكتب تفاصيل واضحة ومختصرة\n"
            "• تجنب أي بيانات حساسة\n"
            "✅ سيتم إرسال الرد مباشرة داخل PP"
        ),
        parse_mode="HTML",
        reply_markup=trader_reply_done_kb(),
        disable_web_page_preview=True,
    )
    

async def trader_reply_done_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await _alert(q, "")
    actor_id = q.from_user.id
    ad = context.user_data.setdefault(actor_id, {})
    ad.pop("trader_reply_order_id", None)
    ad.pop("trader_reply_user_id", None)
    set_stage(context, actor_id, STAGE_NONE)
    await q.message.reply_text("تم انهاء وضع الرد")

# ===== Trader/Admin panel callbacks =====
async def trader_panel_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_workbook()  # ✅ مهم جداً: يضمن قراءة/كتابة بيانات التاجر والطلبات من الإكسل
    q = update.callback_query
    await _alert(q, "")
    parts = (q.data or "").split("|")

    if len(parts) < 2:
        return
    # pp_tprof|edit|field  OR pp_tprof|orders|pending
    action = parts[1].strip() if len(parts) >= 2 else ""
    sub = parts[2].strip() if len(parts) >= 3 else ""

    uid = q.from_user.id
    ud = get_ud(context, uid)

    # ✅ التاجر الموقوف: يسمح بفتح اللوحة فقط، ويمنع الأفعال التنفيذية
    if uid not in ADMIN_IDS and _trader_is_disabled(uid) and action in ("edit", "orders", "sub"):
        await _deny_disabled_trader_q(q, "لا يمكنك استخدام هذه الخاصية لأن حسابك موقوف")
        try:
            await show_trader_panel(q.message, context, uid)
        except Exception:
            pass
        return

    if action == "edit":
        field = sub
        labels = {
            "display_name": "اسم التاجر المعروض",
            "company_name": "اسم المتجر",
            "bank_name": "اسم البنك",
            "iban": "رقم الايبان",
            "stc_pay": "رقم STC Pay",
        }
        title = labels.get(field, "البيان")
        ud["tprof_field"] = field
        set_stage(context, uid, STAGE_TRADER_PROFILE_EDIT)
        await q.message.reply_text(
            f"{_user_name(q)}\n🟦 <b>تعديل {html.escape(title)}</b>\nاكتب القيمة الان وسيتم حفظها مباشرة",
            parse_mode="HTML",
        )
        return

    if action == "orders":
        mode = sub or "pending"
        orders = list_orders_for_trader(uid)
        rows = []
        for o in orders:
            oid = str(o.get("order_id") or "").strip()
            if not oid:
                continue
            gps = str(o.get("goods_payment_status") or "").strip().lower()
            ost = str(o.get("order_status") or "").strip().lower()
            amt = _money(o.get("goods_amount_sar") or "")
            show = False
            if mode == "done":
                show = (gps == "confirmed") or (ost in ("closed", "delivered"))
            else:
                show = not ((gps == "confirmed") or (ost in ("closed", "delivered")))
            if show:
                rows.append(f"• {oid} — {amt or '—'} — {ost or gps or 'pending'}")

        if not rows:
            await _alert(q, "لا توجد طلبات")
            return

        header = "📦 طلباتك المعلقة" if mode != "done" else "✅ طلباتك المنجزة"
        msg = "🟩 <b>%s</b>\n\n%s" % (html.escape(header), html.escape("\n".join(rows)))
        await q.message.reply_text(msg, parse_mode="HTML", disable_web_page_preview=True)
        return

    if action == "sub":
        # 💳 اشتراك شهري للتاجر (99 ر.س)
        month = month_key_utc()
        amount = 99
        ud["sub_month"] = month
        ud["sub_amount_sar"] = amount
        ud["sub_kind"] = "trader_subscription"
        set_stage(context, uid, STAGE_TRADER_SUB_AWAIT_PAY_METHOD)

        try:
            upsert_trader_subscription(uid, month, {
                "amount_sar": amount,
                "payment_status": "awaiting",
            })
        except Exception:
            pass

        msg = (
            "💳 <b>اشتراك المنصة للتاجر</b>\n"
            f"📅 الشهر: <b>{html.escape(month)}</b>\n"
            f"💰 قيمة الاشتراك: <b>{amount}</b> ريال\n\n"
            "⬇️ اختر طريقة الدفع ثم ارسل إيصال السداد هنا."
        )
        await q.message.reply_text(msg, parse_mode="HTML", reply_markup=pay_method_kb(), disable_web_page_preview=True)
        return

    # default: refresh
    try:
        await show_trader_panel(q.message, context, uid)
    except Exception:
        pass

    
async def trader_reply_admin_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await _alert(q, "")
    tid = q.from_user.id

    parts = (q.data or "").split("|")
    admin_id = 0
    if len(parts) >= 2:
        try:
            admin_id = int(parts[1] or 0)
        except Exception:
            admin_id = 0

    if not admin_id:
        await _alert(q, "بيانات غير مكتملة")
        return

    ud = get_ud(context, tid)
    ud["reply_to_admin_id"] = int(admin_id)
    ud[STAGE_KEY] = "trader_reply_admin_msg"

    msg = (
        "💬 <b>رد للإدارة</b>\n\n"
        "اكتب رسالتك الآن وسيتم إرسالها للإدارة مباشرة."
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔒 إغلاق", callback_data="pp_ui_close")],
    ])
    await _admin_edit_or_send(q, msg, kb)  # نفس دالة edit لتفادي التكدس

    
async def _admin_edit_or_send(q, text: str, kb: InlineKeyboardMarkup = None):
    """تحديث نفس رسالة اللوحة قدر الإمكان لتفادي التشوه البصري + عدم الصمت."""
    try:
        await q.edit_message_text(
            text=text,
            parse_mode="HTML",
            reply_markup=kb,
            disable_web_page_preview=True,
        )
        return
    except Exception:
        pass

    # fallback: رسالة جديدة إذا تعذر التعديل
    try:
        await q.message.reply_text(
            text,
            parse_mode="HTML",
            reply_markup=kb,
            disable_web_page_preview=True,
        )
    except Exception:
        # آخر حل: تنبيه فقط
        try:
            await _alert(q, "تعذر عرض الصفحة")
        except Exception:
            pass

async def _notify_admins(context: ContextTypes.DEFAULT_TYPE, text: str, exclude_id: int = 0):
    for aid in ADMIN_IDS:
        if exclude_id and aid == exclude_id:
            continue
        try:
            await context.bot.send_message(
                chat_id=aid,
                text=text,
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
        except Exception:
            pass

async def admin_panel_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_workbook()
    q = update.callback_query
    await _alert(q, "")
    uid = q.from_user.id

    if uid not in ADMIN_IDS:
        await _alert(q, "غير مصرح")
        return

    parts = (q.data or "").split("|")
    action = parts[1].strip() if len(parts) >= 2 else "home"

    async def _go_home():
        try:
            st0 = compute_admin_financials()
            total_amt0 = _money(st0.get("total_confirmed_amount", 0))
            total_cnt0 = int(st0.get("total_confirmed_count", 0) or 0)
        except Exception:
            total_amt0, total_cnt0 = "", 0

        body0 = (
            "🟥 <b>لوحة الادارة</b>\n"
            f"✅ الطلبات المؤكدة (قيمة القطع): <b>{total_cnt0}</b>\n"
            f"💰 اجمالي المبالغ المؤكدة: <b>{html.escape(total_amt0)}</b>\n\n"
            "اختر من الازرار لعرض التفاصيل."
        )
        await _admin_edit_or_send(q, body0, admin_panel_kb())

    async def _admin_show_traders_manage():
        # قائمة التجار -> فتح ملف التاجر + تفعيل/تعطيل مباشر
        try:
            trs = list_traders() or []
        except Exception:
            trs = []

        if not trs:
            msg = "🧑‍💼 <b>إدارة التجار</b>\nلا يوجد تجار مسجلين بعد"
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("↩️ رجوع", callback_data="pp_admin|home")]])
            await _admin_edit_or_send(q, msg, kb)
            return

        # ترتيب: المفعل أولاً ثم الموقوف
        def _en(t):
            try:
                tid0 = int(t.get("trader_id") or 0)
            except Exception:
                tid0 = 0
            if not tid0:
                return 9
            try:
                return 0 if is_trader_enabled(tid0) else 1
            except Exception:
                return 0

        trs = sorted(trs, key=_en)[:40]

        rows = []
        for t in trs:
            try:
                tid = int(t.get("trader_id") or 0)
            except Exception:
                tid = 0
            if not tid:
                continue

            tlabel = _trader_label(tid, "")
            try:
                en_now = is_trader_enabled(tid)
            except Exception:
                en_now = True

            # زر ملف التاجر
            rows.append([InlineKeyboardButton(f"👤 ملف — {tlabel}", callback_data=f"pp_admin|tview|{tid}")])

            # زر تفعيل/تعطيل مباشر
            rows.append([InlineKeyboardButton(
                f"{'⛔ تعطيل' if en_now else '✅ تفعيل'} — {tlabel}",
                callback_data=f"pp_admin|tset|{tid}|{'off' if en_now else 'on'}"
            )])

        msg = "🧑‍💼 <b>إدارة التجار</b>\nاختر تاجر لفتح ملفه أو تفعيل/تعطيل:"
        kb = InlineKeyboardMarkup(rows + [[InlineKeyboardButton("↩️ رجوع", callback_data="pp_admin|home")]])
        await _admin_edit_or_send(q, msg, kb)

    # ===== FINANCE =====
    if action == "finance":
        try:
            st = compute_revenue_breakdown()
        except Exception:
            st = {
                "platform_fees_confirmed": 0,
                "platform_fees_pending": 0,
                "traders_goods_confirmed": 0,
                "shipping_confirmed": 0,
            }

        msg = (
            "💼 <b>التقارير المالية</b>\n\n"
            f"🏦 دخل المنصة (مؤكد): <b>{_money(st.get('platform_fees_confirmed', 0))}</b>\n"
            f"⌛ دخل المنصة (غير مؤكد): <b>{_money(st.get('platform_fees_pending', 0))}</b>\n\n"
            f"🧾 قيمة قطع التجار (مؤكد): <b>{_money(st.get('traders_goods_confirmed', 0))}</b>\n"
            f"🚚 رسوم الشحن (مؤكد): <b>{_money(st.get('shipping_confirmed', 0))}</b>"
        )
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("↩️ رجوع", callback_data="pp_admin|home")]])
        await _admin_edit_or_send(q, msg, kb)
        return

    # ===== FIND ORDER =====
    if action == "find_order":
        try:
            set_stage(context, uid, STAGE_ADMIN_FIND_ORDER)
        except Exception:
            pass
        msg = "🔎 <b>بحث عن طلب</b>\n\nاكتب رقم الطلب الآن:"
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("↩️ رجوع", callback_data="pp_admin|home")]])
        await _admin_edit_or_send(q, msg, kb)
        return

    # ===== HOME =====
    if action in ("home", ""):
        await _go_home()
        return

    # ===== STATS =====
    if action == "stats":
        try:
            st = compute_admin_financials()
            total_amt = _money(st.get("total_confirmed_amount", 0))
            total_cnt = int(st.get("total_confirmed_count", 0) or 0)
            msg = (
                "📊 <b>احصائيات المنصة</b>\n"
                f"✅ عدد الطلبات المؤكدة: <b>{total_cnt}</b>\n"
                f"💰 اجمالي المبالغ المؤكدة: <b>{html.escape(total_amt)}</b>\n"
            )
        except Exception:
            msg = "🟥 <b>احصائيات المنصة</b>\nتعذر قراءة الاحصائيات"

        kb = InlineKeyboardMarkup([[InlineKeyboardButton("↩️ رجوع", callback_data="pp_admin|home")]])
        await _admin_edit_or_send(q, msg, kb)
        return
    
    # ===== Toggle Platform Fee Free Mode =====
    if action == "fee_free":
        enabled = _is_platform_fee_free_mode()
        status = "✅ مفعل (رسوم المنصة = 0)" if enabled else "⛔ غير مفعل (الرسوم طبيعية)"
        msg = f"🎁 <b>العرض المجاني لرسوم المنصة</b>\nالحالة: {status}"

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ تفعيل المجاني", callback_data="pp_admin|fee_free_on")],
            [InlineKeyboardButton("⛔ إلغاء المجاني", callback_data="pp_admin|fee_free_off")],
            [InlineKeyboardButton("🏠 الرئيسية", callback_data="pp_admin|home")],
        ])
        await _admin_edit_or_send(q, msg, kb)
        return

    if action == "fee_free_on":
        _set_platform_fee_free_mode(True)
        await _alert(q, "تم تفعيل العرض المجاني")
        await _admin_edit_or_send(
            q,
            "✅ تم تفعيل العرض المجاني لرسوم المنصة (رسوم المنصة = 0)",
            InlineKeyboardMarkup([[InlineKeyboardButton("↩️ رجوع", callback_data="pp_admin|fee_free")]])
        )
        return

    if action == "fee_free_off":
        _set_platform_fee_free_mode(False)
        await _alert(q, "تم إلغاء العرض المجاني")
        await _admin_edit_or_send(
            q,
            "⛔ تم إلغاء العرض المجاني (رجعت رسوم المنصة كما كانت)",
            InlineKeyboardMarkup([[InlineKeyboardButton("↩️ رجوع", callback_data="pp_admin|fee_free")]])
        )
        return

    # ===== RESTORE DB =====
    if action == "restore_db":
        txt = (
            "🗂 <b>استرجاع قاعدة البيانات</b>\n\n"
            "✅ الطريقة 1: أرسل ملف الإكسل هنا في الخاص (للأدمن فقط).\n"
            "✅ الطريقة 2: أرسل ملف الإكسل داخل مجموعة النسخ.\n\n"
            "🔐 للأمان: فعّل الاسترجاع أولاً بالأمر:\n"
            "<code>/restorepass كلمة_المرور</code>\n"
        )
        try:
            await _admin_edit_or_send(q, txt, InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ رجوع", callback_data="pp_admin|home")],
                [InlineKeyboardButton("✖️ إغلاق", callback_data="ui_close")],
            ]))
        except Exception:
            pass
        return

    # ===== MAINT =====
    if action == "maint":
        on = _is_maintenance_mode()
        state = "🟧 مفعّل" if on else "🟩 غير مفعّل"
        msg = (
            "⚙️ <b>وضع الصيانة</b>\n"
            f"الحالة الحالية: <b>{state}</b>\n\n"
            "عند التفعيل سيتم منع استقبال الطلبات الجديدة وتقديم عروض السعر (لغير الادمن)."
        )
        kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("🟧 تفعيل الصيانة", callback_data="pp_admin|maint_on"),
                InlineKeyboardButton("🟩 إيقاف الصيانة", callback_data="pp_admin|maint_off"),
            ],
            [InlineKeyboardButton("↩️ رجوع", callback_data="pp_admin|home")],
        ])
        await _admin_edit_or_send(q, msg, kb)
        return

    if action in ("maint_on", "maint_off"):
        on = (action == "maint_on")
        try:
            set_setting("maintenance_mode", "on" if on else "off", actor_id=uid, actor_name=(q.from_user.full_name or ""))
            try:
                append_legal_log(uid, (q.from_user.full_name or ""), "maintenance_mode", f"{'on' if on else 'off'}")
            except Exception:
                pass

            try:
                await _notify_admins(
                    context,
                    f"⚙️ <b>تحديث وضع الصيانة</b>\n"
                    f"👤 بواسطة: <b>{html.escape(q.from_user.full_name or str(uid))}</b>\n"
                    f"🔁 الحالة: <b>{'مفعّل' if on else 'متوقف'}</b>",
                    exclude_id=uid
                )
            except Exception:
                pass

            await _alert(q, "تم التحديث ✅")
        except Exception:
            await _alert(q, "فشل التحديث")

        await _go_home()
        return

    # ===== TRADERS STATS =====
    if action == "traders":
        try:
            st = compute_admin_financials()
            per_amt = st.get("per_trader_amount", {}) or {}
            per_cnt = st.get("per_trader_count", {}) or {}
        except Exception:
            per_amt, per_cnt = {}, {}

        if not per_amt:
            msg = "👥 <b>احصائيات التجار</b>\nلا توجد بيانات مؤكدة بعد"
        else:
            lines = []
            for tid, amt in sorted(per_amt.items(), key=lambda x: float(x[1] or 0), reverse=True)[:30]:
                tlabel = _trader_label(int(tid), "")
                lines.append(f"• {tlabel} — {_money(amt)} — {int(per_cnt.get(tid, 0) or 0)} طلب")
            msg = "👥 <b>احصائيات التجار</b>\n\n" + html.escape("\n".join(lines))

        kb = InlineKeyboardMarkup([[InlineKeyboardButton("↩️ رجوع", callback_data="pp_admin|home")]])
        await _admin_edit_or_send(q, msg, kb)
        return

    # ===== ORDERS =====
    if action == "orders":
        try:
            orders = list_orders() or []
        except Exception:
            orders = []

        def _dt(o):
            v = str(o.get("created_at_utc") or "")
            try:
                return datetime.fromisoformat(v.replace("Z", "+00:00"))
            except Exception:
                return datetime.min.replace(tzinfo=timezone.utc)

        orders_sorted = sorted(orders, key=_dt, reverse=True)[:20]

        lines = []
        for o in orders_sorted:
            oid = str(o.get("order_id") or "").strip()
            if not oid:
                continue
            uname = str(o.get("user_name") or "").strip() or "عميل"
            ost = str(o.get("order_status") or o.get("status") or "").strip() or "—"
            amt = _money(o.get("goods_amount_sar") or o.get("quote_amount_sar") or "") or "—"
            lines.append(f"• {oid} — {uname} — {amt} — {ost}")

        msg = "📦 <b>أحدث الطلبات</b>\n\n" + html.escape("\n".join(lines) or "لا يوجد")
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("↩️ رجوع", callback_data="pp_admin|home")]])
        await _admin_edit_or_send(q, msg, kb)
        return

    # ===== SUBS =====
    if action == "subs":
        month = month_key_utc()
        try:
            subs = list_trader_subscriptions(month) or []
        except Exception:
            subs = []

        confirmed = set()
        pending = set()
        for s in subs:
            try:
                tid = int(s.get("trader_id") or 0)
            except Exception:
                tid = 0
            stv = str(s.get("payment_status") or "").strip().lower()
            if stv == "confirmed":
                confirmed.add(tid)
            elif stv in ("pending", "awaiting"):
                pending.add(tid)

        try:
            traders = list_traders() or []
        except Exception:
            traders = []

        overdue_lines = []
        paid_lines = []
        for t in traders:
            try:
                tid = int(t.get("trader_id") or 0)
            except Exception:
                tid = 0
            name = (t.get("display_name") or t.get("company_name") or "").strip() or str(tid)
            if tid in confirmed:
                paid_lines.append(f"🟩 {name} — مدفوع")
            elif tid in pending:
                overdue_lines.append(f"🟨 {name} — قيد التحقق")
            else:
                overdue_lines.append(f"🟥 {name} — متأخر")

        text = (
            f"💳 <b>اشتراكات التجار</b>\n"
            f"📅 الشهر: <b>{html.escape(month)}</b>\n\n"
            f"✅ المدفوع: <b>{len(paid_lines)}</b>\n"
            f"⏳/❌ المتأخر/قيد التحقق: <b>{len(overdue_lines)}</b>\n\n"
            "<b>🟩 المدفوع</b>\n" + (html.escape("\n".join(paid_lines)) if paid_lines else "—") + "\n\n"
            "<b>🟥/🟨 المتأخر / قيد التحقق</b>\n" + (html.escape("\n".join(overdue_lines[:40])) if overdue_lines else "—")
        )

        kb = InlineKeyboardMarkup([[InlineKeyboardButton("↩️ رجوع", callback_data="pp_admin|home")]])
        await _admin_edit_or_send(q, text, kb)
        return

    # ===== TRADERS MANAGE (list -> profiles) =====
    if action == "traders_manage":
        await _admin_show_traders_manage()
        return

    # ===== TRADER PROFILE (tview) =====
    if action == "tview":
        tid = 0
        if len(parts) >= 3:
            try:
                tid = int(parts[2] or 0)
            except Exception:
                tid = 0

        if not tid:
            await _alert(q, "بيانات غير مكتملة")
            return

        try:
            prof = get_trader_profile(tid) or {}
        except Exception:
            prof = {}

        tname = (prof.get("display_name") or "").strip()
        tcompany = (prof.get("company_name") or "").strip()
        bank = (prof.get("bank_name") or "").strip()
        iban = (prof.get("iban") or "").strip()
        stc = (prof.get("stc_pay") or "").strip()
        upd = (prof.get("updated_at_utc") or "").strip()

        label = (tname or "التاجر") + (f" ({tcompany})" if tcompany else "")

        # ✅ الحالة من المصدر الرسمي (بدلاً من prof)
        try:
            enabled = is_trader_enabled(tid)
        except Exception:
            enabled = True

        enabled_txt = "🟩 مفعل" if enabled else "🟥 موقوف"

        # subscription status (current month)
        month = month_key_utc()
        sub_status = "—"
        try:
            subs = list_trader_subscriptions(month) or []
            st_map = {}
            for s in subs:
                try:
                    x = int(s.get("trader_id") or 0)
                except Exception:
                    x = 0
                if not x:
                    continue
                st_map[x] = str(s.get("payment_status") or "").strip().lower()
            stv = st_map.get(int(tid), "")
            if stv == "confirmed":
                sub_status = "🟩 مدفوع"
            elif stv in ("pending", "awaiting"):
                sub_status = "🟨 قيد التحقق"
            else:
                sub_status = "🟥 متأخر"
        except Exception:
            pass

        # orders stats
        total_orders = 0
        confirmed_orders = 0
        confirmed_amt = 0.0
        last_order_id = ""
        last_order_ts = ""

        try:
            orders = list_orders_for_trader(tid) or []
        except Exception:
            orders = []

        def _dt(o):
            v = str(o.get("created_at_utc") or "")
            try:
                return datetime.fromisoformat(v.replace("Z", "+00:00"))
            except Exception:
                return datetime.min.replace(tzinfo=timezone.utc)

        if orders:
            orders_sorted = sorted(orders, key=_dt, reverse=True)
            total_orders = len(orders_sorted)

            lo = orders_sorted[0]
            last_order_id = str(lo.get("order_id") or "").strip()
            last_order_ts = str(lo.get("created_at_utc") or "").strip()

            for o in orders_sorted:
                gps = str(o.get("goods_payment_status") or "").strip().lower()
                ost = str(o.get("order_status") or "").strip().lower()
                if gps == "confirmed" or ost in ("closed", "delivered"):
                    confirmed_orders += 1
                    raw = str(o.get("goods_amount_sar") or "").strip()
                    try:
                        confirmed_amt += float(re.sub(r"[^0-9.]+", "", raw) or 0)
                    except Exception:
                        pass

        msg = (
            "👤 <b>ملف التاجر</b>\n\n"
            f"🆔 ID: <b>{tid}</b>\n"
            f"👤 الاسم: <b>{html.escape(label)}</b>\n"
            f"🔘 الحالة: <b>{enabled_txt}</b>\n"
            f"💳 الاشتراك ({html.escape(month)}): <b>{sub_status}</b>\n\n"
            f"📦 عدد الطلبات: <b>{total_orders}</b>\n"
            f"✅ طلبات مؤكدة (قيمة القطع): <b>{confirmed_orders}</b>\n"
            f"💰 إجمالي مؤكد للتاجر: <b>{html.escape(_money(confirmed_amt))}</b>\n\n"
            f"🏦 البنك: <b>{html.escape(bank or '—')}</b>\n"
            f"🏷️ IBAN: <b>{html.escape(iban or '—')}</b>\n"
            f"📱 STC Pay: <b>{html.escape(stc or '—')}</b>\n"
            f"🕓 آخر تحديث: <b>{html.escape(upd or '—')}</b>\n\n"
            f"🧾 آخر طلب: <b>{html.escape(last_order_id or '—')}</b>\n"
            f"🗓️ وقت آخر طلب: <b>{html.escape(last_order_ts or '—')}</b>"
        )

        kb_rows = [
            [InlineKeyboardButton("💬 مراسلة التاجر", callback_data=f"pp_admin|tmsg|{tid}")],
            [InlineKeyboardButton("📤 كشف معاملات (CSV)", callback_data=f"pp_admin|texport|{tid}")],
            [InlineKeyboardButton("📦 آخر طلبات التاجر", callback_data=f"pp_admin|torders|{tid}")],
            [InlineKeyboardButton("⛔ تعطيل التاجر" if enabled else "✅ تفعيل التاجر",
                                  callback_data=f"pp_admin|tset|{tid}|{'off' if enabled else 'on'}")],
            [InlineKeyboardButton("↩️ رجوع لقائمة التجار", callback_data="pp_admin|traders_manage")],
            [InlineKeyboardButton("🏠 الرئيسية", callback_data="pp_admin|home")],
        ]
        await _admin_edit_or_send(q, msg, InlineKeyboardMarkup(kb_rows))
        return

    # ===== TRADER ORDERS (torders) =====
    if action == "torders":
        tid = 0
        if len(parts) >= 3:
            try:
                tid = int(parts[2] or 0)
            except Exception:
                tid = 0
        if not tid:
            await _alert(q, "بيانات غير مكتملة")
            return

        try:
            orders = list_orders_for_trader(tid) or []
        except Exception:
            orders = []

        def _dt(o):
            v = str(o.get("created_at_utc") or "")
            try:
                return datetime.fromisoformat(v.replace("Z", "+00:00"))
            except Exception:
                return datetime.min.replace(tzinfo=timezone.utc)

        orders_sorted = sorted(orders, key=_dt, reverse=True)[:15]

        try:
            prof = get_trader_profile(tid) or {}
        except Exception:
            prof = {}
        nm = (prof.get("display_name") or "").strip() or str(tid)

        if not orders_sorted:
            msg = f"📦 <b>طلبات التاجر</b>\nالتاجر: <b>{html.escape(nm)}</b>\n\nلا يوجد طلبات بعد"
        else:
            lines = []
            for o in orders_sorted:
                oid = str(o.get("order_id") or "").strip()
                ost = str(o.get("order_status") or "").strip() or "—"
                amt = _money(o.get("goods_amount_sar") or o.get("quote_amount_sar") or "") or "—"
                ts = str(o.get("created_at_utc") or "").strip()
                lines.append(f"• {oid} — {amt} — {ost} — {ts}")
            msg = f"📦 <b>طلبات التاجر</b>\nالتاجر: <b>{html.escape(nm)}</b>\n\n" + html.escape("\n".join(lines))

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("↩️ رجوع لملف التاجر", callback_data=f"pp_admin|tview|{tid}")],
            [InlineKeyboardButton("↩️ رجوع لقائمة التجار", callback_data="pp_admin|traders_manage")],
        ])
        await _admin_edit_or_send(q, msg, kb)
        return

    # ===== MESSAGE TRADER (tmsg) =====
    if action == "tmsg":
        tid = 0
        if len(parts) >= 3:
            try:
                tid = int(parts[2] or 0)
            except Exception:
                tid = 0
        if not tid:
            await _alert(q, "بيانات غير مكتملة")
            return

        ud = get_ud(context, uid)
        ud["admin_msg_to_trader_id"] = int(tid)
        ud[STAGE_KEY] = STAGE_ADMIN_TRADER_MSG

        msg = (
            "💬 <b>مراسلة التاجر</b>\n\n"
            f"🆔 التاجر: <b>{tid}</b>\n"
            "اكتب رسالتك الآن وسيتم إرسالها للتاجر مباشرة."
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("↩️ رجوع لملف التاجر", callback_data=f"pp_admin|tview|{tid}")],
            [InlineKeyboardButton("🏠 الرئيسية", callback_data="pp_admin|home")],
        ])
        await _admin_edit_or_send(q, msg, kb)
        return

    # ===== EXPORT TRADER CSV (texport) =====
    if action == "texport":
        tid = 0
        if len(parts) >= 3:
            try:
                tid = int(parts[2] or 0)
            except Exception:
                tid = 0
        if not tid:
            await _alert(q, "بيانات غير مكتملة")
            return

        try:
            import io, csv
        except Exception:
            await _alert(q, "تعذر التصدير")
            return

        try:
            prof = get_trader_profile(tid) or {}
        except Exception:
            prof = {}
        nm = (prof.get("display_name") or "").strip() or str(tid)

        try:
            orders = list_orders_for_trader(tid) or []
        except Exception:
            orders = []

        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow([
            "order_id",
            "created_at_utc",
            "order_status",
            "user_name",
            "goods_amount_sar",
            "goods_payment_status",
            "shipping_fee_sar",
            "payment_status",
            "price_sar",
        ])
        for o in (orders or []):
            w.writerow([
                str(o.get("order_id") or ""),
                str(o.get("created_at_utc") or ""),
                str(o.get("order_status") or o.get("status") or ""),
                str(o.get("user_name") or ""),
                str(o.get("goods_amount_sar") or ""),
                str(o.get("goods_payment_status") or ""),
                str(o.get("shipping_fee_sar") or ""),
                str(o.get("payment_status") or ""),
                str(o.get("price_sar") or ""),
            ])

        data = buf.getvalue()
        b = io.BytesIO(data.encode("utf-8-sig"))
        b.name = f"trader_{tid}_orders_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"

        try:
            await context.bot.send_document(
                chat_id=uid,
                document=b,
                caption=f"📤 كشف معاملات التاجر (CSV)\nالتاجر: {nm}\nID: {tid}",
            )
            try:
                append_legal_log(uid, (q.from_user.full_name or ""), "export_trader_csv", f"trader_id={tid}; rows={len(orders or [])}")
            except Exception:
                pass
            await _alert(q, "تم إرسال الملف ✅")
        except Exception:
            await _alert(q, "تعذر إرسال الملف")

        await _admin_edit_or_send(
            q,
            f"✅ تم تجهيز كشف التاجر: <b>{html.escape(nm)}</b>\nID: <b>{tid}</b>",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("👤 فتح ملف التاجر", callback_data=f"pp_admin|tview|{tid}")],
                [InlineKeyboardButton("↩️ رجوع لقائمة التجار", callback_data="pp_admin|traders_manage")],
            ])
        )
        return

    # ===== TRADER ENABLE/DISABLE (tset) =====
    if action == "tset":
        tid = 0
        flag = "on"
        if len(parts) >= 3:
            try:
                tid = int(parts[2] or 0)
            except Exception:
                tid = 0
        if len(parts) >= 4:
            flag = (parts[3] or "on").strip().lower()

        if not tid:
            await _alert(q, "بيانات غير مكتملة")
            return

        enable = (flag == "on")
        try:
            set_trader_enabled(tid, enable)
            try:
                append_legal_log(uid, (q.from_user.full_name or ""), "trader_enable",
                                 f"trader_id={tid}; enabled={'yes' if enable else 'no'}")
            except Exception:
                pass

            try:
                await _notify_admins(
                    context,
                    f"🧑‍💼 <b>تحديث حالة تاجر</b>\n"
                    f"👤 بواسطة: <b>{html.escape(q.from_user.full_name or str(uid))}</b>\n"
                    f"🆔 التاجر: <b>{tid}</b>\n"
                    f"🔁 الحالة: <b>{'مفعل' if enable else 'موقوف'}</b>",
                    exclude_id=uid
                )
            except Exception:
                pass

            await _alert(q, "تم تحديث حالة التاجر ✅")
        except Exception:
            await _alert(q, "فشل تحديث التاجر")

        await _admin_edit_or_send(
            q,
            "✅ تم تحديث حالة التاجر",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("👤 فتح ملف التاجر", callback_data=f"pp_admin|tview|{tid}")],
                [InlineKeyboardButton("↩️ رجوع لقائمة التجار", callback_data="pp_admin|traders_manage")],
                [InlineKeyboardButton("🏠 الرئيسية", callback_data="pp_admin|home")],
            ])
        )
        return

    # ===== LOG =====
    if action == "log":
        try:
            logs = list_legal_log(limit=30) or []
        except Exception:
            logs = []

        if not logs:
            msg = "🧾 <b>سجل الإجراءات</b>\nلا يوجد سجل بعد"
        else:
            lines = []
            for e in logs:
                ts = str(e.get("ts_utc") or "")
                an = str(e.get("actor_name") or "") or str(e.get("actor_id") or "")
                ac = str(e.get("action") or "")
                det = str(e.get("details") or "")
                line = f"• {ts} — {an} — {ac}"
                if det:
                    line += f" — {det}"
                lines.append(line)
            msg = "🧾 <b>سجل الإجراءات (آخر 30)</b>\n\n" + html.escape("\n".join(lines))

        kb = InlineKeyboardMarkup([[InlineKeyboardButton("↩️ رجوع", callback_data="pp_admin|home")]])
        await _admin_edit_or_send(q, msg, kb)
        return

    await _alert(q, "أمر غير معروف")


def trader_panel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🧑‍💼 تعديل اسم التاجر", callback_data="pp_tprof|edit|display_name")],
        [InlineKeyboardButton("🏢 تعديل اسم المتجر", callback_data="pp_tprof|edit|company_name")],
        [InlineKeyboardButton("🏦 تعديل اسم البنك", callback_data="pp_tprof|edit|bank_name")],
        [InlineKeyboardButton("💳 تعديل رقم الايبان", callback_data="pp_tprof|edit|iban")],
        [InlineKeyboardButton("📱 تعديل رقم STC Pay", callback_data="pp_tprof|edit|stc_pay")],
      # [InlineKeyboardButton("💳 سداد اشتراك المنصة (99 ر.س)", callback_data="pp_tprof|sub|start")],
        [InlineKeyboardButton("📦 طلباتي المعلقة", callback_data="pp_tprof|orders|pending")],
        [InlineKeyboardButton("✅ طلباتي المنجزة", callback_data="pp_tprof|orders|done")],
        # ✅ فتح قناة الاتصال مع الإدارة (كل ADMIN_IDS من البيئة) داخل الخاص
        [InlineKeyboardButton("📩 اتصل بالمنصة", callback_data="pp_support_open")],
    ])

def admin_panel_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👥 إدارة التجار", callback_data="pp_admin|traders_manage")],
        [InlineKeyboardButton("📊 التقارير المالية", callback_data="pp_admin|finance")],
        [InlineKeyboardButton("🔎 بحث عن طلب", callback_data="pp_admin|find_order")],
        [InlineKeyboardButton("🎁 عرض مجاني لرسوم المنصة", callback_data="pp_admin|fee_free")],  # ✅ جديد
        [InlineKeyboardButton("🗂 استرجاع قاعدة البيانات", callback_data="pp_admin|restore_db")],
        [InlineKeyboardButton("⚙️ الصيانة", callback_data="pp_admin|maint")],
        [InlineKeyboardButton("✖️ إغلاق", callback_data="ui_close")],
    ])

async def show_trader_panel(update_or_q, context: ContextTypes.DEFAULT_TYPE, trader_id: int):
    ensure_workbook()  # ✅ مهم: يضمن قراءة/كتابة بيانات التاجر من الاكسل بشكل سليم

    # ✅ الشرط الأساسي: لازم يكون عضو في مجموعة التجار (عضو عادي يكفي)
    try:
        is_member = await _is_trader_group_member(context, int(trader_id or 0))
    except Exception:
        is_member = False

    # ✅ سماح لفتح لوحة التاجر حتى لو كان موقوف (بدون ربطها بعضوية مجموعة)
    # المنطق:
    # - إذا كان عضو مجموعة التجار => مسموح
    # - إذا كان له ملف تاجر موجود في الاكسل => مسموح
    # - إذا كان أدمن => مسموح
    tp = get_trader_profile(int(trader_id or 0)) or {}
    is_admin = int(trader_id or 0) in (ADMIN_IDS or [])

    # ✅ إضافة آمنة: إذا ليس عضو ولا ملف ولا أدمن
    # نسمح فقط إذا كان "مسجل في شيت التجار" (مثلاً تم تفعيله/إيقافه من الإدارة)
    is_registered_trader = False
    if not is_member and not tp and not is_admin:
        try:
            tid_s = str(int(trader_id or 0))
            for t in (list_traders() or []):
                if str(t.get("trader_id") or "").strip() == tid_s:
                    is_registered_trader = True
                    break
        except Exception:
            is_registered_trader = False

    if not is_member and not tp and not is_admin and not is_registered_trader:
        # 🚫 منع العملاء (غير أعضاء مجموعة التجار ولا لديهم ملف تاجر ولا هم مسجلون كتاجر)
        try:
            if hasattr(update_or_q, "message") and update_or_q.message:
                await update_or_q.message.reply_text("غير مصرح")
            else:
                try:
                    await update_or_q.answer("غير مصرح", show_alert=True)
                except Exception:
                    try:
                        await update_or_q.edit_message_text("غير مصرح")
                    except Exception:
                        pass
        except Exception:
            pass
        return

    # ✅ نحضر ملف التاجر من الشيت
    tp = tp or {}

    # ✅ مهم: لا ننشئ سجل تاجر جديد إلا إذا كان عضو مجموعة أو أدمن
    # حتى ما نكتب صفوف جديدة بسبب وصول "مسجل في شيت التجار" فقط
    if not tp and (is_member or is_admin):
        try:
            upsert_trader_profile(int(trader_id or 0), {"trader_id": int(trader_id or 0)})
            tp = get_trader_profile(int(trader_id or 0)) or {}
        except Exception:
            tp = tp or {}

    dn = (tp.get("display_name") or "").strip() or (
        getattr(update_or_q, "from_user", None).full_name if getattr(update_or_q, "from_user", None) else ""
    ) or "التاجر"
    cn = (tp.get("company_name") or "").strip() or "غير محدد"
    pay_block = _trade_payment_block(tp)

    # ✅ مصدر الحقيقة الوحيد للحالة
    try:
        enabled = is_trader_enabled(int(trader_id or 0))
    except Exception:
        enabled = False  # ✅ آمن: لا نُظهره "مفعل" إذا فشلنا نقرأ الحالة

    status_txt = "مفعل ✅" if enabled else "موقوف ⛔"

    # ✅ بانر واضح للموقوف
    banner = ""
    if not enabled:
        banner = (
            "⛔ <b>تنبيه:</b> حسابك موقوف حاليًا، يمكنك استعراض بياناتك فقط.\n"
            "للاستفسار تواصل مع الإدارة من الزر بالأسفل.\n\n"
        )

    txt = (
        f"{banner}"
        "🟩 <b>لوحة التاجر</b>\n"
        f"🔒 الحالة: <b>{status_txt}</b>\n"
        f"👤 الاسم المعروض: <b>{html.escape(dn)}</b>\n"
        f"🏢 المتجر: <b>{html.escape(cn)}</b>\n"
        f"🧾 بيانات التحويل:\n<pre>{html.escape(pay_block)}</pre>\n"
        "ℹ️ هذه البيانات تحفظ مباشرة داخل ملف المنصة وتبقى حتى بعد اعادة التشغيل.\n"
    )

    # ✅ كيبورد اللوحة:
    # - للتاجر المفعل: كما هو trader_panel_kb()
    # - للتاجر الموقوف: نفس الكيبورد + زر مراسلة الإدارة بالأسفل
    try:
        kb = trader_panel_kb()
    except Exception:
        kb = None

    if kb and not enabled:
        try:
            rows = [row[:] for row in (kb.inline_keyboard or [])]
            kb = InlineKeyboardMarkup(rows)
        except Exception:
            pass

    # ✅ سياسة: ما نرسل شيء للمجموعات (لو انضغط الزر من مجموعة نرسل للخاص فقط)
    try:
        if hasattr(update_or_q, "message") and update_or_q.message:
            chat_type = getattr(update_or_q.message.chat, "type", None)
            if chat_type and str(chat_type).lower() != "private":
                # لا نكتب في المجموعة
                try:
                    await update_or_q.message.reply_text("ℹ️ تم إرسال لوحة التاجر لك في الخاص.")
                except Exception:
                    pass
                try:
                    await context.bot.send_message(
                        chat_id=int(trader_id or 0),
                        text=txt,
                        parse_mode="HTML",
                        reply_markup=kb,
                        disable_web_page_preview=True,
                    )
                except Exception:
                    pass
                return
        else:
            # callback_query
            msg = getattr(update_or_q, "message", None)
            chat = getattr(msg, "chat", None) if msg else None
            chat_type = getattr(chat, "type", None) if chat else None
            if chat_type and str(chat_type).lower() != "private":
                # لا نكتب في المجموعة
                try:
                    await update_or_q.answer("ℹ️ تم إرسال لوحة التاجر لك في الخاص.", show_alert=True)
                except Exception:
                    pass
                try:
                    await context.bot.send_message(
                        chat_id=int(trader_id or 0),
                        text=txt,
                        parse_mode="HTML",
                        reply_markup=kb,
                        disable_web_page_preview=True,
                    )
                except Exception:
                    pass
                return
    except Exception:
        pass

    # ✅ عرض اللوحة في الخاص (Reply أو Edit) + fallback إذا فشل editMessageText (400)
    if hasattr(update_or_q, "message") and update_or_q.message:
        try:
            await update_or_q.message.reply_text(
                txt,
                parse_mode="HTML",
                reply_markup=kb,
                disable_web_page_preview=True,
            )
        except Exception:
            # fallback للخاص
            try:
                await context.bot.send_message(
                    chat_id=int(trader_id or 0),
                    text=txt,
                    parse_mode="HTML",
                    reply_markup=kb,
                    disable_web_page_preview=True,
                )
            except Exception:
                pass
    else:
        # callback query
        try:
            await update_or_q.edit_message_text(
                txt,
                parse_mode="HTML",
                reply_markup=kb,
                disable_web_page_preview=True,
            )
        except Exception:
            # fallback: رسالة جديدة في الخاص
            try:
                await context.bot.send_message(
                    chat_id=int(trader_id or 0),
                    text=txt,
                    parse_mode="HTML",
                    reply_markup=kb,
                    disable_web_page_preview=True,
                )
            except Exception:
                pass
            

async def pp25s_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # ✅ فتح لوحة الإدارة بالأمر /pp25s (خاص فقط + ادمن فقط)
    try:
        chat = update.effective_chat
        user = update.effective_user
        if not chat or not user:
            return
        if chat.type != ChatType.PRIVATE:
            return
        user_id = int(user.id)
        if user_id not in ADMIN_IDS:
            try:
                await update.message.reply_text(f"{_user_name(update)}\nغير مصرح")
            except Exception:
                pass
            return
        set_stage(context, user_id, STAGE_NONE)
        await show_admin_panel(update, context, user_id)
    except Exception:
        try:
            await update.message.reply_text("تعذر فتح لوحة الادارة حاليا")
        except Exception:
            pass


async def show_admin_panel(update_or_q, context: ContextTypes.DEFAULT_TYPE, admin_id: int):
    """لوحة الادارة: تعديل نفس الرسالة قدر الإمكان لتفادي التشوه البصري + ضمان عمل الرجوع."""
    ensure_workbook()  # مهم لقراءة الاحصائيات والاعدادات

    st = compute_admin_financials()
    total_amt = _money(st.get("total_confirmed_amount", 0))
    total_cnt = int(st.get("total_confirmed_count", 0) or 0)

    body = (
        "🟥 <b>لوحة الادارة</b>\n"
        f"✅ الطلبات المؤكدة (قيمة القطع): <b>{total_cnt}</b>\n"
        f"💰 اجمالي المبالغ المؤكدة: <b>{html.escape(total_amt)}</b>\n\n"
        "اختر من الازرار لعرض التفاصيل."
    )

    kb = admin_panel_kb()

    # نحاول نحدد الرسالة التي سنعدلها
    msg = None
    try:
        # CallbackQuery
        if hasattr(update_or_q, "message") and getattr(update_or_q, "message", None):
            msg = update_or_q.message
        # Message
        elif hasattr(update_or_q, "edit_text"):
            msg = update_or_q
        # Update
        elif hasattr(update_or_q, "effective_message") and getattr(update_or_q, "effective_message", None):
            msg = update_or_q.effective_message
    except Exception:
        msg = None

    # edit-in-place اولاً
    if msg is not None:
        try:
            await msg.edit_text(body, parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True)
            return
        except Exception:
            pass

    # fallback: رسالة جديدة
    try:
        await context.bot.send_message(
            chat_id=admin_id,
            text=body,
            parse_mode="HTML",
            reply_markup=kb,
            disable_web_page_preview=True,
        )
    except Exception:
        pass
  

async def admin_sub_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await _alert(q, "")
    actor_id = q.from_user.id
    if actor_id not in ADMIN_IDS:
        await _alert(q, "⛔ غير مصرح")
        return

    data = (q.data or "").strip()
    parts = data.split("|")
    if len(parts) < 4:
        return
    act = (parts[1] or "").strip()
    try:
        trader_id = int(parts[2] or 0)
    except Exception:
        trader_id = 0
    month = (parts[3] or "").strip()

    if not trader_id or not month:
        return

    if act == "confirm":
        try:
            upsert_trader_subscription(trader_id, month, {
                "payment_status": "confirmed",
                "paid_at_utc": utc_now_iso(),
            })
        except Exception:
            pass

        # إشعار التاجر
        try:
            await context.bot.send_message(
                chat_id=trader_id,
                text=(
                    "✅ <b>تم تأكيد اشتراكك في المنصة</b>\n"
                    f"📅 الشهر: <b>{html.escape(month)}</b>\n"
                    "يمكنك الآن تقديم عروض السعر بشكل طبيعي."
                ),
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
        except Exception:
            pass

        # إرسال فاتورة اشتراك بسيطة للتاجر + نسخة للإدارة
        try:
            await send_trader_subscription_invoice_pdf(context, trader_id, month, 99)
        except Exception:
            pass

        try:
            await q.message.reply_text("✅ تم تأكيد الاشتراك")
        except Exception:
            pass
        return

    if act == "reject":
        try:
            upsert_trader_subscription(trader_id, month, {
                "payment_status": "rejected",
            })
        except Exception:
            pass

        try:
            await context.bot.send_message(
                chat_id=trader_id,
                text=(
                    "❌ <b>تم رفض إيصال الاشتراك</b>\n"
                    f"📅 الشهر: <b>{html.escape(month)}</b>\n"
                    "يرجى إعادة إرسال إيصال واضح أو التواصل بكتابة: منصة"
                ),
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
        except Exception:
            pass

        try:
            await q.message.reply_text("تم الرفض")
        except Exception:
            pass
        return


# ===== Backup helpers =====
def _excel_path() -> str:
    # pp_excel يعتمد على PP_EXCEL_PATH
    return (os.getenv("PP_EXCEL_PATH") or "pp_data.xlsx").strip() or "pp_data.xlsx"

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

async def _send_backup_excel(app: Application, reason: str = "scheduled") -> None:
    if not PP_BACKUP_CHAT_ID:
        return
    path = _excel_path()
    if not os.path.exists(path):
        return

    # منع التكرار (بحد أدنى)
    try:
        last = str(get_setting("last_backup_at_utc", "") or "").strip()
    except Exception:
        last = ""

    try:
        if last:
            last_dt = datetime.fromisoformat(last.replace("Z", "+00:00"))
            if (datetime.now(timezone.utc) - last_dt).total_seconds() < PP_BACKUP_MIN_SECONDS:
                return
    except Exception:
        pass

    caption = f"🗂 نسخة احتياطية\n📅 UTC: {_utc_now_iso()}\nسبب: {reason}"
    try:
        with open(path, "rb") as f:
            await app.bot.send_document(
                chat_id=PP_BACKUP_CHAT_ID,
                document=InputFile(f, filename=os.path.basename(path)),
                caption=caption,
            )
        try:
            set_setting("last_backup_at_utc", _utc_now_iso())
        except Exception:
            pass
    except Exception:
        pass

async def _backup_loop(app: Application) -> None:
    # نسخة بعد الإقلاع
    await asyncio.sleep(30)
    await _send_backup_excel(app, reason="startup")

    while True:
        await asyncio.sleep(max(1, PP_BACKUP_EVERY_HOURS) * 3600)
        await _send_backup_excel(app, reason="scheduled")

# ===== Restore helpers (Group + Private) =====
def _restore_is_admin(uid: int) -> bool:
    try:
        return int(uid) in (ADMIN_IDS or [])
    except Exception:
        return False

async def restorepass_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat
    if not user or not chat or chat.type != ChatType.PRIVATE:
        return
    if not _restore_is_admin(user.id):
        return

    # بدون كلمة مرور
    if not PP_RESTORE_PASS:
        context.user_data["restore_ok_until_utc"] = (datetime.now(timezone.utc) + timedelta(minutes=PP_RESTORE_OK_MINUTES)).isoformat()
        await update.message.reply_text("✅ تم تفعيل الاسترجاع مؤقتًا. أرسل ملف الإكسل الآن.")
        return

    args = (context.args or [])
    supplied = (args[0] if args else "").strip()
    if not supplied:
        await update.message.reply_text("🔐 اكتب: /restorepass كلمة_المرور")
        return
    if supplied != PP_RESTORE_PASS:
        await update.message.reply_text("❌ كلمة المرور غير صحيحة")
        return

    context.user_data["restore_ok_until_utc"] = (datetime.now(timezone.utc) + timedelta(minutes=PP_RESTORE_OK_MINUTES)).isoformat()
    await update.message.reply_text("✅ تم تفعيل الاسترجاع لمدة قصيرة. أرسل ملف الإكسل الآن.")

def _restore_private_ok(context: ContextTypes.DEFAULT_TYPE) -> bool:
    if not PP_RESTORE_PASS:
        return True
    v = (context.user_data or {}).get("restore_ok_until_utc") or ""
    try:
        dt = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        return datetime.now(timezone.utc) <= dt
    except Exception:
        return False

async def _restore_excel_from_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg or not msg.document:
        return

    doc = msg.document
    if not (doc.file_name or "").lower().endswith(".xlsx"):
        return

    chat = msg.chat
    uid = (msg.from_user.id if msg.from_user else 0)

    # السماح فقط للأدمن
    if not _restore_is_admin(uid):
        return

    # 1) مجموعة النسخ المحددة
    if PP_BACKUP_CHAT_ID and chat.id == PP_BACKUP_CHAT_ID:
        pass
    # 2) الخاص مع البوت + (اختياري) كلمة مرور
    elif chat.type == ChatType.PRIVATE:
        if not _restore_private_ok(context):
            if PP_RESTORE_PASS:
                await msg.reply_text("🔐 قبل الاسترجاع: اكتب /restorepass كلمة_المرور ثم أعد إرسال ملف الإكسل")
            else:
                await msg.reply_text("🔐 قبل الاسترجاع: اكتب /restorepass ثم أعد إرسال ملف الإكسل")
            return
    else:
        return

    path = _excel_path()
    try:
        f = await doc.get_file()
        await f.download_to_drive(custom_path=path)
        await msg.reply_text("✅ تم استرجاع قاعدة البيانات بنجاح وتم تشغيلها فورًا.")
    except Exception:
        try:
            await msg.reply_text("❌ فشل استرجاع النسخة، حاول مرة أخرى.")
        except Exception:
            pass

def build_app():
    if not BOT_TOKEN:
        raise SystemExit("PP_BOT_TOKEN غير موجود في .env")
    if not TEAM_CHAT_ID:
        raise SystemExit("PARTS_TEAM_CHAT_ID غير صحيح او غير موجود في .env")

    # تحقق اجباري للدفع اليدوي
    missing = []
    if not PP_IBAN:
        missing.append("PP_IBAN")
    if not PP_STC_PAY:
        missing.append("PP_STC_PAY")
    if not PP_BANK_NAME:
        missing.append("PP_BANK_NAME")
    if not PP_BENEFICIARY:
        missing.append("PP_BENEFICIARY")
    if missing:
        raise SystemExit("متغيرات ناقصة في .env: " + ", ".join(missing))

    ensure_workbook()

    # ✅ تحسين اتصال تيليجرام لتفادي TimedOut تحت الضغط
    try:
        request = HTTPXRequest(
            connect_timeout=20.0,
            read_timeout=40.0,
            write_timeout=40.0,
            pool_timeout=20.0,
            connection_pool_size=64,
        )
        app = Application.builder().token(BOT_TOKEN).request(request).build()
    except Exception:
        app = Application.builder().token(BOT_TOKEN).build()

    # 🟢 [HANDLER] Error Handler
    app.add_error_handler(on_error)

    # 🟢 [HANDLER] Commands
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("chatid", chatid))

    # 🟢 [HANDLER] Admin Panel (PP25S) بطريقتين
    app.add_handler(CommandHandler("pp25s", pp25s_cmd))
    app.add_handler(MessageHandler(filters.Regex(r"(?i)^pp25s$"), pp25s_cmd))  # بدون /

    # 🟢 [HANDLER] Support (/منصة)
    app.add_handler(MessageHandler(filters.Regex(r"^/منصة(?:@\w+)?(?:\s|$)"), support_cmd))
    app.add_handler(CommandHandler("help", support_cmd))

    # 🟢 [HANDLER] UI / Cancel / Close
    app.add_handler(CallbackQueryHandler(cancel_cb, pattern="^pp_cancel$"))
    app.add_handler(CallbackQueryHandler(ui_close_cb, pattern="^pp_ui_close$"))

    # 🟢 [HANDLER] Support Close / Admin Reply / Done + Open from Button
    app.add_handler(CallbackQueryHandler(support_close_cb, pattern="^pp_support_close$"))
    app.add_handler(CallbackQueryHandler(support_admin_reply_cb, pattern=r"^pp_support_reply\|"))
    app.add_handler(CallbackQueryHandler(support_admin_done_cb, pattern="^pp_support_admin_done$"))
    app.add_handler(CallbackQueryHandler(support_open_cb, pattern="^pp_support_open$"))

    app.add_handler(CallbackQueryHandler(more_yes_cb, pattern="^pp_more_yes$"))
    app.add_handler(CallbackQueryHandler(more_no_cb, pattern="^pp_more_no$"))

    app.add_handler(CallbackQueryHandler(skip_photo_cb, pattern="^pp_skip_photo$"))
    app.add_handler(CallbackQueryHandler(partno_skip_cb, pattern="^pp_partno_skip$"))
    app.add_handler(CallbackQueryHandler(skip_notes_cb, pattern="^pp_skip_notes$"))
    app.add_handler(CallbackQueryHandler(prepay_notes_skip_cb, pattern="^pp_prepay_notes_skip$"))

    app.add_handler(CallbackQueryHandler(ppq_cb, pattern=r"^ppq_"))
    app.add_handler(CallbackQueryHandler(track_cb, pattern=r"^pp_track\|"))
    app.add_handler(CallbackQueryHandler(admin_reply_cb, pattern=r"^pp_admin_reply\|"))
    app.add_handler(CallbackQueryHandler(admin_reply_done_cb, pattern="^pp_admin_reply_done$"))

    app.add_handler(CallbackQueryHandler(chat_trader_cb, pattern=r"^pp_chat_trader\|"))
    app.add_handler(CallbackQueryHandler(chat_trader_done_cb, pattern="^pp_chat_trader_done$"))
    app.add_handler(CallbackQueryHandler(trader_reply_cb, pattern=r"^pp_trader_reply\|"))
    app.add_handler(CallbackQueryHandler(trader_reply_done_cb, pattern="^pp_trader_reply_done$"))
    app.add_handler(CallbackQueryHandler(trader_reply_admin_cb, pattern=r"^pp_trader_reply_admin\|"))

    app.add_handler(CallbackQueryHandler(copy_iban_cb, pattern="^pp_copy_iban$"))
    app.add_handler(CallbackQueryHandler(copy_beneficiary_cb, pattern="^pp_copy_beneficiary$"))
    app.add_handler(CallbackQueryHandler(copy_stc_cb, pattern="^pp_copy_stc$"))

    app.add_handler(CallbackQueryHandler(pay_bank_cb, pattern="^pp_pay_bank$"))
    app.add_handler(CallbackQueryHandler(pay_stc_cb, pattern="^pp_pay_stc$"))
    app.add_handler(CallbackQueryHandler(pay_link_cb, pattern="^pp_pay_link$"))
    app.add_handler(CallbackQueryHandler(quote_ok_cb, pattern=r"^pp_quote_ok\|"))
    app.add_handler(CallbackQueryHandler(quote_no_cb, pattern=r"^pp_quote_no\|"))

    app.add_handler(CallbackQueryHandler(admin_paylink_cb, pattern=r"^pp_admin_paylink\|"))
    app.add_handler(CallbackQueryHandler(admin_sub_cb, pattern=r"^pp_admin_sub\|"))

    app.add_handler(CallbackQueryHandler(goods_pay_bank_cb, pattern=r"^pp_goods_pay_bank\|"))
    app.add_handler(CallbackQueryHandler(goods_pay_stc_cb, pattern=r"^pp_goods_pay_stc\|"))
    app.add_handler(CallbackQueryHandler(trader_status_cb, pattern=r"^pp_trader_status\|"))
    app.add_handler(CallbackQueryHandler(chat_open_cb, pattern=r"^pp_chat_open\|"))

    app.add_handler(CallbackQueryHandler(admin_chat_client_cb, pattern=r"^pp_admin_chat_client\|"))
    app.add_handler(CallbackQueryHandler(admin_chat_trader_cb, pattern=r"^pp_admin_chat_trader\|"))
    app.add_handler(CallbackQueryHandler(admin_chat_done_cb, pattern=r"^pp_admin_chat_done$"))
    app.add_handler(CallbackQueryHandler(trader_chat_admin_cb, pattern=r"^pp_trader_chat_admin\|"))
    app.add_handler(CallbackQueryHandler(trader_chat_admin_done_cb, pattern=r"^pp_trader_chat_admin_done$"))

    app.add_handler(CallbackQueryHandler(admin_panel_cb, pattern=r"^pp_admin\|"))
    app.add_handler(CallbackQueryHandler(trader_panel_cb, pattern=r"^pp_tprof\|"))

    app.add_handler(CallbackQueryHandler(ui_close_cb, pattern=r"^pp_ui\|close$"))
    app.add_handler(CallbackQueryHandler(goods_pay_link_cb, pattern=r"^pp_goods_pay_link\|"))

    app.add_handler(CallbackQueryHandler(delivery_ship_cb, pattern="^pp_delivery_ship$"))
    app.add_handler(CallbackQueryHandler(delivery_pickup_cb, pattern="^pp_delivery_pickup$"))

    app.add_handler(CallbackQueryHandler(admin_forward_cb, pattern=r"^pp_admin_forward\|"))
    app.add_handler(CallbackQueryHandler(admin_cancel_cb, pattern=r"^pp_admin_cancel\|"))

    app.add_handler(CallbackQueryHandler(team_cb, pattern=r"^(pp_team_|pp_trader_open\|)"))

    # 🟢 [HANDLER] Media Router
    app.add_handler(MessageHandler(
        filters.PHOTO | filters.VIDEO | filters.Document.ALL | filters.VOICE | filters.AUDIO | filters.VIDEO_NOTE,
        media_router
    ))

    # 🟢 [HANDLER] Text Router
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))

    # 🟢 [HANDLER] Jobs
    try:
        if app.job_queue:
            app.job_queue.run_repeating(
                _rebroadcast_noquote_orders_job,
                interval=600,
                first=600,
                name="rebroadcast_noquote_orders",
            )
    except Exception:
        pass

    # 🟢 [HANDLER] Restore DB (Admin only)
    try:
        app.add_handler(CommandHandler("restorepass", restorepass_cmd))
        app.add_handler(MessageHandler(filters.Document.ALL, _restore_excel_from_message), group=0)
    except Exception:
        pass

    # 🟢 [TASK] Backup Loop (بدون JobQueue)
    try:
        app.create_task(_backup_loop(app))
    except Exception:
        pass

    return app

import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
import os

class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"OK")

def _start_health_server():
    port = int(os.getenv("PORT", "10000"))
    HTTPServer(("0.0.0.0", port), _HealthHandler).serve_forever()

threading.Thread(target=_start_health_server, daemon=True).start()


def main():
    app = build_app()
    log.info("PP Bot is running locally (polling)...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()

