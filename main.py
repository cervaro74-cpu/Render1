import json
import logging
import uuid
import asyncio
import httpx
import psycopg2 # استبدال sqlite3 بـ psycopg2
import os
from datetime import datetime, timezone
from urllib.parse import urlparse # للمساعدة في قراءة URL الاتصال

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ConversationHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
    JobQueue
)

# ----- إعدادات التسجيل -----
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# ----- الثوابت -----
# DB_FILE لم تعد ضرورية لأننا نستخدم قاعدة بيانات مُدارة
# DB_FILE = "bot_data.db" 

# حالات المحادثة
(
    WAITING_FOR_GROUP_NAME,
    WAITING_FOR_TEMPLATE_URL,
    WAITING_FOR_TEMPLATE_HEADERS,
    WAITING_FOR_TEMPLATE_BODY,
    WAITING_FOR_TEMPLATE_DELAY,
    CONFIRM_TEMPLATE,
    WAITING_FOR_GROUP_ID_TO_RUN,
    WAITING_FOR_VARIABLE_X,
) = range(8)

# ----- دوال مساعدة لقاعدة بيانات PostgreSQL -----

def get_db_connection_string():
    """يقرأ معلومات الاتصال بقاعدة البيانات من متغيرات البيئة."""

    # الطريقة المفضلة: قراءة Render Connection URL
    conn_url_raw = os.environ.get("POSTGRESQL_ADDON_URI") # اسم متغير بيئة شائع في Render
    if conn_url_raw:
        # استبدال 'postgresql://' بـ 'postgres://' إذا لزم الأمر
        if conn_url_raw.startswith("postgresql://"):
            conn_url_raw = conn_url_raw.replace("postgresql://", "postgres://", 1)
        return conn_url_raw

    # إذا لم يتوفر URL، حاول قراءة المتغيرات المنفصلة (كمثال، إذا كنت لا تستخدم Add-on)
    db_host = os.environ.get("DB_HOST")
    db_port = os.environ.get("DB_PORT", 5432) # المنفذ الافتراضي لـ PostgreSQL
    db_name = os.environ.get("DB_NAME")
    db_user = os.environ.get("DB_USER")
    db_password = os.environ.get("DB_PASSWORD")

    if not all([db_host, db_name, db_user, db_password]):
        logger.error("PostgreSQL connection details not found in environment variables (POSTGRESQL_ADDON_URI or DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD).")
        return None

    return f"postgres://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

def get_db_connection():
    """ينشئ و يعيد اتصال بقاعدة بيانات PostgreSQL."""
    conn_string = get_db_connection_string()
    if not conn_string:
        return None
    try:
        conn = psycopg2.connect(conn_string)
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"Error connecting to PostgreSQL: {e}")
        return None

def init_db():
    """تهيئة قاعدة البيانات وجداولها في PostgreSQL."""
    conn = get_db_connection()
    if not conn:
        logger.error("Failed to get DB connection for initialization.")
        return

    cursor = conn.cursor()
    try:
        # جدول مجموعات القوالب
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS template_groups (
                group_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                variable_name TEXT NOT NULL DEFAULT 'X'
            )
        ''')

        # جدول القوالب الفردية
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS templates (
                template_id SERIAL PRIMARY KEY, -- استخدم SERIAL لـ auto-increment في PostgreSQL
                group_id TEXT NOT NULL,
                url TEXT NOT NULL,
                headers TEXT,
                body_template TEXT NOT NULL,
                delay_seconds INTEGER NOT NULL,
                FOREIGN KEY (group_id) REFERENCES template_groups (group_id) ON DELETE CASCADE
            )
        ''')

        # جدول لتتبع المهام المجدولة
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS scheduled_tasks (
                task_id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL,
                group_id TEXT NOT NULL,
                template_index INTEGER NOT NULL,
                job_name TEXT UNIQUE, 
                variable_value TEXT,
                initial_schedule_time TIMESTAMPTZ, -- استخدام TIMESTAMPTZ
                rescheduled_time TIMESTAMPTZ,    -- استخدام TIMESTAMPTZ
                status TEXT DEFAULT 'SCHEDULED', 
                completion_time TIMESTAMPTZ,    -- استخدام TIMESTAMPTZ
                FOREIGN KEY (group_id) REFERENCES template_groups (group_id) ON DELETE CASCADE
            )
        ''')
        conn.commit()
        logger.info("Database initialized (PostgreSQL).")
    except psycopg2.Error as e:
        logger.error(f"Error during DB initialization: {e}")
        conn.rollback() # التراجع في حالة الخطأ
    finally:
        cursor.close()
        conn.close()

def add_template_group(group_id, group_name, variable_name="X"):
    conn = get_db_connection()
    if not conn: return
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO template_groups (group_id, name, variable_name) VALUES (%s, %s, %s)",
                       (group_id, group_name, variable_name))
        conn.commit()
        logger.info(f"Added template group: ID={group_id}, Name={group_name}")
    except psycopg2.errors.UniqueViolation: # التعامل مع ID مكرر
        logger.warning(f"Group ID {group_id} already exists.")
        conn.rollback()
    except psycopg2.Error as e:
        logger.error(f"Error adding template group {group_id}: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def add_template(group_id, url, headers_json, body_template, delay_seconds):
    conn = get_db_connection()
    if not conn: return
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO templates (group_id, url, headers, body_template, delay_seconds) VALUES (%s, %s, %s, %s, %s)",
                       (group_id, url, headers_json, body_template, delay_seconds))
        conn.commit()
        logger.info(f"Added template to group {group_id}: URL={url}, Delay={delay_seconds}s")
    except psycopg2.Error as e:
        logger.error(f"Error adding template to group {group_id}: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def get_template_groups():
    conn = get_db_connection()
    if not conn: return []
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT group_id, name, variable_name FROM template_groups")
        groups = cursor.fetchall()
        return [{"group_id": g[0], "name": g[1], "variable_name": g[2]} for g in groups]
    except psycopg2.Error as e:
        logger.error(f"Error fetching template groups: {e}")
        return []
    finally:
        cursor.close()
        conn.close()

def get_templates_for_group(group_id):
    conn = get_db_connection()
    if not conn: return []
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT url, headers, body_template, delay_seconds FROM templates WHERE group_id = %s ORDER BY template_id", (group_id,))
        templates = cursor.fetchall()
        # تأكد من تحميل JSON بشكل صحيح
        processed_templates = []
        for t in templates:
            try:
                headers_dict = json.loads(t[1]) if t[1] else {}
                processed_templates.append({
                    "url": t[0], 
                    "headers": headers_dict, 
                    "body_template": t[2], 
                    "delay_seconds": t[3]
                })
            except json.JSONDecodeError:
                logger.warning(f"Could not decode headers JSON for group {group_id}, template row {t[0]}. Using empty headers.")
                processed_templates.append({
                    "url": t[0], 
                    "headers": {}, 
                    "body_template": t[2], 
                    "delay_seconds": t[3]
                })
        return processed_templates
    except psycopg2.Error as e:
        logger.error(f"Error fetching templates for group {group_id}: {e}")
        return []
    finally:
        cursor.close()
        conn.close()

def get_group_variable_name(group_id):
    conn = get_db_connection()
    if not conn: return 'X'
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT variable_name FROM template_groups WHERE group_id = %s", (group_id,))
        result = cursor.fetchone()
        return result[0] if result else 'X'
    except psycopg2.Error as e:
        logger.error(f"Error fetching variable name for group {group_id}: {e}")
        return 'X'
    finally:
        cursor.close()
        conn.close()

def delete_group_and_templates(group_id):
    conn = get_db_connection()
    if not conn: return False
    cursor = conn.cursor()
    try:
        # حذف المهام المجدولة أولاً لتجنب مشاكل FK
        cursor.execute("DELETE FROM scheduled_tasks WHERE group_id = %s", (group_id,))
        # ثم حذف المجموعة (CASCADE سيحذف القوالب)
        cursor.execute("DELETE FROM template_groups WHERE group_id = %s", (group_id,))
        conn.commit()
        logger.info(f"Deleted group {group_id} and its tasks/templates.")
        return True
    except psycopg2.Error as e:
        logger.error(f"Error deleting group {group_id}: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()

# --- دوال لإدارة المهام المجدولة ---
def add_scheduled_task(user_id, group_id, template_index, job_name, variable_value, initial_schedule_time):
    conn = get_db_connection()
    if not conn: return
    cursor = conn.cursor()
    try:
        # استخدم current_timestamp أو datetime.now(timezone.utc).isoformat()
        cursor.execute("""
            INSERT INTO scheduled_tasks (user_id, group_id, template_index, job_name, variable_value, initial_schedule_time, rescheduled_time, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, 'SCHEDULED')
        """, (user_id, group_id, template_index, job_name, variable_value, initial_schedule_time, initial_schedule_time, ))
        conn.commit()
        logger.info(f"Added scheduled task: User={user_id}, Group={group_id}, Template={template_index}, Job={job_name}")
    except psycopg2.errors.UniqueViolation:
        logger.warning(f"Job name '{job_name}' already exists in scheduled_tasks. Not adding again.")
    except psycopg2.Error as e:
        logger.error(f"Error adding scheduled task: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def update_task_status(job_name, status, completion_time=None):
    conn = get_db_connection()
    if not conn: return
    cursor = conn.cursor()
    try:
        if completion_time is None:
            completion_time = datetime.now(timezone.utc)

        cursor.execute("""
            UPDATE scheduled_tasks
            SET status = %s, completion_time = %s
            WHERE job_name = %s
        """, (status, completion_time, job_name))
        conn.commit()
        logger.info(f"Updated task '{job_name}' status to: {status}")
    except psycopg2.Error as e:
        logger.error(f"Error updating task status for job '{job_name}': {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def get_pending_scheduled_tasks():
    conn = get_db_connection()
    if not conn: return []
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT user_id, group_id, template_index, job_name, variable_value, rescheduled_time
            FROM scheduled_tasks
            WHERE status = 'SCHEDULED' OR status = 'RUNNING' 
        """)
        tasks = cursor.fetchall()

        return [{
            "user_id": t[0], "group_id": t[1], "template_index": t[2],
            "job_name": t[3], "variable_value": t[4], "rescheduled_time_str": t[5].isoformat() if t[5] else None
        } for t in tasks]
    except psycopg2.Error as e:
        logger.error(f"Error fetching pending tasks: {e}")
        return []
    finally:
        cursor.close()
        conn.close()

def get_scheduled_tasks_for_user(user_id):
    conn = get_db_connection()
    if not conn: return []
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT group_id, template_index, job_name, variable_value, status, initial_schedule_time, rescheduled_time, completion_time
            FROM scheduled_tasks
            WHERE user_id = %s
            ORDER BY initial_schedule_time DESC 
        """, (user_id,))
        tasks = cursor.fetchall()

        formatted_tasks = []
        for task in tasks:
            group_id, template_index, job_name, variable_value, status, initial_schedule_time, rescheduled_time, completion_time = task

            group_name = group_id
            try:
                group_info = next((g for g in get_template_groups() if g['group_id'] == group_id), None)
                if group_info:
                    group_name = f"{group_info['name']} ({group_id})"
            except Exception:
                pass

            formatted_tasks.append({
                "group_name": group_name,
                "template_index": template_index,
                "job_name": job_name,
                "variable_value": variable_value,
                "status": status,
                "initial_schedule_time": initial_schedule_time.isoformat() if initial_schedule_time else None,
                "rescheduled_time": rescheduled_time.isoformat() if rescheduled_time else None,
                "completion_time": completion_time.isoformat() if completion_time else None
            })
        return formatted_tasks
    except psycopg2.Error as e:
        logger.error(f"Error fetching tasks for user {user_id}: {e}")
        return []
    finally:
        cursor.close()
        conn.close()

# ----- دوال إرسال الطلبات (التي سيتم جدولتها) -----

async def send_api_request_job(context: ContextTypes.DEFAULT_TYPE):
    """
    دالة سيتم استدعاؤها بواسطة JobQueue.
    """
    job = context.job
    job_data = job.data
    group_id = job_data["group_id"]
    user_id = job_data["user_id"]
    template_index = job_data["template_index"]
    variable_value = job_data["variable_value"]
    job_name = job.name

    update_task_status(job_name, 'RUNNING', datetime.now(timezone.utc))

    templates = get_templates_for_group(group_id)
    if not templates or not (0 <= template_index < len(templates)):
        logger.error(f"Templates not found or invalid index for group {group_id}, template {template_index}")
        await context.bot.send_message(chat_id=user_id, text=f"❌ Error: Could not find valid template for group ID {group_id}, template index {template_index}.")
        update_task_status(job_name, 'FAILED', datetime.now(timezone.utc))
        return

    template = templates[template_index]
    group_name = group_id
    try:
        group_info = next((g for g in get_template_groups() if g['group_id'] == group_id), None)
        if group_info:
            group_name = f"{group_info['name']} ({group_id})"
    except Exception:
        pass

    try:
        url = template["url"]
        headers = template["headers"]
        body_template = template["body_template"]

        body_str = body_template.replace('X', variable_value)
        try:
            body_dict = json.loads(body_str)
        except json.JSONDecodeError:
            logger.error(f"Error decoding JSON body for task in group {group_id} (Template index: {template_index}) for user {user_id}")
            await context.bot.send_message(chat_id=user_id, text=f"❌ Invalid JSON in body for template in group {group_id} (Template Index: {template_index}).")
            update_task_status(job_name, 'FAILED', datetime.now(timezone.utc))
            return

        logger.info(f"Sending request for group {group_id} (Template {template_index}) to {url} with variable '{variable_value}'")

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(url, json=body_dict, headers=headers)

            status_code = response.status_code
            response_text = response.text
            final_status = 'COMPLETED' if 200 <= status_code < 300 else 'FAILED'

            summary_text = (
                f"<b>Result for Group '{group_name}'</b>\n"
                f"<b>Template URL:</b> <code>{url}</code>\n"
                f"<b>Status Code:</b> <code>{status_code}</code>\n"
                f"<b>Variable 'X':</b> <code>{variable_value}</code>\n"
                f"<b>Status:</b> {'✅ COMPLETED' if final_status == 'COMPLETED' else '⚠️ FAILED'}\n"
            )

            await context.bot.send_message(chat_id=user_id, text=summary_text, parse_mode="HTML")

            if response_text:
                if len(response_text) > 800:
                    response_text = response_text[:800] + "...\n(Response truncated)"
                await context.bot.send_message(chat_id=user_id, text=f"Response Body:\n{response_text}")

            update_task_status(job_name, final_status, datetime.now(timezone.utc))

    except httpx.RequestError as e:
        logger.error(f"HTTP request error for group {group_id} (Template {template_index}): {e}")
        await context.bot.send_message(
            chat_id=user_id,
            text=f"❌ Error sending request for group '{group_name}' (Template Index: {template_index}): {str(e)}",
            parse_mode="HTML"
        )
        update_task_status(job_name, 'FAILED', datetime.now(timezone.utc))
    except Exception as e:
        logger.error(f"Unexpected error in send_api_request_job for group {group_id} (Template {template_index}): {e}")
        await context.bot.send_message(
            chat_id=user_id,
            text=f"❌ An unexpected error occurred for group '{group_name}' (Template Index: {template_index}): {str(e)}",
            parse_mode="HTML"
        )
        update_task_status(job_name, 'FAILED', datetime.now(timezone.utc))


# ----- دوال المحادثة لإنشاء مجموعة قوالب -----
async def start_create_template_group(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    # توليد ID فريد للمجموعة المستقبلية (يمكن أن يكون UUID أو أي شيء فريد)
    context.user_data['current_group_id_for_saving'] = generate_unique_id()
    await update.message.reply_text("👋 مرحبًا! سنقوم بإنشاء مجموعة قوالب جديدة.\n\n"
                                    "أولاً، ما هو اسم مجموعة القوالب هذه؟ (مثال: 'قوالب خدمة المستخدم')")
    return WAITING_FOR_GROUP_NAME

async def received_group_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    group_name = update.message.text
    context.user_data['new_group_name'] = group_name
    await update.message.reply_text(f"تم تسجيل اسم المجموعة: '{group_name}'.\n\n"
                                    "الآن، أرسل لي الرابط (URL) لأول طلب في هذه المجموعة.")
    return WAITING_FOR_TEMPLATE_URL

async def received_template_url(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data['current_template_url'] = update.message.text
    await update.message.reply_text("ممتاز. الآن، أرسل لي الترويسات (Headers) بصيغة JSON. "
                                    "إذا لم تكن هناك ترويسات، أرسل 'لا يوجد' أو JSON فارغ `{}`.")
    return WAITING_FOR_TEMPLATE_HEADERS

async def received_template_headers(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    headers_str = update.message.text
    headers_json = "{}"
    if headers_str.lower() not in ["لا يوجد", ""]:
        try:
            # التحقق من أن الإدخال هو JSON صالح
            json.loads(headers_str)
            headers_json = headers_str
        except json.JSONDecodeError:
            await update.message.reply_text("❌ خطأ في تنسيق JSON للترويسات. يرجى المحاولة مرة أخرى.")
            return WAITING_FOR_TEMPLATE_HEADERS # البقاء في نفس الحالة

    context.user_data['current_template_headers'] = headers_json
    await update.message.reply_text("حسناً. الآن، أرسل لي بيانات الـ Body بصيغة JSON. "
                                    "قم بتحديد المتغير الذي تريد استخدامه بـ 'X' (مثال: `{\"name\": \"X\", \"age\": 30}`) ")
    return WAITING_FOR_TEMPLATE_BODY

async def received_template_body(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    body_str = update.message.text
    # التأكد من أن الجسم يحتوي على 'X'
    if "X" not in body_str:
        await update.message.reply_text("❌ يجب أن يحتوي الـ Body على المتغير 'X' الذي سيتم استبداله لاحقاً. يرجى المحاولة مرة أخرى.")
        return WAITING_FOR_TEMPLATE_BODY

    context.user_data['current_template_body'] = body_str
    await update.message.reply_text("ممتاز! الآن، ما هو التأخير (بالثواني) الذي تريد استخدامه لإرسال هذا القالب بعد إدخال المتغير X؟ "
                                    "(مثال: 5 لإرساله بعد 5 ثوانٍ)")
    return WAITING_FOR_TEMPLATE_DELAY

async def received_template_delay(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    delay_str = update.message.text
    try:
        delay_seconds = int(delay_str)
        if delay_seconds < 0:
            raise ValueError("Delay must be non-negative.")
        context.user_data['current_template_delay'] = delay_seconds
    except (ValueError, TypeError):
        await update.message.reply_text("❌ خطأ: يرجى إدخال رقم صحيح للتأخير (بالثواني).")
        return WAITING_FOR_TEMPLATE_DELAY

    # بناء ملخص للقالب قبل الحفظ
    summary = (
        f"URL: {context.user_data.get('current_template_url')}\n"
        f"Headers: {context.user_data.get('current_template_headers')}\n"
        f"Body: {context.user_data.get('current_template_body')}\n"
        f"Delay: {context.user_data.get('current_template_delay')} ثانية"
    )
    await update.message.reply_text(
        f"تم استقبال معلومات القالب:\n{summary}\n\n"
        "هل تريد إضافة قالب آخر لهذه المجموعة؟",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ نعم، قالب آخر", callback_data="add_another_template")],
            [InlineKeyboardButton("❌ لا، حفظ المجموعة", callback_data="save_group")],
        ])
    )
    return CONFIRM_TEMPLATE

async def handle_template_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    group_id = context.user_data.get('current_group_id_for_saving')
    if not group_id: 
        logger.error("Group ID missing in context during confirmation.")
        await query.edit_message_text("❌ خطأ داخلي. لم يتم تحديد معرف المجموعة.")
        return ConversationHandler.END

    # تخزين القالب الحالي إذا كان موجودًا
    if 'templates_in_progress' not in context.user_data:
        context.user_data['templates_in_progress'] = []

    # إضافة القالب الأخير الذي تم إدخاله (قبل الضغط على زر التأكيد)
    if 'current_template_url' in context.user_data:
         context.user_data['templates_in_progress'].append({
            "url": context.user_data['current_template_url'],
            "headers": context.user_data['current_template_headers'],
            "body_template": context.user_data['current_template_body'],
            "delay_seconds": context.user_data['current_template_delay'],
        })

    if query.data == "add_another_template":
        await query.edit_message_text("تم إضافة القالب. أرسل لي URL للقالب التالي.")
        # إعادة تعيين لجمع تفاصيل القالب التالي
        context.user_data.pop('current_template_url', None)
        context.user_data.pop('current_template_headers', None)
        context.user_data.pop('current_template_body', None)
        context.user_data.pop('current_template_delay', None)
        return WAITING_FOR_TEMPLATE_URL # العودة لانتظار URL القالب التالي

    elif query.data == "save_group":
        group_name = context.user_data.get('new_group_name', f'Group_{group_id}')
        variable_name = context.user_data.get('new_group_variable_name', 'X') # إذا تم تخصيص المتغير

        add_template_group(group_id, group_name, variable_name)
        for template in context.user_data['templates_in_progress']:
            add_template(group_id, template["url"], 
                         json.dumps(template["headers"]), # حفظ الـ headers كـ JSON string
                         template["body_template"], template["delay_seconds"])

        await query.edit_message_text(
            f"✅ تم حفظ مجموعة القوالب بنجاح!\n"
            f"<b>الاسم:</b> {group_name}\n"
            f"<b>ID:</b> <code>{group_id}</code>\n"
            f"<b>عدد القوالب:</b> {len(context.user_data['templates_in_progress'])}",
            parse_mode="HTML"
        )
        context.user_data.clear() # مسح بيانات المحادثة
        return ConversationHandler.END

    return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """يلغي المحادثة الحالية."""
    await update.message.reply_text("تم إلغاء العملية.")
    context.user_data.clear()
    return ConversationHandler.END

# ----- دوال لتشغيل مجموعة قوالب -----

async def start_run_template_group(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    groups = get_template_groups()

    if not groups:
        await update.message.reply_text("لا توجد مجموعات قوالب محفوظة بعد. استخدم /newgroup لإنشاء واحدة.")
        return ConversationHandler.END

    keyboard = []
    for group in groups:
        keyboard.append([InlineKeyboardButton(f"{group['name']} (ID: {group['group_id']})", callback_data=f"select_group_to_run_{group['group_id']}")])

    await update.message.reply_text("الرجاء اختيار مجموعة القوالب التي تريد تشغيلها:",
                                    reply_markup=InlineKeyboardMarkup(keyboard))
    return WAITING_FOR_GROUP_ID_TO_RUN

async def handle_run_group_selection(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    selected_group_id = query.data.split('_', 3)[2]

    # التحقق من وجود المجموعة
    if not any(g['group_id'] == selected_group_id for g in get_template_groups()):
        await query.edit_message_text("❌ خطأ: مجموعة القوالب هذه غير موجودة. يرجى المحاولة مرة أخرى.")
        return ConversationHandler.END

    context.user_data['selected_group_id'] = selected_group_id
    variable_name = get_group_variable_name(selected_group_id)

    await query.edit_message_text(
        f"لقد اخترت المجموعة '{selected_group_id}'.\n\n"
        f"الرجاء إدخال قيمة للمتغير '{variable_name}' لاستخدامه في الـ Body لجميع قوالب هذه المجموعة.",
        reply_markup=None
    )
    return WAITING_FOR_VARIABLE_X

async def received_variable_x(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    variable_value = update.message.text
    group_id = context.user_data.get('selected_group_id')
    user_id = update.effective_user.id

    if not group_id:
        await update.message.reply_text("❌ حدث خطأ ما. لم يتم تحديد مجموعة القوالب. يرجى البدء من جديد.")
        context.user_data.clear()
        return ConversationHandler.END

    group_name_display = group_id
    try:
        group_info = next((g for g in get_template_groups() if g['group_id'] == group_id), None)
        if group_info:
            group_name_display = f"{group_info['name']} ({group_id})"
    except Exception:
        pass

    variable_name = get_group_variable_name(group_id)
    await update.message.reply_text(f"✅ تم استلام قيمة المتغير '{variable_name}': '{variable_value}'.\n"
                                    f"سيتم الآن جدولة طلبات المجموعة '{group_name_display}'...")

    job_queue = context.application.job_queue
    templates = get_templates_for_group(group_id)

    if not templates:
        await update.message.reply_text(f"❌ خطأ: لم يتم العثور على قوالب للمجموعة '{group_name_display}'.")
        context.user_data.clear()
        return ConversationHandler.END

    # وقت الجدولة الحالي (هام لـ initial_schedule_time)
    now_utc = datetime.now(timezone.utc) 

    scheduled_count = 0
    for i, template in enumerate(templates):
        try:
            job_data = {
                "group_id": group_id,
                "template_index": i,
                "variable_value": variable_value,
                "user_id": user_id,
            }
            # اسم وظيفة فريد لكل مهمة
            job_name = f"template_group_{group_id}_user_{user_id}_template_{i}_{uuid.uuid4()}" 

            scheduled_delay = template["delay_seconds"]

            job_queue.run_once(
                callback=send_api_request_job,
                when=scheduled_delay,
                name=job_name,
                data=job_data,
            )

            add_scheduled_task(user_id, group_id, i, job_name, variable_value, now_utc)

            scheduled_count += 1
            logger.info(f"Scheduled job '{job_name}' for group {group_id}, template {i} with delay {scheduled_delay}s")
        except Exception as e:
            logger.error(f"Error scheduling job for group {group_id}, template {i}: {e}")
            await context.bot.send_message(
                chat_id=user_id,
                text=f"❌ Error scheduling template {i} for group '{group_name_display}'. Please check logs.",
                parse_mode="HTML"
            )
            # تسجيل خطأ في قاعدة البيانات إذا فشلت الجدولة
            update_task_status(f"template_group_{group_id}_user_{user_id}_template_{i}___placeholder_{uuid.uuid4()}", 'FAILED', datetime.now(timezone.utc)) 

    await update.message.reply_text(f"👍 تم جدولة {scheduled_count} طلب(ات) بنجاح للمجموعة '{group_name_display}'! سيتم إرسالها بالتأخيرات المحددة.")

    context.user_data.clear()
    return ConversationHandler.END

# ----- دالة عرض المهام المجدولة -----
async def list_scheduled_tasks(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    tasks = get_scheduled_tasks_for_user(user_id)

    if not tasks:
        await update.message.reply_text("لا توجد لديك أي طلبات مجدولة حاليًا.")
        return

    response = "<b>طلباتك المجدولة:</b>\n\n"
    for task in tasks:
        response += f"<b>المجموعة:</b> {task['group_name']}\n"
        response += f"<b>القالب (Index):</b> {task['template_index']}\n"
        response += f"<b>المتغير:</b> <code>{task['variable_value']}</code>\n"
        response += f"<b>الجدولة الأصلية:</b> {task['initial_schedule_time']}\n"
        response += f"<b>آخر إعادة جدولة:</b> {task['rescheduled_time']}\n"
        response += f"<b>الحالة:</b> {task['status']}\n"
        if task['status'] != 'SCHEDULED':
             response += f"<b>وقت الاكتمال:</b> {task['completion_time']}\n"
        response += "--------------------\n"

    await update.message.reply_text(response, parse_mode="HTML")

# ----- دالة إعادة جدولة المهام عند بدء التشغيل -----
def reschedule_pending_tasks(context: ContextTypes.DEFAULT_TYPE):
    """
    يقوم بجلب المهام التي لم تكتمل ويعيد جدولتها.
    """
    job_queue = context.job_queue
    pending_tasks = get_pending_scheduled_tasks()

    if not pending_tasks:
        logger.info("No pending tasks to reschedule.")
        return

    logger.info(f"Found {len(pending_tasks)} pending tasks to reschedule.")

    for task in pending_tasks:
        user_id = task["user_id"]
        group_id = task["group_id"]
        template_index = task["template_index"]
        job_name_from_db = task["job_name"] # اسم الوظيفة المخزن في قاعدة البيانات
        variable_value = task["variable_value"]

        try:
            templates = get_templates_for_group(group_id)
            if not templates or not (0 <= template_index < len(templates)):
                logger.error(f"Could not find template for rescheduling: Group={group_id}, Index={template_index}. Marking task as FAILED.")
                update_task_status(job_name_from_db, 'FAILED', datetime.now(timezone.utc))
                continue

            template_delay = templates[template_index]["delay_seconds"]

            # إنشاء اسم وظيفة جديد وفريد للمهمة المعاد جدولتها
            new_job_name = f"rescheduled_group_{group_id}_user_{user_id}_template_{template_index}_{uuid.uuid4()}"

            job_data = {
                "group_id": group_id,
                "template_index": template_index,
                "variable_value": variable_value,
                "user_id": user_id,
            }

            # جدولة المهمة لتشتغل الآن (مع التأخير المحدد للقالب)
            job_queue.run_once(
                callback=send_api_request_job,
                when=template_delay, 
                name=new_job_name,
                data=job_data,
            )

            # تحديث المهمة في قاعدة البيانات بالاسم الجديد ووقت إعادة الجدولة
            # يجب أن نستخدم datetime.now(timezone.utc) هنا
            current_reschedule_time = datetime.now(timezone.utc)
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()
                try:
                    cursor.execute("""
                        UPDATE scheduled_tasks
                        SET job_name = %s, rescheduled_time = %s, status = 'SCHEDULED' 
                        WHERE job_name = %s 
                    """, (new_job_name, current_reschedule_time, job_name_from_db))
                    conn.commit()
                    logger.info(f"Rescheduled task '{job_name_from_db}' to '{new_job_name}' with delay {template_delay}s")
                except psycopg2.Error as e:
                    logger.error(f"Failed to update task '{job_name_from_db}' with new name '{new_job_name}': {e}")
                    conn.rollback()
                finally:
                    cursor.close()
                    conn.close()
            else:
                logger.error("Failed to get DB connection to update task during rescheduling.")
                update_task_status(job_name_from_db, 'FAILED', datetime.now(timezone.utc)) # فشلت التحديث، لذا المهمة الأصلية تفشل

        except Exception as e:
            logger.error(f"Error during rescheduling of task (Group: {group_id}, Index: {template_index}, OriginalJob: {job_name_from_db}): {e}")
            update_task_status(job_name_from_db, 'FAILED', datetime.now(timezone.utc))


# ----- دالة main -----
def main() -> None:
    BOT_TOKEN = os.environ.get("YOUR_BOT_TOKEN")
    if not BOT_TOKEN:
        logger.error("Bot token not found. Please set YOUR_BOT_TOKEN environment variable.")
        return

    # تهيئة قاعدة البيانات عند البدء
    init_db()

    application = Application.builder().token(BOT_TOKEN).build()
    job_queue = application.job_queue 

    # تشغيل مهمة إعادة الجدولة بعد فترة قصيرة من بدء البوت
    job_queue.run_once(
        callback=lambda context: reschedule_pending_tasks(context),
        when=10, # انتظر 10 ثوانٍ للتأكد من أن كل شيء جاهز
        name="reschedule_initial_tasks"
    )
    logger.info("Scheduled initial task rescheduling.")

    # تعريف محادثات الـ ConversationHandler
    create_group_conv_handler = ConversationHandler(
        entry_points=[CommandHandler('newgroup', start_create_template_group)],
        states={
            WAITING_FOR_GROUP_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, received_group_name)],
            WAITING_FOR_TEMPLATE_URL: [MessageHandler(filters.TEXT & ~filters.COMMAND, received_template_url)],
            WAITING_FOR_TEMPLATE_HEADERS: [MessageHandler(filters.TEXT & ~filters.COMMAND, received_template_headers)],
            WAITING_FOR_TEMPLATE_BODY: [MessageHandler(filters.TEXT & ~filters.COMMAND, received_template_body)],
            WAITING_FOR_TEMPLATE_DELAY: [MessageHandler(filters.TEXT & ~filters.COMMAND, received_template_delay)],
            CONFIRM_TEMPLATE: [CallbackQueryHandler(handle_template_confirmation, pattern="^(add_another_template|save_group)$")],
        },
        fallbacks=[CommandHandler('cancel', cancel)],
    )

    run_group_conv_handler = ConversationHandler(
        entry_points=[CommandHandler('run', start_run_template_group)],
        states={
            WAITING_FOR_GROUP_ID_TO_RUN: [CallbackQueryHandler(handle_run_group_selection, pattern="^select_group_to_run_.+$")],
            WAITING_FOR_VARIABLE_X: [MessageHandler(filters.TEXT & ~filters.COMMAND, received_variable_x)],
        },
        fallbacks=[CommandHandler('cancel', cancel)],
    )

    # أوامر أخرى
    async def list_groups(update: Update, context: ContextTypes.DEFAULT_TYPE):
        groups = get_template_groups()
        if not groups:
            await update.message.reply_text("لا توجد مجموعات قوالب محفوظة.")
            return

        response = "<b>مجموعات القوالب المتاحة:</b>\n\n"
        for group in groups:
            response += f"<b>اسم المجموعة:</b> {group['name']}\n"
            response += f"<b>ID:</b> <code>{group['group_id']}</code>\n"
            response += f"<b>المتغير:</b> {group['variable_name']}\n"
            response += f"<b>عدد القوالب:</b> {len(get_templates_for_group(group['group_id']))}\n"
            response += "--------------------\n"
        await update.message.reply_text(response, parse_mode="HTML")

    async def delete_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not context.args:
            await update.message.reply_text("الرجاء تحديد ID المجموعة لحذفها. مثال: `/deletegroup ABC123DEF`")
            return

        group_id_to_delete = context.args[0]
        if delete_group_and_templates(group_id_to_delete):
            await update.message.reply_text(f"تم حذف المجموعة ذات ID '{group_id_to_delete}' وجميع قوالبها ومهامها المجدولة بنجاح.")
        else:
            await update.message.reply_text(f"حدث خطأ أثناء حذف المجموعة ذات ID '{group_id_to_delete}'.")

    # إضافة المعالجات (Handlers)
    application.add_handler(CommandHandler("start", lambda update, context: update.message.reply_text("مرحباً! استخدم /newgroup لإنشاء قوالب جديدة، أو /run لتشغيل مجموعة قوالب، أو /groups لعرض المجموعات، أو /scheduledtasks لعرض طلباتك المجدولة.") ))
    application.add_handler(create_group_conv_handler)
    application.add_handler(run_group_conv_handler)
    application.add_handler(CommandHandler("groups", list_groups))
    application.add_handler(CommandHandler("deletegroup", delete_group))
    application.add_handler(CommandHandler("scheduledtasks", list_scheduled_tasks))

    logger.info("Starting bot...")
    application.run_polling()

# ----- دالة مساعدة لتوليد ID فريد -----
def generate_unique_id():
    return uuid.uuid4().hex[:12] # توليد 12 حرفًا فريدًا

if __name__ == "__main__":
    main()
