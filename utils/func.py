import concurrent.futures
import time
import os
import re
import cv2
import logging
import asyncio
import aiosqlite # Import aiosqlite
from datetime import datetime, timedelta

# Assuming config.py defines DB_NAME, but for aiosqlite it will be a file name.
# Let's define it directly for clarity in this example.
DB_FILE = "bot_data.db"

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

PUBLIC_LINK_PATTERN = re.compile(r'(https?://)?(t\.me|telegram\.me)/([^/]+)(/(\d+))?')
PRIVATE_LINK_PATTERN = re.compile(r'(https?://)?(t\.me|telegram\.me)/c/(\d+)(/(\d+))?')
VIDEO_EXTENSIONS = {"mp4", "mkv", "avi", "mov", "wmv", "flv", "webm", "mpeg", "mpg", "3gp"}

# ------- < start > Session Encoder don't change -------

a1 = "c2F2ZV9yZXN0cmljdGVkX2NvbnRlbnRfYm90cw=="
a2 = "Nzk2"
a3 = "Z2V0X21lc3NhZ2Vz"
a4 = "cmVwbHlfcGhvdG8="
a5 = "c3RhcnQ="
attr1 = "cGhvdG8="
attr2 = "ZmlsZV9pZA=="
a7 = "SGkg8J+RiyBXZWxjb21lLCBXYW5uYSBpbnRyby4uLj8gCgrinLPvuI8gSSBjYW4gc2F2ZSBwb3N0cyBmcm9tIGNoYW5uZWxzIG9yIGdyb3VwcyB3aGVyZSBmb3J3YXJkaW5nIGlzIG9mZi4gSSBjYW4gZG93bmxvYWQgdmlkZW9zL2F1ZGlvIGZyb20gWVQsIElOU1RBLCAuLi4gc29jaWFsIHBsYXRmb3JtcwrinLPvuI8gU2ltcGx5IHNlbmQgdGhlIHBvc3QgbGluayBvZiBhIHB1YmxpYyBjaGFubmVsLiBGb3IgcHJpdmF0ZSBjaGFubmVscywgZG8gL2xvZ2luLiBTZW5kIC9oZWxwIHRvIGtub3cgbW9yZS4="
a8 = "Sm9pbiBDaGFubmVs"
a9 = "R2V0IFByZW1pdW0="
a10 = "aHR0cHM6Ly90Lm1lL3RlYW1fc3B5X3Bybw=="
a11 = "aHR0cHM6Ly90Lm1lL2tpbmdvZnBhdGFs"

# ------- < end > Session Encoder don't change --------

async def init_db():
    async with aiosqlite.connect(DB_FILE) as db:
        # Users table: Store user-specific data
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                session_string TEXT,
                bot_token TEXT,
                replacement_words TEXT, -- Storing as JSON string
                delete_words TEXT,      -- Storing as JSON string
                updated_at TEXT
            )
        """)
        # Premium Users table: Store premium subscription details
        await db.execute("""
            CREATE TABLE IF NOT EXISTS premium_users (
                user_id INTEGER PRIMARY KEY,
                subscription_start TEXT,
                subscription_end TEXT
            )
        """)
        # Statistics table (if needed, based on your original db["statistics"])
        # You'll need to define columns based on what 'statistics' collection stores.
        # For now, let's assume it stores simple key-value stats.
        await db.execute("""
            CREATE TABLE IF NOT EXISTS statistics (
                stat_name TEXT PRIMARY KEY,
                stat_value INTEGER
            )
        """)
        # Redeem Code table (if needed, based on your original db["redeem_code"])
        # You'll need to define columns based on what 'codedb' collection stores.
        # Example:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS redeem_codes (
                code TEXT PRIMARY KEY,
                is_used INTEGER DEFAULT 0, -- 0 for false, 1 for true
                user_id_used_by INTEGER,
                used_at TEXT
            )
        """)
        await db.commit()
    logger.info(f"Database {DB_FILE} initialized successfully.")

def is_private_link(link):
    return bool(PRIVATE_LINK_PATTERN.match(link))

def thumbnail(sender):
    return f'{sender}.jpg' if os.path.exists(f'{sender}.jpg') else None

def hhmmss(seconds):
    return time.strftime('%H:%M:%S', time.gmtime(seconds))

def E(L):
    private_match = re.match(r'https://t\.me/c/(\d+)/(?:\d+/)?(\d+)', L)
    public_match = re.match(r'https://t\.me/([^/]+)/(?:\d+/)?(\d+)', L)

    if private_match:
        return f'-100{private_match.group(1)}', int(private_match.group(2)), 'private'
    elif public_match:
        return public_match.group(1), int(public_match.group(2)), 'public'

    return None, None, None

def get_display_name(user):
    if user.first_name and user.last_name:
        return f"{user.first_name} {user.last_name}"
    elif user.first_name:
        return user.first_name
    elif user.last_name:
        return user.last_name
    elif user.username:
        return user.username
    else:
        return "Unknown User"

def sanitize_filename(filename):
    return re.sub(r'[<>:"/\\|?*]', '_', filename)

def get_dummy_filename(info):
    file_type = info.get("type", "file")
    extension = {
        "video": "mp4",
        "photo": "jpg",
        "document": "pdf",
        "audio": "mp3"
    }.get(file_type, "bin")

    return f"downloaded_file_{int(time.time())}.{extension}"

async def is_private_chat(event):
    return event.is_private

async def save_user_data(user_id, key, value):
    async with aiosqlite.connect(DB_FILE) as db:
        # MongoDB's $set allows setting nested fields or new fields directly.
        # SQLite needs explicit column updates. We'll convert complex types to JSON strings.
        if key in ["replacement_words", "delete_words"]:
            import json
            value = json.dumps(value)

        # Check if user exists to decide between INSERT and UPDATE
        cursor = await db.execute("SELECT 1 FROM users WHERE user_id = ?", (user_id,))
        exists = await cursor.fetchone()

        if exists:
            await db.execute(f"UPDATE users SET {key} = ?, updated_at = ? WHERE user_id = ?",
                             (value, datetime.now().isoformat(), user_id))
        else:
            # This is a simplified insert. For a new user, you might want to insert all default columns.
            # For this function, we assume it's primarily for updating existing users or setting a single new field on a new user.
            # A more robust solution might be to have an `add_user` function.
            await db.execute(f"INSERT OR IGNORE INTO users (user_id, {key}, updated_at) VALUES (?, ?, ?)",
                             (user_id, value, datetime.now().isoformat()))
            await db.execute(f"UPDATE users SET {key} = ? WHERE user_id = ?", (value, user_id)) # Update if just inserted a partial
        await db.commit()
    logger.info(f"Saved user data for {user_id}: {key} = {value}")

async def get_user_data_key(user_id, key, default=None):
    async with aiosqlite.connect(DB_FILE) as db:
        cursor = await db.execute(f"SELECT {key} FROM users WHERE user_id = ?", (int(user_id),))
        row = await cursor.fetchone()
        if row:
            value = row[0]
            # Convert JSON strings back to Python objects
            if key in ["replacement_words", "delete_words"] and value is not None:
                import json
                return json.loads(value)
            return value
        return default

async def get_user_data(user_id):
    try:
        async with aiosqlite.connect(DB_FILE) as db:
            cursor = await db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
            row = await cursor.fetchone()
            if row:
                # Get column names from cursor description
                col_names = [description[0] for description in cursor.description]
                user_data = dict(zip(col_names, row))
                # Convert JSON strings back to Python objects
                if 'replacement_words' in user_data and user_data['replacement_words'] is not None:
                    import json
                    user_data['replacement_words'] = json.loads(user_data['replacement_words'])
                if 'delete_words' in user_data and user_data['delete_words'] is not None:
                    import json
                    user_data['delete_words'] = json.loads(user_data['delete_words'])
                return user_data
            return None
    except Exception as e:
        logger.error(f"Error retrieving user data for {user_id}: {e}")
        return None

async def save_user_session(user_id, session_string):
    try:
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute("""
                INSERT OR REPLACE INTO users (user_id, session_string, updated_at)
                VALUES (?, ?, ?)
            """, (user_id, session_string, datetime.now().isoformat()))
            await db.commit()
        logger.info(f"Saved session for user {user_id}")
        return True
    except Exception as e:
        logger.error(f"Error saving session for user {user_id}: {e}")
        return False

async def remove_user_session(user_id):
    try:
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute("UPDATE users SET session_string = NULL WHERE user_id = ?", (user_id,))
            await db.commit()
        logger.info(f"Removed session for user {user_id}")
        return True
    except Exception as e:
        logger.error(f"Error removing session for user {user_id}: {e}")
        return False

async def save_user_bot(user_id, bot_token):
    try:
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute("""
                INSERT OR REPLACE INTO users (user_id, bot_token, updated_at)
                VALUES (?, ?, ?)
            """, (user_id, bot_token, datetime.now().isoformat()))
            await db.commit()
        logger.info(f"Saved bot token for user {user_id}")
        return True
    except Exception as e:
        logger.error(f"Error saving bot token for user {user_id}: {e}")
        return False

async def remove_user_bot(user_id):
    try:
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute("UPDATE users SET bot_token = NULL WHERE user_id = ?", (user_id,))
            await db.commit()
        logger.info(f"Removed bot token for user {user_id}")
        return True
    except Exception as e:
        logger.error(f"Error removing bot token for user {user_id}: {e}")
        return False

async def process_text_with_rules(user_id, text):
    if not text:
        return ""

    try:
        replacements = await get_user_data_key(user_id, "replacement_words", {})
        delete_words = await get_user_data_key(user_id, "delete_words", [])

        processed_text = text
        for word, replacement in replacements.items():
            processed_text = processed_text.replace(word, replacement)

        if delete_words:
            words = processed_text.split()
            filtered_words = [w for w in words if w not in delete_words]
            processed_text = " ".join(filtered_words)

        return processed_text
    except Exception as e:
        logger.error(f"Error processing text with rules: {e}")
        return text


async def screenshot(video: str, duration: int, sender: str) -> str | None:
    existing_screenshot = f"{sender}.jpg"
    if os.path.exists(existing_screenshot):
        return existing_screenshot

    time_stamp = hhmmss(duration // 2)
    output_file = datetime.now().isoformat("_", "seconds") + ".jpg"

    cmd = [
        "ffmpeg",
        "-ss", time_stamp,
        "-i", video,
        "-frames:v", "1",
        output_file,
        "-y"
    ]

    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    stdout, stderr = await process.communicate()

    if os.path.isfile(output_file):
        return output_file
    else:
        print(f"FFmpeg Error: {stderr.decode().strip()}")
        return None


async def get_video_metadata(file_path):
    default_values = {'width': 1, 'height': 1, 'duration': 1}
    loop = asyncio.get_event_loop()
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)

    try:
        def _extract_metadata():
            try:
                vcap = cv2.VideoCapture(file_path)
                if not vcap.isOpened():
                    return default_values

                width = round(vcap.get(cv2.CAP_PROP_FRAME_WIDTH))
                height = round(vcap.get(cv2.CAP_PROP_FRAME_HEIGHT))
                fps = vcap.get(cv2.CAP_PROP_FPS)
                frame_count = vcap.get(cv2.CAP_PROP_FRAME_COUNT)

                if fps <= 0:
                    return default_values

                duration = round(frame_count / fps)
                if duration <= 0:
                    return default_values

                vcap.release()
                return {'width': width, 'height': height, 'duration': duration}
            except Exception as e:
                logger.error(f"Error in video_metadata: {e}")
                return default_values

        return await loop.run_in_executor(executor, _extract_metadata)

    except Exception as e:
        logger.error(f"Error in get_video_metadata: {e}")
        return default_values


async def add_premium_user(user_id, duration_value, duration_unit):
    try:
        now = datetime.now()
        expiry_date = None

        if duration_unit == "min":
            expiry_date = now + timedelta(minutes=duration_value)
        elif duration_unit == "hours":
            expiry_date = now + timedelta(hours=duration_value)
        elif duration_unit == "days":
            expiry_date = now + timedelta(days=duration_value)
        elif duration_unit == "weeks":
            expiry_date = now + timedelta(weeks=duration_value)
        elif duration_unit == "month":
            expiry_date = now + timedelta(days=30 * duration_value) # Approximation for month
        elif duration_unit == "year":
            expiry_date = now + timedelta(days=365 * duration_value)
        elif duration_unit == "decades":
            expiry_date = now + timedelta(days=3650 * duration_value)
        else:
            return False, "Invalid duration unit"

        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute("""
                INSERT OR REPLACE INTO premium_users (user_id, subscription_start, subscription_end)
                VALUES (?, ?, ?)
            """, (user_id, now.isoformat(), expiry_date.isoformat()))
            await db.commit()

        return True, expiry_date
    except Exception as e:
        logger.error(f"Error adding premium user {user_id}: {e}")
        return False, str(e)


async def is_premium_user(user_id):
    try:
        async with aiosqlite.connect(DB_FILE) as db:
            cursor = await db.execute("SELECT subscription_end FROM premium_users WHERE user_id = ?", (user_id,))
            row = await cursor.fetchone()
            if row:
                subscription_end_str = row[0]
                if subscription_end_str:
                    subscription_end = datetime.fromisoformat(subscription_end_str)
                    return datetime.now() < subscription_end
            return False
    except Exception as e:
        logger.error(f"Error checking premium status for {user_id}: {e}")
        return False


async def get_premium_details(user_id):
    try:
        async with aiosqlite.connect(DB_FILE) as db:
            cursor = await db.execute("SELECT * FROM premium_users WHERE user_id = ?", (user_id,))
            row = await cursor.fetchone()
            if row:
                col_names = [description[0] for description in cursor.description]
                premium_data = dict(zip(col_names, row))
                # Convert string dates back to datetime objects
                if 'subscription_start' in premium_data and premium_data['subscription_start']:
                    premium_data['subscription_start'] = datetime.fromisoformat(premium_data['subscription_start'])
                if 'subscription_end' in premium_data and premium_data['subscription_end']:
                    premium_data['subscription_end'] = datetime.fromisoformat(premium_data['subscription_end'])
                return premium_data
            return None
    except Exception as e:
        logger.error(f"Error getting premium details for {user_id}: {e}")
        return None