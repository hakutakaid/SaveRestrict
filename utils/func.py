import concurrent.futures
import time
import os
import re
import cv2
import logging
import asyncio
import aiosqlite # Import aiosqlite
from datetime import datetime, timedelta

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

PUBLIC_LINK_PATTERN = re.compile(r'(https?://)?(t\.me|telegram\.me)/([^/]+)(/(\d+))?')
PRIVATE_LINK_PATTERN = re.compile(r'(https?://)?(t\.me|telegram\.me)/c/(\d+)(/(\d+))?')
VIDEO_EXTENSIONS = {"mp4", "mkv", "avi", "mov", "wmv", "flv", "webm", "mpeg", "mpg", "3gp"}

DB_PATH = 'data.db' # Ganti dengan path yang Anda inginkan, bisa dari config.py

class DatabaseManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self._conn = None # Untuk menyimpan koneksi database

    async def connect(self):
        """Membuat koneksi ke database jika belum ada."""
        if self._conn is None:
            self._conn = await aiosqlite.connect(self.db_path)
            self._conn.row_factory = aiosqlite.Row # Mengatur row_factory agar hasil query bisa diakses seperti dict

            # Inisialisasi tabel jika belum ada
            await self._create_tables()

    async def close(self):
        """Menutup koneksi database."""
        if self._conn:
            await self._conn.close()
            self._conn = None

    async def _execute(self, query, params=()):
        """Mengeksekusi query database dan mengembalikan cursor."""
        await self.connect() # Pastikan koneksi terbuka
        try:
            cursor = await self._conn.execute(query, params)
            await self._conn.commit()
            return cursor
        except Exception as e:
            logger.error(f"Error executing query: {query} with params {params} - {e}")
            raise # Re-raise exception untuk penanganan lebih lanjut

    async def _fetchone(self, query, params=()):
        """Mengeksekusi query dan mengembalikan satu baris."""
        cursor = await self._execute(query, params)
        return await cursor.fetchone()

    async def _fetchall(self, query, params=()):
        """Mengeksekusi query dan mengembalikan semua baris."""
        cursor = await self._execute(query, params)
        return await cursor.fetchall()

    async def _create_tables(self):
        """Membuat tabel yang diperlukan jika belum ada."""
        await self._execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                session_string TEXT,
                bot_token TEXT,
                replacement_words TEXT, -- Simpan sebagai JSON string
                delete_words TEXT,      -- Simpan sebagai JSON string
                updated_at DATETIME
            )
        ''')
        await self._execute('''
            CREATE TABLE IF NOT EXISTS premium_users (
                user_id INTEGER PRIMARY KEY,
                subscription_start DATETIME,
                subscription_end DATETIME
            )
        ''')
        # SQLite tidak memiliki fitur TTL (Time-To-Live) index seperti MongoDB.
        # Anda perlu membersihkan user premium kadaluarsa secara manual atau dengan cron job terpisah.
        # Kolom expireAt di MongoDB adalah untuk TTL, di SQLite kita akan pakai subscription_end
        
        await self._execute('''
            CREATE TABLE IF NOT EXISTS statistics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT,
                timestamp DATETIME,
                user_id INTEGER
                -- Tambahkan kolom statistik lainnya sesuai kebutuhan Anda
            )
        ''')
        await self._execute('''
            CREATE TABLE IF NOT EXISTS redeem_code (
                code TEXT PRIMARY KEY,
                duration_value INTEGER,
                duration_unit TEXT,
                used_by INTEGER,
                used_at DATETIME
            )
        ''')
        logger.info("Database tables initialized.")

    # Metode untuk mengakses 'collections'
    async def get_users_collection(self):
        return UsersCollection(self)

    async def get_premium_users_collection(self):
        return PremiumUsersCollection(self)

    async def get_statistics_collection(self):
        return StatisticsCollection(self)

    async def get_codedb_collection(self):
        return RedeemCodeCollection(self)

# Inisialisasi DatabaseManager global
db_manager = DatabaseManager(DB_PATH)

# Kelas-kelas pembantu untuk 'collections' agar panggilan tetap mirip
class UsersCollection:
    def __init__(self, db_manager):
        self.db_manager = db_manager

    async def update_one(self, filter_query, update_query, upsert=False):
        user_id = filter_query.get("user_id")
        if not user_id:
            raise ValueError("user_id is required for update_one in users collection.")

        set_fields = update_query.get("$set", {})
        unset_fields = update_query.get("$unset", {})
        
        # Konversi dict/list ke JSON string untuk penyimpanan
        if "replacement_words" in set_fields and isinstance(set_fields["replacement_words"], dict):
            set_fields["replacement_words"] = json.dumps(set_fields["replacement_words"])
        if "delete_words" in set_fields and isinstance(set_fields["delete_words"], list):
            set_fields["delete_words"] = json.dumps(set_fields["delete_words"])

        # Bangun SET clause
        set_clauses = [f"{k} = ?" for k in set_fields]
        set_values = list(set_fields.values())

        # Bangun UNSET (set to NULL) clause
        unset_clauses = [f"{k} = NULL" for k in unset_fields]

        if set_clauses or unset_clauses:
            update_parts = []
            if set_clauses:
                update_parts.append(", ".join(set_clauses))
            if unset_clauses:
                update_parts.append(", ".join(unset_clauses))
            
            update_sql = "SET " + ", ".join(update_parts)
            values = set_values + [user_id]
            
            # Cek apakah user_id sudah ada
            existing_user = await self.db_manager._fetchone("SELECT 1 FROM users WHERE user_id = ?", (user_id,))
            
            if existing_user:
                query = f"UPDATE users {update_sql} WHERE user_id = ?"
                await self.db_manager._execute(query, values)
            elif upsert:
                # Untuk upsert, kita harus membangun INSERT atau UPDATE secara manual
                # Ambil semua kolom yang mungkin di set atau di unset (untuk NULL)
                columns = []
                placeholders = []
                insert_values = []
                
                # Tambahkan user_id
                columns.append("user_id")
                placeholders.append("?")
                insert_values.append(user_id)

                for k, v in set_fields.items():
                    columns.append(k)
                    placeholders.append("?")
                    insert_values.append(v)
                
                for k in unset_fields:
                    columns.append(k)
                    placeholders.append("NULL") # Set to NULL for unset

                insert_sql = f"INSERT OR REPLACE INTO users ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
                await self.db_manager._execute(insert_sql, insert_values)
            else:
                logger.warning(f"User {user_id} not found and upsert is false.")
        else:
            logger.debug(f"No fields to update for user {user_id}.")

    async def find_one(self, filter_query):
        user_id = filter_query.get("user_id")
        if not user_id:
            return None
        
        row = await self.db_manager._fetchone("SELECT * FROM users WHERE user_id = ?", (user_id,))
        if row:
            data = dict(row)
            # Konversi JSON string kembali ke dict/list
            if 'replacement_words' in data and data['replacement_words']:
                data['replacement_words'] = json.loads(data['replacement_words'])
            else:
                data['replacement_words'] = {} # Pastikan defaultnya dict
            if 'delete_words' in data and data['delete_words']:
                data['delete_words'] = json.loads(data['delete_words'])
            else:
                data['delete_words'] = [] # Pastikan defaultnya list
            return data
        return None

    # Anda mungkin perlu menambahkan metode lain seperti find() jika digunakan
    # async def find(self, filter_query={}):
    #     # Implementasi find untuk banyak user jika diperlukan
    #     pass


class PremiumUsersCollection:
    def __init__(self, db_manager):
        self.db_manager = db_manager

    async def update_one(self, filter_query, update_query, upsert=False):
        user_id = filter_query.get("user_id")
        if not user_id:
            raise ValueError("user_id is required for update_one in premium_users collection.")

        set_fields = update_query.get("$set", {})
        
        set_clauses = [f"{k} = ?" for k in set_fields]
        set_values = list(set_fields.values())

        existing_user = await self.db_manager._fetchone("SELECT 1 FROM premium_users WHERE user_id = ?", (user_id,))
        
        if existing_user:
            query = f"UPDATE premium_users SET {', '.join(set_clauses)} WHERE user_id = ?"
            await self.db_manager._execute(query, set_values + [user_id])
        elif upsert:
            columns = []
            placeholders = []
            insert_values = []
            
            columns.append("user_id")
            placeholders.append("?")
            insert_values.append(user_id)

            for k, v in set_fields.items():
                columns.append(k)
                placeholders.append("?")
                insert_values.append(v)
            
            insert_sql = f"INSERT INTO premium_users ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
            await self.db_manager._execute(insert_sql, insert_values)
        else:
            logger.warning(f"Premium user {user_id} not found and upsert is false.")

    async def find_one(self, filter_query):
        user_id = filter_query.get("user_id")
        if not user_id:
            return None
        
        row = await self.db_manager._fetchone("SELECT * FROM premium_users WHERE user_id = ?", (user_id,))
        if row:
            return dict(row)
        return None

    async def create_index(self, field_name, expireAfterSeconds=None):
        # SQLite tidak memiliki fungsi TTL index seperti MongoDB.
        # Fungsi ini hanya akan dicatat, dan Anda harus menangani penghapusan entri kadaluarsa secara manual
        # atau menggunakan mekanisme lain (misalnya, cron job yang memanggil fungsi _cleanup_expired_premium_users).
        logger.info(f"SQLite does not support TTL indexes like MongoDB. "
                    f"Index creation for '{field_name}' with expireAfterSeconds={expireAfterSeconds} "
                    f"will not automatically delete expired records. "
                    f"You need to implement a separate cleanup mechanism for premium_users based on subscription_end.")
        pass # Tidak ada yang perlu dilakukan untuk TTL index di SQLite secara langsung


class StatisticsCollection:
    def __init__(self, db_manager):
        self.db_manager = db_manager

    async def insert_one(self, document):
        # Asumsi document memiliki event_type, timestamp, user_id
        await self.db_manager._execute(
            "INSERT INTO statistics (event_type, timestamp, user_id) VALUES (?, ?, ?)",
            (document.get('event_type'), document.get('timestamp'), document.get('user_id'))
        )
    
    async def count_documents(self, filter_query={}):
        where_clauses = []
        params = []
        if 'event_type' in filter_query:
            where_clauses.append("event_type = ?")
            params.append(filter_query['event_type'])
        
        if 'user_id' in filter_query:
            where_clauses.append("user_id = ?")
            params.append(filter_query['user_id'])

        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)
        
        row = await self.db_manager._fetchone(f"SELECT COUNT(*) as count FROM statistics {where_sql}", tuple(params))
        return row['count'] if row else 0

    async def find(self, filter_query={}, sort_query=None, limit=None):
        where_clauses = []
        params = []
        if 'event_type' in filter_query:
            where_clauses.append("event_type = ?")
            params.append(filter_query['event_type'])
        
        if 'user_id' in filter_query:
            where_clauses.append("user_id = ?")
            params.append(filter_query['user_id'])

        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)
        
        order_by_sql = ""
        if sort_query:
            # Contoh: sort_query = [("timestamp", -1)] untuk DESC, [("timestamp", 1)] untuk ASC
            sort_parts = []
            for field, order in sort_query:
                direction = "DESC" if order == -1 else "ASC"
                sort_parts.append(f"{field} {direction}")
            order_by_sql = "ORDER BY " + ", ".join(sort_parts)

        limit_sql = ""
        if limit is not None:
            limit_sql = f"LIMIT {limit}"

        query = f"SELECT * FROM statistics {where_sql} {order_by_sql} {limit_sql}"
        rows = await self.db_manager._fetchall(query, tuple(params))
        return [dict(row) for row in rows] if rows else []


class RedeemCodeCollection:
    def __init__(self, db_manager):
        self.db_manager = db_manager

    async def find_one(self, filter_query):
        code = filter_query.get("code")
        if not code:
            return None
        row = await self.db_manager._fetchone("SELECT * FROM redeem_code WHERE code = ?", (code,))
        if row:
            return dict(row)
        return None

    async def insert_one(self, document):
        # Asumsi document memiliki code, duration_value, duration_unit, used_by, used_at
        await self.db_manager._execute(
            "INSERT INTO redeem_code (code, duration_value, duration_unit, used_by, used_at) VALUES (?, ?, ?, ?, ?)",
            (document.get('code'), document.get('duration_value'), document.get('duration_unit'), 
             document.get('used_by'), document.get('used_at'))
        )
    
    async def update_one(self, filter_query, update_query):
        code = filter_query.get("code")
        if not code:
            raise ValueError("code is required for update_one in redeem_code collection.")

        set_fields = update_query.get("$set", {})
        
        set_clauses = [f"{k} = ?" for k in set_fields]
        set_values = list(set_fields.values())

        query = f"UPDATE redeem_code SET {', '.join(set_clauses)} WHERE code = ?"
        await self.db_manager._execute(query, set_values + [code])

    async def delete_one(self, filter_query):
        code = filter_query.get("code")
        if not code:
            raise ValueError("code is required for delete_one in redeem_code collection.")
        await self.db_manager._execute("DELETE FROM redeem_code WHERE code = ?", (code,))

users_collection = None
premium_users_collection = None
statistics_collection = None
codedb = None

async def init_db_collections():
    """Menginisialisasi objek koleksi setelah koneksi database siap."""
    global users_collection, premium_users_collection, statistics_collection, codedb
    await db_manager.connect() # Pastikan koneksi dan tabel dibuat
    users_collection = await db_manager.get_users_collection()
    premium_users_collection = await db_manager.get_premium_users_collection()
    statistics_collection = await db_manager.get_statistics_collection()
    codedb = await db_manager.get_codedb_collection()
    logger.info("Database collections initialized for aiosqlite.")

import json

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
    # Menggunakan objek `users_collection` yang sudah diinisialisasi
    await users_collection.update_one(
        {"user_id": user_id},
        {"$set": {key: value}},
        upsert=True
    )


async def get_user_data_key(user_id, key, default=None):
    user_data = await users_collection.find_one({"user_id": int(user_id)})
    return user_data.get(key, default) if user_data else default


async def get_user_data(user_id):
    try:
        user_data = await users_collection.find_one({"user_id": user_id})
        return user_data
    except Exception as e:
        logger.error(f"Error retrieving user data for {user_id}: {e}")
        return None


async def save_user_session(user_id, session_string):
    try:
        await users_collection.update_one(
            {"user_id": user_id},
            {"$set": {
                "session_string": session_string,
                "updated_at": datetime.now()
            }},
            upsert=True
        )
        logger.info(f"Saved session for user {user_id}")
        return True
    except Exception as e:
        logger.error(f"Error saving session for user {user_id}: {e}")
        return False


async def remove_user_session(user_id):
    try:
        await users_collection.update_one(
            {"user_id": user_id},
            {"$unset": {"session_string": ""}} # $unset akan diubah menjadi set ke NULL di SQLite
        )
        logger.info(f"Removed session for user {user_id}")
        return True
    except Exception as e:
        logger.error(f"Error removing session for user {user_id}: {e}")
        return False


async def save_user_bot(user_id, bot_token):
    try:
        await users_collection.update_one(
            {"user_id": user_id},
            {"$set": {
                "bot_token": bot_token,
                "updated_at": datetime.now()
            }},
            upsert=True
        )
        logger.info(f"Saved bot token for user {user_id}")
        return True
    except Exception as e:
        logger.error(f"Error saving bot token for user {user_id}: {e}")
        return False


async def remove_user_bot(user_id):
    try:
        await users_collection.update_one(
            {"user_id": user_id},
            {"$unset": {"bot_token": ""}} # $unset akan diubah menjadi set ke NULL di SQLite
        )
        logger.info(f"Removed bot token for user {user_id}")
        return True
    except Exception as e:
        logger.error(f"Error removing bot token for user {user_id}: {e}")
        return False


async def process_text_with_rules(user_id, text):
    if not text:
        return ""

    try:
        # Menggunakan get_user_data_key yang sudah diperbarui
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
            expiry_date = now + timedelta(days=30 * duration_value)
        elif duration_unit == "year":
            expiry_date = now + timedelta(days=365 * duration_value)
        elif duration_unit == "decades":
            expiry_date = now + timedelta(days=3650 * duration_value)
        else:
            return False, "Invalid duration unit"

        await premium_users_collection.update_one(
            {"user_id": user_id},
            {"$set": {
                "user_id": user_id,
                "subscription_start": now,
                "subscription_end": expiry_date,
                # "expireAt": expiry_date # expireAt tidak relevan lagi untuk TTL di SQLite
            }},
            upsert=True
        )

        # Tidak perlu create_index dengan expireAfterSeconds di SQLite
        # await premium_users_collection.create_index("expireAt", expireAfterSeconds=0)

        return True, expiry_date
    except Exception as e:
        logger.error(f"Error adding premium user {user_id}: {e}")
        return False, str(e)


async def is_premium_user(user_id):
    try:
        user = await premium_users_collection.find_one({"user_id": user_id})
        if user and "subscription_end" in user:
            # Pastikan subscription_end adalah objek datetime
            if isinstance(user["subscription_end"], str):
                subscription_end = datetime.strptime(user["subscription_end"], '%Y-%m-%d %H:%M:%S.%f')
            else:
                subscription_end = user["subscription_end"]
                
            now = datetime.now()
            return now < subscription_end
        return False
    except Exception as e:
        logger.error(f"Error checking premium status for {user_id}: {e}")
        return False


async def get_premium_details(user_id):
    try:
        user = await premium_users_collection.find_one({"user_id": user_id})
        if user and "subscription_end" in user:
            # Konversi string datetime ke objek datetime jika diperlukan
            if isinstance(user["subscription_end"], str):
                user["subscription_end"] = datetime.strptime(user["subscription_end"], '%Y-%m-%d %H:%M:%S.%f')
            if isinstance(user["subscription_start"], str):
                user["subscription_start"] = datetime.strptime(user["subscription_start"], '%Y-%m-%d %H:%M:%S.%f')
            return user
        return None
    except Exception as e:
        logger.error(f"Error getting premium details for {user_id}: {e}")
        return None