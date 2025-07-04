import concurrent.futures
import time
import os
import re
import cv2
import logging
import asyncio
import aiosqlite
from datetime import datetime, timedelta
import json # Import json for serialization/deserialization

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# It's better to define DB_PATH in config.py and import it,
# but for now, we'll keep it here as in your provided code.
DB_PATH = 'data.db' 

PUBLIC_LINK_PATTERN = re.compile(r'(https?://)?(t\.me|telegram\.me)/([^/]+)(/(\d+))?')
PRIVATE_LINK_PATTERN = re.compile(r'(https?://)?(t\.me|telegram\.me)/c/(\d+)(/(\d+))?')
VIDEO_EXTENSIONS = {"mp4", "mkv", "avi", "mov", "wmv", "flv", "webm", "mpeg", "mpg", "3gp"}

class DatabaseManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self._conn = None # To hold the database connection

    async def connect(self):
        """Establishes database connection if not already open."""
        if self._conn is None:
            # Ensure the directory for the database file exists
            db_dir = os.path.dirname(self.db_path)
            if db_dir and not os.path.exists(db_dir):
                os.makedirs(db_dir, exist_ok=True)
            
            self._conn = await aiosqlite.connect(self.db_path)
            self._conn.row_factory = aiosqlite.Row # Allows accessing columns by name (like a dict)

            # Initialize tables if they don't exist
            await self._create_tables()

    async def close(self):
        """Closes the database connection."""
        if self._conn:
            await self._conn.close()
            self._conn = None

    async def _execute(self, query, params=()):
        """Executes a database query and returns the cursor."""
        await self.connect() # Ensure connection is open
        try:
            cursor = await self._conn.execute(query, params)
            await self._conn.commit()
            return cursor
        except Exception as e:
            logger.error(f"Error executing query: {query} with params {params} - {e}")
            raise # Re-raise exception for further handling

    async def _fetchone(self, query, params=()):
        """Executes a query and returns a single row."""
        cursor = await self._execute(query, params)
        return await cursor.fetchone()

    async def _fetchall(self, query, params=()):
        """Executes a query and returns all rows."""
        cursor = await self._execute(query, params)
        return await cursor.fetchall()

    async def _create_tables(self):
        """Creates necessary tables if they don't exist."""
        await self._execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                session_string TEXT,
                bot_token TEXT,
                chat_id TEXT,             -- Added for "no such column" error fix
                rename_tag TEXT,          -- Added for "no such column" error fix
                caption TEXT,             -- Added for "no such column" error fix
                replacement_words TEXT,   -- Stored as JSON string
                delete_words TEXT,        -- Stored as JSON string
                updated_at DATETIME
            )
        ''')
        await self._execute('''
            CREATE TABLE IF NOT EXISTS premium_users (
                user_id INTEGER PRIMARY KEY,
                subscription_start DATETIME,
                subscription_end DATETIME,
                expireAt DATETIME,         -- Keep for compatibility if used elsewhere, though not for TTL
                transferred_from INTEGER,
                transferred_from_name TEXT
            )
        ''')
        # SQLite doesn't have TTL (Time-To-Live) index like MongoDB.
        # You need to clean up expired premium users manually or with a separate cron job.
        
        await self._execute('''
            CREATE TABLE IF NOT EXISTS statistics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT,
                timestamp DATETIME, -- Ensure this stores string representation of datetime
                user_id INTEGER
                -- Add other statistics columns as needed
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

    # Methods to access 'collections'
    async def get_users_collection(self):
        return UsersCollection(self)

    async def get_premium_users_collection(self):
        return PremiumUsersCollection(self)

    async def get_statistics_collection(self):
        return StatisticsCollection(self)

    async def get_codedb_collection(self):
        return RedeemCodeCollection(self)

# Global DatabaseManager instance
db_manager = DatabaseManager(DB_PATH)

# Helper classes for 'collections' to mimic MongoDB calls
class UsersCollection:
    def __init__(self, db_manager):
        self.db_manager = db_manager

    async def update_one(self, filter_query, update_query, upsert=False):
        user_id = filter_query.get("user_id")
        if not user_id:
            raise ValueError("user_id is required for update_one in users collection.")

        set_fields = update_query.get("$set", {})
        unset_fields = update_query.get("$unset", {})
        
        # Prepare data for insertion/update
        data_to_set = {}

        # Handle $set fields
        for k, v in set_fields.items():
            if k in ["replacement_words", "delete_words"]:
                # Serialize dict/list to JSON string for storage
                if isinstance(v, (dict, list)):
                    data_to_set[k] = json.dumps(v)
                else: # Handle cases where it might already be a string or None
                    data_to_set[k] = v 
            else:
                data_to_set[k] = v

        # Handle $unset fields by setting them to None
        for k in unset_fields:
            data_to_set[k] = None

        # Check if user_id already exists
        existing_user = await self.db_manager._fetchone("SELECT 1 FROM users WHERE user_id = ?", (user_id,))
        
        if existing_user:
            # Update existing user
            update_clauses = []
            params = []
            for k, v in data_to_set.items():
                update_clauses.append(f"{k} = ?")
                params.append(v)
            
            if update_clauses: # Only execute if there's something to update
                query = f"UPDATE users SET {', '.join(update_clauses)} WHERE user_id = ?"
                await self.db_manager._execute(query, params + [user_id])
        elif upsert:
            # Insert new user or replace existing one if upsert=True
            # Use INSERT OR REPLACE to handle upsert logic
            all_fields = {'user_id': user_id, **data_to_set}
            columns = ', '.join(all_fields.keys())
            placeholders = ', '.join(['?'] * len(all_fields))
            values = tuple(all_fields.values())
            
            insert_sql = f"INSERT OR REPLACE INTO users ({columns}) VALUES ({placeholders})"
            await self.db_manager._execute(insert_sql, values)
        else:
            logger.warning(f"User {user_id} not found and upsert is false. No operation performed.")

    async def find_one(self, filter_query):
        user_id = filter_query.get("user_id")
        if not user_id:
            return None
        
        row = await self.db_manager._fetchone("SELECT * FROM users WHERE user_id = ?", (user_id,))
        if row:
            data = dict(row)
            # Deserialize JSON string back to dict/list
            for key in ["replacement_words", "delete_words"]:
                if key in data and data[key] is not None:
                    try:
                        data[key] = json.loads(data[key])
                    except json.JSONDecodeError:
                        logger.warning(f"Failed to decode JSON for user {user_id}, key '{key}'. Data: {data[key]}")
                        data[key] = {} if key == "replacement_words" else []
                else: # Ensure default empty dict/list if value is None or missing
                    data[key] = {} if key == "replacement_words" else [] 
            return data
        return None

    async def delete_one(self, filter_query):
        user_id = filter_query.get("user_id")
        if not user_id:
            raise ValueError("user_id is required for delete_one in users collection.")
        await self.db_manager._execute("DELETE FROM users WHERE user_id = ?", (user_id,))


class PremiumUsersCollection:
    def __init__(self, db_manager):
        self.db_manager = db_manager

    async def update_one(self, filter_query, update_query, upsert=False):
        user_id = filter_query.get("user_id")
        if not user_id:
            raise ValueError("user_id is required for update_one in premium_users collection.")

        set_fields = update_query.get("$set", {})
        
        # Convert datetime objects to string for storage
        prepared_set_fields = {}
        for k, v in set_fields.items():
            if isinstance(v, datetime):
                prepared_set_fields[k] = v.isoformat() # ISO format for datetime strings
            else:
                prepared_set_fields[k] = v

        set_clauses = [f"{k} = ?" for k in prepared_set_fields]
        set_values = list(prepared_set_fields.values())

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

            for k, v in prepared_set_fields.items():
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
            data = dict(row)
            # Convert datetime strings back to datetime objects
            for dt_field in ['subscription_start', 'subscription_end', 'expireAt']:
                if dt_field in data and data[dt_field] is not None:
                    try:
                        data[dt_field] = datetime.fromisoformat(data[dt_field])
                    except ValueError:
                        logger.warning(f"Failed to parse datetime for user {user_id}, field '{dt_field}'. Value: {data[dt_field]}")
            return data
        return None

    async def delete_one(self, filter_query):
        user_id = filter_query.get("user_id")
        if not user_id:
            raise ValueError("user_id is required for delete_one in premium_users collection.")
        await self.db_manager._execute("DELETE FROM premium_users WHERE user_id = ?", (user_id,))

    async def create_index(self, field_name, expireAfterSeconds=None):
        # SQLite does not have TTL index functionality like MongoDB.
        # This function will only log a warning. You must handle expiration cleanup manually.
        logger.info(f"SQLite does not support TTL indexes like MongoDB. "
                    f"Index creation for '{field_name}' with expireAfterSeconds={expireAfterSeconds} "
                    f"will not automatically delete expired records. "
                    f"You need to implement a separate cleanup mechanism for premium_users based on subscription_end.")
        pass # No direct action for TTL index in SQLite


class StatisticsCollection:
    def __init__(self, db_manager):
        self.db_manager = db_manager

    async def insert_one(self, document):
        # Ensure timestamp is stored as a string
        timestamp_str = document.get('timestamp')
        if isinstance(timestamp_str, datetime):
            timestamp_str = timestamp_str.isoformat()

        await self.db_manager._execute(
            "INSERT INTO statistics (event_type, timestamp, user_id) VALUES (?, ?, ?)",
            (document.get('event_type'), timestamp_str, document.get('user_id'))
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
        # Ensure used_at is stored as a string
        used_at_str = document.get('used_at')
        if isinstance(used_at_str, datetime):
            used_at_str = used_at_str.isoformat()

        await self.db_manager._execute(
            "INSERT INTO redeem_code (code, duration_value, duration_unit, used_by, used_at) VALUES (?, ?, ?, ?, ?)",
            (document.get('code'), document.get('duration_value'), document.get('duration_unit'), 
             document.get('used_by'), used_at_str)
        )
    
    async def update_one(self, filter_query, update_query):
        code = filter_query.get("code")
        if not code:
            raise ValueError("code is required for update_one in redeem_code collection.")

        set_fields = update_query.get("$set", {})
        
        # Convert datetime to string if present
        prepared_set_fields = {}
        for k, v in set_fields.items():
            if isinstance(v, datetime):
                prepared_set_fields[k] = v.isoformat()
            else:
                prepared_set_fields[k] = v

        set_clauses = [f"{k} = ?" for k in prepared_set_fields]
        set_values = list(prepared_set_fields.values())

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
    """Initializes collection objects after database connection is ready."""
    global users_collection, premium_users_collection, statistics_collection, codedb
    await db_manager.connect() # Ensure connection and tables are created
    users_collection = await db_manager.get_users_collection()
    premium_users_collection = await db_manager.get_premium_users_collection()
    statistics_collection = await db_manager.get_statistics_collection()
    codedb = await db_manager.get_codedb_collection()
    logger.info("Database collections initialized for aiosqlite.")

# ------- < start > Session Encoder don't change -------

a1 = "c2F2ZV9yZXN0cmljdGVkX2NvbnRlbnRfYm90cw=="
a2 = "Nzk2"
a3 = "Z2V0X21lc3NhZ2Vz"
a4 = "cmVwbHlfcGhvdG8="
a5 = "c3RhcnQ="
attr1 = "cGhvdG8="
attr2 = "ZmlsZV9pZA=="
a7 = "SGkg8J+RiyBXZWxjb21lLCBXYW5uYSBpbnRyby4uLj8gQgrinLPvuI8gSSBjYW4gc2F2ZSBwb3N0cyBmcm9tIGNoYW5uZWxzIG9yIGdyb3VwcyB3aGVyZSBmb3J3YXJkaW5nIGlzIG9mZi4gSSBjYW4gZG93bmxvYWQgdmlkZW9zL2F1ZGlvIGZyb20gWVQsIElOU1RBLCAuLi4gc29jaWFsIHBsYXRmb3JtcwrinLPvuI8gU2ltcGx5IHNlbmQgdGhlIHBvc3QgbGluayBvZiBhIHB1YmxpYyBjaGFubmVsLiBGb3IgcHJpdmF0ZSBjaGFubmVscywgZG8gL2xvZ2luLiBTZW5kIC9oZWxwIHRvIGtub3cgbW9yZS4="
a8 = "Sm9pbiBDaGFubmVs"
a9 = "R2V0IFByZW1pdW0="
a10 = "aHR0cHM6Ly90Lm1lL3RlYW1fc3B5X3Bybw=="
a11 = "aHR0cHM6Ly90Lm1lL2tpbmdvZnBhdGFs"

# ------- < end > Session Encoder don't change --------

def is_private_link(link):
    return bool(PRIVATE_LINK_PATTERN.match(link))


def thumbnail(sender):
    # Adjust path if thumbnails are stored in data/thumbnails
    thumb_path = f'./data/thumbnails/{sender}.jpg' 
    return thumb_path if os.path.exists(thumb_path) else None


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
    # The UsersCollection.update_one will handle JSON serialization.
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
                "updated_at": datetime.now().isoformat() # Store as ISO format string
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
            {"$unset": {"session_string": ""}} # $unset will set to NULL in SQLite
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
                "updated_at": datetime.now().isoformat() # Store as ISO format string
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
            {"$unset": {"bot_token": ""}} # $unset will set to NULL in SQLite
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
        replacements = await get_user_data_key(user_id, "replacement_words", {})
        delete_words = await get_user_data_key(user_id, "delete_words", [])

        processed_text = text
        for word, replacement in replacements.items():
            # Use re.escape to handle special characters in 'word' if needed for re.sub
            # For simple replace, string.replace() is fine.
            processed_text = processed_text.replace(word, replacement)

        if delete_words:
            # Re-implement filtering words by exact match for delete_words
            words_in_text = processed_text.split()
            filtered_words = [w for w in words_in_text if w not in delete_words]
            processed_text = " ".join(filtered_words)

        return processed_text
    except Exception as e:
        logger.error(f"Error processing text with rules for user {user_id}: {e}")
        return text


async def screenshot(video: str, duration: int, sender: str) -> str | None:
    # Ensure the directory exists for thumbnails
    thumbnail_dir = './data/thumbnails'
    os.makedirs(thumbnail_dir, exist_ok=True)

    existing_screenshot = os.path.join(thumbnail_dir, f"{sender}.jpg")
    if os.path.exists(existing_screenshot):
        return existing_screenshot

    time_stamp = hhmmss(duration // 2)
    output_file = os.path.join(thumbnail_dir, f"{datetime.now().isoformat('_', 'seconds')}_{sender}.jpg")

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
        logger.error(f"FFmpeg Error for screenshot: {stderr.decode().strip()}")
        return None


async def get_video_metadata(file_path):
    default_values = {'width': 1, 'height': 1, 'duration': 1}
    loop = asyncio.get_event_loop()
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)

    try:
        def _extract_metadata():
            try:
                # Use ffprobe instead of cv2 for better reliability and less overhead
                # Install ffprobe if not already available (usually comes with ffmpeg)
                import subprocess
                cmd = ["ffprobe", "-v", "error", "-select_streams", "v:0",
                       "-show_entries", "stream=width,height,duration", "-of", "json", file_path]
                
                result = subprocess.run(cmd, capture_output=True, text=True, check=True)
                metadata = json.loads(result.stdout)
                
                if "streams" in metadata and len(metadata["streams"]) > 0:
                    stream = metadata["streams"][0]
                    width = int(stream.get("width", default_values['width']))
                    height = int(stream.get("height", default_values['height']))
                    duration = float(stream.get("duration", default_values['duration']))
                    return {'width': width, 'height': height, 'duration': int(duration)} # Duration as int
                return default_values
            except FileNotFoundError:
                logger.error("ffprobe not found. Please ensure ffmpeg/ffprobe is installed and in your PATH.")
                return default_values
            except Exception as e:
                logger.error(f"Error in _extract_metadata with ffprobe: {e}")
                return default_values

        return await loop.run_in_executor(executor, _extract_metadata)

    except Exception as e:
        logger.error(f"Error in get_video_metadata: {e}")
        return default_values


async def add_premium_user(user_id, duration_value, duration_unit):
    try:
        now = datetime.now()
        expiry_date = now

        if duration_unit == "min":
            expiry_date += timedelta(minutes=duration_value)
        elif duration_unit == "hours":
            expiry_date += timedelta(hours=duration_value)
        elif duration_unit == "days":
            expiry_date += timedelta(days=duration_value)
        elif duration_unit == "weeks":
            expiry_date += timedelta(weeks=duration_value)
        elif duration_unit == "month":
            expiry_date += timedelta(days=30 * duration_value) # Approximation for month
        elif duration_unit == "year":
            expiry_date += timedelta(days=365 * duration_value) # Approximation for year
        elif duration_unit == "decades":
            expiry_date += timedelta(days=3650 * duration_value)
        else:
            return False, "Invalid duration unit"

        await premium_users_collection.update_one(
            {"user_id": user_id},
            {"$set": {
                "user_id": user_id, # Ensure user_id is in $set for upsert
                "subscription_start": now,
                "subscription_end": expiry_date,
                "expireAt": expiry_date # Keep for compatibility
            }},
            upsert=True
        )
        return True, expiry_date
    except Exception as e:
        logger.error(f"Error adding premium user {user_id}: {e}")
        return False, str(e)


async def is_premium_user(user_id):
    try:
        user = await premium_users_collection.find_one({"user_id": user_id})
        if user and "subscription_end" in user:
            # find_one now ensures subscription_end is datetime object
            now = datetime.now()
            return now < user["subscription_end"]
        return False
    except Exception as e:
        logger.error(f"Error checking premium status for {user_id}: {e}")
        return False


async def get_premium_details(user_id):
    try:
        user = await premium_users_collection.find_one({"user_id": user_id})
        # find_one now ensures datetime objects are returned
        return user
    except Exception as e:
        logger.error(f"Error getting premium details for {user_id}: {e}")
        return None

