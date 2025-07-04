# Copyright (c) 2025 devgagan : https://github.com/devgaganin.
# Licensed under the GNU General Public License v3.0.
# See LICENSE file in the repository root for full license text.

import os, re, time, asyncio, json
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import UserNotParticipant, FloodWait, PeerIdInvalid, ChannelPrivate, UserDeactivated, RPCError
from config import API_ID, API_HASH, LOG_GROUP, STRING, FORCE_SUB, FREEMIUM_LIMIT, PREMIUM_LIMIT
from utils.func import get_user_data, screenshot, thumbnail, get_video_metadata
from utils.func import get_user_data_key, process_text_with_rules, is_premium_user, E, save_user_session
from shared_client import app as X # Assuming 'app' is your main bot client (Pyrogram Client)
from plugins.settings import rename_file # Assuming rename_file is now async safe and depends on utils.func
from plugins.start import subscribe as sub
from utils.custom_filters import login_in_progress
from utils.encrypt import dcs
from pyrogram.sessions import StringSession
from typing import Dict, Any, Optional
import logging

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger('batch_plugin')

# Global Caches for Clients (Userbots and User Clients)
# UB: User Bot (custom bot created by user's bot_token)
# UC: User Client (user's own account session string)
UB: Dict[int, Client] = {}
UC: Dict[int, Client] = {}
emp = {} # For tracking empty messages or channel joins in get_msg

# Main Userbot for large file handling, derived from STRING
Y: Optional[Client] = None
if STRING:
    try:
        # Initialize Y if STRING is provided. This assumes STRING is a Pyrogram session string.
        # This part might need to be initialized in shared_client.py and imported if 'Y' is a shared resource.
        # For now, let's keep it here, assuming shared_client.py doesn't set Y directly.
        Y = Client(":userbot:", api_id=API_ID, api_hash=API_HASH, session_string=STRING, no_updates=True)
        # We'll start it later when needed to avoid issues if STRING is bad
        logger.info("Main userbot (Y) initialized from STRING.")
    except Exception as e:
        logger.error(f"Failed to initialize main userbot (Y) from STRING: {e}")
        Y = None

# Progress bar related variables
P: Dict[int, int] = {} # Message ID to progress step

# User state management for commands
Z: Dict[int, Dict[str, Any]] = {}

ACTIVE_USERS: Dict[str, Dict[str, Any]] = {}
ACTIVE_USERS_FILE = "active_users.json"

# --- Active Users (Batch Progress) Management ---

def sanitize(filename):
    """Sanitizes filename for OS compatibility."""
    return re.sub(r'[<>:"/\\|?*\']', '_', filename).strip(" .")[:255]

def load_active_users():
    """Loads active users' batch info from a JSON file."""
    try:
        if os.path.exists(ACTIVE_USERS_FILE):
            with open(ACTIVE_USERS_FILE, 'r') as f:
                return json.load(f)
        return {}
    except Exception as e:
        logger.error(f"Error loading active users: {e}")
        return {}

async def save_active_users_to_file():
    """Saves active users' batch info to a JSON file."""
    try:
        with open(ACTIVE_USERS_FILE, 'w') as f:
            json.dump(ACTIVE_USERS, f)
    except Exception as e:
        logger.error(f"Error saving active users: {e}")

async def add_active_batch(user_id: int, batch_info: Dict[str, Any]):
    """Adds a user to the active batch list."""
    ACTIVE_USERS[str(user_id)] = batch_info
    await save_active_users_to_file()

def is_user_active(user_id: int) -> bool:
    """Checks if a user has an active batch."""
    return str(user_id) in ACTIVE_USERS

async def update_batch_progress(user_id: int, current: int, success: int):
    """Updates batch progress for a user."""
    user_str = str(user_id)
    if user_str in ACTIVE_USERS:
        ACTIVE_USERS[user_str]["current"] = current
        ACTIVE_USERS[user_str]["success"] = success
        await save_active_users_to_file()

async def request_batch_cancel(user_id: int):
    """Requests cancellation for an active batch."""
    user_str = str(user_id)
    if user_str in ACTIVE_USERS:
        ACTIVE_USERS[user_str]["cancel_requested"] = True
        await save_active_users_to_file()
        return True
    return False

def should_cancel(user_id: int) -> bool:
    """Checks if cancellation has been requested for a user's batch."""
    user_str = str(user_id)
    return user_str in ACTIVE_USERS and ACTIVE_USERS[user_str].get("cancel_requested", False)

async def remove_active_batch(user_id: int):
    """Removes a user from the active batch list."""
    user_str = str(user_id)
    if user_str in ACTIVE_USERS:
        del ACTIVE_USERS[user_str]
        await save_active_users_to_file()

def get_batch_info(user_id: int) -> Optional[Dict[str, Any]]:
    """Retrieves batch info for a user."""
    return ACTIVE_USERS.get(str(user_id))

# Load active users on startup
ACTIVE_USERS = load_active_users()

# --- Client Management Functions ---

async def upd_dlg(client_obj: Client) -> bool:
    """Updates dialogs for a Pyrogram client."""
    try:
        await client_obj.get_dialogs(limit=1) # Fetching just one is usually enough to refresh
        return True
    except FloodWait as e:
        logger.warning(f"FloodWait for client {client_obj.name}: {e.value} seconds. Sleeping...")
        await asyncio.sleep(e.value)
        return False # Indicate failure due to floodwait
    except Exception as e:
        logger.error(f'Failed to update dialogs for {client_obj.name}: {e}')
        return False

async def get_msg(bot_client: Client, user_client: Optional[Client], channel_id_or_username: str | int, message_id: int, link_type: str) -> Optional[Message]:
    """Fetches a message from a channel/group using either bot or user client."""
    try:
        if link_type == 'public':
            try:
                # Try fetching with main bot client (X) first for public channels
                msg = await X.get_messages(channel_id_or_username, message_id)
                if getattr(msg, "empty", False):
                    # If empty, try joining and then fetching with user client if available
                    if user_client:
                        try:
                            # Try to join the chat first if it's a public channel
                            await user_client.join_chat(channel_id_or_username)
                            await upd_dlg(user_client) # Refresh dialogs after joining
                            # Fetch again with user client using resolved ID
                            chat = await user_client.get_chat(channel_id_or_username)
                            msg = await user_client.get_messages(chat.id, message_id)
                        except Exception as join_e:
                            logger.warning(f"User client failed to join or fetch from public chat {channel_id_or_username}: {join_e}")
                            msg = None # Indicate failure if join/fetch fails
                return msg
            except Exception as e:
                logger.error(f'Error fetching public message {message_id} from {channel_id_or_username} with main bot: {e}')
                # Fallback to user client if main bot fails for public
                if user_client:
                    try:
                        chat = await user_client.get_chat(channel_id_or_username)
                        return await user_client.get_messages(chat.id, message_id)
                    except Exception as user_e:
                        logger.error(f"User client also failed for public chat {channel_id_or_username}: {user_e}")
                        return None
                return None
        else: # 'private' link_type
            if not user_client:
                logger.warning(f"Attempted to fetch private message for {channel_id_or_username} without a user client.")
                return None
            try:
                # Ensure channel_id is correctly formatted for private channels (-100)
                chat_id_int = int(str(channel_id_or_username).replace('-100', ''))
                # Pyrogram automatically handles the -100 prefix for channel IDs if provided as int
                resolved_chat_id = channel_id_int if str(channel_id_int) == str(channel_id_or_username) else int(f'-100{chat_id_int}')

                # Attempt to get message directly
                msg = await user_client.get_messages(resolved_chat_id, message_id)
                if not msg or getattr(msg, "empty", False):
                    # If direct fetch fails or message is empty, try resolving peer and re-fetching
                    # This helps in cases where the user client might not have full dialog data yet
                    await upd_dlg(user_client) # Refresh dialogs
                    msg = await user_client.get_messages(resolved_chat_id, message_id)
                return msg
            except UserNotParticipant:
                logger.warning(f"User {user_client.id} is not a participant in private chat {channel_id_or_username}. Attempting to join...")
                try:
                    # For private channels, joining usually requires an invite link, but sometimes can be done via ID if already in dialog
                    await user_client.join_chat(channel_id_or_username) # This might fail if no invite link
                    await upd_dlg(user_client)
                    return await user_client.get_messages(channel_id_or_username, message_id)
                except Exception as join_e:
                    logger.error(f"User client failed to join or fetch from private chat {channel_id_or_username}: {join_e}")
                    return None
            except PeerIdInvalid:
                logger.error(f"Invalid Peer ID for private chat {channel_id_or_username}. User client might not have access or chat doesn't exist for user.")
                return None
            except Exception as e:
                logger.error(f'Error fetching private message {message_id} from {channel_id_or_username} with user client: {e}')
                return None
    except Exception as e:
        logger.critical(f'Unhandled error in get_msg: {e}')
        return None


async def get_ubot(uid: int) -> Optional[Client]:
    """Retrieves or creates a Pyrogram bot client for a given user ID."""
    if uid in UB and UB[uid].is_connected:
        return UB.get(uid)
    
    bot_token = await get_user_data_key(uid, "bot_token", None)
    if not bot_token:
        logger.info(f"No bot token found for user {uid}.")
        return None
    
    try:
        bot = Client(name=f"user_bot_{uid}", bot_token=bot_token, api_id=API_ID, api_hash=API_HASH, no_updates=True)
        await bot.start()
        UB[uid] = bot
        logger.info(f"User bot for {uid} started successfully.")
        return bot
    except Exception as e:
        logger.error(f"Error starting user bot for {uid}: {e}")
        if uid in UB: # Remove from cache if it failed to start
            del UB[uid]
        return None

async def get_uclient(uid: int) -> Optional[Client]:
    """Retrieves or creates a Pyrogram user client for a given user ID."""
    if uid in UC and UC[uid].is_connected:
        return UC.get(uid)
    
    user_data = await get_user_data(uid)
    if not user_data:
        logger.info(f"No user data found for {uid}.")
        return None
    
    session_string_encrypted = user_data.get('session_string')
    if not session_string_encrypted:
        logger.info(f"No session string found for user {uid}.")
        return None
    
    try:
        session_string = dcs(session_string_encrypted)
        user_client = Client(name=f"user_client_{uid}", api_id=API_ID, api_hash=API_HASH, 
                             session_string=session_string, device_model="v3saver", no_updates=True)
        await user_client.start()
        await upd_dlg(user_client) # Refresh dialogs after login
        UC[uid] = user_client
        logger.info(f"User client for {uid} started successfully.")
        return user_client
    except FloodWait as e:
        logger.warning(f"FloodWait when starting user client {uid}: {e.value} seconds. Please wait.")
        # Do not put to UC cache if floodwait. User should try again later.
        return None
    except (UserDeactivated, PeerIdInvalid) as e:
        logger.error(f"User client for {uid} failed to start due to invalid session (UserDeactivated/PeerIdInvalid): {e}. Removing session.")
        # Invalidate session if it's bad
        await save_user_session(uid, None) # Set session_string to NULL
        if uid in UC: del UC[uid]
        return None
    except Exception as e:
        logger.error(f"Error starting user client for {uid}: {e}")
        if uid in UC: del UC[uid]
        return None

# --- Progress Bar Update Function ---
async def prog(current: int, total: int, client: Client, message_id: int, start_time: float):
    """Updates the progress message during file download/upload."""
    global P
    if not total: return # Avoid division by zero
    
    percentage = current * 100 / total
    
    # Update progress every ~10% or if 100% is reached
    # Adjust interval dynamically based on total size for smoother updates on small files
    interval = 10 if total >= 100 * 1024 * 1024 else 20 if total >= 50 * 1024 * 1024 else 30 if total >= 10 * 1024 * 1024 else 50
    step = int(percentage // interval) * interval

    if message_id not in P or P[message_id] != step or percentage >= 100:
        P[message_id] = step
        
        c_mb = current / (1024 * 1024)
        t_mb = total / (1024 * 1024)
        
        bar = '🟢' * int(percentage / 10) + '🔴' * (10 - int(percentage / 10))
        
        elapsed_time = time.time() - start_time
        speed = c_mb / elapsed_time if elapsed_time > 0 else 0
        
        remaining_bytes = total - current
        eta_seconds = remaining_bytes / (speed * 1024 * 1024) if speed > 0 else 0
        eta = time.strftime('%M:%S', time.gmtime(eta_seconds))

        try:
            await client.edit_message_text(
                chat_id=client._current_chat, # This needs to be the actual chat ID where the progress message is
                message_id=message_id,
                text=f"__**Pyro Handler...**__\n\n{bar}\n\n⚡**__Completed__**: {c_mb:.2f} MB / {t_mb:.2f} MB\n📊 **__Done__**: {percentage:.2f}%\n🚀 **__Speed__**: {speed:.2f} MB/s\n⏳ **__ETA__**: {eta}\n\n**__Powered by Team SPY__**"
            )
            if percentage >= 100:
                P.pop(message_id, None)
        except FloodWait as e:
            logger.warning(f"FloodWait during progress update for message {message_id}: {e.value} seconds.")
            await asyncio.sleep(e.value)
        except RPCError as e:
            logger.error(f"RPCError updating progress message {message_id}: {e}")
            P.pop(message_id, None) # Stop updating if message is gone/error
        except Exception as e:
            logger.error(f"Error updating progress message {message_id}: {e}")
            P.pop(message_id, None) # Stop updating on generic error

# --- File Sending Functions ---

async def send_direct(client: Client, message: Message, target_chat_id: int, caption: Optional[str] = None, reply_to_message_id: Optional[int] = None) -> bool:
    """Sends a message's media directly to a target chat."""
    try:
        if message.video:
            await client.send_video(target_chat_id, message.video.file_id, caption=caption, duration=message.video.duration, width=message.video.width, height=message.video.height, reply_to_message_id=reply_to_message_id)
        elif message.video_note:
            await client.send_video_note(target_chat_id, message.video_note.file_id, reply_to_message_id=reply_to_message_id)
        elif message.voice:
            await client.send_voice(target_chat_id, message.voice.file_id, reply_to_message_id=reply_to_message_id)
        elif message.sticker:
            await client.send_sticker(target_chat_id, message.sticker.file_id, reply_to_message_id=reply_to_message_id)
        elif message.audio:
            await client.send_audio(target_chat_id, message.audio.file_id, caption=caption, duration=message.audio.duration, performer=message.audio.performer, title=message.audio.title, reply_to_message_id=reply_to_message_id)
        elif message.photo:
            # Pyrogram handles photo_id for different sizes, usually message.photo.file_id is the largest.
            photo_id = message.photo.file_id
            await client.send_photo(target_chat_id, photo_id, caption=caption, reply_to_message_id=reply_to_message_id)
        elif message.document:
            await client.send_document(target_chat_id, message.document.file_id, caption=caption, file_name=message.document.file_name, reply_to_message_id=reply_to_message_id)
        else:
            return False # No recognizable media type
        return True
    except FloodWait as e:
        logger.warning(f"FloodWait during direct send: {e.value} seconds. Sleeping...")
        await asyncio.sleep(e.value)
        return False
    except RPCError as e:
        logger.error(f'Direct send RPC error: {e}')
        return False
    except Exception as e:
        logger.error(f'Direct send general error: {e}')
        return False

# --- Message Processing Core Logic ---

async def process_msg(bot_client: Client, user_client: Optional[Client], message: Message, target_user_id: int, link_type: str, source_chat_id: int) -> str:
    """Processes a single message (downloads, renames, uploads)."""
    uid = target_user_id # Renamed for clarity in this function
    
    try:
        # Determine target chat ID and reply message ID
        cfg_chat_str = await get_user_data_key(uid, 'chat_id', None)
        target_chat_id = uid # Default to user's private chat
        reply_to_message_id = None

        if cfg_chat_str:
            if '/' in cfg_chat_str:
                parts = cfg_chat_str.split('/', 1)
                try:
                    target_chat_id = int(parts[0])
                    reply_to_message_id = int(parts[1]) if len(parts) > 1 else None
                except ValueError:
                    logger.error(f"Invalid chat_id format in user data for {uid}: {cfg_chat_str}")
                    return "Error: Invalid target chat ID format."
            else:
                try:
                    target_chat_id = int(cfg_chat_str)
                except ValueError:
                    logger.error(f"Invalid chat_id format in user data for {uid}: {cfg_chat_str}")
                    return "Error: Invalid target chat ID format."
        
        if message.media:
            orig_caption = message.caption.markdown if message.caption else ''
            processed_caption = await process_text_with_rules(uid, orig_caption)
            user_defined_caption = await get_user_data_key(uid, 'caption', '')
            
            # Combine original processed text and user-defined caption
            final_caption = ""
            if processed_caption:
                final_caption += processed_caption
            if user_defined_caption:
                if final_caption: final_caption += "\n\n"
                final_caption += user_defined_caption
            
            # Attempt direct forwarding if it's a public channel and not empty
            # NOTE: Pyrogram's `copy_message` is generally better than `send_direct` for preserving media attributes
            # and works well for most cases, even between clients.
            # If `send_direct` is meant for force-sending or modifying, keep it.
            # Otherwise, consider `client.copy_message(target_chat_id, message.chat.id, message.id, ...)`
            
            # For now, keeping your `send_direct` logic:
            if link_type == 'public' and not emp.get(source_chat_id, False):
                if await send_direct(bot_client, message, target_chat_id, final_caption, reply_to_message_id):
                    return 'Sent directly (public).'
                else:
                    logger.warning(f"Direct send failed for public message {message.id} from {source_chat_id}. Attempting download.")


            progress_message = await bot_client.send_message(uid, 'Downloading...', disable_notification=True)
            download_start_time = time.time()
            
            file_name_for_download = None
            if message.video: file_name_for_download = message.video.file_name or f"{message.video.file_id}.mp4"
            elif message.audio: file_name_for_download = message.audio.file_name or f"{message.audio.file_id}.mp3"
            elif message.document: file_name_for_download = message.document.file_name or f"{message.document.file_id}.bin"
            elif message.photo: file_name_for_download = f"{message.photo.file_id}.jpg"

            # Sanitize before downloading to avoid OS issues with temp files
            sanitized_download_name = sanitize(file_name_for_download or f"download_{message.id}")
            
            downloaded_file_path = None
            try:
                # Use the user_client for downloading if available, otherwise bot_client
                client_to_download = user_client if user_client else bot_client
                downloaded_file_path = await client_to_download.download_media(
                    message,
                    file_name=sanitized_download_name, # Use sanitized name
                    progress=prog,
                    progress_args=(bot_client, progress_message.id, download_start_time) # Pass bot_client for progress update
                )
            except FloodWait as e:
                await bot_client.edit_message_text(uid, progress_message.id, f'FloodWait during download: {e.value} seconds. Try again later.')
                return 'Failed: FloodWait.'
            except Exception as e:
                logger.error(f"Error downloading media from {message.id}: {e}")
                await bot_client.edit_message_text(uid, progress_message.id, f'Download failed: {str(e)[:50]}')
                return 'Failed: Download Error.'
            
            if not downloaded_file_path:
                await bot_client.edit_message_text(uid, progress_message.id, 'Failed to download file.')
                return 'Failed: No file downloaded.'
            
            await bot_client.edit_message_text(uid, progress_message.id, 'Renaming...')
            
            # The rename_file function should handle os.remove on old name and return new path
            final_file_path = await rename_file(downloaded_file_path, uid, progress_message) # Pass progress_message if rename_file uses it
            
            file_size_gb = os.path.getsize(final_file_path) / (1024 * 1024 * 1024)
            
            # --- Large File Handling (if Y is available and file > 2GB) ---
            if file_size_gb > 2 and Y and Y.is_connected: # Check if Y is initialized and connected
                upload_start_time = time.time()
                await bot_client.edit_message_text(uid, progress_message.id, 'File is larger than 2GB. Using main userbot for upload...')
                
                # Ensure Y's dialogs are updated
                await upd_dlg(Y) 
                
                # Get video metadata if it's a video
                metadata = {}
                if message.video or os.path.splitext(final_file_path)[1].lower() == '.mp4':
                    metadata = await get_video_metadata(final_file_path)

                # Get thumbnail
                thumb_path = await thumbnail(uid) # User's custom thumbnail
                if not thumb_path and (message.video or message.photo): # If no custom thumb, try generating from video/photo
                    if message.video:
                        thumb_path = await screenshot(final_file_path, metadata.get('duration', 1), uid)
                    elif message.photo and message.photo.file_id:
                        # For photos, download a small version as thumb if needed
                        # Pyrogram typically handles this when sending photo.
                        # If a specific local thumb is needed, you might download message.photo directly here.
                        pass # No explicit thumb download needed for photos, Pyrogram handles internal thumbs
                
                try:
                    # Determine send function based on message type
                    if message.video or os.path.splitext(final_file_path)[1].lower() == '.mp4':
                        sent_message = await Y.send_video(
                            chat_id=LOG_GROUP, # Send to LOG_GROUP first for large files
                            video=final_file_path,
                            caption=final_caption,
                            duration=metadata.get('duration'),
                            width=metadata.get('width'),
                            height=metadata.get('height'),
                            thumb=thumb_path,
                            progress=prog,
                            progress_args=(bot_client, progress_message.id, upload_start_time)
                        )
                    elif message.audio:
                        sent_message = await Y.send_audio(
                            chat_id=LOG_GROUP,
                            audio=final_file_path,
                            caption=final_caption,
                            duration=message.audio.duration,
                            performer=message.audio.performer,
                            title=message.audio.title,
                            thumb=thumb_path, # Audio can have a thumb
                            progress=prog,
                            progress_args=(bot_client, progress_message.id, upload_start_time)
                        )
                    elif message.photo:
                        sent_message = await Y.send_photo(
                            chat_id=LOG_GROUP,
                            photo=final_file_path,
                            caption=final_caption,
                            progress=prog,
                            progress_args=(bot_client, progress_message.id, upload_start_time)
                        )
                    else: # Default to document
                        sent_message = await Y.send_document(
                            chat_id=LOG_GROUP,
                            document=final_file_path,
                            caption=final_caption,
                            thumb=thumb_path,
                            progress=prog,
                            progress_args=(bot_client, progress_message.id, upload_start_time)
                        )
                    
                    # Copy the uploaded message from LOG_GROUP to the user's target chat
                    await bot_client.copy_message(
                        chat_id=target_chat_id,
                        from_chat_id=LOG_GROUP,
                        message_id=sent_message.id,
                        reply_to_message_id=reply_to_message_id
                    )
                    
                    # Cleanup downloaded file and progress message
                    if os.path.exists(final_file_path): os.remove(final_file_path)
                    if thumb_path and os.path.exists(thumb_path) and thumb_path != f'{uid}.jpg': # Remove temp screenshot if generated
                        os.remove(thumb_path)
                    await bot_client.delete_messages(uid, progress_message.id)
                    
                    return 'Done (Large file uploaded via userbot).'
                
                except FloodWait as e:
                    await bot_client.edit_message_text(uid, progress_message.id, f'Large file upload failed (FloodWait): {e.value}s.')
                    if os.path.exists(final_file_path): os.remove(final_file_path)
                    if thumb_path and os.path.exists(thumb_path) and thumb_path != f'{uid}.jpg': os.remove(thumb_path)
                    return 'Failed: Large file FloodWait.'
                except RPCError as e:
                    await bot_client.edit_message_text(uid, progress_message.id, f'Large file upload failed (RPCError): {str(e)[:50]}')
                    logger.error(f"RPCError during large file upload: {e}")
                    if os.path.exists(final_file_path): os.remove(final_file_path)
                    if thumb_path and os.path.exists(thumb_path) and thumb_path != f'{uid}.jpg': os.remove(thumb_path)
                    return 'Failed: Large file RPCError.'
                except Exception as e:
                    await bot_client.edit_message_text(uid, progress_message.id, f'Large file upload failed: {str(e)[:50]}')
                    logger.error(f"Error during large file upload: {e}")
                    if os.path.exists(final_file_path): os.remove(final_file_path)
                    if thumb_path and os.path.exists(thumb_path) and thumb_path != f'{uid}.jpg': os.remove(thumb_path)
                    return 'Failed: Large file Upload Error.'

            # --- Normal File Handling (Upload with main bot client) ---
            upload_start_time = time.time()
            await bot_client.edit_message_text(uid, progress_message.id, 'Uploading...')
            
            thumb_path = thumbnail(uid) # Get user's custom thumbnail if exists

            try:
                if message.video or os.path.splitext(final_file_path)[1].lower() == '.mp4':
                    metadata = await get_video_metadata(final_file_path)
                    dur, h, w = metadata.get('duration'), metadata.get('width'), metadata.get('height')
                    # Generate screenshot thumbnail only if no custom thumbnail
                    if not thumb_path:
                        thumb_path = await screenshot(final_file_path, dur, uid)
                    
                    await bot_client.send_video(
                        chat_id=target_chat_id,
                        video=final_file_path,
                        caption=final_caption,
                        thumb=thumb_path,
                        width=w, height=h, duration=dur,
                        progress=prog,
                        progress_args=(bot_client, progress_message.id, upload_start_time),
                        reply_to_message_id=reply_to_message_id
                    )
                elif message.video_note:
                    await bot_client.send_video_note(
                        chat_id=target_chat_id,
                        video_note=final_file_path,
                        progress=prog,
                        progress_args=(bot_client, progress_message.id, upload_start_time),
                        reply_to_message_id=reply_to_message_id
                    )
                elif message.voice:
                    await bot_client.send_voice(
                        chat_id=target_chat_id,
                        voice=final_file_path,
                        progress=prog,
                        progress_args=(bot_client, progress_message.id, upload_start_time),
                        reply_to_message_id=reply_to_message_id
                    )
                elif message.sticker: # Stickers are usually sent by file_id, not local path
                    await bot_client.send_sticker(target_chat_id, message.sticker.file_id) # No local file to upload
                elif message.audio:
                    await bot_client.send_audio(
                        chat_id=target_chat_id,
                        audio=final_file_path,
                        caption=final_caption,
                        thumb=thumb_path,
                        progress=prog,
                        progress_args=(bot_client, progress_message.id, upload_start_time),
                        reply_to_message_id=reply_to_message_id
                    )
                elif message.photo:
                    await bot_client.send_photo(
                        chat_id=target_chat_id,
                        photo=final_file_path,
                        caption=final_caption,
                        progress=prog,
                        progress_args=(bot_client, progress_message.id, upload_start_time),
                        reply_to_message_id=reply_to_message_id
                    )
                else: # Default to document for other media types
                    await bot_client.send_document(
                        chat_id=target_chat_id,
                        document=final_file_path,
                        caption=final_caption,
                        thumb=thumb_path, # Document can have a thumb
                        progress=prog,
                        progress_args=(bot_client, progress_message.id, upload_start_time),
                        reply_to_message_id=reply_to_message_id
                    )
            except FloodWait as e:
                await bot_client.edit_message_text(uid, progress_message.id, f'Upload failed (FloodWait): {e.value}s. Try again later.')
                if os.path.exists(final_file_path): os.remove(final_file_path)
                if thumb_path and os.path.exists(thumb_path) and thumb_path != f'{uid}.jpg': os.remove(thumb_path)
                return 'Failed: FloodWait during upload.'
            except RPCError as e:
                await bot_client.edit_message_text(uid, progress_message.id, f'Upload failed (RPCError): {str(e)[:50]}')
                logger.error(f"RPCError during normal upload: {e}")
                if os.path.exists(final_file_path): os.remove(final_file_path)
                if thumb_path and os.path.exists(thumb_path) and thumb_path != f'{uid}.jpg': os.remove(thumb_path)
                return 'Failed: RPCError during upload.'
            except Exception as e:
                await bot_client.edit_message_text(uid, progress_message.id, f'Upload failed: {str(e)[:50]}')
                logger.error(f"Error during normal upload: {e}")
                if os.path.exists(final_file_path): os.remove(final_file_path)
                if thumb_path and os.path.exists(thumb_path) and thumb_path != f'{uid}.jpg': os.remove(thumb_path)
                return 'Failed: Upload Error.'
            
            # Final cleanup
            if os.path.exists(final_file_path): os.remove(final_file_path)
            if thumb_path and os.path.exists(thumb_path) and thumb_path != f'{uid}.jpg': # Remove temp screenshot if generated
                os.remove(thumb_path)
            await bot_client.delete_messages(uid, progress_message.id)
            
            return 'Done.'
            
        elif message.text:
            # Apply text processing rules to text messages as well
            processed_text = await process_text_with_rules(uid, message.text.markdown)
            await bot_client.send_message(target_chat_id, text=processed_text, reply_to_message_id=reply_to_message_id)
            return 'Sent Text.'
        
        else: # Unhandled message type
            return "Skipped: Unhandled message type (not media or text)."

    except Exception as e:
        logger.critical(f'Unhandled error in process_msg for user {uid}: {e}', exc_info=True)
        return f'Critical Error: {str(e)[:50]}'

# --- Pyrogram Handlers ---

@X.on_message(filters.command(['batch', 'single']))
async def process_cmd(c: Client, m: Message):
    """Handles /batch and /single commands."""
    uid = m.from_user.id
    cmd = m.command[0]
    
    if FREEMIUM_LIMIT == 0 and not await is_premium_user(uid):
        await m.reply_text("This bot does not provide free services, please get a subscription from the OWNER.")
        return
    
    if await sub(c, m) == 1: return # Force subscribe check
    
    pro = await m.reply_text('Doing some checks, please hold on...')
    
    if is_user_active(uid):
        await pro.edit('You have an active task. Use /stop to cancel it.')
        return
    
    ubot = await get_ubot(uid)
    if not ubot:
        await pro.edit('Please add your custom bot using /setbot first.')
        return
    
    # Initialize the Z state for the user
    Z[uid] = {'step': 'start' if cmd == 'batch' else 'start_single'}
    await pro.edit(f'Send the {"start link..." if cmd == "batch" else "link you want to process"}.')

@X.on_message(filters.command(['cancel', 'stop']))
async def cancel_cmd(c: Client, m: Message):
    """Handles /cancel and /stop commands for batch processes."""
    uid = m.from_user.id
    if is_user_active(uid):
        if await request_batch_cancel(uid):
            batch_info = get_batch_info(uid)
            progress_msg_id = batch_info.get("progress_message_id") if batch_info else None
            if progress_msg_id:
                try:
                    await c.edit_message_text(uid, progress_msg_id, "Batch cancellation requested. Finishing current item then stopping...")
                except Exception as e:
                    logger.warning(f"Failed to edit progress message for {uid} during cancel request: {e}")
            await m.reply_text('Cancellation requested. The current batch will stop after the current download/upload completes.')
        else:
            await m.reply_text('Failed to request cancellation. No active batch process or an error occurred.')
    else:
        await m.reply_text('No active batch process found.')

@X.on_message(filters.text & filters.private & ~login_in_progress & ~filters.command([
    'start', 'batch', 'cancel', 'login', 'logout', 'stop', 'set',
    'pay', 'redeem', 'gencode', 'single', 'generate', 'keyinfo', 'encrypt', 'decrypt', 'keys', 'setbot', 'rembot', 'settings']))
async def text_handler(c: Client, m: Message):
    """Handles user input during batch/single message processing flow."""
    uid = m.from_user.id
    
    if uid not in Z: return # Not in a command flow

    current_state = Z[uid].get('step')

    if current_state == 'start': # For /batch command - expecting start link
        link = m.text.strip()
        channel_info, message_id, link_type = E(link)
        if not channel_info or not message_id:
            await m.reply_text('Invalid link format. Please send a valid Telegram link.')
            Z.pop(uid, None)
            return
        
        # Verify link validity and access using userbot/userclient
        pt = await m.reply_text("Checking link access, please wait...")
        uc = await get_uclient(uid) # Try to get user client first
        ubot = await get_ubot(uid) # Then get user bot
        
        # Determine which client to use for initial message check
        client_to_check = uc if uc else ubot
        if not client_to_check:
            await pt.edit("Cannot verify link. No user client or custom bot available. Please login with /login or set your bot with /setbot.")
            Z.pop(uid, None)
            return

        try:
            # Fetch just to check access, no need for full message object yet
            temp_msg = await get_msg(ubot, uc, channel_info, message_id, link_type)
            if not temp_msg:
                await pt.edit("Failed to access the provided link/message. Make sure your account/bot has access to it.")
                Z.pop(uid, None)
                return
            await pt.delete() # Delete initial check message if successful
        except Exception as e:
            await pt.edit(f"Error checking link: {e}. Make sure your account/bot has access to it.")
            Z.pop(uid, None)
            return
        
        Z[uid].update({'step': 'count', 'cid': channel_info, 'sid': message_id, 'lt': link_type})
        await m.reply_text('How many messages do you want to process from this starting point?')

    elif current_state == 'start_single': # For /single command - expecting single link
        link = m.text.strip()
        channel_info, message_id, link_type = E(link)
        if not channel_info or not message_id:
            await m.reply_text('Invalid link format. Please send a valid Telegram link.')
            Z.pop(uid, None)
            return

        Z[uid].update({'step': 'process_single', 'cid': channel_info, 'sid': message_id, 'lt': link_type})
        
        pt = await m.reply_text('Processing single message...')
        
        # Get clients
        uc = await get_uclient(uid)
        ubot = await get_ubot(uid)
        
        if not uc and not ubot:
            await pt.edit('Cannot proceed without user client or custom bot. Please login with /login or set your bot with /setbot.')
            Z.pop(uid, None)
            return
            
        if is_user_active(uid):
            await pt.edit('You have an active task. Please use /stop first before starting another.')
            Z.pop(uid, None)
            return

        try:
            # Prioritize user client for message fetching, fallback to userbot
            client_to_fetch = uc if uc else ubot

            msg = await get_msg(ubot, uc, channel_info, message_id, link_type) # Passed both for robustness
            if msg:
                res = await process_msg(ubot, uc, msg, uid, link_type, channel_info) # Corrected arguments
                await pt.edit(f'1/1: {res}')
            else:
                await pt.edit('Message not found or not accessible.')
        except Exception as e:
            logger.error(f"Error during single message processing for {uid}: {e}", exc_info=True)
            await pt.edit(f'Error: {str(e)[:100]}') # Show more error info
        finally:
            Z.pop(uid, None)

    elif current_state == 'count': # For /batch command - expecting message count
        if not m.text.isdigit():
            await m.reply_text('Please enter a valid number for the message count.')
            return # Don't pop, let user retry entering number
        
        count = int(m.text)
        max_limit = PREMIUM_LIMIT if await is_premium_user(uid) else FREEMIUM_LIMIT

        if count <= 0:
            await m.reply_text("Please enter a number greater than 0.")
            return

        if count > max_limit:
            await m.reply_text(f'Maximum limit for your current plan is {max_limit}.')
            return

        Z[uid].update({'step': 'process', 'did': str(m.chat.id), 'num': count})
        
        channel_info, start_id, total_num_messages, link_type = Z[uid]['cid'], Z[uid]['sid'], Z[uid]['num'], Z[uid]['lt']
        success_count = 0

        pt = await m.reply_text('Starting batch processing...')
        
        uc = await get_uclient(uid)
        ubot = await get_ubot(uid)
        
        if not uc and not ubot:
            await pt.edit('Cannot proceed without user client or custom bot. Please login with /login or set your bot with /setbot.')
            Z.pop(uid, None)
            return
            
        if is_user_active(uid): # Double check
            await pt.edit('An active task already exists for you. Use /stop to cancel it first.')
            Z.pop(uid, None)
            return
        
        batch_progress_message = await m.reply_text(
            f"**Batch Progress:**\nTotal: {total_num_messages}\nProcessed: 0\nSuccess: 0\nStatus: Initializing...",
            disable_notification=True
        )

        await add_active_batch(uid, {
            "total": total_num_messages,
            "current": 0,
            "success": 0,
            "cancel_requested": False,
            "progress_message_id": batch_progress_message.id
            })
        
        try:
            for j in range(total_num_messages):
                # Check for cancellation request at the beginning of each iteration
                if should_cancel(uid):
                    await pt.edit(f'Batch cancelled by user at {j}/{total_num_messages}. Successfully processed: {success_count}.')
                    break
                
                await update_batch_progress(uid, j, success_count)
                
                message_id_to_process = int(start_id) + j
                
                try:
                    # Pass both clients to get_msg for robust fetching
                    msg = await get_msg(ubot, uc, channel_info, message_id_to_process, link_type)
                    
                    if msg:
                        # Process the message
                        process_result = await process_msg(ubot, uc, msg, uid, link_type, channel_info)
                        if 'Done' in process_result or 'Sent' in process_result or 'Copied' in process_result:
                            success_count += 1
                        
                        # Update the main batch progress message
                        await X.edit_message_text(
                            chat_id=uid,
                            message_id=batch_progress_message.id,
                            text=f"**Batch Progress:**\nTotal: {total_num_messages}\nProcessed: {j+1}\nSuccess: {success_count}\nStatus: {process_result}"
                        )
                    else:
                        # Message not found or accessible, update status
                        await X.edit_message_text(
                            chat_id=uid,
                            message_id=batch_progress_message.id,
                            text=f"**Batch Progress:**\nTotal: {total_num_messages}\nProcessed: {j+1}\nSuccess: {success_count}\nStatus: Message {message_id_to_process} not found/accessible."
                        )
                except FloodWait as e:
                    logger.warning(f"FloodWait during batch processing for {uid}: {e.value} seconds. Attempting to continue after delay.")
                    await X.edit_message_text(uid, batch_progress_message.id, f"**Batch Progress:**\nTotal: {total_num_messages}\nProcessed: {j}\nSuccess: {success_count}\nStatus: FloodWait, sleeping for {e.value}s.")
                    await asyncio.sleep(e.value)
                except Exception as e:
                    logger.error(f"Error processing message {message_id_to_process} for {uid}: {e}", exc_info=True)
                    await X.edit_message_text(uid, batch_progress_message.id, f"**Batch Progress:**\nTotal: {total_num_messages}\nProcessed: {j+1}\nSuccess: {success_count}\nStatus: Error on Msg {message_id_to_process}: {str(e)[:50]}")
                
                await asyncio.sleep(2) # Small delay between messages to avoid rate limits
            
            # Final message after loop (completed or cancelled)
            final_status_text = ""
            if should_cancel(uid):
                final_status_text = f'Batch Cancelled by user. Successfully processed: {success_count}/{total_num_messages}.'
            else:
                final_status_text = f'Batch Completed ✅ Successfully processed: {success_count}/{total_num_messages}.'
            
            await m.reply_text(final_status_text)
        
        finally:
            await remove_active_batch(uid)
            Z.pop(uid, None) # Clear user state

