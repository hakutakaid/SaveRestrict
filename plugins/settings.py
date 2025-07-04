# Copyright (c) 2025 devgagan : https://github.com/devgaganin.
# Licensed under the GNU General Public License v3.0.
# See LICENSE file in the repository root for full license text.

from telethon import events, Button
import re
import os
import asyncio
import string
import random
from shared_client import client as gf
from config import OWNER_ID
from utils.func import get_user_data_key, save_user_data, remove_user_session # Import remove_user_session
import aiosqlite # Import aiosqlite for direct DB ops if needed, though utils.func functions are preferred

# Assuming DB_FILE is defined in utils.func or needs to be passed.
# For now, let's keep the local definition for this file, assuming it's consistent.
DB_FILE = "bot_data.db"

VIDEO_EXTENSIONS = {
    'mp4', 'mkv', 'avi', 'mov', 'wmv', 'flv', 'webm',
    'mpeg', 'mpg', '3gp'
}
SET_PIC = 'settings.jpg' # This seems to be a placeholder, check if it's used
MESS = 'Customize settings for your files...'

active_conversations = {}

@gf.on(events.NewMessage(incoming=True, pattern='/settings'))
async def settings_command(event):
    user_id = event.sender_id
    await send_settings_message(event.chat_id, user_id)

async def send_settings_message(chat_id, user_id):
    buttons = [
        [
            Button.inline('📝 Set Chat ID', b'setchat'),
            Button.inline('🏷️ Set Rename Tag', b'setrename')
        ],
        [
            Button.inline('📋 Set Caption', b'setcaption'),
            Button.inline('🔄 Replace Words', b'setreplacement')
        ],
        [
            Button.inline('🗑️ Remove Words', b'delete'),
            Button.inline('🔄 Reset Settings', b'reset')
        ],
        [
            Button.inline('🔑 Session Login', b'addsession'),
            Button.inline('🚪 Logout', b'logout')
        ],
        [
            Button.inline('🖼️ Set Thumbnail', b'setthumb'),
            Button.inline('❌ Remove Thumbnail', b'remthumb')
        ],
        [
            Button.url('🆘 Report Errors', 'https://t.me/team_spy_pro')
        ]
    ]
    await gf.send_message(chat_id, MESS, buttons=buttons)

@gf.on(events.CallbackQuery)
async def callback_query_handler(event):
    user_id = event.sender_id

    callback_actions = {
        b'setchat': {
            'type': 'setchat',
            'message': """Send me the ID of that chat(with -100 prefix):
__👉 **Note:** if you are using custom bot then your bot should be admin that chat if not then this bot should be admin.__
👉 __If you want to upload in topic group and in specific topic then pass chat id as **-100CHANNELID/TOPIC_ID** for example: **-1004783898/12**__"""
        },
        b'setrename': {
            'type': 'setrename',
            'message': 'Send me the rename tag:'
        },
        b'setcaption': {
            'type': 'setcaption',
            'message': 'Send me the caption:'
        },
        b'setreplacement': {
            'type': 'setreplacement',
            'message': "Send me the replacement words in the format: 'WORD(s)' 'REPLACEWORD'"
        },
        b'addsession': {
            'type': 'addsession',
            'message': 'Send Pyrogram V2 session string:'
        },
        b'delete': {
            'type': 'deleteword',
            'message': 'Send words separated by space to delete them from caption/filename...'
        },
        b'setthumb': {
            'type': 'setthumb',
            'message': 'Please send the photo you want to set as the thumbnail.'
        }
    }

    if event.data in callback_actions:
        action = callback_actions[event.data]
        await start_conversation(event, user_id, action['type'], action['message'])
    elif event.data == b'logout':
        # --- AIOSQLITE MIGRATION FOR LOGOUT ---
        success = await remove_user_session(user_id) # Call the dedicated function in utils.func
        if success:
            await event.respond('Logged out and deleted session successfully.')
        else:
            await event.respond('Error logging out. You might not have been logged in or an error occurred.')
        # --- END AIOSQLITE MIGRATION FOR LOGOUT ---
    elif event.data == b'reset':
        try:
            # --- AIOSQLITE MIGRATION FOR RESET ---
            async with aiosqlite.connect(DB_FILE) as db:
                # Update specific columns to NULL
                await db.execute("""
                    UPDATE users
                    SET delete_words = NULL,
                        replacement_words = NULL,
                        rename_tag = NULL,
                        caption = NULL,
                        chat_id = NULL
                    WHERE user_id = ?
                """, (user_id,))
                await db.commit()
            # --- END AIOSQLITE MIGRATION FOR RESET ---

            thumbnail_path = f'{user_id}.jpg'
            if os.path.exists(thumbnail_path):
                os.remove(thumbnail_path)
            await event.respond('✅ All settings reset successfully. To logout, click /logout')
        except Exception as e:
            await event.respond(f'Error resetting settings: {e}')
    elif event.data == b'remthumb':
        try:
            os.remove(f'{user_id}.jpg')
            await event.respond('Thumbnail removed successfully!')
        except FileNotFoundError:
            await event.respond('No thumbnail found to remove.')

async def start_conversation(event, user_id, conv_type, prompt_message):
    if user_id in active_conversations:
        await event.respond('Previous conversation cancelled. Starting new one.')

    msg = await event.respond(f'{prompt_message}\n\n(Send /cancel to cancel this operation)')
    active_conversations[user_id] = {'type': conv_type, 'message_id': msg.id}

@gf.on(events.NewMessage(pattern='/cancel'))
async def cancel_conversation(event):
    user_id = event.sender_id
    if user_id in active_conversations:
        await event.respond('Cancelled enjoy baby...')
        del active_conversations[user_id]

@gf.on(events.NewMessage())
async def handle_conversation_input(event):
    user_id = event.sender_id
    # Ensure it's not a command and there's an active conversation
    if user_id not in active_conversations or event.message.text and event.message.text.startswith('/'):
        return

    conv_type = active_conversations[user_id]['type']

    handlers = {
        'setchat': handle_setchat,
        'setrename': handle_setrename,
        'setcaption': handle_setcaption,
        'setreplacement': handle_setreplacement,
        'addsession': handle_addsession,
        'deleteword': handle_deleteword,
        'setthumb': handle_setthumb
    }

    if conv_type in handlers:
        # Check if the message is a photo for setthumb
        if conv_type == 'setthumb' and not event.photo:
            await event.respond('❌ Please send a photo for the thumbnail. Operation cancelled.')
            del active_conversations[user_id] # Cancel if no photo for setthumb
            return
        
        await handlers[conv_type](event, user_id)

    # Only delete conversation if it was successfully handled (or intentionally cancelled by handler)
    # This prevents deleting it if, for instance, setthumb got a text message
    if user_id in active_conversations and conv_type not in ['setthumb']: # For setthumb, handler decides to delete or not
        del active_conversations[user_id]


async def handle_setchat(event, user_id):
    try:
        chat_id = event.text.strip()
        # Basic validation for chat_id format (optional but good practice)
        if not (chat_id.startswith('-100') and chat_id[4:].isdigit() or chat_id.isdigit()):
             await event.respond('❌ Invalid Chat ID format. Please provide a valid ID (e.g., -1001234567890 or 123456789).')
             return # Don't remove from active_conversations, let user retry
        
        await save_user_data(user_id, 'chat_id', chat_id)
        await event.respond('✅ Chat ID set successfully!')
        del active_conversations[user_id] # End conversation
    except Exception as e:
        await event.respond(f'❌ Error setting chat ID: {e}')

async def handle_setrename(event, user_id):
    rename_tag = event.text.strip()
    await save_user_data(user_id, 'rename_tag', rename_tag)
    await event.respond(f'✅ Rename tag set to: {rename_tag}')
    del active_conversations[user_id] # End conversation

async def handle_setcaption(event, user_id):
    caption = event.text
    await save_user_data(user_id, 'caption', caption)
    await event.respond(f'✅ Caption set successfully!')
    del active_conversations[user_id] # End conversation

async def handle_setreplacement(event, user_id):
    match = re.match(r"'(.*?)'\s+'(.*?)'", event.text, re.DOTALL) # Use re.DOTALL for multiline matches
    if not match:
        await event.respond("❌ Invalid format. Usage: 'WORD(s)' 'REPLACEWORD'. Make sure to use single quotes and a space in between.")
    else:
        word, replace_word = match.groups()
        # Strip leading/trailing whitespace from extracted words
        word = word.strip()
        replace_word = replace_word.strip()

        delete_words = await get_user_data_key(user_id, 'delete_words', [])
        if word in delete_words:
            await event.respond(f"❌ The word '{word}' is in the delete list and cannot be replaced. Please remove it from delete list first or choose a different word.")
        else:
            replacements = await get_user_data_key(user_id, 'replacement_words', {})
            replacements[word] = replace_word
            await save_user_data(user_id, 'replacement_words', replacements)
            await event.respond(f"✅ Replacement saved: '{word}' will be replaced with '{replace_word}'")
            del active_conversations[user_id] # End conversation

async def handle_addsession(event, user_id):
    session_string = event.text.strip()
    # You might want to add some basic validation for session_string format here
    await save_user_data(user_id, 'session_string', session_string)
    await event.respond('✅ Session string added successfully!')
    del active_conversations[user_id] # End conversation

async def handle_deleteword(event, user_id):
    words_to_delete = event.message.text.split()
    # It's usually better to maintain existing delete words and remove duplicates.
    # The current logic is adding to existing and taking a set.
    # If you intend to *replace* the list with new words, simply:
    # delete_words = list(set(words_to_delete))
    
    # If you want to *add* to existing:
    existing_delete_words = await get_user_data_key(user_id, 'delete_words', [])
    updated_delete_words = list(set(existing_delete_words + words_to_delete))

    await save_user_data(user_id, 'delete_words', updated_delete_words)
    await event.respond(f"✅ Words added to delete list: {', '.join(words_to_delete)}\nYour current delete list: {', '.join(updated_delete_words)}")
    del active_conversations[user_id] # End conversation

async def handle_setthumb(event, user_id):
    # This handler is called only if event.photo is True due to handle_conversation_input checks
    temp_path = await event.download_media()
    try:
        thumb_path = f'{user_id}.jpg'
        if os.path.exists(thumb_path):
            os.remove(thumb_path)
        os.rename(temp_path, thumb_path)
        await event.respond('✅ Thumbnail saved successfully!')
    except Exception as e:
        await event.respond(f'❌ Error saving thumbnail: {e}')
    finally:
        # Always delete the temporary downloaded file
        if os.path.exists(temp_path):
            os.remove(temp_path)
        del active_conversations[user_id] # End conversation

def generate_random_name(length=7):
    characters = string.ascii_letters + string.digits
    return ''.join(random.choice(characters) for _ in range(length))

async def rename_file(file, sender, edit): # 'edit' parameter seems unused
    try:
        delete_words = await get_user_data_key(sender, 'delete_words', [])
        custom_rename_tag = await get_user_data_key(sender, 'rename_tag', '')
        replacements = await get_user_data_key(sender, 'replacement_words', {})

        last_dot_index = str(file).rfind('.')
        if last_dot_index != -1 and last_dot_index != 0:
            ggn_ext = str(file)[last_dot_index + 1:]
            if ggn_ext.isalpha() and len(ggn_ext) <= 9: # Check if it's a valid looking extension
                if ggn_ext.lower() in VIDEO_EXTENSIONS:
                    original_file_name = str(file)[:last_dot_index]
                    file_extension = 'mp4' # Force video to mp4
                else:
                    original_file_name = str(file)[:last_dot_index]
                    file_extension = ggn_ext # Keep original extension
            else: # If extension looks weird or too long, default to mp4 and include the extension in name
                original_file_name = str(file) # Treat whole thing as name if extension is not typical
                file_extension = 'mp4'
        else: # No extension found
            original_file_name = str(file)
            file_extension = 'mp4' # Default to mp4 if no extension

        # Apply delete words
        processed_file_name = original_file_name
        for word in delete_words:
            # Use regex for whole word matching to avoid partial deletions
            processed_file_name = re.sub(r'\b' + re.escape(word) + r'\b', '', processed_file_name, flags=re.IGNORECASE).strip()

        # Apply replacements
        for word, replace_word in replacements.items():
            processed_file_name = processed_file_name.replace(word, replace_word)

        # Clean up multiple spaces, leading/trailing spaces
        processed_file_name = re.sub(r'\s+', ' ', processed_file_name).strip()
        
        # Ensure a base name exists, if after processing it becomes empty
        if not processed_file_name:
            processed_file_name = generate_random_name() # Or use a default like "Untitled"

        # Append custom tag if present
        if custom_rename_tag:
            new_file_name = f'{processed_file_name} {custom_rename_tag}.{file_extension}'
        else:
            new_file_name = f'{processed_file_name}.{file_extension}'
            
        # Sanitize final filename to remove invalid characters for OS
        new_file_name = re.sub(r'[<>:"/\\|?*]', '_', new_file_name)


        os.rename(file, new_file_name)
        return new_file_name
    except Exception as e:
        print(f"Rename error: {e}")
        return file # Return original file path on error

