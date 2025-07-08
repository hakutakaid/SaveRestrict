from pyrogram import Client, filters
from pyrogram.types import Message, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from pyrogram.errors import BadRequest, SessionPasswordNeeded, PhoneCodeInvalid, PhoneCodeExpired, MessageNotModified
import logging
import os
from config import API_HASH, API_ID
from shared_client import app as bot # Pastikan 'app' dari shared_client adalah instance Client Anda
from utils.func import save_user_session, get_user_data, remove_user_session, save_user_bot, remove_user_bot
from utils.encrypt import ecs, dcs
from plugins.batch import UB, UC
from utils.custom_filters import login_in_progress, set_user_step, get_user_step

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
model = "v3saver Team SPY"

STEP_PHONE = 1
STEP_CODE = 2
STEP_PASSWORD = 3
login_cache = {}

# --- Helper Function for Safe Message Editing ---
async def edit_message_safely(client: Client, message: Message, text: str, reply_markup=None) -> Message:
    """
    Helper function to edit message and handle errors, with a fallback to sending a new message.
    Returns the updated Message object (either edited or a new one).
    """
    try:
        if reply_markup is not None:
            updated_message = await message.edit_text(text, reply_markup=reply_markup)
        else:
            updated_message = await message.edit_text(text)
        logger.info(f"Successfully edited message ID: {message.id} in chat ID: {message.chat.id}")
        return updated_message
    except MessageNotModified:
        logger.warning(f"Message ID {message.id} in chat {message.chat.id} not modified.")
        return message # Return original message if no modification needed
    except BadRequest as e:
        if "MESSAGE_ID_INVALID" in str(e):
            logger.error(f'MESSAGE_ID_INVALID for message {message.id} in chat {message.chat.id}. Sending new message as fallback. Error: {e}')
            # Fallback: Send a new message if the old one is invalid
            new_message = await client.send_message(message.chat.id, text, reply_markup=reply_markup)
            return new_message
        else:
            logger.error(f'BadRequest (other than MESSAGE_ID_INVALID) when editing message {message.id}: {e}')
            # If it's another BadRequest, try sending a new message as well, or just log and return original
            new_message = await client.send_message(message.chat.id, text, reply_markup=reply_markup)
            return new_message
    except Exception as e:
        logger.error(f'General error editing message {message.id} in chat {message.chat.id}: {e}')
        # Fallback: Send a new message for any other unexpected error
        new_message = await client.send_message(message.chat.id, text, reply_markup=reply_markup)
        return new_message

# --- Command Handlers ---

@bot.on_message(filters.command('login') & filters.private)
async def login_command(client, message):
    user_id = message.from_user.id
    set_user_step(user_id, STEP_PHONE)
    login_cache.pop(user_id, None) # Clear any previous login attempt

    # Create a keyboard with a request_contact button
    keyboard = ReplyKeyboardMarkup(
        [[KeyboardButton("Share My Phone Number", request_contact=True)]],
        resize_keyboard=True,
        one_time_keyboard=True
    )

    try:
        await message.delete() # Delete the /login command message
    except Exception as e:
        logger.warning(f"Could not delete /login command message: {e}")

    status_msg = await message.reply(
        """Please share your phone number by clicking the button below. This will securely send your contact to me.""",
        reply_markup=keyboard
    )
    login_cache[user_id] = {'status_msg': status_msg}
    logger.info(f"User {user_id} initiated login. Status message ID: {status_msg.id}")


@bot.on_message(filters.command("setbot") & filters.private)
async def set_bot_token(C, m):
    user_id = m.from_user.id
    args = m.text.split(" ", 1)
    if user_id in UB:
        try:
            await UB[user_id].stop()
            if UB.get(user_id, None): # Ensure it exists before deleting
                del UB[user_id]
            logger.info(f"Stopped and removed old bot for user {user_id}")
        except Exception as e:
            logger.error(f"Error stopping old bot for user {user_id}: {e}")
            if UB.get(user_id, None): # Ensure it exists before deleting
                del UB[user_id]

        try:
            if os.path.exists(f"user_{user_id}.session"):
                os.remove(f"user_{user_id}.session")
                logger.info(f"Removed user session file for {user_id}")
        except Exception as e:
            logger.error(f"Error removing user session file for {user_id}: {e}")

    if len(args) < 2:
        await m.reply_text("⚠️ Please provide a bot token. Usage: `/setbot token`", quote=True)
        return

    bot_token = args[1].strip()
    await save_user_bot(user_id, bot_token)
    await m.reply_text("✅ Bot token saved successfully.", quote=True)
    logger.info(f"Bot token set for user {user_id}")


@bot.on_message(filters.command("rembot") & filters.private)
async def rem_bot_token(C, m):
    user_id = m.from_user.id
    if user_id in UB:
        try:
            await UB[user_id].stop()
            if UB.get(user_id, None): # Ensure it exists before deleting
                del UB[user_id]
            logger.info(f"Stopped and removed old bot for user {user_id}")
        except Exception as e:
            logger.error(f"Error stopping old bot for user {user_id}: {e}")
            if UB.get(user_id, None): # Ensure it exists before deleting
                del UB[user_id]

        try:
            if os.path.exists(f"user_{user_id}.session"):
                os.remove(f"user_{user_id}.session")
                logger.info(f"Removed user session file for {user_id}")
        except Exception as e:
            logger.error(f"Error removing user session file for {user_id}: {e}")

    await remove_user_bot(user_id)
    await m.reply_text("✅ Bot token removed successfully.", quote=True)
    logger.info(f"Bot token removed for user {user_id}")


@bot.on_message(login_in_progress & filters.private & ~filters.command([
    'start', 'batch', 'cancel', 'login', 'logout', 'stop', 'set', 'pay',
    'redeem', 'gencode', 'generate', 'keyinfo', 'encrypt', 'decrypt', 'keys', 'setbot', 'rembot']))
async def handle_login_steps(client, message):
    user_id = message.from_user.id
    step = get_user_step(user_id)
    text_input = message.text.strip() if message.text else ""

    try:
        await message.delete() # Delete the user's input message
    except Exception as e:
        logger.warning(f'Could not delete user message {message.id} from user {user_id}: {e}')

    status_msg = login_cache[user_id].get('status_msg')
    if not status_msg:
        # This should ideally not happen if login_cache is properly managed, but as a safeguard
        status_msg = await client.send_message(user_id, 'Processing...')
        login_cache[user_id]['status_msg'] = status_msg
        logger.info(f"Re-created status_msg for user {user_id} (fallback): ID {status_msg.id}")
    else:
        logger.info(f"Using existing status_msg for user {user_id}: ID {status_msg.id}")

    try:
        if step == STEP_PHONE:
            if message.contact: # Check if the message contains contact information
                phone_number = message.contact.phone_number
                logger.info(f"Received contact for user {user_id}: {phone_number}")

                status_msg = await edit_message_safely(client, status_msg,
                    '🔄 Processing phone number...', reply_markup=ReplyKeyboardRemove())
                login_cache[user_id]['status_msg'] = status_msg # Update status_msg in cache

                temp_client = Client(f'temp_{user_id}', api_id=API_ID, api_hash=API_HASH, device_model=model, in_memory=True)
                try:
                    await temp_client.connect()
                    sent_code = await temp_client.send_code(phone_number)
                    login_cache[user_id]['phone'] = phone_number
                    login_cache[user_id]['phone_code_hash'] = sent_code.phone_code_hash
                    login_cache[user_id]['temp_client'] = temp_client
                    set_user_step(user_id, STEP_CODE)

                    status_msg = await edit_message_safely(client, status_msg,
                        """✅ Verification code sent to your Telegram account.

Please enter the code you received like `1 2 3 4 5` (i.e separated by space):"""
                    )
                    login_cache[user_id]['status_msg'] = status_msg # Update status_msg in cache
                    logger.info(f"Phone code sent to {user_id}.")

                except BadRequest as e:
                    error_message = f"""❌ Error: {str(e)}
Please try again with /login."""
                    status_msg = await edit_message_safely(client, status_msg, error_message, reply_markup=ReplyKeyboardRemove())
                    login_cache[user_id]['status_msg'] = status_msg # Update status_msg in cache
                    logger.error(f"BadRequest when sending code to {user_id}: {e}")
                    await temp_client.disconnect()
                    set_user_step(user_id, None)
                except Exception as e:
                    error_message = f"""❌ An unexpected error occurred: {str(e)}
Please try again with /login."""
                    status_msg = await edit_message_safely(client, status_msg, error_message, reply_markup=ReplyKeyboardRemove())
                    login_cache[user_id]['status_msg'] = status_msg # Update status_msg in cache
                    logger.error(f"Unexpected error when sending code to {user_id}: {e}")
                    if 'temp_client' in login_cache[user_id]:
                        await login_cache[user_id]['temp_client'].disconnect()
                    login_cache.pop(user_id, None)
                    set_user_step(user_id, None)

            else:
                status_msg = await edit_message_safely(client, status_msg,
                    '❌ Please share your phone number by clicking the button, or use /cancel to stop.')
                login_cache[user_id]['status_msg'] = status_msg # Update status_msg in cache
                logger.warning(f"User {user_id} did not share contact during STEP_PHONE.")

        elif step == STEP_CODE:
            code = text_input.replace(' ', '')
            phone = login_cache[user_id]['phone']
            phone_code_hash = login_cache[user_id]['phone_code_hash']
            temp_client = login_cache[user_id]['temp_client']

            try:
                status_msg = await edit_message_safely(client, status_msg, '🔄 Verifying code...')
                login_cache[user_id]['status_msg'] = status_msg # Update status_msg in cache

                await temp_client.sign_in(phone, phone_code_hash, code)
                session_string = await temp_client.export_session_string()
                encrypted_session = ecs(session_string)
                await save_user_session(user_id, encrypted_session)
                await temp_client.disconnect()

                # Clean up login_cache for this user, but keep status_msg reference for final edit
                final_status_msg_ref = login_cache[user_id]['status_msg']
                login_cache.pop(user_id, None)
                login_cache[user_id] = {'status_msg': final_status_msg_ref} # Re-add only status_msg for final update

                status_msg = await edit_message_safely(client, final_status_msg_ref,
                    """✅ Logged in successfully!!"""
                )
                # No need to update login_cache anymore for status_msg as login is complete
                set_user_step(user_id, None)
                logger.info(f"User {user_id} successfully logged in.")

            except SessionPasswordNeeded:
                set_user_step(user_id, STEP_PASSWORD)
                status_msg = await edit_message_safely(client, status_msg,
                    """🔒 Two-step verification is enabled.
Please enter your password:"""
                )
                login_cache[user_id]['status_msg'] = status_msg # Update status_msg in cache
                logger.info(f"User {user_id} requires 2FA password.")

            except (PhoneCodeInvalid, PhoneCodeExpired) as e:
                error_message = f'❌ {str(e)}. Please try again with /login.'
                status_msg = await edit_message_safely(client, status_msg, error_message, reply_markup=ReplyKeyboardRemove())
                login_cache[user_id]['status_msg'] = status_msg # Update status_msg in cache
                logger.warning(f"Invalid/Expired phone code for user {user_id}: {e}")
                await temp_client.disconnect()
                login_cache.pop(user_id, None)
                set_user_step(user_id, None)
            except BadRequest as e:
                error_message = f"""❌ Error during sign-in: {str(e)}
Please try again with /login."""
                status_msg = await edit_message_safely(client, status_msg, error_message, reply_markup=ReplyKeyboardRemove())
                login_cache[user_id]['status_msg'] = status_msg # Update status_msg in cache
                logger.error(f"BadRequest during sign-in for user {user_id}: {e}")
                await temp_client.disconnect()
                login_cache.pop(user_id, None)
                set_user_step(user_id, None)

        elif step == STEP_PASSWORD:
            temp_client = login_cache[user_id]['temp_client']
            try:
                status_msg = await edit_message_safely(client, status_msg, '🔄 Verifying password...')
                login_cache[user_id]['status_msg'] = status_msg # Update status_msg in cache

                await temp_client.check_password(text_input)
                session_string = await temp_client.export_session_string()
                encrypted_session = ecs(session_string)
                await save_user_session(user_id, encrypted_session)
                await temp_client.disconnect()

                # Clean up login_cache for this user, but keep status_msg reference for final edit
                final_status_msg_ref = login_cache[user_id]['status_msg']
                login_cache.pop(user_id, None)
                login_cache[user_id] = {'status_msg': final_status_msg_ref} # Re-add only status_msg for final update

                status_msg = await edit_message_safely(client, final_status_msg_ref,
                    """✅ Logged in successfully!!"""
                )
                # No need to update login_cache anymore for status_msg as login is complete
                set_user_step(user_id, None)
                logger.info(f"User {user_id} successfully logged in with 2FA.")

            except BadRequest as e:
                error_message = f"""❌ Incorrect password: {str(e)}
Please try again:"""
                status_msg = await edit_message_safely(client, status_msg, error_message)
                login_cache[user_id]['status_msg'] = status_msg # Update status_msg in cache
                logger.warning(f"Incorrect 2FA password for user {user_id}: {e}")
            except Exception as e:
                error_message = f"""❌ An unexpected error occurred: {str(e)}
Please try again with /login."""
                status_msg = await edit_message_safely(client, status_msg, error_message, reply_markup=ReplyKeyboardRemove())
                login_cache[user_id]['status_msg'] = status_msg # Update status_msg in cache
                logger.error(f"Unexpected error during 2FA check for user {user_id}: {e}")
                if 'temp_client' in login_cache[user_id]:
                    await login_cache[user_id]['temp_client'].disconnect()
                login_cache.pop(user_id, None)
                set_user_step(user_id, None)

    except Exception as e:
        logger.error(f'Critical error in login flow for user {user_id}: {str(e)}', exc_info=True)
        if status_msg:
            status_msg = await edit_message_safely(client, status_msg,
                f"""❌ An unhandled error occurred: {str(e)}
Please try again with /login.""", reply_markup=ReplyKeyboardRemove())
            login_cache[user_id]['status_msg'] = status_msg # Update status_msg in cache
        else:
            await client.send_message(user_id, f"""❌ An unhandled error occurred: {str(e)}
Please try again with /login.""", reply_markup=ReplyKeyboardRemove())

        if user_id in login_cache and 'temp_client' in login_cache[user_id]:
            try:
                await login_cache[user_id]['temp_client'].disconnect()
            except Exception as disconnect_e:
                logger.error(f"Error during temp_client disconnect for user {user_id}: {disconnect_e}")
        login_cache.pop(user_id, None)
        set_user_step(user_id, None)


@bot.on_message(filters.command('cancel') & filters.private)
async def cancel_command(client, message):
    user_id = message.from_user.id
    try:
        await message.delete()
    except Exception as e:
        logger.warning(f"Could not delete /cancel command message: {e}")

    if get_user_step(user_id):
        status_msg = login_cache.get(user_id, {}).get('status_msg')
        if user_id in login_cache and 'temp_client' in login_cache[user_id]:
            try:
                await login_cache[user_id]['temp_client'].disconnect()
            except Exception as e:
                logger.error(f"Error disconnecting temp_client during cancel for user {user_id}: {e}")
        login_cache.pop(user_id, None)
        set_user_step(user_id, None)
        logger.info(f"Login process cancelled for user {user_id}.")

        if status_msg:
            await edit_message_safely(client, status_msg,
                '✅ Login process cancelled. Use /login to start again.', reply_markup=ReplyKeyboardRemove())
        else:
            temp_msg = await message.reply(
                '✅ Login process cancelled. Use /login to start again.', reply_markup=ReplyKeyboardRemove())
            await temp_msg.delete(5) # Auto-delete this temporary message

    else:
        temp_msg = await message.reply('No active login process to cancel.')
        await temp_msg.delete(5) # Auto-delete this temporary message


@bot.on_message(filters.command('logout') & filters.private)
async def logout_command(client, message):
    user_id = message.from_user.id
    try:
        await message.delete()
    except Exception as e:
        logger.warning(f"Could not delete /logout command message: {e}")

    status_msg = await client.send_message(user_id, '🔄 Processing logout request...')
    try:
        session_data = await get_user_data(user_id)

        if not session_data or 'session_string' not in session_data:
            await edit_message_safely(client, status_msg,
                '❌ No active session found for your account.')
            logger.info(f"No active session for user {user_id} to logout.")
            return

        encss = session_data['session_string']
        session_string = dcs(encss)
        temp_client = Client(f'temp_logout_{user_id}', api_id=API_ID,
            api_hash=API_HASH, session_string=session_string, in_memory=True) # Use in_memory for temporary logout client
        try:
            await temp_client.connect()
            await temp_client.log_out()
            await edit_message_safely(client, status_msg,
                '✅ Telegram session terminated successfully. Removing from database...')
            logger.info(f"Telegram session terminated for user {user_id}.")
        except Exception as e:
            logger.error(f'Error terminating Telegram session for user {user_id}: {str(e)}')
            await edit_message_safely(client, status_msg,
                f"""⚠️ Error terminating Telegram session: {str(e)}
Still removing from database...""")
        finally:
            try:
                await temp_client.disconnect()
            except Exception as e:
                logger.error(f"Error disconnecting temp_logout_client for user {user_id}: {e}")

        await remove_user_session(user_id)
        await edit_message_safely(client, status_msg,
            '✅ Logged out successfully!!')
        logger.info(f"User {user_id} logged out successfully from database.")

        # Clean up session file if it exists
        try:
            if os.path.exists(f"{user_id}_client.session"):
                os.remove(f"{user_id}_client.session")
                logger.info(f"Removed local session file for user {user_id}.")
        except Exception as e:
            logger.error(f"Error removing local session file for user {user_id}: {e}")

        # Clean up client in UC if it's there
        if UC.get(user_id, None):
            del UC[user_id]
            logger.info(f"Removed UC client for user {user_id}.")

    except Exception as e:
        logger.error(f'Critical error in logout command for user {user_id}: {str(e)}', exc_info=True)
        try:
            await remove_user_session(user_id) # Attempt to remove from DB even if other errors occur
        except Exception as db_e:
            logger.error(f"Error removing user session from DB during logout error for {user_id}: {db_e}")

        if UC.get(user_id, None):
            del UC[user_id]

        status_msg_final = await edit_message_safely(client, status_msg,
            f'❌ An error occurred during logout: {str(e)}')
        # No need to update login_cache as this is logout, not login.

        try:
            if os.path.exists(f"{user_id}_client.session"):
                os.remove(f"{user_id}_client.session")
        except Exception:
            pass

