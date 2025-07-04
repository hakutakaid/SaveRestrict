# Copyright (c) 2025 devgagan : https://github.com/devgaganin.
# Licensed under the GNU General Public License v3.0.
# See LICENSE file in the repository root for full license text.

from datetime import timedelta, datetime
from shared_client import client as bot_client
from telethon import events
from utils.func import (
    get_premium_details,
    is_private_chat,
    get_display_name,
    get_user_data,
    is_premium_user,
    add_premium_user # We will use this to "transfer" by adding to new user
)
from config import OWNER_ID
import logging
import aiosqlite # Import aiosqlite for direct DB operations here if needed, or pass through utils.func

logging.basicConfig(format=
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger('teamspy')

# Define DB_FILE globally or pass it. It's better to get it from utils.func if defined there.
# For consistency, let's assume utils.func defines it.
# If not, you might need: from utils.func import DB_FILE
DB_FILE = "bot_data.db" # Or get it from utils.func if it's there

@bot_client.on(events.NewMessage(pattern='/status'))
async def status_handler(event):
    if not await is_private_chat(event):
        await event.respond("This command can only be used in private chats for security reasons.")
        return

    """Handle /status command to check user session and bot status"""
    user_id = event.sender_id
    user_data = await get_user_data(user_id)

    session_active = False
    # bot_active is not directly used in the response, but kept for completeness if needed elsewhere
    # bot_active = False

    if user_data and user_data.get("session_string"): # Check for existence and non-empty string
        session_active = True

    # Check if user has a custom bot (this logic seems separate from status command response currently)
    # if user_data and user_data.get("bot_token"):
    #     bot_active = True

    # Add premium status check
    premium_status = "❌ Not a premium member"
    premium_details = await get_premium_details(user_id) # This function should be aiosqlite-ready
    if premium_details:
        # premium_details['subscription_end'] should already be a datetime object if get_premium_details is correct
        expiry_utc = premium_details["subscription_end"]
        expiry_ist = expiry_utc + timedelta(hours=5, minutes=30)
        formatted_expiry = expiry_ist.strftime("%d-%b-%Y %I:%M:%S %p")
        premium_status = f"✅ Premium until {formatted_expiry} (IST)"

    await event.respond(
        "**Your current status:**\n\n"
        f"**Login Status:** {'✅ Active' if session_active else '❌ Inactive'}\n"
        f"**Premium:** {premium_status}"
    )

@bot_client.on(events.NewMessage(pattern='/transfer'))
async def transfer_premium_handler(event):
    if not await is_private_chat(event):
        await event.respond(
            'This command can only be used in private chats for security reasons.'
            )
        return
    user_id = event.sender_id
    sender = await event.get_sender()
    sender_name = get_display_name(sender)

    # Check if sender is premium
    if not await is_premium_user(user_id):
        await event.respond(
            "❌ You don't have a premium subscription to transfer.")
        return

    args = event.text.split()
    if len(args) != 2:
        await event.respond(
            'Usage: /transfer user_id\nExample: /transfer 123456789')
        return

    try:
        target_user_id = int(args[1])
    except ValueError:
        await event.respond(
            '❌ Invalid user ID. Please provide a valid numeric user ID.')
        return

    if target_user_id == user_id:
        await event.respond('❌ You cannot transfer premium to yourself.')
        return

    # Check if target user is already premium
    if await is_premium_user(target_user_id):
        await event.respond(
            '❌ The target user already has a premium subscription.')
        return

    try:
        premium_details = await get_premium_details(user_id) # This returns a dict with datetime objects
        if not premium_details:
            await event.respond('❌ Error retrieving your premium details.')
            return

        target_name = 'Unknown'
        try:
            target_entity = await bot_client.get_entity(target_user_id)
            target_name = get_display_name(target_entity)
        except Exception as e:
            logger.warning(f'Could not get target user name ({target_user_id}): {e}')

        # Get the expiry date of the *current* premium user
        expiry_date = premium_details['subscription_end']
        now = datetime.now()

        # --- AIOSQLITE MIGRATION FOR TRANSFER ---
        async with aiosqlite.connect(DB_FILE) as db:
            # 1. Add premium to target user
            # We need to decide how to handle 'duration_value' and 'duration_unit' for add_premium_user.
            # The current add_premium_user takes value/unit. To transfer, we'll calculate a 'duration'
            # in a common unit (e.g., days) from the expiry_date.

            # Calculate remaining duration in seconds
            remaining_seconds = (expiry_date - now).total_seconds()
            
            if remaining_seconds <= 0:
                await event.respond('❌ Your premium has already expired, cannot transfer.')
                # Optionally, you might want to remove their expired premium from DB here too
                await db.execute("DELETE FROM premium_users WHERE user_id = ?", (user_id,))
                await db.commit()
                return

            # For simplicity in add_premium_user, let's pass a large number of seconds
            # or directly set the subscription_end for the target.
            # The add_premium_user function might need a slight adjustment to accept a direct expiry date
            # or we pass a 'dummy' unit/value and then override the expiry date.
            # Let's directly update/insert for `transfer` here.

            await db.execute("""
                INSERT OR REPLACE INTO premium_users (user_id, subscription_start, subscription_end)
                VALUES (?, ?, ?)
            """, (target_user_id, now.isoformat(), expiry_date.isoformat())) # Store dates as ISO strings

            # 2. Remove premium from original user
            await db.execute("DELETE FROM premium_users WHERE user_id = ?", (user_id,))
            await db.commit() # Commit both operations

        # --- END AIOSQLITE MIGRATION FOR TRANSFER ---

        expiry_ist = expiry_date + timedelta(hours=5, minutes=30)
        formatted_expiry = expiry_ist.strftime('%d-%b-%Y %I:%M:%S %p')

        await event.respond(
            f'✅ Premium subscription successfully transferred to {target_name} ({target_user_id}). Your premium access has been removed.'
            )
        try:
            await bot_client.send_message(target_user_id,
                f'🎁 You have received a premium subscription transfer from {sender_name} ({user_id}). Your premium is valid until {formatted_expiry} (IST).'
                )
        except Exception as e:
            logger.error(f'Could not notify target user {target_user_id}: {e}')

        try:
            # Assuming OWNER_ID can be a single int or a list of ints
            owner_ids = [int(OWNER_ID)] if isinstance(OWNER_ID, (int, str)) else [int(oid) for oid in OWNER_ID]
            for owner_id in owner_ids:
                await bot_client.send_message(owner_id,
                    f'♻️ Premium Transfer: {sender_name} ({user_id}) has transferred their premium to {target_name} ({target_user_id}). Expiry: {formatted_expiry}'
                    )
        except Exception as e:
            logger.error(f'Could not notify owner(s) about premium transfer: {e}')
        return
    except Exception as e:
        logger.error(
            f'Error transferring premium from {user_id} to {target_user_id}: {e}'
            )
        await event.respond(f'❌ Error transferring premium: {str(e)}')
        return

@bot_client.on(events.NewMessage(pattern='/rem'))
async def remove_premium_handler(event):
    user_id = event.sender_id
    if not await is_private_chat(event):
        return
    # OWNER_ID can be a single int or a list of ints
    if not (isinstance(OWNER_ID, int) and user_id == OWNER_ID) and \
       not (isinstance(OWNER_ID, list) and user_id in OWNER_ID):
        return # Not an owner

    args = event.text.split()
    if len(args) != 2:
        await event.respond('Usage: /rem user_id\nExample: /rem 123456789')
        return
    try:
        target_user_id = int(args[1])
    except ValueError:
        await event.respond(
            '❌ Invalid user ID. Please provide a valid numeric user ID.')
        return

    if not await is_premium_user(target_user_id): # This function should be aiosqlite-ready
        await event.respond(
            f'❌ User {target_user_id} does not have a premium subscription.')
        return

    try:
        target_name = 'Unknown'
        try:
            target_entity = await bot_client.get_entity(target_user_id)
            target_name = get_display_name(target_entity)
        except Exception as e:
            logger.warning(f'Could not get target user name ({target_user_id}): {e}')

        # --- AIOSQLITE MIGRATION FOR REMOVE PREMIUM ---
        async with aiosqlite.connect(DB_FILE) as db:
            cursor = await db.execute("DELETE FROM premium_users WHERE user_id = ?", (target_user_id,))
            deleted_count = cursor.rowcount # Get number of rows affected
            await db.commit()
        # --- END AIOSQLITE MIGRATION FOR REMOVE PREMIUM ---

        if deleted_count > 0:
            await event.respond(
                f'✅ Premium subscription successfully removed from {target_name} ({target_user_id}).'
                )
            try:
                await bot_client.send_message(target_user_id,
                    '⚠️ Your premium subscription has been removed by the administrator.'
                    )
            except Exception as e:
                logger.error(
                    f'Could not notify user {target_user_id} about premium removal: {e}'
                    )
        else:
            await event.respond(
                f'❌ Failed to remove premium from user {target_user_id}. (User might not have had premium or already expired)')
        return
    except Exception as e:
        logger.error(f'Error removing premium from {target_user_id}: {e}')
        await event.respond(f'❌ Error removing premium: {str(e)}')
        return

