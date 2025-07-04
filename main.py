# Copyright (c) 2025 devgagan : https://github.com/devgaganin.
# Licensed under the GNU General Public License v3.0.
# See LICENSE file in the repository root for full license text.

import asyncio
from shared_client import start_client
import importlib
import os
import sys
from utils.func import init_db # This import correctly points to init_db in utils.func

async def load_and_run_plugins():
    await start_client() # Assuming this starts your bot client(s)
    plugin_dir = "plugins"
    plugins = [f[:-3] for f in os.listdir(plugin_dir) if f.endswith(".py") and f != "__init__.py"]

    for plugin in plugins:
        module = importlib.import_module(f"plugins.{plugin}")
        if hasattr(module, f"run_{plugin}_plugin"):
            print(f"Running {plugin} plugin...")
            await getattr(module, f"run_{plugin}_plugin")()

async def main():
    # This is the crucial part for the aiosqlite migration:
    # init_db() will create the SQLite database file and tables if they don't exist.
    await init_db()
    await load_and_run_plugins()
    # Keep the main loop running to prevent the script from exiting
    while True:
        await asyncio.sleep(1)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    print("Starting clients ...")
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        print("Shutting down...")
    except Exception as e:
        print(e)
        sys.exit(1)
    finally:
        try:
            loop.close()
        except Exception:
            pass