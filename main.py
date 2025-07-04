import asyncio
from shared_client import start_client
import importlib
import os
import sys
from utils.func import init_db_collections # Import init_db_collections

async def load_and_run_plugins():
    # Inisialisasi database sebelum memulai klien dan plugin
    await init_db_collections() # Panggil fungsi inisialisasi database di sini
    
    await start_client()
    plugin_dir = "plugins"
    plugins = [f[:-3] for f in os.listdir(plugin_dir) if f.endswith(".py") and f != "__init__.py"]

    for plugin in plugins:
        module = importlib.import_module(f"plugins.{plugin}")
        if hasattr(module, f"run_{plugin}_plugin"):
            print(f"Running {plugin} plugin...")
            await getattr(module, f"run_{plugin}_plugin")()  

async def main():
    await load_and_run_plugins()
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
            # Anda mungkin juga ingin menambahkan `await db_manager.close()` di sini
            # jika `db_manager` di `utils.func` adalah singleton dan perlu ditutup eksplisit.
            # Namun, aiosqlite cukup baik dalam mengelola ini secara internal saat program berakhir.
            loop.close()
        except Exception:
            pass
