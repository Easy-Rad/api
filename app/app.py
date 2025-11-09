import logging
import pandas as pd
from quart import Quart
from flask_orjson import OrjsonProvider
from zoneinfo import ZoneInfo
from .database import local_pool, comrad_pool

TZ = ZoneInfo("Pacific/Auckland")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)-8s %(message)s')
logging.getLogger("httpx").setLevel(logging.WARNING)

app = Quart(__name__)
app.jinja_env.trim_blocks = True
app.jinja_env.lstrip_blocks = True
app.json = OrjsonProvider(app)
app.json.option = None

def custom_orjson_default(obj):
    if isinstance(obj, pd.Timestamp):
        return int(obj.timestamp()*1000)
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

app.json.default = custom_orjson_default

@app.before_serving
async def create_db_pool():
    await local_pool.open()
    await comrad_pool.open()
    logging.info("Opened connection pools")

@app.after_serving
async def close_db_pool():
    await comrad_pool.close()
    await local_pool.close()
    logging.info("Closed connection pools")

@app.get('/health')
async def health():
    return dict(
        local=local_pool.get_stats(),
        comrad=comrad_pool.get_stats(),
    )

from . import api, wally, registrar_numbers
