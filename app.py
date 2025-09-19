from gevent import monkey
monkey.patch_all()
import logging
from flask import Flask
from flask_orjson import OrjsonProvider
from zoneinfo import ZoneInfo

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)-8s %(message)s')
logging.getLogger("httpx").setLevel(logging.WARNING)

TZ = ZoneInfo("Pacific/Auckland")

app = Flask(__name__)
app.jinja_env.trim_blocks = True
app.jinja_env.lstrip_blocks = True
app.json = OrjsonProvider(app)
app.json.option = None

import comrad
import coolify
import physician_scheduler
import wally
import registrar_numbers

@app.get('/health')
def health():
    return dict(
        comrad=comrad.pool.get_stats(),
        coolify=coolify.pool.get_stats(),
    )