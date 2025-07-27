from gevent import monkey
monkey.patch_all()

from flask import Flask
from flask_orjson import OrjsonProvider
app = Flask(__name__)
app.json = OrjsonProvider(app)
app.json.option = None

import comrad
import coolify
import physician_scheduler

@app.get('/health')
def health():
    return dict(
        comrad=comrad.pool.get_stats(),
        coolify=coolify.pool.get_stats(),
    )