from gevent import monkey
monkey.patch_all()

from flask import Flask
app = Flask(__name__)

import comrad
import autotriage
import physician_scheduler

@app.get('/health')
def health():
    return dict(
        comrad=comrad.pool.get_stats(),
        autotriage=autotriage.pool_autotriage.get_stats(),
    )