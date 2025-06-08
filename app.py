from flask import Flask
app = Flask(__name__)

@app.get('/health')
def health():
    return dict(status="healthy")

import comrad
import physician_scheduler