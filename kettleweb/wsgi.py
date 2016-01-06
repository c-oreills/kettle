from kettle import settings
settings.load_settings("settings")
# This must be imported after the settings are loaded or the database
# connection which is created will have the incorrect path
from kettle.db import make_session
make_session()

from app import app
app.debug = settings.FLASK_DEBUG
