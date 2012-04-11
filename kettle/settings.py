import sys
import os

sys.path.append(os.getcwd())

def get_cls(cls_name):
    module, cls = cls_name.split(':')
    module = __import__(module)
    return getattr(module, cls)

def load_settings(module_name='settings'):
    try:
        user_settings_module = __import__(module_name)
    except ImportError:
        print sys.path
        print >> sys.stderr, 'Unable to find settings module "%s"' % module_name
        sys.exit(1)
    else:
        settings = sys.modules[__name__]

        for setting in dir(user_settings_module):
            if setting.startswith('_'):
                continue
            setattr(settings, setting, getattr(user_settings_module, setting))


ROLLOUT_CLS = None

ROLLOUT_FORM_CLS = None

ENGINE_STRING = 'sqlite:////tmp/kettle.sqlite'

SECRET_KEY = None

FLASK_BIND = '0.0.0.0:5000'

FLASK_DEBUG = True

LOG_DIR = '/var/log/kettle'
