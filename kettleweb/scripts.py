def kettleweb(bind=None, settings_module='settings'):
    from kettle import settings
    settings.load_settings(settings_module)
    if bind is not None:
        settings.FLASK_BIND = bind

    from kettleweb.app import run_app
    run_app()
