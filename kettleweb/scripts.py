def kettleweb(settings_module='settings', host=None, port=None):
    from kettle import settings
    settings.load_settings(settings_module)
    if host is not None:
        settings.APP_HOST = host
    if port is not None:
        settings.APP_PORT = port

    from kettleweb.app import run_app
    run_app()
