from collections import OrderedDict
from datetime import datetime, timedelta
from os import path

from flask import flash, Flask, request, render_template, redirect, Response, url_for

from logbook import FileHandler
from logbook.compat import redirect_logging
redirect_logging()

from kettle import settings
from kettle.db import session, make_session
from kettle.log_utils import log_filename
from kettle.rollout import ALL_SIGNALS, SIGNAL_DESCRIPTIONS

from kettleweb.middleware import ReverseProxied, RemoteUserMiddleware


app = Flask(__name__)
app.wsgi_app = ReverseProxied(RemoteUserMiddleware(app.wsgi_app))
app.secret_key = settings.SECRET_KEY
rollout_cls = settings.get_cls(settings.ROLLOUT_CLS)
rollout_form_cls = settings.get_cls(settings.ROLLOUT_FORM_CLS)

SIGNAL_LABELS = OrderedDict((sig, sig.replace('_', ' ').title()) for sig in ALL_SIGNALS)

def available_signals(rollout_id):
    result = []

    for sig, sig_label in SIGNAL_LABELS.iteritems():
        if not rollout_cls._can_signal(rollout_id, sig):
            continue

        result.append(tuple([
            url_for('rollout_signal', rollout_id=rollout_id, signal_name=sig),
            sig_label,
            SIGNAL_DESCRIPTIONS.get(sig, '')]))

    return tuple(result)

app.jinja_env.globals['available_signals'] = available_signals

@app.teardown_request
def shutdown_session(exception=None):
    session.Session.remove()

@app.route('/rollout/<rollout_id>/edit/', methods=['GET', 'POST'])
def rollout_edit(rollout_id):
    if rollout_id == 'new':
        rollout = None
    else:
        rollout = get_rollout(rollout_id)
    form = rollout_form_cls(request.form)
    if request.method == 'POST' and form.validate():
        if rollout_id == 'new':
            rollout = rollout_cls(form.data)
        else:
            rollout.config = form.data

        rollout.user = request.environ.get('REMOTE_USER')
        try:
            rollout.generate_tasks()
        except Exception, e:
            flash('Finalisation failed with the following error: %s' % e)
            if app.debug:
                raise
        rollout.save()
        if rollout_id == 'new':
            flash('Saved as Rollout %s' % (rollout.id,))
            return redirect(url_for('rollout_edit', rollout_id=rollout.id))
    elif rollout:
        form = rollout_form_cls(**dict(rollout.config))
    return render_template('rollout_edit.html', form=form, rollout=rollout)

@app.route('/rollout/<int:rollout_id>/run/')
def rollout_run(rollout_id):
    rollout = get_rollout(rollout_id)
    if rollout.generate_tasks_dt > datetime.now() - timedelta(minutes=5):
        rollout.rollout_async()
    else:
        flash('Cannot run - finalised more than 5 minutes ago. Refinalise!')
    return redirect(url_for('rollout_view', rollout_id=rollout_id))

@app.route('/rollout/<int:rollout_id>/signal/<signal_name>')
def rollout_signal(rollout_id, signal_name):
    if rollout_cls._do_signal(rollout_id, signal_name):
        status = 'succeeded'
    else:
        status = 'failed'
    flash('{label} {status} for Rollout {id}'.format(
        label=SIGNAL_LABELS[signal_name], status=status, id=rollout_id))
    return redirect(url_for('rollout_view', rollout_id=rollout_id))

@app.route('/rollout/<int:rollout_id>/hide/')
def rollout_hide(rollout_id):
    session.Session.query(rollout_cls).filter_by(id=rollout_id).update({'hidden': True})
    session.Session.commit()
    flash('Successfully hid Rollout %s' % (rollout_id,))
    return redirect(url_for('rollout_index'))

@app.route('/rollout/<rollout_id>/')
def rollout_view(rollout_id):
    if rollout_id == 'latest':
        rollout = latest_rollout_query().first()
    else:
        rollout = get_rollout(rollout_id)
    return render_template('rollout_view.html', rollout=rollout)

@app.route('/rollout/')
def rollout_index():
    rollouts = latest_rollout_query()[:10]
    return render_template('rollout_index.html', rollouts=rollouts)

@app.route('/log/<int:rollout_id>/<path:args>/')
def log_view(rollout_id, args):
    args = args.split('/')
    log = log_filename(rollout_id, *args)
    if path.exists(log):
        return Response(open(log).read(), mimetype='text/plain')
    else:
        return 'No such log file'

@app.route('/')
def index():
    latest = latest_rollout_query().first()
    return render_template('index.html', latest=latest)

def latest_rollout_query(query_obj=None):
    if query_obj is None:
        query_obj = rollout_cls
    return session.Session.query(query_obj).filter_by(hidden=False).order_by(rollout_cls.id.desc())

def get_rollout(rollout_id):
    return session.Session.query(rollout_cls).filter_by(id=rollout_id).one()

def run_app():
    make_session()
    app.debug = settings.FLASK_DEBUG
    with FileHandler(log_filename('flask')):
        app.run(host=settings.APP_HOST, port=settings.APP_PORT)


if __name__ == '__main__':
    run_app()
