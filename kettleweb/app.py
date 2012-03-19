from datetime import datetime, timedelta
from os import path

from flask import flash, Flask, request, render_template, redirect, Response, url_for

from kettle import settings
from kettle.db import session, make_session
from kettle.steps import friendly_step_html

app = Flask(__name__)
app.secret_key = settings.SECRET_KEY
rollout_cls = settings.get_cls(settings.ROLLOUT_CLS)
rollout_form_cls = settings.get_cls(settings.ROLLOUT_FORM_CLS)

app.jinja_env.globals['friendly_step'] = friendly_step_html


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
        rollout.generate_stages()
        rollout.save()
        if rollout_id == 'new':
            flash('Saved as Rollout %s' % (rollout.id,))
            return redirect(url_for('rollout_edit', rollout_id=rollout.id))
    return render_template('rollout_edit.html', form=form, rollout=rollout)

@app.route('/rollout/<int:rollout_id>/deploy/')
def rollout_deploy(rollout_id):
    rollout = get_rollout(rollout_id)
    if rollout.generate_stages_dt > datetime.now() - timedelta(minutes=5):
        rollout.rollout_async()
    else:
        flash('Cannot deploy - finalised more than 5 minutes ago. Refinalise!')
    return redirect(url_for('rollout_view', rollout_id=rollout_id))

@app.route('/rollout/<int:rollout_id>/abort/')
def rollout_abort(rollout_id):
    rollout_cls.abort(rollout_id)
    flash('Aborted Rollout %s' % (rollout_id,))
    return redirect(url_for('rollout_view', rollout_id=rollout_id))

@app.route('/rollout/<rollout_id>/hide/')
def rollout_hide(rollout_id):
    if rollout_id == 'latest':
        (rollout_id,) = latest_rollout_query(rollout_cls.id).first()
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
def log(rollout_id, args):
    args = args.split('/')
    rollout = get_rollout(rollout_id)
    log_filename = rollout.log_filename(*args)
    if path.exists(log_filename):
        return Response(open(log_filename).read(), mimetype='text/plain')
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
    app.run()

if __name__ == '__main__':
    run_app()
