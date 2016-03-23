# Kettle
## Reversible rollout system

Kettle is a library to assist with creating and running rollouts. A rollout is a set of tasks that need to be run and monitored in order to accomplish something, e.g. a code deployment.

### Primer

Kettle revolves around two concepts:

* Task - A Task represents an atomic action performed towards your outcome. It knows how to reverse itself. It is nested in a tree, so that each Task save the root Task has a parent and each Task can have many children.

* Rollout - A Rollout is the context around a set of Tasks: it takes user configuration, generates a tree of Tasks, sets the root task running and monitors it. If something goes wrong, it rolls everything back.

### Getting started

#### Configuration

Create a directory called my_kettle.

Create a file called rollout.py in my_kettle that contains the following:

```python
from kettle.rollout import Rollout
from kettle.tasks import DelayTask

class MyRollout(Rollout):
    def _generate_tasks(self):
        self.save()
        config = self.config
        root_task = DelayTask(self.id, seconds=15)
        root_task.save()
```
Create a file called settings.py
Put the following variable definitions into it:

```python
ROLLOUT_CLS = 'rollout:MyRollout'

ROLLOUT_REFRESH_TIMEOUT = 1000

ENGINE_STRING = 'sqlite:///kettle.sqlite'

SECRET_KEY = '<insert a randomly generated string for use as the Flask secret key>'

FLASK_DEBUG = True

CHECKLIST_URL = None

CHECKLIST_HEIGHT = None
```

In order to generate a secret key, using the following method in an interactive Python session is suggested:

```python
>>> import os
>>> os.urandom(24)
```

Copy and paste the result into your settings.py SECRET_KEY variable.

#### Database

Run the following from your my_kettle dir:

```python -c 'from kettle.settings import load_settings; load_settings(); from kettle import db; db.create_all()'```

This should create a file called kettle.sqlite in your my_kettle dir. The sqlite db will contain two tables, rollout and tasks.

#### Kettleweb

From your my_kettle dir, run
```kettleweb```

Point your browser at http://localhost:5000

From this web interface you can create, run and monitor rollouts.

Click on new and you will be taken to a form for your rollout. Since we didn't specify any configuration options, the form will be empty. Click Finalise.

You should now see a Rollout with a single task, Delay for 15 seconds. Click on Run.

The rollout will start and you will see a countdown as the delay ticks away.
