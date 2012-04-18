# Kettle
## Reversible rollout system

Kettle is a library to assist with creating and running rollouts. A rollout is a set of tasks that need to be run and monitored in order to accomplish something, e.g. a code deployment.

### Primer

Kettle revolves around two concepts:

* Task - A Task represents an atomic action performed towards your outcome. It knows how to reverse itself. It is nested in a tree, so that each Task save the root Task has a parent and each Task can have many children.

* Rollout - A Rollout is the context around a set of Tasks: it takes user configuration, generates a tree of Tasks, sets the root task running and monitors it. If something goes wrong, it rolls everything back.

### Getting started

#### Configuration

Create a file called rollout.py that contains the following:

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

ENGINE_STRING = 'sqlite:///kettle.sqlite'

SECRET_KEY = '<insert a randomly generated string for use as the Flask secret key>'

FLASK_DEBUG = True
```

#### Kettleweb

In the directory you saved your configuration files in, run
```kettleweb```

Point your browser at http://localhost:5000