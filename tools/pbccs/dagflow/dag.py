import os.path
from collections import OrderedDict
import json
import logging
import subprocess
import time


LOG = logging.getLogger(__name__)


class DAG(object):

    def __init__(self, dag_id):
        self.id = dag_id
        self.tasks = OrderedDict()
        LOG.info("create DAG %r" % self.id)

    def add_task(self, *tasks):
        for task in tasks:
            assert task.id not in self.tasks, "task id %r already exists in DAG" % task.id
            self.tasks[task.id] = task

        return 1

    def add_dag(self, *dags):
        """
        add DAG object to DAG
        :param dags:
        :return:
        """
        for dag in dags:
            assert isinstance(dag, DAG)

            for task in dag.tasks.values():

                if not task.depends:
                    task.set_upstream(*self.last_task)

            self.add_task(*dag.tasks.values())

        return 1

    @property
    def last_task(self):
        depends = []
        last_task = []
        for id, task in self.tasks.items():
            depends += task.depends
        depends = set(depends)

        for id, task in self.tasks.items():
            if task.id not in depends:
                last_task.append(task)

        return last_task

    def to_json(self):

        jsn = OrderedDict()

        for id, task in self.tasks.items():

            jsn.update(task.to_json())

        fn = os.path.abspath("%s.json" % self.id)

        with open(fn, "w") as fh:
            json.dump(jsn, fh, indent=2)

        LOG.info("Write DAG %r tasks to %r" % (self.id, fn))

        return fn

    def print_task(self):

        for id, task in self.tasks.items():
            print(task.id, task.depends, task.option)
            task.run()

    @classmethod
    def from_json(cls, filename):
        assert filename.endswith(".json")

        dag = DAG(os.path.basename(filename.rstrip(".json")))

        with open(filename) as fh:
            task_dict = json.load(fh, object_pairs_hook=OrderedDict)

            for id, task in task_dict.items():
                dag.add_task(Task.from_json(task))

        return dag


class Task(object):
    """
    A Task object in DAG
    task status:
    preparing  the job depends on other jobs, but not all of these jobs are running.
    waiting    the job is waiting for submit to run due to max jobs
    running    the job is submitted
    success    the job was done and success
    failed     the job was done but failed
    """

    TASKS = []

    def __init__(self, id, script, work_dir=".", type="sge", option="", sh_path=""):

        assert type in ["sge", "local"], "type must be sge or local "

        self.id = id
        self.TASKS.append(id)
        self.work_dir = os.path.abspath(work_dir)
        self.script = script
        self.script_path = sh_path
        self.type = type
        self._option = option
        self.done = os.path.join(self.work_dir, "%s_done" % id)

        self.depends = []
        self.status = None
        self.run_id = -1
        self.start_time = 0
        self.end_time = 0

    @property
    def option(self):
        """
        the run option of the task
        :return:
        """
        option = str2dict(self._option)
        out = os.path.join(self.work_dir, "%s.STDOUT" % self.id)
        err = os.path.join(self.work_dir, "%s.STDERR" % self.id)

        if "o" not in option:
            option["o"] = out

        if "e" not in option:
            option["e"] = err

        return option

    @property
    def run_time(self):
        """
        the run time of the task
        :return:
        """
        if self.end_time and self.start_time:
            _time = self.end_time - self.start_time
            return "%s" % int(_time)
        else:
            return "0"

    """
    Functions to describe relationship between tasks
    """
    def set_downstream(self, *tasks):
        """
        set the down stream tasks
        :param tasks: task objects
        :return:
        """
        for task in tasks:
            task.depends.append(self.id)

        return 1

    def set_upstream(self, *tasks):
        """
        set the upstream tasks
        :param tasks: task objects
        :return:
        """
        for task in tasks:
            self.depends.append(task.id)

        return 1

    def write_script(self):
        """
        write script to .sh
        :return:
        """
        script = """\
#!/bin/bash
set -vex
hostname
date
cd {}
{}
touch {}
date
""".format(self.work_dir, self.script, self.done)

        mkdir(self.work_dir)

        script_path = os.path.join(self.work_dir, "%s.sh" % self.id)
        self.script_path = script_path
        with open(script_path, "w") as fh:
            fh.write(script)

        return 1

    """
    Functions to job control
    """
    def init(self):
        """
        init the job status
        :return:
        """
        if os.path.isfile(self.done):
            self.status = "success"
        elif not self.depends:
            self.status = "waiting"
        else:
            self.status = "preparing"

    def run(self):
        """
        run the job
        :return:
        """

        self.write_script()

        if self.type == "sge":

            qsub_option = dict2str(self.option)
            _id = self.id
            script_path = self.script_path
            run_cmd = "qsub -cwd {qsub_option} -N {_id} {script_path}".format(**locals())

            _id = os.popen(run_cmd).read().strip().split()[2]

            try:
                int(_id)
            except:
                LOG.error(_id)
                raise Exception(_id)

            self.run_id = _id
            self.start_time = time.time()
            self.status = "running"
            LOG.info("qsub task %r on sge, job_id: %r" % (self.id, self.run_id))

            return 0

        elif self.type == "local":
            
            run_cmd = "sh %s" % self.script_path

            child = subprocess.Popen(
                run_cmd,
                stdout=open(self.option["o"], "w"),
                stderr=open(self.option["e"], "w"),
                shell=True
            )

            self.run_id = child
            self.start_time = time.time()
            self.status = "running"
            LOG.info("running task %r on local, pid: %r" % (self.id, self.run_id.pid))

            return 0
        else:
            pass

        return 1

    def kill(self):
        """

        :return:
        """
        if self.status != "running":
            return 1

        kill_cmd = ""
        if self.type == "sge":
            kill_cmd = "qdel %s" % self.run_id
            LOG.warning("kill task %r on sge, job_id: %r" % (self.id, self.run_id))
        elif self.type == "local":
            kill_cmd = "kill -9 %s" % self.run_id.pid
            LOG.warning("kill task %r on local, pid: %r" % (self.id, self.run_id.pid))
        else:
            pass

        os.popen(kill_cmd)
        self.check_done()

        return 1

    def check_done(self):
        """
        check the status of done task
        :return: success 1 or fail 0
        """
        if os.path.isfile(self.done):
            self.status = "success"
            self.end_time = time.time()
            LOG.info("task %r finished in %s seconds" % (self.id, self.run_time))

            return 1
        else:
            self.status = "failed"
            LOG.error("task %r run but failed" % self.id)

            return 0

    """
    methods to read and write Task from json
    """

    @classmethod
    def from_json(cls, task_dict):
        """
        create task from json
        :param task_dict:
        :return:
        """
        task = Task(
            id=task_dict["id"],
            work_dir=task_dict["work_dir"],
            script=task_dict["script"],
            type=task_dict["type"],
        )

        task._option = dict2str(task_dict["option"])
        task.depends = task_dict["depends"]

        return task

    def to_json(self):
        """
        convert Task object to dict
        :return:
        """

        r = {self.id: OrderedDict(
            {
                "id": self.id,
                "work_dir": self.work_dir,
                "script": self.script,
                "type": self.type,
                "option": self.option,
                "depends": self.depends,
                "status": self.status,
                "start": self.start_time,
                "end": self.end_time
            }
        )}

        return r


def mkdir(d):
    """
    from FALCON_KIT
    :param d:
    :return:
    """
    d = os.path.abspath(d)
    if not os.path.isdir(d):
        LOG.debug('mkdir {!r}'.format(d))
        os.makedirs(d)
    else:
        LOG.debug('mkdir {!r}, {!r} exist'.format(d, d))

    return d


def ParallelTask(id, script="", work_dir="", type="sge", option="", **extra):
    parallel_num = 0

    args = {}
    my_list = []

    for key, value in extra.items():
        if isinstance(value, list):
            if parallel_num == 0:
                parallel_num = len(value)

            else:
                assert len(value) == parallel_num, "diverse list length in options"

            my_list.append(key)
        else:
            args[key] = value

    tasks = []

    id_format = "%s_{order:0>%s}" % (id, len(str(parallel_num)))

    for n in range(parallel_num):

        for i in my_list:
            args[i] = extra[i][n]

        _id = id_format.format(order=n+1)

        task = Task(
            id=_id.format(**args),
            work_dir=work_dir.format(id=_id, **args),
            script=script.format(**args),
            type=type,
            option=option.format(**args)
        )

        tasks.append(task)

    return tasks


def set_tasks_order(task1, task2):

    assert isinstance(task1, list)
    assert isinstance(task2, list)

    for task in task2:
        task.set_upstream(*task1)

    return 0


def str2dict(string):
    """
    transform string "-a b " or "--a b" to dict {"a": "b"}
    :param string:
    :return:
    """
    assert isinstance(string, str)
    r = {}

    param = ""
    value = []
    for p in string.split():

        if not p:
            continue

        if p.startswith("-"):
            if param:
                if value:
                    r[param] = " ".join(value)
                else:
                    r[param] = True
            param = p.lstrip("-")
            value = []
        else:
            value.append(p)

    if param:
        r[param] = " ".join(value)

    return r


def dict2str(params, header="-"):
    """
    transform **params to real program param
     to
    :param params: params from test* eg: {"m": "a.fasta", "n": True, "i": False}
    :param header:
    :return: real param eg: "-query a.fasta -n "

    """
    params = dict(params)
    r = []
    for param, value in params.items():

        if value is True:
            value = ""
        else:
            pass

        r.append("{header}{param} {value}".format(**locals()))

    return " ".join(r)
