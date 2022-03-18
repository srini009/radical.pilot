
import io
import os
import sys
import time
import shlex

import threading           as mt
import radical.utils       as ru

from .worker            import Worker

from ..pytask           import PythonTask
from ..task_description import MPI   as RP_MPI
from ..task_description import TASK_FUNCTION
from ..task_description import TASK_EXEC, TASK_PROC, TASK_SHELL, TASK_EVAL


# MPI message tags
TAG_REGISTER_REQUESTS    = 110
TAG_REGISTER_REQUESTS_OK = 111
TAG_SEND_TASK            = 120
TAG_RECV_RESULT          = 121
TAG_REGISTER_RESULTS     = 130
TAG_REGISTER_RESULTS_OK  = 131

# message payload constants
MSG_PING = 210
MSG_PONG = 211
MSG_OK   = 220
MSG_NOK  = 221

# resource allocation flags
FREE = 0
BUSY = 1


# ------------------------------------------------------------------------------
#
class _Resources(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, log, prof, ranks):

        self._log   = log
        self._prof  = prof
        self._ranks = ranks

        # FIXME: RP considers MPI tasks to be homogeneous, in that all ranks of
        #        the task have the same set of resources allocated.  That
        #        implies that all ranks in this MPI worker have the same
        #        resources allocated.  That implies that, in most cases, no rank
        #        has GPUs alloated (we place one rank per core, but the number
        #        of cores per node is in general different than the number of
        #        GPUs per node).
        #
        #        RP will need to support heterogeneous MPI tasks to allow this
        #        worker to also assign GPUs to specific ranks.
        #
        self._res_evt   = mt.Event()  # signals free resources
        self._res_lock  = mt.Lock()   # lock resource for alloc / deallock
        self._resources = {
                'cores': [0] * self._ranks
              # 'gpus' : [0] * self._n_gpus
        }

        # resources are initially all free
        self._res_evt.set()

    @property
    def log(self): return self._log

    @property
    def prof(self): return self._prof

    @property
    def ranks(self): return self._ranks


    # --------------------------------------------------------------------------
    #
    def alloc(self, task):

        # FIXME: handle threads
        # FIXME: handle GPUs

        uid = task['uid']

        self._log.debug_5('alloc %s', uid)
        self._prof.prof('schedule_try', uid=uid)

        cores = task['description'].get('cpu_processes', 1)

        if cores > self._ranks:
            raise ValueError('insufficient resources to run task (%d > %d'
                    % (cores, self._ranks))

        self._log.debug_5('alloc %s: %s', task['uid'], cores)

        while True:

            if self._res_evt.is_set():

                with self._res_lock:

                    if cores > self._resources['cores'].count(FREE):
                        self._res_evt.clear()
                        continue

                    ranks = list()
                    for rank in range(self._ranks):

                        if self._resources['cores'][rank] == FREE:

                            self._resources['cores'][rank] = BUSY
                            ranks.append(rank)

                            if len(ranks) == cores:
                                self._prof.prof('schedule_ok', uid=uid)
                                return ranks
            else:
                self._res_evt.wait(timeout=0.1)


    # --------------------------------------------------------------------------
    #
    def dealloc(self, task):
        '''
        deallocate task ranks
        '''

        uid   = task['uid']
        ranks = task['ranks']
        self._prof.prof('unschedule_start', uid=uid)

        with self._res_lock:

            for rank in ranks:
                self._resources['cores'][rank] = FREE

            # signal available resources
            self._res_evt.set()

            self._prof.prof('unschedule_stop', uid=uid)
            return True


# ------------------------------------------------------------------------------
#
class _TaskPuller(mt.Thread):
    '''
    This class will pull tasks from the master, allocate suitable ranks for
    it's execution, and push the task to those ranks
    '''

    def __init__(self, worker_task_q_get, worker_result_q_put,
                       rank_task_q_put, event, resources, log, prof):

        super().__init__()

        self.daemon               = True
        self._worker_task_q_get   = worker_task_q_get
        self._worker_result_q_put = worker_result_q_put
        self._rank_task_q_put     = rank_task_q_put
        self._event               = event
        self._resources           = resources
        self._log                 = log
        self._prof                = prof
        self._ranks               = self._resources.ranks


    # --------------------------------------------------------------------------
    #
    def run(self):
        '''
        This callback gets tasks from the master, schedules resources for the
        tasks, and pushes them out to the respective ranks for execution.  If
        a task arrives for which no resources are available, the thread will
        block until such resources do become available.
        '''
        self._log.debug('init task puller 0 wtq_get:%s wrq_put:%s rtq_put:%s',
                        self._worker_task_q_get, self._worker_result_q_put,
                        self._rank_task_q_put)

        try:
            # register callback to receive tasks
            # connect to the master's task queue
            worker_task_q = ru.zmq.Getter('raptor_tasks',
                                          url=self._worker_task_q_get,
                                          log=self._log, prof=self._prof)

            # send tasks to worker ranks
            rank_task_q = ru.zmq.Putter('rank_tasks', url=self._rank_task_q_put,
                                        log=self._log, prof=self._prof)

            # also connect to the master's result queue to inform about errors
            worker_result_q = ru.zmq.Putter('raptor_results',
                                            url=self._worker_result_q_put,
                                            log=self._log, prof=self._prof)

            time.sleep(1)
            # setup is completed - signal main thread
            self._event.set()

            while True:

                tasks = None
                try:
                    tasks = worker_task_q.get_nowait(timeout=1)
                except:
                    self._log.exception('pull error')

                if not tasks:
                    continue

                tasks = ru.as_list(tasks)
                self._log.debug('wtq tasks: %s', len(tasks))

                # TODO: sort tasks by size
                for task in ru.as_list(tasks):

                    self._log.debug('wtq %s 0 - task pulled', task['uid'])

                    try:
                        task['ranks'] = self._resources.alloc(task)
                        for rank in task['ranks']:
                            self._log.debug('wtq %s 1 - task send to %d',
                                             task['uid'], rank)
                            rank_task_q.put(task, qname=str(rank))

                    except Exception as e:
                        self._log.exception('failed to place task')
                        task['error'] = str(e)
                        worker_result_q.put(task)

        except:
            self._log.exception('task puller cb failed')


# --------------------------------------------------------------------------
#
class _ResultPusher(mt.Thread):
    '''
    This helper class will wait for result messages from ranks which completed
    the execution of a task.  It will collect results from all ranks which
    belong to that specific task and then send the results back to the master.
    '''

    def __init__(self, worker_result_q_put, rank_result_q_get, event,
                       resources, log, prof):

        super().__init__()

        self.daemon               = True
        self._worker_result_q_put = worker_result_q_put
        self._rank_result_q_get   = rank_result_q_get
        self._event               = event
        self._resources           = resources
        self._log                 = log
        self._prof                = prof


    # --------------------------------------------------------------------------
    #
    def _check_mpi(self, task):
        '''
        collect results of MPI ranks

        Returns `True` once all ranks are collected - the task then contains the
        collected results
        '''

        uid   = task['uid']
        ranks = task['description'].get('cpu_processes', 1)

        if uid not in self._cache:
            self._cache[uid] = list()

        self._cache[uid].append(task)

        cpt = task['description'].get('cpu_process_type')
        if cpt == RP_MPI:
            if len(self._cache[uid]) < ranks:
                self._log.info('< results - recv: %s [%d < %d]', task['uid'],
                        len(self._cache[uid]), ranks)

                return False

        self._log.info('< results - recv: %s [%d = %d]', task['uid'],
                        len(self._cache[uid]), ranks)

        task['stdout']       = [t['stdout']       for t in self._cache[uid]]
        task['stderr']       = [t['stderr']       for t in self._cache[uid]]
        task['return_value'] = [t['return_value'] for t in self._cache[uid]]

        exit_codes           = [t['exit_code']    for t in self._cache[uid]]
        task['exit_code']    = sorted(list(set(exit_codes)))[-1]

        return True


    # --------------------------------------------------------------------------
    #
    def run(self):
        '''
        This thread pulls tasks from the master, schedules resources for the
        tasks, and pushes them out to the respective ranks for execution.  If
        a task arrives for which no resources are available, the thread will
        block until such resources do become available.
        '''

        try:
            self._log.debug('init result pusher wrq_put:%s rrq_get:%s',
                            self._worker_result_q_put, self._rank_result_q_get)

            # collect the results from all MPI ranks before returning
            self._cache = dict()

            # collect results from worker ranks
            rank_result_q = ru.zmq.Getter(channel='rank_results',
                                          url=self._rank_result_q_get,
                                          log=self._log,
                                          prof=self._prof)

            # collect results from worker ranks
            worker_result_q = ru.zmq.Putter(channel='raptor_results',
                                            url=self._worker_result_q_put,
                                            log=self._log, prof=self._prof)

            time.sleep(1)
            # signal success
            self._event.set()

            while True:

                self._log.debug('<-  rank_result? recv')
                tasks = ru.as_list(rank_result_q.get_nowait(timeout=100))
                self._log.debug('<-  rank_result! recv [%s]', len(tasks))

                for task in tasks:

                    self._log.debug('<-  rank_result! recv [%s]', task['uid'])
                    if not self._check_mpi(task):
                        continue

                    self._resources.dealloc(task)
                    worker_result_q.put(task)

        except:
            self._log.exception('result pusher thread failed')


# ------------------------------------------------------------------------------
#
class _Worker(mt.Thread):

    # --------------------------------------------------------------------------
    #
    def __init__(self, rank_task_q_get, rank_result_q_put,
                       event, log, prof, base):

        super().__init__()

        self.daemon             = True
        self._rank_task_q_get   = rank_task_q_get
        self._rank_result_q_put = rank_result_q_put
        self._event             = event
        self._log               = log
        self._prof              = prof
        self._base              = base


    # --------------------------------------------------------------------------
    #
    def run(self):

        from mpi4py import MPI

        self._world = MPI.COMM_WORLD
        self._group = self._world.Get_group()
        self._rank  = self._world.rank
        self._ranks = self._world.size


        try:
            self._log.debug('init worker [%d] [%d] rtq_get:%s rrq_put:%s',
                            self._rank, self._ranks,
                            self._rank_task_q_get, self._rank_result_q_put)

            # get tasks from rank 0
            rank_task_q = ru.zmq.Getter('rank_tasks', url=self._rank_task_q_get,
                                        log=self._log, prof=self._prof)
            # send results back to rank 0
            rank_result_q = ru.zmq.Putter('rank_results',
                                          url=self._rank_result_q_put,
                                          log=self._log,  prof=self._prof)

            # signal success
            self._event.set()

            # get tasks, do them, push results back
            while True:

                tasks = rank_task_q.get_nowait(qname=str(self._rank), timeout=100)

                if not tasks:
                    continue

                assert(len(tasks) == 1)
                task = tasks[0]

                # FIXME: how can that be?
                if self._rank not in task['ranks']:
                    raise RuntimeError('internal error: inconsistent rank info')

                comm  = None
                group = None

                # FIXME: task_exec_start
                try:
                    out, err, ret, val, exc = self._dispatch(task)
                    self._log.debug('dispatch result: %s: %s', task['uid'], out)

                    task['error']        = None
                    task['stdout']       = out
                    task['stderr']       = err
                    task['exit_code']    = ret
                    task['return_value'] = val
                    task['exception']    = exc

                except Exception as e:
                    import pprint
                    self._log.exception('work failed: \n%s',
                                        pprint.pformat(task))
                    task['error']        = repr(e)
                    task['stdout']       = ''
                    task['stderr']       = str(e)
                    task['exit_code']    = -1
                    task['return_value'] = None
                    task['exception']    = [e.__class__.__name__, str(e)]
                    self._log.exception('recv err  %s  to  0' % (task['uid']))

                finally:
                    # sub-communicator must always be destroyed
                    if group: group.Free()
                    if comm : comm.Free()

                    # send task back to rank 0
                    # FIXME: task_exec_stop
                    rank_result_q.put(task)

        except:
            self._log.exception('work thread failed [%s]', self._rank)


    # --------------------------------------------------------------------------
    #
    def _dispatch(self, task):

        env = {'RP_TASK_ID'         : task['uid'],
               'RP_TASK_NAME'       : task.get('name'),
               'RP_TASK_SANDBOX'    : os.environ['RP_TASK_SANDBOX'],  # FIXME?
               'RP_PILOT_ID'        : os.environ['RP_PILOT_ID'],
               'RP_SESSION_ID'      : os.environ['RP_SESSION_ID'],
               'RP_RESOURCE'        : os.environ['RP_RESOURCE'],
               'RP_RESOURCE_SANDBOX': os.environ['RP_RESOURCE_SANDBOX'],
               'RP_SESSION_SANDBOX' : os.environ['RP_SESSION_SANDBOX'],
               'RP_PILOT_SANDBOX'   : os.environ['RP_PILOT_SANDBOX'],
               'RP_GTOD'            : os.environ['RP_GTOD'],
               'RP_PROF'            : os.environ['RP_PROF'],
               'RP_PROF_TGT'        : os.environ['RP_PROF_TGT']}


        uid = task['uid']
        self._prof.prof('rp_exec_start', uid=uid)
        try:
            if task['description'].get('cpu_process_type') == RP_MPI:
                return self._dispatch_mpi(task, env)
            else:
                return self._dispatch_non_mpi(task, env)

        finally:
            self._prof.prof('rp_exec_stop', uid=uid)



    # --------------------------------------------------------------------------
    #
    def _dispatch_mpi(self, task, env):

      # # we can only handle task modes where the new communicator can be passed
      # # as additional argument to a function call
      # if task['description']['mode'] not in [FUNCTION]:
      #     raise RuntimeError('only FUNCTION tasks can use mpi')

        # NOTE: we cannot pass the new MPI communicator to shell, proc, exec or
        #       eval tasks.  Nevertheless, we *can* run the requested number of
        #       ranks.

        # create new communicator with all workers assigned to this task
        group = self._group.Incl(task['ranks'])
        comm  = self._world.Create_group(group)
        if not comm:
            out = None
            err = 'MPI setup failed'
            ret = 1
            val = None
            exc = None
            return out, err, ret, val, exc

        env['RP_RANK']  = str(comm.rank)
        env['RP_RANKS'] = str(comm.size)

        task['description']['args'].insert(0, comm)

        try:
            return self._dispatch_non_mpi(task, env)

        finally:
            # remove comm from args again
            task['description']['args'].pop(0)


    # --------------------------------------------------------------------------
    #
    def _dispatch_non_mpi(self, task, env):

        # work on task
        mode = task['description']['mode']
        if   mode == TASK_FUNCTION: return self._dispatch_function(task, env)
        elif mode == TASK_EVAL    : return self._dispatch_eval(task, env)
        elif mode == TASK_EXEC    : return self._dispatch_exec(task, env)
        elif mode == TASK_PROC    : return self._dispatch_proc(task, env)
        elif mode == TASK_SHELL   : return self._dispatch_shell(task, env)
        else: raise ValueError('cannot handle task mode %s' % mode)


    # --------------------------------------------------------------------------
    #
    def _dispatch_function(self, task, env):
        '''
        We expect three attributes: 'function', containing the name of the
        member method or free function to call, `args`, an optional list of
        unnamed parameters, and `kwargs`, and optional dictionary of named
        parameters.

        NOTE: MPI function tasks will get a private communicator passed as first
              unnamed argument.
        '''

        uid  = task['uid']
        func = task['description']['function']

        to_call = ''
        args    = task['description'].get('args',   [])
        kwargs  = task['description'].get('kwargs', {})

        # check if we have a serialized object
        try:

            to_call, args, kwargs = PythonTask.get_func_attr(func)

            # Inject the communicator in the kwargs
            if task['description'].get('cpu_process_type') == RP_MPI:
                kwargs['comm'] = task['description']['args'][0]


        except Exception:
            self._log.error('failed to obtain callable from task function')

        if not to_call:
            assert(func)
            # check if `func_name` is a global name
            names   = dict(list(globals().items()) + list(locals().items()))
            to_call = names.get(func)


        # if not, check if this is a class method of this worker implementation
        if not to_call:
            to_call = getattr(self._base, func, None)


        if not to_call:
            self._log.error('no %s in \n%s\n\n%s', func, names, dir(self._base))
            raise ValueError('callable %s not found: %s' % (to_call, task))

        bak_stdout = sys.stdout
        bak_stderr = sys.stderr

        strout = None
        strerr = None

        old_env = os.environ.copy()

        for k, v in env.items():
            os.environ[k] = v

        try:
            # redirect stdio to capture them during execution
            sys.stdout = strout = io.StringIO()
            sys.stderr = strerr = io.StringIO()

            self._prof.prof('app_start', uid=uid)
            val = to_call(*args, **kwargs)
            self._prof.prof('app_stop', uid=uid)
            out = strout.getvalue()
            err = strerr.getvalue()
            exc = None
            ret = 0

        except Exception as e:
            self._log.exception('_call failed: %s' % task['uid'])
            val = None
            out = strout.getvalue()
            err = strerr.getvalue() + ('\ncall failed: %s' % e)
            exc = [e.__class__.__name__, str(e)]
            ret = 1

        finally:
            # restore stdio
            sys.stdout = bak_stdout
            sys.stderr = bak_stderr

            os.environ = old_env

        return out, err, ret, val, exc


    # --------------------------------------------------------------------------
    #
    def _dispatch_eval(self, task, env):
        '''
        We expect a single attribute: 'code', containing the Python
        code to be eval'ed
        '''

        uid  = task['uid']
        code = task['description']['code']
        assert(code)

        bak_stdout = sys.stdout
        bak_stderr = sys.stderr

        strout = None
        strerr = None

        old_env = os.environ.copy()

        for k, v in env.items():
            os.environ[k] = v

        try:
            # redirect stdio to capture them during execution
            sys.stdout = strout = io.StringIO()
            sys.stderr = strerr = io.StringIO()

            self._log.debug('eval [%s] [%s]' % (code, task['uid']))

            self._prof.prof('app_start', uid=uid)
            val = eval(code)
            self._prof.prof('app_stop', uid=uid)
            out = strout.getvalue()
            err = strerr.getvalue()
            exc = None
            ret = 0

        except Exception as e:
            self._log.exception('_eval failed: %s' % task['uid'])
            val = None
            out = strout.getvalue()
            err = strerr.getvalue() + ('\neval failed: %s' % e)
            exc = [e.__class__.__name__, str(e)]
            ret = 1

        finally:
            # restore stdio
            sys.stdout = bak_stdout
            sys.stderr = bak_stderr

            os.environ = old_env

        return out, err, ret, val, exc


    # --------------------------------------------------------------------------
    #
    def _dispatch_exec(self, task, env):
        '''
        We expect a single attribute: 'code', containing the Python code to be
        exec'ed.  The optional attribute `pre_exec` can be used for any import
        statements and the like which need to run before the executed code.
        '''

        bak_stdout = sys.stdout
        bak_stderr = sys.stderr

        strout = None
        strerr = None

        old_env = os.environ.copy()

        for k, v in env.items():
            os.environ[k] = v

        try:
            # redirect stdio to capture them during execution
            sys.stdout = strout = io.StringIO()
            sys.stderr = strerr = io.StringIO()

            uid  = task['uid']
            pre  = task['description'].get('pre_exec', [])
            code = task['description']['code']

            # create a wrapper function around the given code
            lines = code.split('\n')
            outer = 'def _my_exec():\n'
            for line in lines:
                outer += '    ' + line + '\n'

            # call that wrapper function via exec, and keep the return value
            src = '%s\n\n%s\n\nresult=_my_exec()' % ('\n'.join(pre), outer)

            # assign a local variable to capture the code's return value.
            loc = dict()
            self._prof.prof('app_start', uid=uid)
            exec(src, {}, loc)
            self._prof.prof('app_stop', uid=uid)
            val = loc['result']
            out = strout.getvalue()
            err = strerr.getvalue()
            exc = None
            ret = 0

        except Exception as e:
            self._log.exception('_exec failed: %s' % task['uid'])
            val = None
            out = strout.getvalue()
            err = strerr.getvalue() + ('\nexec failed: %s' % e)
            exc = [e.__class__.__name__, str(e)]
            ret = 1

        finally:
            # restore stdio
            sys.stdout = bak_stdout
            sys.stderr = bak_stderr

            os.environ = old_env

        return out, err, ret, val, exc


    # --------------------------------------------------------------------------
    #
    def _dispatch_proc(self, task, env):
        '''
        We expect two attributes: 'executable', containing the executabele to
        run, and `arguments` containing a list of arguments (strings) to pass as
        command line arguments.  The `environment` attribute can be used to pass
        additional env variables. We use `sp.Popen` to run the fork/exec, and to
        collect stdout, stderr and return code
        '''

        try:
            import subprocess as sp

            uid  = task['uid']
            exe  = task['description']['executable']
            args = task['description'].get('arguments', list())
            tenv = task['description'].get('environment', dict())

            for k, v in env.items():
                tenv[k] = v

            cmd  = '%s %s' % (exe, ' '.join([shlex.quote(arg) for arg in args]))
          # self._log.debug('proc: --%s--', args)
            self._prof.prof('app_start', uid=uid)
            proc = sp.Popen(cmd, env=tenv,  stdin=None,
                            stdout=sp.PIPE, stderr=sp.PIPE,
                            close_fds=True, shell=True)
            out, err = proc.communicate()
            ret      = proc.returncode
            exc      = None
            self._prof.prof('app_stop', uid=uid)

        except Exception as e:
            self._log.exception('proc failed: %s' % task['uid'])
            out = None
            err = 'exec failed: %s' % e
            exc = [e.__class__.__name__, str(e)]
            ret = 1

        return out, err, ret, None, exc


    # --------------------------------------------------------------------------
    #
    def _dispatch_shell(self, task, env):
        '''
        We expect a single attribute: 'command', containing the command
        line to be called as string.
        '''

      # old_env = os.environ.copy()
      #
      # for k, v in env.items():
      #     os.environ[k] = v

        try:
            uid = task['uid']
            cmd = task['description']['command']
          # self._log.debug('shell: --%s--', cmd)
            self._prof.prof('app_start', uid=uid)
            out, err, ret = ru.sh_callout(cmd, shell=True, env=env)
            exc = None
            self._prof.prof('app_stop', uid=uid)

        except Exception as e:
            self._log.exception('_shell failed: %s' % task['uid'])
            out = None
            err = 'shell failed: %s' % e
            exc = [e.__class__.__name__, str(e)]
            ret = 1

      # os.environ = old_env

        return out, err, ret, None, exc


# ------------------------------------------------------------------------------
#
class MPIWorker(Worker):
    '''
    This worker manages a certain number of cores and gpus.  The master will
    start this worker by placing one rank per managed core (the GPUs are used
    dynamically).

    The first rank (rank 0) will manage the worker and for that purpose spawns
    two threads.  The first will pull tasks from the master's queue, and upon
    arrival will:

      - schedule incoming tasks over the available ranks
      - sent each target rank the required task startup info

    The second thread will collect the results from the tasks and send them back
    to the master.  The communication between rank 0 and the other ranks is
    through two ZMQ queues which are created and managed by rank 0.

    The main thread of rank 0 will function like the other threads: wait for
    task startup info and enact them.
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg=None, session=None):

        self._my_term = mt.Event()

        from mpi4py import MPI

        self._world = MPI.COMM_WORLD
        self._group = self._world.Get_group()

        self._rank  = self._world.rank
        self._ranks = self._world.size

        if self._rank == 0: self._manager = True
        else              : self._manager = False

        # rank 0 will register the worker with the master and connect
        # to the task and result queues
        super().__init__(cfg=cfg, session=session, register=self._manager)


        # rank 0 starts two ZMQ queues: one to send tasks to the worker ranks
        # (rank_task_q), and one to collect results from the ranks
        # (rank_result_q)
        info = None
        if self._manager:

            self._worker_task_q_get   = self._cfg.info.req_addr_get
            self._worker_result_q_put = self._cfg.info.res_addr_put

            self._rank_task_q   = ru.zmq.Queue(channel='rank_task_q')
            self._rank_result_q = ru.zmq.Queue(channel='rank_result_q')

            self._rank_task_q.start()
            self._rank_result_q.start()

            info = {'rank_task_q_put'  : str(self._rank_task_q.addr_put),
                    'rank_task_q_get'  : str(self._rank_task_q.addr_get),
                    'rank_result_q_put': str(self._rank_result_q.addr_put),
                    'rank_result_q_get': str(self._rank_result_q.addr_get)}

            # let channels settle
            time.sleep(1)

        # broadcast the queue endpoint addresses to all worker ranks
        info = self._world.bcast(info, root=0)

        self._rank_result_q_put = info['rank_result_q_put']
        self._rank_result_q_get = info['rank_result_q_get']
        self._rank_task_q_put   = info['rank_task_q_put']
        self._rank_task_q_get   = info['rank_task_q_get']


    # --------------------------------------------------------------------------
    #
    def start(self):

        # all ranks run a worker thread
        # the worker should be started before the managers as the manager
        # contacts the workers with queue endpoint information
        self._log.info('rank %s starts [%s]', self._rank, self._manager)
        worker_ok = mt.Event()
        self._work_thread = _Worker(rank_task_q_get   = self._rank_task_q_get,
                                    rank_result_q_put = self._rank_result_q_put,
                                    event             = worker_ok,
                                    log               = self._log,
                                    prof              = self._prof,
                                    base              = self)
        self._work_thread.start()
        worker_ok.wait(timeout=60)
        self._log.info('rank %s starts worker [%s]', self._rank, self._manager)

        if not worker_ok.is_set():
            raise RuntimeError('failed to start worker thread')


        # the manager (rank 0) will start two threads - one to pull tasks from
        # the master, one to push results back to the master
        if self._manager:

            self._log.info('rank %s starts managers', self._rank)

            resources = _Resources(self._log,   self._prof, self._ranks)
            self._log.info('resources: %s', resources)

            # rank 0 spawns manager threads
            pull_ok = mt.Event()
            push_ok = mt.Event()

            self._log.debug('wrq_put:%s wtq_get:%s',
                            self._worker_result_q_put, self._worker_task_q_get)

            self._pull = _TaskPuller(
                    worker_task_q_get   = self._worker_task_q_get,
                    worker_result_q_put = self._worker_result_q_put,
                    rank_task_q_put     = self._rank_task_q.addr_put,
                    event               = pull_ok,
                    resources           = resources,
                    log                 = self._log,
                    prof                = self._prof)

            self._push = _ResultPusher(
                    worker_result_q_put = self._worker_result_q_put,
                    rank_result_q_get   = self._rank_result_q.addr_get,
                    event               = push_ok,
                    resources           = resources,
                    log                 = self._log,
                    prof                = self._prof)

            self._pull.start()
            self._push.start()

            pull_ok.wait(timeout=60)
            push_ok.wait(timeout=60)

            if not pull_ok.is_set():
                raise RuntimeError('failed to start pull thread')

            if not push_ok.is_set():
                raise RuntimeError('failed to start push thread')

        self._log.info('rank %s starts [%s] ok', self._rank, self._manager)


    # --------------------------------------------------------------------------
    #
    def stop(self):

        self._my_term.set()


    # --------------------------------------------------------------------------
    #
    def join(self):

        # FIXME
        while True:
            if self._my_term.wait(1):
                break


    # --------------------------------------------------------------------------
    #
    def _call(self, task):
        '''
        We expect data to have a three entries: 'method' or 'function',
        containing the name of the member method or the name of a free function
        to call, `args`, an optional list of unnamed parameters, and `kwargs`,
        and optional dictionary of named parameters.
        '''

        data = task['data']

        if 'method' in data:
            to_call = getattr(self, data['method'], None)

        elif 'function' in data:
            names   = dict(list(globals().items()) + list(locals().items()))
            to_call = names.get(data['function'])

        else:
            raise ValueError('no method or function specified: %s' % data)

        if not to_call:
            raise ValueError('callable not found: %s' % data)

        args   = data.get('args',   [])
        kwargs = data.get('kwargs', {})

        bak_stdout = sys.stdout
        bak_stderr = sys.stderr

        strout = None
        strerr = None

        try:
            # redirect stdio to capture them during execution
            sys.stdout = strout = io.StringIO()
            sys.stderr = strerr = io.StringIO()

            val = to_call(*args, **kwargs)
            out = strout.getvalue()
            err = strerr.getvalue()
            exc = None
            ret = 0

        except Exception as e:
            self._log.exception('_call failed: %s' % (data))
            val = None
            out = strout.getvalue()
            err = strerr.getvalue() + ('\ncall failed: %s' % e)
            exc = [e.__class__.__name__, str(e)]
            ret = 1

        finally:
            # restore stdio
            sys.stdout = bak_stdout
            sys.stderr = bak_stderr

        res = [task, out, err, ret, val, exc]

        return res


    # --------------------------------------------------------------------------
    #
    def hello(self, msg, sleep=0):

        print('hello: %s' % msg)
        time.sleep(sleep)


    # --------------------------------------------------------------------------
    #
    def hello_mpi(self, comm, msg, sleep=0):

        print('hello %d/%d: %s' % (comm.rank, comm.size, msg))
        time.sleep(sleep)


# ------------------------------------------------------------------------------