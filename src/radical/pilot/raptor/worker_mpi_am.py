
import io
import os
import sys
import time
import shlex

import threading         as mt
import radical.utils     as ru

from .worker            import Worker
from ..task_description import MPI
from ..task_description import TASK_FUNCTION, TASK_EVAL
from ..task_description import TASK_EXEC, TASK_PROC, TASK_SHELL


# MPI message tags
TAG_REGISTER_REQUESTS    = 10
TAG_REGISTER_REQUESTS_OK = 11
TAG_REGISTER_RESULTS     = 20
TAG_REGISTER_RESULTS_OK  = 21

# message payload constants
MSG_OK  = 10
MSG_NOK = 20

# resource allocation flags
FREE = 0
BUSY = 1


from inspect import currentframe


def debug():
    cf = currentframe()
    print('%s' % cf.f_back.f_lineno)


# ------------------------------------------------------------------------------
#
# small helper to collect reply messages from all ranks
#
def _collect_replies(comm, ranks, tag, expected, timeout=10):
    '''
    small helper to collect reply messages from all ranks
    '''

    # wait for workers to ping back
    start = time.time()
    ok    = 0

    while ok < ranks:

        if time.time() - start > timeout:
            break

        check = comm.Iprobe(tag=tag)
        if not check:
            time.sleep(0.1)
            continue

        msg = comm.recv(tag=tag)

        if msg != expected:
            raise RuntimeError('worker rank failed: %s' % msg)
        ok += 1

    if ok != ranks:
        raise RuntimeError('could not collect from all workers')


# ------------------------------------------------------------------------------
#
class _Resources(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, log, prof, ranks, term):

        self._log   = log
        self._prof  = prof
        self._ranks = ranks
        self._term  = term

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

    @property
    def term(self): return self._term


    # --------------------------------------------------------------------------
    #
    def alloc(self, task):

        # FIXME: handle threads
        # FIXME: handle GPUs

        self._log.debug_5('alloc %s', task['uid'])

        cores = task['description'].get('cpu_processes', 1)

        if cores >= self._ranks:
            raise ValueError('insufficient resources to run task (%d >= %d'
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
                                return ranks
            else:
                self._res_evt.wait(timeout=0.1)


    # --------------------------------------------------------------------------
    #
    def dealloc(self, task):
        '''
        deallocate task ranks
        '''

        ranks = task['ranks']

        with self._res_lock:

            for rank in ranks:
                self._resources['cores'][rank] = FREE

            # signal available resources
            self._res_evt.set()

            return True


# ------------------------------------------------------------------------------
#
class _TaskPuller(mt.Thread):
    '''
    This class will pull tasks from the master, allocate suitable ranks for
    it's execution, and push the task to those ranks
    '''

    def __init__(self, pull_addr, push_addr, event, resources):

        super().__init__()

        self.daemon = True

        from mpi4py import MPI

        self._pull_addr = pull_addr
        self._push_addr = push_addr
        self._event     = event
        self._resources = resources

        self._world     = MPI.COMM_WORLD
        self._log       = self._resources.log
        self._prof      = self._resources.prof
        self._ranks     = self._resources.ranks
        self._term      = self._resources.term

        self._log.debug('init task puller')


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

            self._log.debug('start task puller')

            # connect to the master's task queue
            self._task_getter = ru.zmq.Getter('request', self._pull_addr)

            # also connect to the master's result queue to inform about errors
            self._result_pusher = ru.zmq.Putter('results', self._push_addr)

            # for each worker, create one push pipe endpoint.  The worker will
            # pull work from that pipe once work gets assigned to it
            self._pipes  = dict()
            for rank in range(self._ranks):
                pipe = ru.zmq.Pipe()
                pipe.connect_push()
                self._pipes[rank] = pipe

            # inform the ranks about their pipe endpoint
            for rank in range(self._ranks):
                data = {'pipe_requests': self._pipes[rank].url}
                self._world.send(data, dest=rank, tag=TAG_REGISTER_REQUESTS)

            self._log.debug('pipe info sent')

            _collect_replies(self._world, self._ranks,
                             tag=TAG_REGISTER_REQUESTS_OK, expected=MSG_OK)
            self._log.debug('pipe info acknowledged')

            # setup is completed - signal main thread
            self._event.set()

            # pull tasks, allocate ranks to it, send task to those ranks
            while not self._term.is_set():

                tasks = self._task_getter.get_nowait(timeout=0.1)

                if not tasks:
                    continue

                self._log.debug('tasks: %s', len(tasks))

                # TODO: sort tasks by size
                for task in tasks:

                    try:
                        task['ranks'] = self._resources.alloc(task)
                        for rank in task['ranks']:
                            self._pipes[rank].put(task)

                    except Exception as e:
                        self._log.exception('failed to place task')
                        task['error'] = str(e)
                        self._result_pusher.put(task)

        except:
            self._log.exception('task puller thread failed')


# --------------------------------------------------------------------------
#
class _ResultPusher(mt.Thread):
    '''
    This helper class will wait for result messages from ranks which completed
    the execution of a task.  It will collect results from all ranks which
    belong to that specific task and then send the results back to the master.
    '''

    def __init__(self, push_addr, event, resources):

        super().__init__()

        self.daemon = True

        from mpi4py import MPI

        self._push_addr = push_addr
        self._event     = event
        self._resources = resources

        self._world     = MPI.COMM_WORLD
        self._log       = self._resources.log
        self._prof      = self._resources.prof
        self._ranks     = self._resources.ranks
        self._term      = self._resources.term

        # collect the results from all MPI ranks before returning
        self._cache = dict()


    # --------------------------------------------------------------------------
    #
    def _check_mpi(self, task):
        '''
        collect results of MPI ranks

        Returns `True` once all ranks are collected - the task then contains the
        collected results
        '''

        cpt = task['description'].get('cpu_process_type')

        if cpt == MPI:

            uid   = task['uid']
            ranks = task['description'].get('cpu_processes', 1)

            if uid not in self._cache:
                self._cache[uid] = list()


            self._cache[uid].append(task)

            if len(self._cache[uid]) < ranks:
                return False

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
            # connect to the master's result queue
            self._result_pusher = ru.zmq.Putter('results', self._push_addr)

            # create a result pipe for the workers to report results back
            self._getter = ru.zmq.Pipe()
            self._getter.connect_pull()

            # inform the ranks about the pipe endpoint
            # FIXME: use scatter
            for rank in range(self._ranks):
                data = {'pipe_results': self._getter.url}
                self._world.send(data, dest=rank, tag=TAG_REGISTER_RESULTS)

            # wait for workers to ping back
            _collect_replies(self._world, self._ranks,
                             tag=TAG_REGISTER_RESULTS_OK, expected=MSG_OK)

            # signal success
            self._event.set()

            while not self._term.is_set():

                task = self._getter.get_nowait(timeout=0.1)

                if not task:
                    continue

                try:
                    if not self._check_mpi(task):
                        continue

                    self._resources.dealloc(task)
                    self._result_pusher.put(task)

                except Exception as e:
                    self._log.exception('failed to collect task')
                    task['error'] = str(e)
                    self._result_pusher.put(task)

        except:
            self._log.exception('result pusher thread failed')


# ------------------------------------------------------------------------------
#
class _Worker(mt.Thread):

    # --------------------------------------------------------------------------
    #
    def __init__(self, log, prof, term, base):

        self._log  = log
        self._prof = prof
        self._term = term
        self._base = base

        super().__init__()

        self.daemon = True


    # --------------------------------------------------------------------------
    #
    def run(self):

        from mpi4py import MPI

        self._world = MPI.COMM_WORLD
        self._group = self._world.Get_group()
        self._rank  = self._world.rank

        try:
            # wait for a first initialization message which will provide us
            # with the addresses of the pipe to pull tasks from and the pipe
            # to push results to.
            req_info = self._world.recv(source=0, tag=TAG_REGISTER_REQUESTS)
            res_info = self._world.recv(source=0, tag=TAG_REGISTER_RESULTS)

            self._world.send(MSG_OK, dest=0, tag=TAG_REGISTER_REQUESTS_OK)
            self._world.send(MSG_OK, dest=0, tag=TAG_REGISTER_RESULTS_OK)

            # get tasks, do them, push results back
            getter = ru.zmq.Pipe()
            getter.connect_pull(req_info['pipe_requests'])

            putter = ru.zmq.Pipe()
            putter.connect_push(res_info['pipe_results'])

            # FIXME: when does this rank stop?
            while not self._term.is_set():

                # fetch task, join communicator, run task
                task = getter.get_nowait(timeout=0.1)

                if not task:
                    continue

                self._log.debug('recv task %s: %s', task['uid'], task['ranks'])

                if self._rank not in task['ranks']:
                    raise RuntimeError('internal error: inconsistent rank info')

                comm  = None
                group = None

                try:
                    out, err, ret, val = self._dispatch(task)
                    self._log.debug('dispatch result: %s: %s', task['uid'], out)

                    task['error']        = None
                    task['stdout']       = out
                    task['stderr']       = err
                    task['exit_code']    = ret
                    task['return_value'] = val

                except Exception as e:
                    import pprint
                    self._log.exception('work failed: \n%s',
                                        pprint.pformat(task))
                    task['error']        = repr(e)
                    task['stdout']       = ''
                    task['stderr']       = str(e)
                    task['exit_code']    = -1
                    task['return_value'] = None
                    self._log.error('recv err  %s  to  0' % (task['uid']))

                finally:
                    # sub-communicator must alwaus be destroyed
                    if group: group.Free()
                    if comm : comm.Free()

                    putter.put(task)

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

        if task['description'].get('cpu_process_type') == MPI:
            return self._dispatch_mpi(task, env)

        else:
            return self._dispatch_non_mpi(task, env)


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
        assert(comm)

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

        func_name = task['description']['function']
        assert(func_name)

        # check if `func_name` is a global name
        names   = dict(list(globals().items()) + list(locals().items()))
        to_call = names.get(func_name)

        # if not, check if this is a class method of this worker implementation
        if not to_call:
            to_call = getattr(self._base, func_name, None)

        if not to_call:
            self._log.error('no %s in \n%s\n\n%s', func_name, names, dir(self._base))
            raise ValueError('callable %s not found: %s' % (to_call, task['uid']))


        args   = task['description'].get('args',   [])
        kwargs = task['description'].get('kwargs', {})

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

            val = to_call(*args, **kwargs)
            out = strout.getvalue()
            err = strerr.getvalue()
            ret = 0

        except Exception as e:
            self._log.exception('_call failed: %s' % task['uid'])
            val = None
            out = strout.getvalue()
            err = strerr.getvalue() + ('\ncall failed: %s' % e)
            ret = 1

        finally:
            # restore stdio
            sys.stdout = bak_stdout
            sys.stderr = bak_stderr

            os.environ = old_env


        return out, err, ret, val


    # --------------------------------------------------------------------------
    #
    def _dispatch_eval(self, task, env):
        '''
        We expect a single attribute: 'code', containing the Python
        code to be eval'ed
        '''

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

            val = eval(code)
            out = strout.getvalue()
            err = strerr.getvalue()
            ret = 0

        except Exception as e:
            self._log.exception('_eval failed: %s' % task['uid'])
            val = None
            out = strout.getvalue()
            err = strerr.getvalue() + ('\neval failed: %s' % e)
            ret = 1

        finally:
            # restore stdio
            sys.stdout = bak_stdout
            sys.stderr = bak_stderr

            os.environ = old_env

        return out, err, ret, val


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
            exec(src, {}, loc)
            val = loc['result']
            out = strout.getvalue()
            err = strerr.getvalue()
            ret = 0

        except Exception as e:
            self._log.exception('_exec failed: %s' % task['uid'])
            val = None
            out = strout.getvalue()
            err = strerr.getvalue() + ('\nexec failed: %s' % e)
            ret = 1

        finally:
            # restore stdio
            sys.stdout = bak_stdout
            sys.stderr = bak_stderr

            os.environ = old_env

        return out, err, ret, val


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

            exe  = task['description']['executable']
            args = task['description'].get('arguments', list())
            tenv = task['description'].get('environment', dict())

            for k, v in env.items():
                tenv[k] = v

            cmd  = '%s %s' % (exe, ' '.join([shlex.quote(arg) for arg in args]))
          # self._log.debug('proc: --%s--', args)
            proc = sp.Popen(cmd, env=tenv,  stdin=None,
                            stdout=sp.PIPE, stderr=sp.PIPE,
                            close_fds=True, shell=True)
            out, err = proc.communicate()
            ret      = proc.returncode

        except Exception as e:
            self._log.exception('proc failed: %s' % task['uid'])
            out = None
            err = 'exec failed: %s' % e
            ret = 1

        return out, err, ret, None


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
            cmd = task['description']['command']
          # self._log.debug('shell: --%s--', cmd)
            out, err, ret = ru.sh_callout(cmd, shell=True, env=env)

        except Exception as e:
            self._log.exception('_shell failed: %s' % task['uid'])
            out = None
            err = 'shell failed: %s' % e
            ret = 1

      # os.environ = old_env

        return out, err, ret, None


# ------------------------------------------------------------------------------
#
class MPIWorkerAM(Worker):
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
    to the master.

    The main thread of rank 0 will function like the other threads: wait for
    task startup info and enact them.
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg=None, session=None):

        from mpi4py import MPI

        self._world = MPI.COMM_WORLD
        self._group = self._world.Get_group()

        self._rank  = int(os.environ.get('RP_RANK',  -1))
        self._ranks = int(os.environ.get('RP_RANKS', -1))

        self._term  = mt.Event()

        if self._rank < 0:
            raise RuntimeError('MPI worker needs MPI')

        if self._ranks < 1:
            raise RuntimeError('MPI worker needs more than one rank')

        if self._rank == 0: self._manager = True
        else              : self._manager = False

        # rank 0 will register the worker with the master and connect
        # to the task and result queues
        super().__init__(cfg=cfg, session=session, register=self._manager)


    # --------------------------------------------------------------------------
    #
    def start(self):

        # all ranks run a worker thread
        # the worker should be started before the managers as the manager
        # contacts the workers with queue endpoint information
        self._work_thread = _Worker(self._log, self._prof, self._term, self)
        self._work_thread.start()

        # the manager (rank 0) will start two threads - one to pull tasks from
        # the master, one to push results back to the master
        if self._manager:

            self._log.debug('rank %s starts managers', self._rank)
            resources = _Resources(self._log,   self._prof,
                                   self._ranks, self._term)

            # rank 0 spawns manager threads
            pull_ok = mt.Event()
            push_ok = mt.Event()

            addr_pull = self._cfg.info.req_addr_get
            addr_push = self._cfg.info.res_addr_put

            self._pull_thread = _TaskPuller(addr_pull, addr_push, pull_ok, resources)
            self._push_thread = _ResultPusher(addr_push, push_ok, resources)

            self._pull_thread.start()
            self._push_thread.start()

            pull_ok.wait(timeout=5)
            push_ok.wait(timeout=5)

            if not pull_ok.is_set():
                raise RuntimeError('failed to start pull thread')

            if not push_ok.is_set():
                raise RuntimeError('failed to start push thread')


    # --------------------------------------------------------------------------
    #
    def stop(self):

        self._term.set()


    # --------------------------------------------------------------------------
    #
    def join(self):

        self._work_thread.join()

        if self._manager:
            self._pull_thread.join()
            self._push_thread.join()


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
            ret = 0

        except Exception as e:
            self._log.exception('_call failed: %s' % (data))
            val = None
            out = strout.getvalue()
            err = strerr.getvalue() + ('\ncall failed: %s' % e)
            ret = 1

        finally:
            # restore stdio
            sys.stdout = bak_stdout
            sys.stderr = bak_stderr

        res = [task, str(out), str(err), int(ret), val]

        return res


    # --------------------------------------------------------------------------
    #
    def test(self, msg):

        print('hello: %s' % msg)


    # --------------------------------------------------------------------------
    #
    def test_mpi(self, comm, msg):

        print('hello %d/%d: %s' % (comm.rank, comm.size, msg))


# ------------------------------------------------------------------------------
