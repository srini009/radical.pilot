
import sys
import time
import queue

import threading       as mt
import multiprocessing as mp
import radical.utils   as ru


_TIMEOUT         =   300  # time to keep the bridge alive
_LINGER_TIMEOUT  =   250  # ms to linger after close
_HIGH_WATER_MARK =     0  # number of messages to buffer before dropping
                          # 0:  infinite


# ------------------------------------------------------------------------------
# This ZMQ bridge links clients and agents, and bridges network gaps.  As such
# it needs to run on a resource which has a public IP address that can be
# reached from both the client and the server machine.
#
# The bridge listens on a `REP` socket (`bridge_request`) for incoming client or
# agent connections, identified by a common session ID.  A client connection
# will trigger the creation of the following communication channels:
#
#   - proxy_control_pubsub_bridge
#     links client and agent control pubsubs (includes heartbeat)
#   - proxy_state_pubsub_bridge
#     forwards task state updates from agents to client
#   - proxy_task_queue
#     forwards tasks from the client to the agents and vice versa
#
#
# The protocol on the `bridge_request` channel is as follows:
#
# client_register
# ---------------
#
#    request:
#       'cmd': 'client_register'
#       'arg': 'sid': <sid>
#
#    reply:
#       'res': {'proxy_control_pubsub': {'sub': <url>, 'pub': <url>},
#               'proxy_state_pubsub'  : {'sub': <url>, 'pub': <url>},
#               'proxy_task_queue'    : {'put': <url>, 'get': <url>}}
#
#    notes:
#      - the request will fail if the session ID is known from another
#        `client_register` call
#        'err': 'sid already connected'
#      - this request should otherwise always succeed
#      - the created pubsub channels will be terminated if the control channel
#        has not seen a client heartbeat for <10 * heartbeat_interval> seconds
#        - see semantics of the 'client_unregister' request for details.
#      - the same termination semantics holds for the 'client_unregister'
#        request.
#      - any task queues which exist for that session at the time of
#        termination will also be closed, disregarding any data held in those
#        queues.
#
#
# client_lookup
# ---------------
#
#    request:
#       'cmd': 'client_lookup'
#       'arg': 'sid': <sid>
#
#    reply:
#       'res': {'proxy_control_pubsub': {'sub': <url>, 'pub': <url>},
#               'proxy_state_pubsub'  : {'sub': <url>, 'pub': <url>},
#               'proxy_task_queue'    : {'put': <url>, 'get': <url>}}
#
#    notes:
#      - the request will fail if the session ID is not registered (anymore)
#      - this request should otherwise always succeed
#      - the call returns the same information as `client_register`, but does
#        not alter the state of the client's bridge in any other way.
#      - the request does not count as a heartbeat
#
#
# client_unregister
# -----------------
#
#    request:
#       'cmd': 'client_unregister'
#       'arg': 'sid': <sid>
#
#    reply:
#       'res': 'ok'
#
#   - this method only fails when the session is not connected, with
#     'err': 'session not connected'
#   - in all other cases, the request will cause the immediate termination of
#     all ZMQ bridges (pubsubs and queues) previously created for that
#     session, disregarding of their state, and disposing all undelivered
#     messages still held in the bridges.
#
#
# client_heartbeat
# ----------------
#
#    request:
#      'cmd': 'client_heartbeat'
#      'arg': 'sid': <sid>
#
#    reply:
#      'res': {'time': <heartbeat timestamp>}
#
# notes:
#    - this request will fail if the session is either not connected or timed
#      because of an earlier heartbeat failure:
#      'err': 'session not connected'
#    - it will otherwise ensure the server that the client is still alive and
#      requires the bridge to be up.  If the server does not receive a heartbeat
#      for longer than TIMEOUT seconds, the bridge will be terminated.
#
#
# default error mode
# ------------------
#
# To any request other than the above, the ZMQ bridge will respond:
#      'err': 'invalid request'
#
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
#
class Proxy(ru.zmq.Server):

    def __init__(self):

        self._lock    = mt.Lock()
        self._clients = dict()

        ru.zmq.Server.__init__(self, url='tcp://*:10000+')

        self._monitor_thread = mt.Thread(target=self._monitor)
        self._monitor_thread.daemon = True
        self._monitor_thread.start()

        self.register_request('client_register',   self._client_register)
        self.register_request('client_lookup',     self._client_lookup)
        self.register_request('client_unregister', self._client_unregister)
        self.register_request('client_heartbeat',  self._client_heartbeat)
        self.register_request('service_stop',      self._service_stop)


    # --------------------------------------------------------------------------
    #
    def _monitor(self):

        # this is a daemon thread - it never exits until process termination
        while True:

            time.sleep(10)
            now  = time.time()

            # iterate w/o lock, and thus get a snapshot of the known sids
            sids = list(self._clients.keys())

            to_terminate = list()
            for sid in sids:

                client = self._clients.get(sid)
                if not client:
                    continue

                if now > (client['hb'] + _TIMEOUT):
                    self._log.warn('client %s timed out' % sid)
                    to_terminate.append(sid)

            if not to_terminate:
                continue

            with self._lock:

                for sid in to_terminate:

                    client = self._clients.get(sid)
                    if not client:
                        continue

                    client['term'].set()
                    client['proc'].join()
                    del(self._clients[sid])


    # --------------------------------------------------------------------------
    #
    def stop(self):

        for sid in self._clients:
            self._log.info('stop client %s' % sid)
            self._clients[sid]['term'].set()

        self._log.info('stop proxy service')
        ru.zmq.Server.stop(self)


    # --------------------------------------------------------------------------
    #
    def _client_register(self, arg):

        sid = arg['sid']

        if sid in self._clients:
            raise RuntimeError('client already registered')

        q = mp.Queue()
        term = mp.Event()
        proc = mp.Process(target=self._worker, args=(sid, q, term))
        proc.start()

        try:
            data = q.get(timeout=10)
        except queue.Empty:
            proc.terminate()
            raise RuntimeError('worker startup failed')

        self._clients[sid] = {'proc': proc,
                              'term': term,
                              'data': data,
                              'hb'  : time.time()}

        return self._clients[sid]['data']


    # --------------------------------------------------------------------------
    #
    def _worker(self, sid, q, term):

        log = ru.Logger('radical.pilot.bridge', level='debug', path=sid)

        proxy_cp = None
        proxy_sp = None
        proxy_aq = None

        try:
            proxy_cp = ru.zmq.PubSub(cfg={'channel': 'proxy_control_pubsub',
                                          'uid'    : 'proxy_control_pubsub',
                                          'type'   : 'pubsub',
                                          'log_lvl': 'debug',
                                          'path'   : sid})

            proxy_sp = ru.zmq.PubSub(cfg={'channel': 'proxy_state_pubsub',
                                          'uid'    : 'proxy_state_pubsub',
                                          'type'   : 'pubsub',
                                          'log_lvl': 'debug',
                                          'path'   : sid})

            proxy_aq = ru.zmq.Queue (cfg={'channel': 'proxy_task_queue',
                                          'uid'    : 'proxy_task_queue',
                                          'type'   : 'queue',
                                          'log_lvl': 'debug',
                                          'path'   : sid})

            proxy_cp.start()
            proxy_sp.start()
            proxy_aq.start()

            data = {'proxy_control_pubsub': {'pub': str(proxy_cp.addr_pub),
                                             'sub': str(proxy_cp.addr_sub)},
                    'proxy_state_pubsub'  : {'pub': str(proxy_sp.addr_pub),
                                             'sub': str(proxy_sp.addr_sub)},
                    'proxy_task_queue'    : {'put': str(proxy_aq.addr_put),
                                             'get': str(proxy_aq.addr_get)}}

            # inform service about endpoint details
            q.put(data)

            # we run forever until we receive a termination command
            log.info('work')
            term.wait()


        except:
            log.exception('worker failed')

        finally:

            if proxy_cp: proxy_cp.stop()
            if proxy_sp: proxy_sp.stop()
            if proxy_aq: proxy_aq.stop()

            log.info('terminated')


    # --------------------------------------------------------------------------
    #
    def _client_lookup(self, arg):

        sid = arg['sid']

        with self._lock:
            if sid not in self._clients:
                raise RuntimeError('client %s not registered' % sid)

            return self._clients[sid]['data']


    # --------------------------------------------------------------------------
    #
    def _client_unregister(self, arg):

        sid = arg['sid']

        with self._lock:

            if sid not in self._clients:
                raise RuntimeError('client %s not registered' % sid)

            self._clients[sid]['term'].set()
            self._clients[sid]['proc'].join()

            del(self._clients[sid])


    # --------------------------------------------------------------------------
    #
    def _client_heartbeat(self, arg):

        sid = arg['sid']
        now = time.time()

        with self._lock:

            if sid not in self._clients:
                raise RuntimeError('client %s not registered' % sid)

            self._clients[sid]['hb'] = now


    # --------------------------------------------------------------------------
    #
    def _service_stop(self):

        self.stop()


# ------------------------------------------------------------------------------
