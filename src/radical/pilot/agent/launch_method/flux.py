
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

<<<<<<< HEAD
import os
import time
import signal

import threading       as mt
import subprocess      as sp

=======
>>>>>>> devel
import radical.utils   as ru

from .base import LaunchMethod


# ------------------------------------------------------------------------------
#
class Flux(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, lm_cfg, rm_info, session, prof):

        LaunchMethod.__init__(self, name, lm_cfg, rm_info, session, prof)


    # --------------------------------------------------------------------------
    #
    def _terminate(self):

        if self._fh:
            self._fh.reset()


    # --------------------------------------------------------------------------
    #
<<<<<<< HEAD
    @classmethod
    def rm_shutdown_hook(cls, name, cfg, rm, lm_info, log, prof):

        log.debug('terminate flux')
        os.kill(lm_info['flux_pid'], signal.SIGKILL)


    # --------------------------------------------------------------------------
    #
    @classmethod
    def rm_config_hook(cls, name, cfg, rm, log, prof):

        prof.prof('flux_start')

        flux = ru.which('flux')
        if not flux:
            raise Exception("Couldn't find flux")

        try:
            import sys
            print(sys.path)
            import flux
        except Exception as e:
            raise Exception("Couldn't import flux") from e

        with open('flux_launcher.sh', 'w') as fout:
            fout.write('''#/bin/sh
  export PMIX_MCA_gds='^ds12,ds21'
  echo "flux env; echo -n 'hostname:'; hostname -f; echo OK; while true; do echo ok; sleep 10; done" | \\
  jsrun -a 1 -c ALL_CPUS -g ALL_GPUS -n %d --bind none --smpiargs '-disable_gpu_hooks' \\
  flux start -o,-v,-S,log-filename=flux.log
  ''' % len(rm.node_list))
#             fout.write('''#/bin/sh
# export PMIX_MCA_gds='^ds12,ds21'
# echo "flux env; echo -n 'hostname:'; hostname -f; echo OK; while true; do echo ok; sleep 1; done" | \\
# flux start -o,-v,-S,log-filename=flux.log
# ''')

        cmd  = '/bin/sh ./flux_launcher.sh'
        proc = sp.Popen(cmd, shell=True,
                        stdin=sp.PIPE, stdout=sp.PIPE, stderr=sp.STDOUT)

        log.debug('=== flux cmd %s', cmd)

        hostname = None
        flux_env = dict()
        while True:
=======
    def _init_from_scratch(self, env, env_sh):

        self._prof.prof('flux_start')
        self._fh = ru.FluxHelper()

      # self._fh.start_flux(env=env)  # FIXME

        self._log.debug('starting flux')
        self._fh.start_flux()
>>>>>>> devel

        self._details = {'flux_uri': self._fh.uri,
                         'flux_env': self._fh.env}

        self._prof.prof('flux_start_ok')

<<<<<<< HEAD
            elif line.startswith('hostname:'):
                hostname = line.split(':')[1].strip()
                log.debug('hostname = %s' % hostname)

            elif line == 'OK':
                break
=======
        lm_info = {'env'    : env,
                   'env_sh' : env_sh,
                   'details': self._details}
>>>>>>> devel

        return lm_info

<<<<<<< HEAD
        assert('FLUX_URI' in flux_env)
        assert(hostname)

        # TODO check perf implications
        flux_url = ru.Url(flux_env['FLUX_URI'])
=======
>>>>>>> devel

    # --------------------------------------------------------------------------
    #
    def _init_from_info(self, lm_info):

<<<<<<< HEAD
        flux_env['FLUX_URI'] = str(flux_url)
        prof.prof('flux_started')

=======
        self._prof.prof('flux_reconnect')

        self._env     = lm_info['env']
        self._env_sh  = lm_info['env_sh']
        self._details = lm_info['details']
>>>>>>> devel

        self._fh = ru.FluxHelper()
        self._fh.connect_flux(uri=self._details['flux_uri'])

<<<<<<< HEAD
            log.info('starting flux watcher')
=======
        self._prof.prof('flux_reconnect_ok')
>>>>>>> devel


<<<<<<< HEAD
            try:

                while True:

                    time.sleep(1)
                    _, err, ret = ru.sh_callout('flux ping -c 1 kvs')
                  # log.debug('=== flux watcher out: %s', out)

                    if ret:
                        log.error('=== flux watcher err: %s', err)
                        break

            except Exception:
                log.exception('ERROR: flux stopped?')
                # FIXME: trigger termination
                raise

            # FIXME: trigger termination
        # ----------------------------------------------------------------------
=======
    # --------------------------------------------------------------------------
    #
    @property
    def fh(self):
        return self._fh


    def can_launch(self, task):
        raise RuntimeError('method cannot be used on Flux LM')


    def get_launch_cmds(self, task, exec_path):
        raise RuntimeError('method cannot be used on Flux LM')
>>>>>>> devel


    def get_launcher_env(self):
        raise RuntimeError('method cannot be used on Flux LM')


    def get_rank_cmd(self):
        raise RuntimeError('method cannot be used on Flux LM')


    def get_rank_exec(self, task, rank_id, rank):
        raise RuntimeError('method cannot be used on Flux LM')


# ------------------------------------------------------------------------------

