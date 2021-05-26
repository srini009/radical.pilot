import os
import shutil
import errno
import tasktest
import json

import radical.utils as ru
import radical.pilot as rp
from radical.pilot.agent.lm.ssh import SSH

try:
    import mock
except ImportError:
    from tasktest import mock

session_id = 'rp.session.test.ssh'

class TestSSHlaunchMethod(tasktest.TestCase):


    def setUp(self):

        self._session = rp.Session(uid=session_id)

        self._cu = dict()

        self._cu['description'] = {"arguments": [],
                                   "cleanup": False,
                                   "cpu_process_type": '',
                                   "cpu_processes": 1,
                                   "cpu_thread_type": "OpenMP",
                                   "cpu_threads": 1,
                                   "environment": {}, 
                                   "executable": "test_exe",
                                   "gpu_process_type": None,
                                   "gpu_processes": 0,
                                   "gpu_thread_type": None,
                                   "gpu_threads": 0,
                                   "input_staging": [],
                                   "kernel": None,
                                   "name": None,
                                   "output_staging": [],
                                   "pilot": None,
                                   "post_exec": [],
                                   "pre_exec": [],
                                   "restartable": False,
                                   "stderr": None,
                                   "stdout": None
                                  }
        self._cu['uid'] = 'task.000000'
        self._cu['slots'] = {'nodes': [{'name': 'node1',
                                        'uid': 1,
                                        'core_map': [[0]],
                                        'gpu_map': [],
                                        'lfs': {'size': 10, 'path': '/tmp'}}],
                             'cores_per_node': 16,
                             'gpus_per_node': 0,
                             'lfs_per_node': 100,
                             'lm_info': 'test'
                            }

        return

    def tearDown(self):

        self._session.close(cleanup=True)
        shutil.rmtree(os.getcwd()+'/'+session_id)

    @mock.patch.object(SSH,'__init__',return_value=None)
    @mock.patch('radical.utils.raise_on')       
    def test_ssh_construct(self,mocked_init,mocked_raise_on):
        launch_method = SSH(cfg={'Testing'}, session = self._session)
        launch_method.launch_command = 'ssh'

        task_command, ssh_cmd = launch_method.construct_command(self._cu, launch_script_hop=1)
        env_string  = ' '.join(['%s=%s' % (var, os.environ[var])
                                for var in ['LD_LIBRARY_PATH','PATH','PYTHONPATH']
                                if  var in os.environ])

        self.assertTrue(task_command == 'test_exe')
        self.assertTrue(ssh_cmd == 'ssh node1 %s 1' % env_string)


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    suite = tasktest.TestLoader().loadTestsFromTestCase(TestSSHlaunchMethod)
    tasktest.TextTestRunner(verbosity=2).run(suite)
