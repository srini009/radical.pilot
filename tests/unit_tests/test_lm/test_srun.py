# pylint: disable=protected-access, unused-argument, no-value-for-parameter

import os

from unittest import mock, TestCase

from .test_common import setUp
from radical.pilot.agent.launch_method.srun import MIN_NNODES_IN_LIST
from radical.pilot.agent.launch_method.srun import MIN_VSLURM_IN_LIST
from radical.pilot.agent.launch_method.srun import Srun


# ------------------------------------------------------------------------------
#
class TestSrun(TestCase):

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Srun, '__init__', return_value=None)
    @mock.patch('radical.utils.which', return_value='/bin/srun')
    @mock.patch('radical.utils.sh_callout', return_value=['foo\nsrun 19.05.2', '', 0])
    @mock.patch('radical.utils.Logger')
    def test_init_from_scratch(self, mocked_logger, mocked_sh_callout,
                               mocked_which, mocked_init):

        lm_srun = Srun('', {}, None, None, None)
        lm_srun.name = 'SRUN'
        lm_srun._log = mocked_logger

        env    = {'test_env': 'test_value'}
        env_sh = 'env/lm_%s.sh' % lm_srun.name.lower()

        lm_info = lm_srun._init_from_scratch(env, env_sh)
        lm_srun._init_from_info(lm_info)
        self.assertEqual(lm_info, {'env'    : env,
                                   'env_sh' : env_sh,
                                   'command': mocked_which(),
                                   'version': '19.05.2',
                                   'vmajor' : 19})
        self.assertEqual(lm_srun._version, '19.05.2')


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Srun, '__init__', return_value=None)
    @mock.patch('radical.utils.which', return_value='/bin/srun')
    @mock.patch('radical.utils.sh_callout', return_value=['', 'error', 1])
    def test_init_from_scratch_fail(self, mocked_sh_callout,
                                    mocked_which, mocked_init):

        lm_srun = Srun('', {}, None, None, None)
        with self.assertRaises(RuntimeError):
            # error while getting version of the launch command
            lm_srun._init_from_scratch({}, '')


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Srun, '__init__', return_value=None)
    def test_init_from_info(self, mocked_init):

        lm_srun = Srun('', {}, None, None, None)

        lm_info = {'env'    : {'test_env': 'test_value'},
                   'env_sh' : 'env/lm_srun.sh',
                   'command': '/bin/srun',
                   'version': '19.05.2',
                   'vmajor' : 19}
        lm_srun._init_from_info(lm_info)
        self.assertEqual(lm_srun._env,     lm_info['env'])
        self.assertEqual(lm_srun._env_sh,  lm_info['env_sh'])
        self.assertEqual(lm_srun._command, lm_info['command'])
        self.assertEqual(lm_srun._version, lm_info['version'])
        self.assertEqual(lm_srun._vmajor,  lm_info['vmajor'])

        lm_info['command'] = ''
        with self.assertRaises(AssertionError):
            lm_srun._init_from_info(lm_info)


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Srun, '__init__', return_value=None)
    def test_can_launch(self, mocked_init):

        lm_srun = Srun('', {}, None, None, None)
        self.assertTrue(lm_srun.can_launch(
            task={'description': {'executable': 'script'}})[0])
        self.assertFalse(lm_srun.can_launch(
            task={'description': {'executable': None}})[0])


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Srun, '__init__', return_value=None)
    def test_get_launcher_env(self, mocked_init):

        lm_srun = Srun('', {}, None, None, None)
        lm_info = {'env'    : {'test_env': 'test_value'},
                   'env_sh' : 'env/lm_srun.sh',
                   'command': '/bin/srun',
                   'version': '19.05.2',
                   'vmajor' : 19}
        lm_srun._init_from_info(lm_info)
        lm_env = lm_srun.get_launcher_env()

        self.assertIn('. $RP_PILOT_SANDBOX/%s' % lm_info['env_sh'], lm_env)


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Srun, '__init__', return_value=None)
    def test_get_launch_rank_cmds(self, mocked_init):

        lm_srun = Srun('', {}, None, None, None)
        lm_srun._rm_info  = {}
        lm_srun._command  = 'srun'
        lm_srun._vmajor   = MIN_VSLURM_IN_LIST + 1
        lm_srun._traverse = False

        test_cases = setUp('lm', 'srun')
        for task, result in test_cases:
            if result != 'RuntimeError':

                td = task.get('description', {})
                lm_srun._rm_info.update({
                    'requested_gpus'  : td['ranks'] *
                                        td.get('gpus_per_rank', 0),
                    'threads_per_core': task.get('resource_cfg', {}).
                                        get('system_architecture', {}).
                                        get('smt', 1)})

                command = lm_srun.get_launch_cmds(task, '')
                self.assertEqual(command, result['launch_cmd'], msg=task['uid'])

                if task.get('slots'):

                    # mimic that we have n_nodes more than MIN_NNODES_IN_LIST
                    if len(task['slots']['ranks']) <= MIN_NNODES_IN_LIST:
                        rank_base = task['slots']['ranks'][0]
                        del task['slots']['ranks'][:]
                        for idx in range(MIN_NNODES_IN_LIST + 1):
                            task['slots']['ranks'].append(dict(rank_base))
                            task['slots']['ranks'][-1]['node_name'] = str(idx)

                    if len(task['slots']['ranks']) > MIN_NNODES_IN_LIST:
                        nodefile = '%(task_sandbox_path)s/%(uid)s.nodes' % task

                        # `nodefile` will be (or is already) created
                        lm_srun.get_launch_cmds(task, '')
                        self.assertTrue(os.path.isfile(nodefile))
                        os.unlink(nodefile)

                        # with min Slurm version `nodefile` will not be created
                        lm_srun._vmajor = MIN_VSLURM_IN_LIST
                        lm_srun.get_launch_cmds(task, '')
                        self.assertFalse(os.path.isfile(nodefile))
                        lm_srun._vmajor = MIN_VSLURM_IN_LIST + 1

                command = lm_srun.get_rank_exec(task, None, None)
                self.assertEqual(command, result['rank_exec'], msg=task['uid'])


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Srun, '__init__', return_value=None)
    def test_get_rank_cmd(self, mocked_init):

        lm_srun = Srun('', {}, None, None, None)

        command = lm_srun.get_rank_cmd()
        self.assertIn('$SLURM_PROCID', command)
        self.assertIn('$MPI_RANK',     command)
        self.assertIn('$PMIX_RANK',    command)


# ------------------------------------------------------------------------------


if __name__ == '__main__':

    tc = TestSrun()
    tc.test_init_from_scratch()
    tc.test_init_from_scratch_fail()
    tc.test_init_from_info()
    tc.test_can_launch()
    tc.test_get_launcher_env()
    tc.test_get_launch_rank_cmds()
    tc.test_get_rank_cmd()


# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
