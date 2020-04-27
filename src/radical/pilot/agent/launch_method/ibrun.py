
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

import radical.utils as ru

from .base import LaunchMethod


# ------------------------------------------------------------------------------
#
class IBRun(LaunchMethod):

    node_list = None

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, cfg, session):

        LaunchMethod.__init__(self, name, cfg, session)

        self._node_list = self._cfg.rm_info.node_list


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        # ibrun: wrapper for mpirun at TACC
        self.launch_command = ru.which('ibrun')


    # --------------------------------------------------------------------------
    #
    def construct_command(self, cu, launch_script_hop):

        slots        = cu['slots']
        cud          = cu['description']

        task_exec    = cud['executable']
        task_args    = cud.get('arguments') or []
        task_argstr  = self._create_arg_string(task_args)
        task_env     = cud.get('environment') or dict()

        n_tasks      = cud['cpu_processes']

        # Usage of env variable TACC_TASKS_PER_NODE is purely for MPI tasks,
        #  and threads are not considered (info provided by TACC support)
        n_node_tasks = int(task_env.get('TACC_TASKS_PER_NODE') or
                           self._cfg.get('cores_per_node', 1))

        assert (slots.get('nodes') is not None), 'unit.slots.nodes is not set'

        ibrun_offset = 0
        offsets      = list()
        node_id      = 0

        for node in self._node_list:
            for slot_node in slots['nodes']:
                if slot_node['uid'] == node[0]:
                    for core_map in slot_node['core_map']:
                        assert core_map, 'core_map is not set'
                        # core_map contains core ids for each thread,
                        # but threads are ignored for offsets
                        offsets.append(node_id + (core_map[0] // len(core_map)))
            node_id += n_node_tasks

        if offsets:
            ibrun_offset = min(offsets)

        if task_argstr:
            task_command = "%s %s" % (task_exec, task_argstr)
        else:
            task_command = task_exec

        ibrun_command = "%s -n %s -o %d %s" % \
                        (self.launch_command, n_tasks,
                         ibrun_offset, task_command)

        return ibrun_command, None


# ------------------------------------------------------------------------------
