#!/usr/bin/env python

import os
import sys
import time

import radical.pilot as rp
import radical.utils as ru

def test_da_scheduler_local_integration():

# if __name__ == '__main__':


    # Create a new session. No need to try/except this: if session creation
    # fails, there is not much we can do anyways...
    session = rp.Session()

    # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
    pmgr = rp.PilotManager(session=session)

    # Update localhost lfs path and size
    cfg = session.get_resource_config('local.localhost')
    new_cfg = rp.ResourceConfig('local.localhost', cfg)
    new_cfg.lfs_path_per_node = '/tmp'
    new_cfg.lfs_size_per_node = 1024 # MB
    session.add_resource_config(new_cfg)
    cfg = session.get_resource_config('local.localhost')


    # Check that the updated config is read by the session
    assert 'lfs_path_per_node' in cfg.keys()
    assert 'lfs_size_per_node' in cfg.keys()
    assert cfg['lfs_path_per_node'] == '/tmp'
    assert cfg['lfs_size_per_node'] == 1024

    # Define an [n]-core local pilot that runs for [x] minutes
    # Here we use a dict to initialize the description object
    pd_init = {'resource'      : 'local.localhost',
               'runtime'       : 15,  # pilot runtime (min)
               'cores'         : 4
              }
    pdesc = rp.ComputePilotDescription(pd_init)

    # Launch the pilot.
    pilot = pmgr.submit_pilots(pdesc)

    # Register the ComputePilot in a UnitManager object.
    umgr = rp.UnitManager(session=session)
    umgr.add_pilots(pilot)

    # Run 16 tasks that each require 1 core and 10MB of LFS
    n = 16  
    cuds = list()
    for i in range(0, n):

        # create a new CU description, and fill it.
        # Here we don't use dict initialization.
        cud = rp.ComputeUnitDescription()
        cud.executable       = '/bin/hostname'
        cud.cpu_processes    = 1
        cud.cpu_threads      = 1
        # cud.cpu_process_type = rp.MPI
        cud.lfs_per_process  = 10   # MB
        cuds.append(cud)


    # Submit the previously created ComputeUnit descriptions to the
    # PilotManager. This will trigger the selected scheduler to start
    # assigning ComputeUnits to the ComputePilots.
    cus = umgr.submit_units(cuds)

    # Wait for all units to finish
    umgr.wait_units()

    # Check that all units succeeded
    for cu in cus:
        assert cu.exit_code == 0
        assert cu.state == rp.DONE

    session.close() 


# ------------------------------------------------------------------------------

