#!/usr/bin/env python3

__copyright__ = 'Copyright 2013-2014, http://radical.rutgers.edu'
__license__   = 'MIT'

import os
import sys

import radical.pilot as rp
import radical.utils as ru


# ------------------------------------------------------------------------------
#
# READ the RADICAL-Pilot documentation: https://radicalpilot.readthedocs.io/
#
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    # we use a reporter class for nicer output
    report = ru.Reporter(name='radical.pilot')
    report.title('Getting Started (RP version %s)' % rp.version)

    # use the resource specified as argument, fall back to localhost
    if len(sys.argv) >= 2: resources = sys.argv[1:]
    else                 : resources = ['local.localhost']

    # Create a new session. No need to try/except this: if session creation
    # fails, there is not much we can do anyways...
    session = rp.Session()

    # all other pilot code is now tried/excepted.  If an exception is caught, we
    # can rely on the session object to exist and be valid, and we can thus tear
    # the whole RP stack down via a 'session.close()' call in the 'finally'
    # clause...
    try:

        # read the config used for resource details
        report.info('read config')
        config = ru.read_json('%s/config.json' % os.path.dirname(os.path.abspath(__file__)))
        report.ok('>>ok\n')

        report.header('submit pilots')

        # Add a Pilot Manager. Pilot managers manage one or more Pilots.
        pmgr = rp.PilotManager(session=session)

        # Register the Pilot in a TaskManager object.
        tmgr = rp.TaskManager(session=session)

        n = 1
        pdescs = list()
        for resource in resources:

            # Define an [n]-core local pilot that runs for [x] minutes
            # Here we use a dict to initialize the description object
            for i in range(n):
                pd_init = {
                      'resource'      : resource,
                      'runtime'       : 60,   # pilot runtime (min)
                      'exit_on_error' : True,
                      'project'       : config[resource].get('project', None),
                      'queue'         : config[resource].get('queue', None),
                      'access_schema' : config[resource].get('schema', None),
                      'cores'         : config[resource].get('cores', 1),
                      'gpus'          : config[resource].get('gpus', 0),
                }
                pdesc = rp.PilotDescription(pd_init)
                pdescs.append(pdesc)

        # Launch the pilot.
        pilots = pmgr.submit_pilots(pdescs)
        tmgr.add_pilots(pilots)

        report.header('submit tasks')


        # Create a workload of Tasks.
        # Each task runs '/bin/date'.
        n = 128  # number of tasks to run
        report.info('create %d task description(s)\n\t' % n)

        tds = list()
        for i in range(0, n):

            # create a new Task description, and fill it.
            # Here we don't use dict initialization.
            td = rp.TaskDescription()
            if i % 10:
                td.executable = '/bin/date'
            else:
                # trigger an error now and then
                td.executable = '/bin/data'  # does not exist
            tds.append(td)
            report.progress()

        report.ok('>>ok\n')

        # Submit the previously created Task descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning Tasks to the Pilots.
        tasks = tmgr.submit_tasks(tds)

        # Wait for all tasks to reach a final state (DONE, CANCELED or FAILED).
        report.header('gather results')
        tmgr.wait_tasks()

        report.info('\n')
        for task in tasks:
            if task.state in [rp.FAILED, rp.CANCELED]:
                report.plain('  * %s: %s, exit: %5s, err: -%35s-'
                            % (task.uid, task.state[:4],
                               task.exit_code, task.stderr))
                report.error('>>err\n')

            else:
                report.plain('  * %s: %s, exit: %5s, out: %35s'
                            % (task.uid, task.state[:4],
                               task.exit_code, task.stdout))
                report.ok('>>ok\n')


    except Exception as e:
        # Something unexpected happened in the pilot code above
        session._log.exception('oops')
        report.error('caught Exception: %s\n' % e)
        raise

    except (KeyboardInterrupt, SystemExit):
        # the callback called sys.exit(), and we can here catch the
        # corresponding KeyboardInterrupt exception for shutdown.  We also catch
        # SystemExit (which gets raised if the main threads exits for some other
        # reason).
        report.warn('exit requested\n')

    finally:
        # always clean up the session, no matter if we caught an exception or
        # not.  This will kill all remaining pilots.
        report.header('finalize')
        if session:
            session.close(cleanup=False)

    report.header()


# ------------------------------------------------------------------------------

