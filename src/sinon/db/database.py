#!/usr/bin/env python
# encoding: utf-8

from pymongo import MongoClient

#-----------------------------------------------------------------------------
#

class Session():

    #---------------------------------------------------------------------------
    #
    def __init__(self, db_url, db_name="sinon"):
        """ Le constructeur. Should not be called directrly, but rather
            via the static methods new() or reconnect().
        """
        self._client = MongoClient(db_url)
        self._db     = self._client[db_name]

        self._session_id = None

        self._s = None
        self._w = None
        self._p = None
        self._q = None

    #---------------------------------------------------------------------------
    #
    @staticmethod
    def new(sid, db_url, db_name="sinon"):
        """ Creates a new session (factory method).
        """
        s = Session(db_url, db_name)
        s._create(sid)
        return s

    #---------------------------------------------------------------------------
    #
    def _create(self, sid):
        """ Creates a new session (private).

            A session is a distinct collection with three sub-collections 
            in MongoDB: 

            sinon.<sid>    | Base collection. Holds some metadata. | self._s
            sinon.<sid>.w  | Collection holding all work units.    | self._w
            sinon.<sid>.p  | Collection holding all pilots.        | self._p
            sinon.<sid>.q  | Collection holding all queues.        | self._q

            All collections are created with a new session. Since MongoDB 
            uses lazy-create, they only appear in the database after the 
            first insert. That's ok. 
        """
        # make sure session doesn't exist already
        if sid in self._db.collection_names():
            raise Exception("Session ID '%s' already exists in DB." % sid)

        # remember session id
        self._session_id = sid

        self._s = self._db["%s" % sid]
        self._s.insert({'created': 'DATE'})

        self._w = self._db["%s.w" % sid]
        self._p = self._db["%s.p" % sid]
        self._q = self._db["%s.q" % sid] 

    #---------------------------------------------------------------------------
    #
    @staticmethod
    def reconnect(sid, db_url, db_name="sinon"):
        """ Reconnects to an existing session.

            Here we simply check if a sinon.<sid> collection exists.
        """
        s = Session(db_url, db_name)
        s._reconnect(sid)
        return s

    #---------------------------------------------------------------------------
    #
    def _reconnect(self, sid):
        """ Reconnects to an existing session (private).
        """
        # make sure session exists
        if sid not in self._db.collection_names():
            raise Exception("Session ID '%s' doesn't exists in DB." % sid)

        # remember session id
        self._session_id = sid

        self._s = self._db["%s" % sid]
        self._s.insert({'reconnected': 'DATE'})

        self._w = self._db["%s.w" % sid]
        self._p = self._db["%s.p" % sid]
        self._q = self._db["%s.q" % sid] 

    #---------------------------------------------------------------------------
    #
    @property
    def session_id(self):
        """ Returns the session id.
        """
        return self._session_id

    #---------------------------------------------------------------------------
    #
    def delete(self):
        """ Removes a session and all associated collections from the DB.
        """
        if self._s is None:
            raise Exception("No active session.")

        for collection in [self._s, self._w, self._p, self._q]:
            collection.drop()
            collection = None

    #---------------------------------------------------------------------------
    #
    def insert_pilots(self, pilots):
        """ Adds one or more pilots to the database.

            Input is a list of sinon pilot descriptions.

            Inserting any number of pilots costs one roundtrip. 

                (1) Inserting pilot into pilot collection
        """
        if self._s is None:
            raise Exception("No active session.")

        # Construct and insert workunit documents
        pilot_docs = []
        for p_desc in pilots:
            pilot = {
                "description"   : p_desc,
                "wu_queue"      : [],
                "info"          : {
                    "submitted" : "<DATE>",
                    "started"   : None,
                    "finished"  : None,
                    "state"     : "UNKNOWN"
                }
            } 
            pilot_docs.append(pilot)
        pilot_ids = self._p.insert(pilot_docs)
        return pilot_ids

    #---------------------------------------------------------------------------
    #
    def get_raw_pilots(self, pilot_ids=None):
        """ Returns the raw pilot documents.

            Great for debugging shit. 
        """
        pilots = []
        if pilot_ids is not None:
            cursor = self._p.find({"_id": { "$in": pilot_ids}})
        else:
            cursor = self._p.find()

        # cursor -> dict
        for obj in cursor:
            pilots.append(obj)
        return pilots

    #---------------------------------------------------------------------------
    #
    def insert_workunits(self, pilot_id, workunits):
        """ Adds one or more workunits to the database.

            A workunit must have the following format:

            {
                "description": sinon.wu_description,  # work_unit description
                "queue_id"   : <queue_id>,            # the assigned queue
            }

            Inserting any number of work units costs 
            1 * (number of different pilots) round-trips: 

                (1) Inserting work units into the work unit collection
                (2) Add work unit id's to the pilot's queue.
        """
        if self._s is None:
            raise Exception("No active session.")

        # Construct and insert workunit documents
        workunit_docs = []
        for wu in workunits:
            workunit = {
                "description"   : wu["description"],
                "assignment"    : {
                    "pilot"     : pilot_id,
                    "queue"     : wu["queue_id"]
                },
                "info"          : {
                    "submitted" : "<DATE>",
                    "started"   : None,
                    "finished"  : None,
                    "state"     : "UNKNOWN"
                }
            } 
            workunit_docs.append(workunit)
        wu_ids = self._w.insert(workunit_docs)

        # Add the ids to the pilot's queue
        self._p.update({"_id": pilot_id}, 
                       {"$pushAll": {"wu_queue" : wu_ids}})
        return wu_ids

    #---------------------------------------------------------------------------
    #
    def get_raw_workunits(self, workunit_ids=None):
        """ Returns the raw workunit documents.

            Great for debugging shit. 
        """
        workunits = []
        if workunit_ids is not None:
            cursor = self._w.find({"_id": { "$in": workunit_ids}})
        else:
            cursor = self._w.find()

        # cursor -> dict
        for obj in cursor:
            workunits.append(obj)
        return workunits

    # def get_pilots(self, pilot_ids=None):
    #     """ Get one or more pilot entries. If pilot_ids is None, all
    #         pilots are returned.

    #         The returned pilot entry dict has the following format:

    #         {
    #             "pilot_id"   : "unique string",
    #             "name"       : "descriptive name"
    #             "description : {

    #             },
    #             "info"       : {
    #                 "state:       : "STATE",
    #                 "started"     : "date",
    #                 "terminated"  : "date",
    #                 "working_dir" : "local wd"
    #             } 
    #         }
    #     """
    #     pass

    # def get_pilot_infos(self, pilot_ids=None):
    #     """ Get the 'info' dict for one or more pilot entries. If 
    #         pilot_ids is None, infos for all pilots are returned.

    #         'info' is the part of a pilot entry that can change 
    #         after it has been added to the database. For example, 
    #         info.state can change from 'running' to 'finished'. 

    #         The returned pilot info dict has the following format:

    #         {
    #             "pilot_id"    : "id of the pilot to modify",
    #             "state:       : "STATE",
    #             "started"     : "date",
    #             "terminated"  : "date",
    #             "working_dir" : "local wd"
    #         }

    #         An 'info' dict can be modified via the modify_pilot_infos method. 
    #     """
    #     pass

    # def pilots_update(self, pilot_updates):
    #     """ Updates the state of one or more pilots.

    #         A pilot_update dict has the following format:

    #         {
    #             "pilot_id"    : "ID",
    #             "state"       : "X"  
    #         }
    #     """
    #     pass

    # def pilots_command_push(self, commands):
    #     """ Sends a command to one or more pilots, i.e., pushes a 
    #         command to a pilot entry's command field. 

    #         A command has the following format:

    #         {
    #             "pilot_id"  : "id of the pilot to control",
    #             "command:   : "COMMAND"
    #         }
    #     """
    #     pass

    # def pilot_wu_queue_push(self, pilot_id, work_unit_ids):
    #     """ Adds one or more work units to a pilot queue.
    #     """
    #     pass
    #     # (1) put work_unit_ids into pilot work queue
    #     # (2) change 'queue' in work_unit document to pilot_id

    # def pilot_wu_queue_pop(self, pilot_id, count):
    #     """ Returns and removes up to 'count' work units from 
    #         a pilot queue. 
    #     """
    #     # (1) remove pilot_ids from pilot work queue
    #     pass


    # # --------------------------------------------------------------------------
    # # WorkUnits 
    # #
    # def work_units_add(self, work_units):
    #     """ Add one or more work unit entries to the database.

    #         A work_unit has the following format:

    #         {
    #             "work_unit_id"  : "unique work unit ID",
    #             "description"   : {
    #                 ...
    #             },
    #             "assignment"    : { 
    #                 "queue" : "queue id",
    #                 "pilot" : "pilot id"
    #             }
    #         }
    #     """
    #     # (1) Add work unit to work unit collection
    #     # (2) Add work unit id to pilot identified by 'pilot_id'
    #     ids = self._wu_collection.insert(work_units)
    #     return ids


    # def work_units_update(self, work_unit_updates):
    #     """ Updates the state of one or more work units.

    #         A work_unit_update dict has the following format:

    #         {
    #             "work_unit_id"    : "ID",
    #             "state"           : "X"  
    #         }
    #     """
    #     # (1) Update the work units in work unit collection
    #     pass

    # def work_units_get(self, work_unit_ids=None): 
    #     """ Returns one or more work units.

    #         The returned work units have the following format:

    #         {
    #             "work_unit_id"  : "unique work unit ID",
    #             "description"   : {
    #                 ...
    #             },
    #             "assignment"    : { 
    #                 "queue" : "queue id",
    #                 "pilot" : "pilot id"
    #             }
    #             "info"          : {
    #                 "state" : "STATE"
    #                 ...
    #             }
    #         }
    #     """
    #     wus = []
    #     for obj in self._wu_collection.find():
    #         wus.append(obj)
    #     return wus

    # # ------------------------------------------------------------
    # # ------------------------------------------------------------
    # # Queues 
    # def add_queues(self, queue_entries):
    #     """ Add one or more queue entries to the database.

    #         A queue entry has the following format:

    #         {
    #             "queue_id"  : "unique string",
    #             "name"      : "descriptive name",
    #             "scheduler" : "scheduler name"
    #             "pilots"    : ["pilot_id 1", "pilot_id 2", "..."]
    #         }

    #     """
    #     pass

    # def remove_queue(self, queue_ids):
    #     """ Remove one or more queue entries from the database.
    #     """
    #     pass

    # def get_queues(self, queue_ids=None):
    #     """ Get one or more queue entries. If pilot_ids is None, all
    #         pilots are returned. 
    #     """
    #     pass

    # def attach_pilots_to_queue(self, pilot_queue_pairs):
    #     """ Attach one or more pilots to one or more queues.

    #         A pilot_queue_pair has the following format:

    #         {
    #             "queue_id"  : "queue ID",
    #             "pilots"    : ["pilot_id 1", "pilot_id 2", "..."]
    #         }
    #     """
    #     pass

    # def detach_pilots_from_queue(self, pilot_queue_pairs):
    #     """ Detach one or more pilots from one or more queues.
    #     """
    #     pass



        
