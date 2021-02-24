"""
Code to query the orbfit-results tables (in vmsops)
MJP 2021-02-24 ++

It is not clear where this code should live
It will likely overlap with code already written by MPan
 - Need to tidy-up at some point
"""

# --------- Third-Party imports -----
import sys
import os
import psycopg2

class QueryOrbfitResults():

    def __init__(self, db_host='localhost', db_user ='postgres', db_name='vmsops'):
        """
        Initialize ...
        """
        try:
            self.dbConn = psycopg2.connect(host=db_host, user=db_user, database=db_name)
            self.dbCur = self.dbConn.cursor()
        except (Exception, psycopg2.Error) as error :
            error_message = "Error while connecting via QueryOrbfitResults :%r" % error
            #send_email_exit.send_mail_exit(email_data,error_message)


    def execute_query(self, query):
        """
        Execute a generic supplied query
        """
        try:
            self.dbCur.execute(query)
        except (Exception, psycopg2.Error) as error :
            error_message = "Error while querying identification tables :%r" % error
            #send_email_exit.send_mail_exit(email_data,error_message)

        # Fetch the data and return a list not tuples!
        data = [r[0] for r in self.dbCur.fetchall()]

        return data


    # --------------------------------
    # --------------------------------
    # Data queries
    # --------------------------------
    # --------------------------------

    
    def has_orbfit_result(self, unpacked_primary_desig):
        """ Is there any entry in the orbit table for this one ? """
        
        query = f"""
        SELECT to_json(t)
        FROM (
        SELECT
            id
        FROM
            orbfit_results
        WHERE
             unpacked_primary_provisional_designation = '{unpacked_primary_desig}'
        ) as t
        ;
        """

        # execute query and return data
        data = self.execute_query(query)

        if data:
            return True
        else:
            return False


    def get_quality_json(self, unpacked_primary_desig):
        """ Get quality-json for supplied desig """
        
        query = f"""
        SELECT to_json(t)
        FROM (
        SELECT
            quality_json
        FROM
            orbfit_results
        WHERE
             unpacked_primary_provisional_designation = '{unpacked_primary_desig}'
        ) as t
        ;
        """

        # execute query and return data
        return self.execute_query(query)
