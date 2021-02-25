"""
Code that is intended to cycle through the known designations and do a few orbit-related checks.
E.g.
 - Do we have an orbit
 - Do we have an orbfit orbit
 - Is the orbit 'good'
 - What 'cleaning' needs to be done
 - Flag up any of the above problems in a standardized way

Intended to be used as both
(a) a 'one-off cleaning' capacity, and
(b) a back-ground 'monitoring' script
 
MJP 2021-02-24
"""

# --------- Third-Party imports -----
import sys
import os
import numpy as np
import subprocess


# --------- Local imports -----------
sys.path.insert(0,'/share/apps/identifications_pipeline/dbchecks/')
import query_ids

import db_query_orbits as query_orbs

sys.path.insert(0,'/sa/orbit_pipeline/')




# Codes to define possible orbit/designation "status"
status_dict = {
    0   :  "Not a primary designation",
    
    1   :  "Orbit Absent: No observations exist",
    2   :  "Orbit Absent: Insufficient observations exist to form a reasonable orbit",
    9   :  "Orbit Absent: ...other...",
    
    10  : "Orbit Poor:   Short-Arc / Few observations",
    11  : "Orbit Poor:   Significant fraction of observations in outlying tracklet: Removal may cause inability to calculate orbit",
    19  : "Orbit Poor:   ...other...",
    
    20  : "Orbit Exists: Orbit consistent with all observations (no massive outliers)",
    21  : "Orbit Exists: Orbit consistent with most observations (one or more tracklets to be dealt with)",
    29  : "Orbit Exists: ...other...",
}

def check_multiple_designations( method = None , size=0 ):
    """
    """
    # Setting up connection object to PP's ID-Query routines ...
    dbConnIDs  = query_ids.QueryCurrentID()
    
    # Setting up connection object to MJP's Orb-Query routines ...
    dbConnOrbs = query_orbs.QueryOrbfitResults()

    # Setting up a default array for development ...
    primary_designations_array = np.array( ['2016 EN210', '2009 DU157', '2015 XB53', '2015 XX229', '2011 BU37'] )
        
    # Get a list of primary designations from the current_identifications table in the database
    if method in ['ALL' ,'RANDOM']:
        pass
        #primary_designations_list_of_dicts = dbConn.get_unpacked_primary_desigs_list()
        #primary_designations_array         =  np.array( [ d['unpacked_primary_provisional_designation'] for d in primary_designations_list_of_dicts ] )

    # Choose a random subset
    if method == 'RANDOM':
        primary_designations_array = np.random.choice(primary_designations_array, size=size, replace=False)
        
    # Check that there is some data to work with
    assert len(primary_designations_array) > 0 , 'You probably did not supply *n*, so it defaulted to zero'
    
    # Cycle through each of the designations and run a check on each designation
    for desig in primary_designations_array:
        check_single_designation( desig , dbConnIDs, dbConnOrbs)
    
    
    
def check_single_designation( unpacked_provisional_designation , dbConnIDs, dbConnOrbs, FIX=False):
    '''
    
    '''

    # Is this actually a primary unpacked_provisional_designation ?
    # - If being called from a list pulled from the identifications tables, then this step is unnecessary
    # - But I provide it for safety
    assert dbConnIDs.is_valid_unpacked_primary_desig(unpacked_provisional_designation)


    # Update the primary_objects table to flag whether we have an orbit in the orbfit-results table
    # NB : If there is *no* match, then the returned value for orbfit_results_id == False
    orbfit_results_id = dbConnOrbs.has_orbfit_result(unpacked_provisional_designation)
    #dbConnOrbs.set_orbfit_results_id_in_primary_objects(orbfit_results_id   )
    
    # Understand the quality of any orbfit-orbit currently in the database ...
    # - Not clear where we want to be doing this, but while developing I am doing this here ...
    #quality_dict = dbConnOrbs.get_quality_json(unpacked_provisional_designation)

    # Attempt to fit the orbit using the "orbit_pipeline_wrapper"
    # - Obviously I don't like doing a Gareth-like command-line call
    # - But I'll do it for now while MPan is developing/converging the codes
    call_orbit_fit(unpacked_provisional_designation)

    # Evaluate the result from the orbit_pipeline_wrapper & assign a status
    
    # If possible & if necessary, attempt to fix anything bad
    # E.g. status==21, ... (one or more tracklets to be dealt with) ...
    if FIX:
        pass

    # If appropriate, ...
    # Save the updated orbit to the db
    # Save the status to the db
    
def call_orbit_fit(unpacked_provisional_designation):

    # Make a local "designation file" as per Margaret's instructions
    designation_file = '.temp_desig_file.txt'
    with open(designation_file,'w') as fh:
        fh.write(unpacked_provisional_designation+'\n')
    print('designation_file=', designation_file)

    # Run the orbit fit & Capture the name of the processing directory
    command = f'python3 /sa/orbit_pipeline/update_wrapper.py -b {designation_file} -n -s check_obj'
    print('command=', command)
    #process = subprocess.Popen(["command"],        stdout = subprocess.PIPE,
    #    stderr = subprocess.STDOUT,
    #)
    #stdout, stderr = process.communicate()
    #print('stdout=', stdout )
    #print('stderr=', stderr )
    
    #os.system(command)
    
    subprocess = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    subprocess_return = subprocess.stdout.read()
    print('subprocess_return = ',subprocess_return)
    
    # Delete the local file
    #os.remove(designation_file)
    
    return designation_file

if __name__ == '__main__':
    check_multiple_designations(method = 'RANDOM' , size=2 )
