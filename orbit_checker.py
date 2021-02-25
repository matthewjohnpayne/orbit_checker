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
import json
import glob

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
    9   :  "Orbit Absent: As yet unclassified",
    
    10  : "Orbit Poor:   Short-Arc / Few observations",
    11  : "Orbit Poor:   Significant fraction of observations in outlying tracklet: Removal may cause inability to calculate orbit",
    19  : "Orbit Poor:   As yet unclassified",
    
    20  : "Orbit Exists: Orbit consistent with all observations (no massive outliers)",
    21  : "Orbit Exists: Orbit consistent with most observations (one or more tracklets to be dealt with)",
    29  : "Orbit Exists: As yet unclassified",
}

def check_multiple_designations( method = None , size=0 ):
    """
    Outer loop-function to allow us to check a long list of designations
    """
    
    # Setting up connection objects...
    # (i)to PP's ID-Query routines ...
    dbConnIDs  = query_ids.QueryCurrentID()
    # (ii) to MJP's Orb-Query routines ...
    dbConnOrbs = query_orbs.QueryOrbfitResults()

    # Setting up a default array for development ...
    primary_designations_array = np.array( ['2016 EN210'])#, '2009 DU157', '2015 XB53', '2015 XX229', '2011 BU37'] )
        
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
    Do a buunch of checks on a single designation
    WIP
    
    '''
    print(unpacked_provisional_designation)
    
    # Is this actually a primary unpacked_provisional_designation ?
    # - If being called from a list pulled from the identifications tables, then this step is unnecessary
    # - But I provide it for safety
    isValidPrimary = dbConnIDs.is_valid_unpacked_primary_desig(unpacked_provisional_designation)
    assert isValidPrimary

    # Update the primary_objects table to flag whether we have an orbit in the orbfit-results table
    # NB(1) : If there is *no* match, then the returned value for orbfit_results_id == False
    # NB(2) : This is not logically great, should really be checking not in comets, etc,
    #         but for now, while developing, this is not something to worry about while the comet tble is empty
    orbfit_results_boolean       = dbConnOrbs.has_orbfit_result(unpacked_provisional_designation)
    print('orbfit_results_boolean=', orbfit_results_boolean)
    if orbfit_results_boolean:
        dbConnOrbs.set_orbfit_results_flags_in_primary_objects( unpacked_provisional_designation ,
                                                                orbfit_results_boolean   )
    
    # Understand the quality of any orbfit-orbit currently in the database ...
    # - Not clear where we want to be doing this, but while developing I am doing this here ...
    quality_dict = dbConnOrbs.get_quality_json(unpacked_provisional_designation)[0]['quality_json']
    print('quality_dict', quality_dict)
    status_dict  = assess_quality_dict(quality_dict)
    print('status_dict',status_dict)
    
    # Attempt to fit the orbit using the "orbit_pipeline_wrapper"
    #result_dict = call_orbfit_via_commandline_update_wrapper(unpacked_provisional_designation)
    #print('result_dict.keys() = ',result_dict.keys() )
    
    # Evaluate the result from the orbit_pipeline_wrapper & assign a status
    
    # If possible & if necessary, attempt to fix anything bad
    # E.g. status==21, ... (one or more tracklets to be dealt with) ...
    if FIX:
        pass

    # If appropriate, ...
    # Save the updated orbit to the db
    # Save the status to the db
    
def call_orbfit_via_commandline_update_wrapper(unpacked_provisional_designation):
    """
    # Attempt to fit the orbit using the "orbit_pipeline_wrapper"
    # - Obviously I don't like doing a Gareth-like command-line call
    # - But I'll do it for now while MPan is developing/converging the codes
    """
    
    # Make a local "designation file" as per Margaret's instructions
    designation_file = os.path.join( os.path.expanduser("~") , '.temp_desig_file.txt')
    with open(designation_file,'w') as fh:
        fh.write(unpacked_provisional_designation+'\n')

    # Run the orbit fit & Capture the output
    command = f'python3 /sa/orbit_pipeline/update_wrapper.py -b {designation_file} -n -s check_obj'
    process = subprocess.Popen( command,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                shell=True
    )
    stdout, stderr = process.communicate()
    stdout = stdout.decode("utf-8").split('\n')
    
    # Delete the local file
    os.remove(designation_file)

    # Extract the name of the processing directory from the stdout
    for line in stdout:
        if "Created processing directory" in line:
            proc_dir = line.split()[-1]
    
    # Grab the results file(s) that I want & return it
    result_json = glob.glob(proc_dir + "/resultdict_*json")[0]
    with open(result_json) as rj:
        result_dict = json.load(rj)
    return result_dict
    

def assess_quality_dict(quality_dict):
    """ At present this is just saying Bad/Red(-1), SlightProblem/Amber(0), Good/Green(1) """
    
    # Things to loop through
    expected_topline_keys = ["mid_epoch","std_epoch"]
    problems_A  = ["no orbit"]
    problems_B  = ["no CAR covariance", "no COM covariance"]
    
    # Default result dict
    result_dict = {'status':1}
    
    # Go through problems: most severe first
    for k in expected_topline_keys:
        message_string = quality_dict[k]
        for problem in problems_A:
            if problem in message_string:
                    result_dict['status'] = -1
                    break
        for problem in problems_B:
            if problem in message_string:
                    result_dict['status'] = 0
                    break
    # return
    return result_dict
    
    
if __name__ == '__main__':
    check_multiple_designations(method = 'RANDOM' , size=1 )
