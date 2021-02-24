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

# --------- Local imports -----------
sys.path.insert(0,'/share/apps/identifications_pipeline/dbchecks/')
import query_ids
import db_query_orbits as query_orbs





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
        check_single_designation( desig , dbConn, dbConnOrbs)
    
    
    
def check_single_designation( unpacked_provisional_designation , dbConn, dbConnOrbs, FIX=False):
    '''
    
    '''

    # Is this actually a primary unpacked_provisional_designation ?
    # - If being called from a list pulled from the identifications tables, then this step is unnecessary
    # - But I provide it for safety
    print('...', unpacked_provisional_designation, dbConn.is_valid_unpacked_primary_desig(unpacked_provisional_designation) )

    # Do we want to look for a flat-file orbit ?

    # Do we already have an orbit in the db ?
    print( 'orb...', dbConnOrbs.has_orbfit_result(unpacked_provisional_designation) )

    # Attempt to fit the orbit using the "orbit_pipeline_wrapper"
    
    # Evaluate the result from the orbit_pipeline_wrapper & assign a status

    # If possible & if necessary, attempt to fix anything bad
    # E.g. status==21, ... (one or more tracklets to be dealt with) ...
    if FIX:
        pass

    # If appropriate, ...
    # Save the updated orbit to the db
    # Save the status to the db
    

if __name__ == '__main__':
    check_multiple_designations(method = 'RANDOM' , size=2 )
