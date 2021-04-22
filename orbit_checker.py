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
import mpc_convert as mc

sys.path.insert(0,'/share/apps/identifications_pipeline/dbchecks/')
import query_ids
import db_query_orbits_dev as query_orbs

sys.path.insert(0,'/sa/orbit_pipeline/')
import update_wrapper
import update_existing_orbits
import orbfit_to_dict as o2d

sys.path.insert(0,'/sa/orbit_utils/')
import iod_wrapper_mjp as iod
import comet_orbits_mjp as comet

import to_orbfit_db_tables_dev as to_db

import mpc_new_processing_sub_directory as newsub


# Codes to define possible orbit/designation "status"

def generate_status_code(assessment_dict):
    ''' Current status is a little primitive as not all categorization data has been fetched/implemented as yet ... '''

    status_dict = {
        ### 0-99 Numbers    : "Orbit Absent"
        '001'  :  "Orbit Absent: No known observations",
        '002'  :  "Orbit Absent: Orbfit IOD Failed: N_obs <= 3",
        '009'  :  "Orbit Absent: Orbfit IOD Failed: Reason for failure has not been established",
        '099'  :  "Orbit Absent: Reason for absence has not been established",
        ### 100-199 Numbers  : "Orbit Present but Poor"
        '100'  : "Orbit Poor:   Short-Arc / Few observations",
        '101'  : "Orbit Poor:   Significant fraction of observations in outlying tracklet: Removal may cause inability to calculate orbit",
        '199'  : "Orbit Poor:   Reason for poor orbit has not been established",
        ### 200-299 Numbers  : "Orbit Present and Good"
        '200'  : "Good Orbit Exists: Orbit consistent with all observations (no massive outliers)",
        '201'  : "Good Orbit Exists: Orbit consistent with most observations (one or more tracklets to be dealt with)",
        '299'  : "Good Orbit Exists: As yet unclassified",
    }

    #if assessment_dict['IS_IN_ORBFIT_RESULTS']:
    #    dbConnOrbs.set_orbfit_results_flags_in_primary_objects( unpacked_provisional_designation ,
    #                                                                assessment_dict['IS_IN_ORBFIT_RESULTS']   )
    
    # --- --- --- If have results of some kind --- --- ---

    # If we have STANDARD ASTEROID results ...
    if assessment_dict['IS_IN_ORBFIT_RESULTS']:
        
        if assessment_dict['HAS_GOOD_QUALITY_DICT']:
            status = '299'  # "Good Orbit Exists: As yet unclassified",

        elif assessment_dict['HAS_BAD_QUALITY_DICT'] or assessment_dict['HAS_INTERMEDIATE_QUALITY_DICT']:
            status = '199'  # "Orbit Poor:   Reason for poor orbit has not been established",

 

    # If we have COMET results ...
    elif assessment_dict['IS_IN_COMET_RESULTS']:
        status = '099'  #  "Orbit Absent: Reason for absence has not been established",

        # If we have SATELLITE results ...
    elif assessment_dict['IS_IN_SATELLITE_RESULTS']:
        status = '099'  #  "Orbit Absent: Reason for absence has not been established",

    # --- --- --- If we have NO results at all --- --- ---
    elif assessment_dict['HAS_NO_RESULTS']:
        status = '099'  #  "Orbit Absent: Reason for absence has not been established",

    else:
        sys.exit('Should not get to this secn...')
    
    assert status in status_dict
    return status
    


def check_multiple_designations( method = None , size=0 ):
    """
    Outer loop-function to allow us to check a long list of designations
     - Most of the code in here is just to create some lists of designations to check
     
    
    """
    
    # Setting up connection objects...
    # (i)to PP's ID-Query routines ...
    # (ii) to MJP's Orb-Query routines ...
    dbConnIDs  = query_ids.QueryCurrentID()
    dbConnOrbs = query_orbs.QueryOrbfitResults()



    # Get a list of primary designations from the current_identifications table in the database
    if method in ['ALL' ,'RANDOM']:

        # --------- SOME ONE-OFF DESIGNATIONS WITH VARIOUS ISSUES. USEFUL WHILE TESTING ---------
        #primary_designations_array =  np.array(['2015 XX229'])###, "2015 KX97" cauused some error ...
        # primary_designations_array =  np.array(['2008 WJ19'] )###, "2008 WJ19"  was not in the db & went to IOD & IOD worked & dict inserted !
        # primary_designations_array =  np.array(['2016 QW66'] )###, "2016 QW66"  is not in the db & will go to IOD & IOD will fail
        primary_designations_array =  np.array(['2006 WU224'])###, "2006 WU224" is already in the db: Fit selects 52/57 obs: May be useful for understanding bad-tracklets
        # primary_designations_array =  np.array(['2015 XX229'])###, "2015 XX229" has only 7 obs & no orbit ...

        
        '''
        print("\n... Searching db for all primary designations ... ")
        primary_designations_list_of_dicts = dbConnIDs.get_unpacked_primary_desigs_list()
        
        # make into an array
        # filter-out "A" at the start of the designation, as this currently causes packed_to_unpacked_desig to crash
        primary_designations_array         = np.array( [ d['unpacked_primary_provisional_designation'] for d in primary_designations_list_of_dicts if \
            "A" != d['unpacked_primary_provisional_designation'][0] and \
            d['unpacked_primary_provisional_designation'] not in ['2014 QT388','2019 FH14'] and \
            d['unpacked_primary_provisional_designation'][-3:] != " PL" ] )
        '''
        
    # Choose a random subset
    if method == 'RANDOM':
        primary_designations_array = np.random.choice(primary_designations_array, size=size, replace=False)
        
    # Select only comets (for now, while developing, using only C/) s...
    if method == 'COMET':
        #primary_designations_list_of_dicts = dbConnIDs.get_unpacked_primary_desigs_list()
        #primary_designations_array = np.array( [ _ for _ in primary_designations_array if "C/" in _ ] )
        primary_designations_array = np.random.choice( ['C/2020 K2', 'C/2006 M8', 'C/2010 X6', 'C/2006 Y16', 'C/2000 Y5'] , size=size, replace=False)

    # Check that there is some data to work with
    assert len(primary_designations_array) > 0 , 'You probably did not supply *n*, so it defaulted to zero'
    print(f'Checking N={len(primary_designations_array)} designations')
    
    # Cycle through each of the designations and run a check on each designation
    for desig in primary_designations_array:
    
        status = check_single_designation( desig , dbConnIDs, dbConnOrbs)
        
        # Write the status values to the database
        print('\t', desig, ' : status=', status)


def check_single_designation( unpacked_provisional_designation , dbConnIDs, dbConnOrbs, FIX=False):
    '''
    Do a bunch of checks on a single designation
    WIP Code:
    (i) does not yet perform all required checks
    (ii) does not permany many/any db updates
    
    '''

    # Define an assessment-dict to flag the condition of the orbit
    # (either in the db or as calculated in this routine)
    assessment_dict = {

        # Overall designation status: Check whether actually a primary unpacked_provisional_designation
        # - If being called from a list pulled from the identifications tables, then this step is unnecessary
        # - But I provide it for safety
        'IS_PRIMARY_UNPACKED_DESIGNATION'   : dbConnIDs.is_valid_unpacked_primary_desig(unpacked_provisional_designation),

        # Whether an orbit exists anywhere
        'IS_IN_ORBFIT_RESULTS'              : False,
        'IS_IN_COMET_RESULTS'               : False,
        'IS_IN_SATELLITE_RESULTS'           : False,
        'HAS_NO_RESULTS'                    : False,

        # If has orbfit results in table, what is overall summary of the "quality-json"
        'HAS_BAD_QUALITY_DICT'              : False,
        'HAS_INTERMEDIATE_QUALITY_DICT'     : False,
        'HAS_GOOD_QUALITY_DICT'             : False,

        # Were there problems running orbfit (I.E. complete failures in start-up / crashes / etc )
        # NB: Should subsequently flag-up problems with input obs/orbits/other
        'SUCCESSFUL_ORBFIT_EXECUTION'       : False,

        # Given successful execution, could an orbit of any kind be generated ( even if there are some outliers, or the fit is "weak" )
        'SUCCESSFUL_ORBIT_GENERATION'       : False,

        # Are there any obvious outlier tracklets when we run fit (bad tracklet dict)
        'HAS_BAD_TRACKLETS'                 : False,

        # Are there any obvious outlier tracklets when we run fit (bad tracklet dict)
        'HAS_WEAK_ORBIT_FIT'                : False,
        
        # IF THE IOD FAILED, TRY TO UNDERSTAND WHY ...
        'HAS_NOBS_LTE_3'                    : False,
        'HAS_NOBS_LTE_10'                   : False,
        'HAS_ARC_LTE_1_DAY'                 : False,
        'HAS_ARC_LTE_2_DAY'                 : False,
        'HAS_NOBS_C51_GTE_5'                : False,
        'HAS_FRAC_C51_GTE_0.5'              : False,

    }

    # Fucking designations
    designation_dict = {
        'unpacked_provisional_designation'  : unpacked_provisional_designation,
        'packed_provisional_designation'    : mc.unpacked_to_packed_desig(unpacked_provisional_designation),
        'orbfitname'                        : update_existing_orbits.packeddes_to_orbfitdes(mc.unpacked_to_packed_desig(unpacked_provisional_designation))
    }
    
    # (1) Assess any extant database-orbit & set flags in assessment_dict
    assess_quality_of_any_database_orbit(designation_dict, assessment_dict, dbConnOrbs)


    # (2) If no orbit at all, do orbit fit
    if True : # assessment_dict['HAS_NO_RESULTS'] :
        print('\n'*3,'HAS_NO_RESULTS', unpacked_provisional_designation)

        
        # Standard asteroid ...
        if   "/" not in unpacked_provisional_designation:
            destination = 'asteroid' ; print(destination)

            # (a) Orbfit & Dictionary conversion in one
            print("\t*"*3,"Standard Orbit Fit ...")
            result_dict = direct_call_orbfit_update_wrapper(unpacked_provisional_designation)
            
            # (b) Evaluate the result from the orbfit run & assign a status
            assess_result_dict(designation_dict , result_dict , assessment_dict , RESULT_DICT_ORIGIN = 'EXTENSION' )
        
            # (c) Save results to the database (only done if we have a useable result ... )
            if assessment_dict['SUCCESSFUL_ORBFIT_EXECUTION'] :
                # NB: Extracting the single-object part of the dictionary Margaret's code returns ...
                to_db.save_result_dict_to_db( result_dict_to_upsert[designation_dict['packed_provisional_designation']], destination, db=dbConnOrbs)
        
            # (d) if the init orbit is missing, but there are obs, then might want to try IOD of some sort ...
            if  not assessment_dict['SUCCESSFUL_ORBFIT_EXECUTION'] and \
                not assessment_dict['INPUT_GENERATION_SUCCESS'] and \
                assessment_dict['enough_obs'] and \
                not assessment_dict['existing_orbit']:
        
                # Call IOD (results returned as  dictionaries)
                assessment_dict['SUCCESSFUL_ORBFIT_EXECUTION'] , result_dict   = direct_call_IOD(designation_dict, destination)
                # Assess IOD results
                assess_result_dict(designation_dict , result_dict , assessment_dict , RESULT_DICT_ORIGIN = 'IOD' )
                # Save IOD results to db
                if assessment_dict['SUCCESSFUL_ORBFIT_EXECUTION'] :
                    to_db.save_result_dict_to_db( result_dict_to_upsert, destination, db=dbConnOrbs)
                    
            # (e) (Re)Assess result written to db
            if assessment_dict['SUCCESSFUL_ORBFIT_EXECUTION'] :
                assess_quality_of_any_database_orbit(designation_dict, assessment_dict, dbConnOrbs)

        # Comet
        elif "C/" in unpacked_provisional_designation:
            destination = 'comet'

            # Orbfit
            assessment_dict['SUCCESSFUL_ORBFIT_EXECUTION'] , result_dict  = direct_call_orbfit_comet_wrapper(designation_dict, FORCEOBS80=False )
            # Assess IOD results
            assess_result_dict(designation_dict , result_dict , assessment_dict , RESULT_DICT_ORIGIN = 'COMET' )
            # Save comet results to db
            if assessment_dict['SUCCESSFUL_ORBFIT_EXECUTION'] :
                to_db.save_result_dict_to_db( result_dict_to_upsert, destination, db=dbConnOrbs)

        # (2c) Satellite
        else:
            destination = 'satellite'

        

    # Generate status-code & return
    return generate_status_code(assessment_dict)
    


    
    
    
# ------------------ ORBIT EXTENSION -------------------------------------------

    
def direct_call_orbfit_update_wrapper(unpacked_provisional_designation):
    """
    # Attempt to fit the orbit using the "orbit_pipeline_wrapper"
    """
    arg_dict = {
        'obs80_filepath'        :       None,
        'psv_filepath'          :       None,
        'xml_filepath'          :       None,
        'object_list'           :       [unpacked_provisional_designation],
        'els_ext'               :       None,
        'primary_desig_file'    :       None,
        'usefindn'              :       True,
        'queue_name'            :       'mba/mopp',
        'elements_dir'          :       'neofitels/',
        'obs_dir'               :       'res/',
        'mpecfiles_dir'         :       'mpecfiles/',
        'cov_dir'               :       'cov/',
        'res_analysis_dir'      :       'badtrkfiles/',
        'findn_dir'             :       'findnfiles/',
        'proc_subdir'           :       'check_obj',
        'std_epoch'             :       '59200'
    }
    return update_wrapper.update_wrapper( arg_dict )
    
    
    
# -------------------- IOD ---------------------------------------------------------


def direct_call_IOD( designation_dict , destination):
    """
    # Attempt to fit the orbit using the "orbit_pipeline_wrapper"
    
    NB Will automatically attempt to write to db if successful
    """
    
    # Set up the DEFAULT arguments
    # (NB I am changing some of them to default to what I want for this orbit_checking code)
    parser = argparse.ArgumentParser()
    parser.add_argument("-trksub","--trksub", help="Provide asteroid trksub or provisional designation", type=str, default=designation_dict['orbfitname']) ### *** CHANGED DEFAULT *** ###
    parser.add_argument("-istrksub", "--istrksub", help="Is it a trksub?", type=str, choices=['Y','N'], default="N") ### *** CHANGED DEFAULT *** ###
    parser.add_argument("-c",   "--center",    help="Choose Gravity Center: H(Heliocentric)/P(Planet): default='H': Planet: 1=Mercury,...,8=Neptune",type=str, choices=['H', 'P'],default="H")
    parser.add_argument("-o80", "--obs80",     help="Observations in the 80-col format: default = False", default="ades")
    parser.add_argument("-orb", "--orbit",     help="Preliminary orbit to be used: default = False", default=False)
    parser.add_argument("-p",   "--plot",      help="Make analysis plots: default = False", action="store_true")
    parser.add_argument("-v",   "--verbose",   help="Make output verbose: default = True",  action="store_false")
    parser.add_argument("-d",   "--directory", help="Directory to be used", default=' ')
    parser.add_argument("-neocp", "--neocp", help="NEOCP object", default='N') ### *** CHANGED DEFAULT *** ###
    parser.add_argument("-obsfile", "--obsfile", help="Use an obs file instead of the database", default='N')
    parser.add_argument("-w",   "--write_to_db", help="Attempt to write to db if successful", action="store_false")### *** CHANGED DEFAULT *** ###
    parser.add_argument("-ot",  "--orbit_type", help="type of orbit & hence destination in db of orbit that is being written.", type=str, choices=['asteroid','comet','satellite'], default=destination)### *** CHANGED DEFAULT *** ###

    args = parser.parse_args()
        
    # Run the fitting wrapper ...
    proc_dir            = newsub.generate_subdirectory( 'iod' )
    done, results_dict  = iod.manage_tracklet_fitting(args, proc_dir )
    SUCCESS = True if done == 0 else False
    
    return SUCCESS, results_dict

"""
def command_line_call_IOD( designation_dict ):
    ''' FS's IOD code  '''

    # Set up the tmp-proc-dir
    proc_dir = newsub.generate_subdirectory( 'iod' )

    # Run the fit
    command = f"python3 /sa/orbit_utils/neocp_wrapper.py {designation_dict['orbfitname']} --istrksub 'N' --neocp 'N' --directory {proc_dir}"
    print('\nTrying IOD using command ...\n', command )
    process = subprocess.Popen( command,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                shell=True
    )
    stdout, stderr = process.communicate()
    stdout = stdout.decode("utf-8").split('\n')

    # Parse the output to look for the 'success' flag ...
    SUCCESS = True if np.any([ 'Initial return code = 0' in _ for _ in stdout]) else False
    return SUCCESS , proc_dir

"""

# ------------------ COMET ORBIT-FIT -----------------------------------------------
    
def direct_call_orbfit_comet_wrapper(designation_dict , FORCEOBS80=False):
    ''' Copied from MJP's comet/process_comet.py code '''
    
    arg_parser.add_argument('--cmt_desig', help="Provide comet MPC packed designation", type=str, default=designation_dict['packed_provisional_designation'] ) ### *** CHANGED DEFAULT *** ###
    arg_parser.add_argument('--trksub', dest='trksub', help='Is it a trksub? Y=Yes, N=No', default='N')
    arg_parser.add_argument('--obsfile', dest='obsfile', help="Observations file to use (file name, DB for database, ades for ADES format)",default='DB')
    arg_parser.add_argument('--orbit', dest='orbit', help="Preliminary orbit to be used (Y=Yes, N=No or file name)",default='N') ### *** CHANGED DEFAULT *** ###
    arg_parser.add_argument('--frag', dest='frag', help="Fragment (Y=yes,N=No)", default='N')
    arg_parser.add_argument('--t_std', dest='t_std', help="Current epoch", default=59200.)
    arg_parser.add_argument('--nongravs', dest='nongrav', help="Non-gravitational perturbations",choices=['Y','N'],default='N')
    arg_parser.add_argument('--model', dest='model', help="Non-gravs model: 1=Marsden1973, 2=Yeomans&Chodas, 3=Yabushita", default='1')
    arg_parser.add_argument('--params', dest='params', help="Non-gravs parameters, 1=A1,A2; 2=A1,A2,A3; 3=A1,A2,A3,DT",default='1')
    arg_parser.add_argument('--firstobs', dest='firstobs', help="First observation to be used (YYYY/MM/DD)",default='0000/00/00')
    arg_parser.add_argument('--lastobs', dest='lastobs', help="Last observation to be used (YYYY/MM/DD)",default='0000/00/00')
    arg_parser.add_argument('--a1ng', dest='a1ng', help='A1 non-gravs if you want to detect DT', default='0.')
    arg_parser.add_argument('--a2ng', dest='a2ng', help='A2 non-gravs if you want to detect DT', default='0.')
    arg_parser.add_argument('--a3ng', dest='a3ng', help='A3 non-gravs if you want to detect DT', default='0.')
    arg_parser.add_argument('--add_obsfile', dest='addobs', help='Add observation file to the obs in the DB', default='N')
    arg_parser.add_argument('--directory', dest='directory', help="Directory to be used", default=newsub.generate_subdirectory( 'comets' ) )
    args = parser.parse_args()

    # Run a fit
    done, results_dict = comet.main(args.cmt_desig,args.trksub,args.obsfile,args.orbit,args.frag,args.t_std,args.nongrav,args.model,args.params,args.firstobs,args.lastobs,args.a1ng,args.a2ng,args.a3ng,args.addobs,args.directory)
    
    return SUCCESS , proc_dir


"""
def commandline_call_orbfit_comet_wrapper(designation_dict , FORCEOBS80=False):
    ''' Copied from MJP's comet/process_comet.py code '''
    
    # comet code wants packed version ...
    packed_cmt_desig = designation_dict['packed_provisional_designation']
    
    # Set up the tmp-proc-dir
    proc_dir = newsub.generate_subdirectory( 'comets' )

    # Run a fit
    #print('FORCEOBS80 = ', FORCEOBS80)
    #command = f"python3 /sa/orbit_utils/comet_orbits.py {packed_cmt_desig} --orbit N --obsfile ades --directory {proc_dir}"
    command = f"python3 /sa/orbit_utils/comet_orbits.py {packed_cmt_desig} --orbit N --directory {proc_dir}"

    print("Running\n", command , "...\n")
    process = subprocess.Popen( command,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                shell=True
    )
    stdout, stderr = process.communicate()
    stdout = stdout.decode("utf-8").split('\n')
    print('*direct_call_orbfit_comet_wrapper* ... stdout:\n', stdout)
    
    # Parse the output to look for the 'success' flag ...
    SUCCESS = True if np.any([ 'comet_orbits succeeded' in _ for _ in stdout]) else False
    
    return SUCCESS , proc_dir

"""


    


# ------------------ GENERIC RESULTS ASSESSMENT  -----------------------------------------------

def assess_quality_of_any_database_orbit(designation_dict , assessment_dict, dbConnOrbs):
    """
    At present this is just setting one booleans in the assessment_dict ...
    """
    unpacked_provisional_designation = designation_dict['unpacked_provisional_designation']
    
    # ----------- (1) Check the database for any extant orbfit results ---------------
    
    # NB(1) : If there is *no* match, then the returned value for orbfit_results_id == False
    assessment_dict['IS_IN_ORBFIT_RESULTS']        = dbConnOrbs.has_orbfit_result(unpacked_provisional_designation)
    assessment_dict['IS_IN_COMET_RESULTS']         = False
    assessment_dict['IS_IN_SATELLITE_RESULTS']     = False
    assessment_dict['HAS_NO_RESULTS']              = not ( assessment_dict['IS_IN_ORBFIT_RESULTS'] or assessment_dict['IS_IN_COMET_RESULTS'] or assessment_dict['IS_IN_SATELLITE_RESULTS'] )
    
    
        
    # ------------- (2) Assess the quality of any results that exist in the database --
    if assessment_dict['IS_IN_ORBFIT_RESULTS'] :
    
        quality_dict = dbConnOrbs.get_quality_json(unpacked_provisional_designation)
        
        # Things to loop through
        expected_topline_keys = ["mid_epoch","std_epoch"]
        problems_A  = ["no orbit"]
        problems_B  = ["no CAR covariance", "no COM covariance"]

        # Default is "good"
        assessment_dict['HAS_BAD_QUALITY_DICT']            = False
        assessment_dict['HAS_INTERMEDIATE_QUALITY_DICT']   = False
        assessment_dict['HAS_GOOD_QUALITY_DICT']           = True

            
        # Severe problems
        for problem in problems_A:
            # Loop through the different epoch-keys, examining the message-string for each
            for k in expected_topline_keys:
                message_string = quality_dict[k]
                if problem in message_string:
                    assessment_dict['HAS_BAD_QUALITY_DICT']            = True
                    assessment_dict['HAS_INTERMEDIATE_QUALITY_DICT']   = False
                    assessment_dict['HAS_GOOD_QUALITY_DICT']           = False
                    return
                    
        # Intermediate problems
        for problem in problems_B:
            # Loop through the different epoch-keys, examining the message-string for each
            for k in expected_topline_keys:
                message_string = quality_dict[k]
                if problem in message_string:
                    assessment_dict['HAS_BAD_QUALITY_DICT']            = False
                    assessment_dict['HAS_INTERMEDIATE_QUALITY_DICT']   = True
                    assessment_dict['HAS_GOOD_QUALITY_DICT']           = False
                    return
        # return
        return
  
    elif assessment_dict['IS_IN_COMET_RESULTS'] :
        sys.exit('Assessment not in place for IS_IN_COMET_RESULTS ...')
    elif assessment_dict['IS_IN_SATELLITE_RESULTS'] :
        sys.exit('Assessment not in place for IS_IN_SATELLITE_RESULTS ...')
    elif assessment_dict['HAS_NO_RESULTS'] :
        return
    else:
        sys.exit('Should be impossible to get here...')

    
    
def assess_result_dict(designation_dict , result_dict, assessment_dict, RESULT_DICT_ORIGIN = 'EXTENSION'):
    """
        Assess the results in the dictionaries that were constructed from the orbfit flat-files
        Store assessment in assessment_dict
    """
    
    # Setting default values
    internal = {
        'SUCCESSFUL_ORBFIT_EXECUTION'   : None,
        'INPUT_GENERATION_SUCCESS'      : None,
        'enough_obs'                    : None,
        'existing_orbit'                : None,
        'new_obs_in_db'                 : None
    }
    
    # -------- IF THE RESULT CAME FROM MARGARET'S EXTENSION WRAPPER, THERE IS A LOT OF PRE-POPULATED INFORMATION ----------
    if RESULT_DICT_ORIGIN == 'EXTENSION' :
            
        # For whatever reason, the fit-wrapper returns packed designation
        packed = designation_dict['packed_provisional_designation']
        
        # There can be problems w.r.t. the input generation ...
        # If certain keys are absent, => didn't run => look for set-up failure ...
        # Expect keys like 'K15XM9X' , 'batch', 'obs_summary', 'time', 'top_level'
        internal['SUCCESSFUL_ORBFIT_EXECUTION'] = False if 'failedfits' not in result_dict else True
        if internal['SUCCESSFUL_ORBFIT_EXECUTION']:
        
            for k,v in result_dict.items(): print(k,v)
            # Perhaps it ran but we get an explicit indicate of failure
            # (1) If we see something in failedfits, then this is a failure
            # (2) If we don't see the packed designation in the result then this is a failure
            # (3) If ['INPUT_GENERATION_SUCCESS'] was not successful, then this is a failure ...
            if result_dict['failedfits'] or packed not in result_dict or not result_dict[packed]['INPUT_GENERATION_SUCCESS']:
                internal['SUCCESSFUL_ORBFIT_EXECUTION'] = False
            else:
                internal['SUCCESSFUL_ORBFIT_EXECUTION'] = True


        # If there was a problem with input-generation, it will be useful to grab the info
        if not internal['SUCCESSFUL_ORBFIT_EXECUTION'] and packed in result_dict and not result_dict[packed]['INPUT_GENERATION_SUCCESS']:
            internal.update(result_dict[packed])




    # -------- IF THE RESULT CAME FROM PAYNE'S WRAPPER(S), ... *** NEED TO REWRITE THIS : WHAT CHECKS ARE NECESSARY ??? *** ----------
    elif RESULT_DICT_ORIGIN in ['IOD','COMET'] :
        for k,v in result_dict.keys(): print(k,v)
        sys.exit('Need to rewrite...')
        
    else:
        sys.exit('Unknown RESULT_DICT_ORIGIN:', RESULT_DICT_ORIGIN)





    # -------- Update the assessment_dict with the internal results --------
    assessment_dict.update(internal)
    
    return
    
    


if __name__ == '__main__':
    check_multiple_designations(method = 'ALL' , size=10 )
