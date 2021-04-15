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

import to_orbfit_db_tables_dev as to_db

import mpc_new_processing_sub_directory as newsub


# Codes to define possible orbit/designation "status"
"""
status_dict = {
    ### Negative Numbers: "Not a primary designation"

    ### 0-99 Numbers    : "Orbit Absent"
    001  :  "Orbit Absent: No known observations",
    002  :  "Orbit Absent: Orbfit Failed: Insufficient observations exist to form any orbit",
    009  :  "Orbit Absent: Orbfit Failed: Reason unknown",
    099  :  "Orbit Absent: Reason for absence has not been established",
    
    ### 100-199 Numbers  : "Orbit Present but Poor"
    100  : "Orbit Poor:   Short-Arc / Few observations",
    101  : "Orbit Poor:   Significant fraction of observations in outlying tracklet: Removal may cause inability to calculate orbit",
    199  : "Orbit Poor:   Reason for poor orbit has not been established",
    
    ### 200-299 Numbers  : "Orbit Present and Good"
    200  : "Orbit Exists: Orbit consistent with all observations (no massive outliers)",
    201  : "Orbit Exists: Orbit consistent with most observations (one or more tracklets to be dealt with)",
    299  : "Orbit Exists: As yet unclassified",
}
"""

# Alternative way of define possible orbit/designation "status"
# Need to go back-and-forth with Margaret to define a list of all possible failure modes / metrics / etc
assessment_dict = {

    # Overall designation status
    'IS_PRIMARY_UNPACKED_DESIGNATION'   : False,

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

}


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
        
        #         # There can be problems w.r.t. the input generation ...
        # *** Need to discuss with MPan how to interpret ***
        # An example known failure is K15XM9X == 2015 XX229
        #
        # primary_designations_array =  np.array(['2008 WJ19'] )###, "2008 WJ19"  is not in the db & will go to IOD (IOD will work)
        # primary_designations_array =  np.array(['2016 QW66'] )###, "2016 QW66"  is not in the db & will go to IOD & IOD will fail
        primary_designations_array =  np.array(['2006 WU224'])###, "2006 WU224" is already in the db
        
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
    # Current return is rather primitive
    results= { desig:check_single_designation( desig , dbConnIDs, dbConnOrbs) for desig in primary_designations_array }
    
    # Primitive categorization:
    # - Doing this purely to facilitate having a pretty print-out
    for value, txt in zip( [-1,1,0,2], ['Missing...', 'Fixed...', 'Poor...', 'Good...'] ):
        desigs = [k for k,v in results.items() if v == value]
        print('\n'+txt)
        print('\tN=',len(desigs))
        for d in desigs: print(d)
        
        
    # Update field(s) in the primary_objects table


def make_designation_dict(unpacked_provisional_designation):
    return {
        'unpacked_provisional_designation'  : unpacked_provisional_designation,
        'packed_provisional_designation'    : mc.unpacked_to_packed_desig(unpacked_provisional_designation),
        'orbfitname'                        : update_existing_orbits.packeddes_to_orbfitdes(mc.unpacked_to_packed_desig(unpacked_provisional_designation))
    }
    
def check_single_designation( unpacked_provisional_designation , dbConnIDs, dbConnOrbs, FIX=False):
    '''
    Do a bunch of checks on a single designation
    WIP Code:
    (i) does not yet perform all required checks
    (ii) does not permany many/any db updates
    
    '''
    # Fucking designations
    designation_dict = make_designation_dict(unpacked_provisional_designation)
    
    # Check whether actually a primary unpacked_provisional_designation
    # - If being called from a list pulled from the identifications tables, then this step is unnecessary
    # - But I provide it for safety
    assessment_dict['IS_PRIMARY_UNPACKED_DESIGNATION'] = dbConnIDs.is_valid_unpacked_primary_desig(unpacked_provisional_designation)

    # Update the primary_objects table to flag whether we have an orbit in the orbfit-results table
    # NB(1) : If there is *no* match, then the returned value for orbfit_results_id == False
    # NB(2) : This is not logically great, should really be checking not in comets, etc,
    #         but for now, while developing, this is not something to worry about while the comet tble is empty
    assessment_dict['IS_IN_ORBFIT_RESULTS']        = dbConnOrbs.has_orbfit_result(unpacked_provisional_designation)
    assessment_dict['IS_IN_COMET_RESULTS']         = False
    assessment_dict['IS_IN_SATELLITE_RESULTS']     = False
    assessment_dict['HAS_NO_RESULTS']              = not assessment_dict['IS_IN_ORBFIT_RESULTS']
    
    
    # Understand the quality of any orbfit-orbit currently in the database ...
    # - Not clear where we want to be doing this, but while developing I am doing this here ...
    if assessment_dict['IS_IN_ORBFIT_RESULTS'] :
        quality_dict = dbConnOrbs.get_quality_json(unpacked_provisional_designation)
        assess_quality_dict(quality_dict , assessment_dict)


    # If no orbit at all...
    if True:#assessment_dict['HAS_NO_RESULTS'] :
        #print('\n'*3,'HAS_NO_RESULTS', unpacked_provisional_designation)

        
        # (1) Standard asteroid ...
        if   "C/" not in unpacked_provisional_designation:
            destination = 'asteroid ' ; print(destination)

            ##result_dict = call_orbfit_via_commandline_update_wrapper(unpacked_provisional_designation)
            # (a) Orbfit & Dictionary conversion in one
            print("\t*"*3,"Standard Orbit Fit ...")
            result_dict = direct_call_orbfit_update_wrapper(unpacked_provisional_designation)
            
            # (b) Evaluate the result from the orbfit run & assign a status
            assess_result_dict(designation_dict , result_dict , assessment_dict , RESULT_DICT_ORIGIN = 'Pan' )

            # (c) if the init orbit is missing, but there are obs, then might want to try IOD of some sort ...
            if not assessment_dict['SUCCESSFUL_ORBFIT_EXECUTION'] and assessment_dict['enough_obs'] and not assessment_dict['existing_orbit']:
            
                # IOD
                print("\t*"*3,"IOD ...")
                assessment_dict['SUCCESSFUL_ORBFIT_EXECUTION'] , proc_dir   = direct_call_IOD(designation_dict)
                
                if assessment_dict['SUCCESSFUL_ORBFIT_EXECUTION']:
                    # Convert
                    result_dict     = convert_orbfit_output_to_dictionaries(designation_dict , assessment_dict, proc_dir)
                    # Assess
                    print('IOD: Pre-Assessment ...:\n', assessment_dict)
                    assess_result_dict(designation_dict , result_dict , assessment_dict, RESULT_DICT_ORIGIN = 'Payne' )
                    print('IOD: PostAssessment ...:\n', assessment_dict)

        # (2) Comet
        elif "C/" in unpacked_provisional_designation:
            destination = 'comet '

            # Orbfit
            assessment_dict['SUCCESSFUL_ORBFIT_EXECUTION'] , proc_dir  = direct_call_orbfit_comet_wrapper(designation_dict, FORCEOBS80=False )

            # Convert results from files to dictionaries (only done if possible)
            result_dict = convert_orbfit_output_to_dictionaries(designation_dict , assessment_dict, proc_dir)
            
            # Assess
            assess_result_dict(designation_dict , result_dict , assessment_dict)

        # (3) Satellite
        else:
            destination = 'satellite '

        
        

        # (4) Save results to the database (only done if we have a useable result ... )
        save_results_to_database( designation_dict, assessment_dict, result_dict , destination = destination )
        


    #if assessment_dict['IS_IN_ORBFIT_RESULTS']:
    #    dbConnOrbs.set_orbfit_results_flags_in_primary_objects( unpacked_provisional_designation ,
    #                                                                assessment_dict['IS_IN_ORBFIT_RESULTS']   )




    # Primitive categorization
    if assessment_dict['HAS_NO_RESULTS'] and not assessment_dict['SUCCESSFUL_ORBFIT_EXECUTION']:
        return -1
    if assessment_dict['HAS_NO_RESULTS'] and assessment_dict['SUCCESSFUL_ORBFIT_EXECUTION']:
        return 1
    elif assessment_dict['HAS_BAD_QUALITY_DICT']:
        return 0
    else:
        return 2
    
    
    
    
    
    
    
# ------------------ ORBIT EXTENSION -------------------------------------------
    
'''
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
'''
    
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

def direct_call_IOD( designation_dict ):
    ''' FS's IOD code  '''

    # Set up the tmp-proc-dir
    proc_dir = newsub.generate_subdirectory( 'iod' )

    # Run the fit
    command = f"python3 /sa/orbit_utils/neocp_wrapper.py {designation_dict['orbfitname']} --istrksub 'N' --neocp 'N' --directory {proc_dir}"
    print('Trying IOD using command ...\n', command )
    process = subprocess.Popen( command,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                shell=True
    )
    stdout, stderr = process.communicate()
    stdout = stdout.decode("utf-8").split('\n')
    #print('*direct_call_IOD* ... stdout:\n', stdout)

    # Parse the output to look for the 'success' flag ...
    SUCCESS = True if np.any([ 'Initial return code = 0' in _ for _ in stdout]) else False
    return SUCCESS , proc_dir

# ------------------ COMET ORBIT-FIT -----------------------------------------------
    
def direct_call_orbfit_comet_wrapper(designation_dict , FORCEOBS80=False):
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

def convert_orbfit_output_to_dictionaries(designation_dict , assessment_dict, proc_dir):
    '''
    converting the comet results to dictionaries
    
    This needs to be added into the comet code itself
    (or at the very least, as some kind of option afterwards)
    *** THE AGREEMENT WITH FEDERICA IS ...
    *** (i) upgrade the comet_orbits.py code to return
    ***     succ, dict
    ***     where
    ***     succ is the current boolean
    ***     dict is a dictionary of the output that I will add
    ***
    ***(ii) She/I will then also need to upgrade the "comet list" wrapper to handle that.
    ***
    ***(iii) The dictionaries will then be written to the db by a separate peice of code
    
    But for now, while developing, putting it here
    
    heavily following the update_wrapper.py/setup_obj_dicts code ...
    (1) update_wrapper.update_wrapper returns *result_dict* which comes out of *run_fits*
    (2) *update_wrapper.run_fits* adds the results of "update_existing_orbits.update_existing_orbits" into an overall result_dict
    (3) *update_existing_orbits.update_existing_orbits* seems to
        (a) primarily generate *result_dict* from its own *run_fits*
        (b) format the results into results_dict using its own *save_fits*
    (4) (a) *update_existing_orbits.run_fits* is calling orbit_update_psv.main()
        (b) *update_existing_orbits.save_fits* goes through the results from *update_existing_orbits.run_fits* and creates result_dict[desig]['eq0dict'], result_dict[desig]['rwodict'], etc
    '''

    """
    # I think I got this code from update_wrapper.py
    try:
            obslist = o2d.ades_to_dicts(subdirname+obj_name+'.ades')
    except:
            print(desig+' : problem with ADES obs file?')
            obslist = []
        try:
            eq0dict = o2d.fel_to_dict(subdirname+obj_name+'.eq0')
        except:
            print(desig+' : problem with starting orbit file (eq0)?')
            eq0dict = {}

        obj_dicts[desig] = {'obslist':obslist,'eq0dict':eq0dict}
    """
    
    """
    # update_existing_orbits.save_fits()
    def save_fits(arg_dict,result_dict,datadicts):

    # write orbits/.rwo to database; write orbits/obs to flat files

    subdirname = arg_dict['subdirname']
    primdesiglist = arg_dict['primdesiglist']
    obs_dir = subdirname+arg_dict['obs_dir']
    elements_dir = subdirname+arg_dict['elements_dir']
    proc_subdir = arg_dict['proc_subdir']
    mpecfiles_dir = subdirname+arg_dict['mpecfiles_dir']
    queue_name = arg_dict['queue_name']

    fit_count = 0

    # excise faulty fits from list of fitted objects; write remaining obs/orbits to database
    datadicts = [datadict for datadict in datadicts if not (datadict['num_obs']==0 and datadict['num_obs_selected']==0 and datadict['num_rad']==0 and datadict['num_rad_selected']==0)]
    for datadict in datadicts:
        desig = datadict['packed_provisional_id']
        try:
            orbfitname = packeddes_to_orbfitdes(desig)
            result_dict[desig]['eq0dict'] = o2d.fel_to_dict(elements_dir+orbfitname+'.eq0_postfit',allcoords=True)
            result_dict[desig]['eq1dict'] = o2d.fel_to_dict(elements_dir+orbfitname+'.eq1_postfit',allcoords=True)
            result_dict[desig]['rwodict'] = o2d.rwo_to_dict(obs_dir+orbfitname+'.rwo')
            if desig in result_dict['badtrkdict'].keys():
                result_dict[desig]['badtrkdict'] = result_dict['badtrkdict'][desig]
                result_dict[desig]['fit_status'] = str(len(result_dict['badtrkdict'][desig]))+' bad tracklet(s)'
            else:
                result_dict[desig]['fit_status'] = 'no problems'
            fit_count += 1
        except:
            print(desig+' : issues with elements/rwo files')

    result_dict['saver'] = 'elements and rwo converted to dictionaries for '+str(fit_count)+' objects'

    return arg_dict, result_dict
    
    """
    
    result_dict = {}
    if assessment_dict['SUCCESSFUL_ORBFIT_EXECUTION']:
        
        # name of the directory that orbfit stores things in
        orbfitname       = designation_dict['orbfitname']
        result_dict[orbfitname] =  {}

        # loop through the comet output files that could/should exist in the processing directory
        eq_filelist      = ['eq0', 'eq1', 'eq2', 'eq3']
        rwo_file         = '.rwo'
        
        # Read the eq* files
        for f in eq_filelist :
            filepath = os.path.join(proc_dir , orbfitname , 'epoch', orbfitname + '.' + f + '_postfit' )
            print(os.path.isfile(filepath) , ' : ', filepath)
            if os.path.isfile(filepath):
                result_dict[orbfitname][f + 'dict'] = o2d.fel_to_dict(filepath, allcoords=True)
                
        # Read the rwo file
        filepath                        = os.path.join(proc_dir , orbfitname , 'mpcobs', orbfitname + rwo_file )
        print(os.path.isfile(filepath) , ' : ', filepath)
        result_dict[orbfitname]['rwodict']   = o2d.rwo_to_dict(filepath)
        
        # Add an (empty) 'failedfits' element
        result_dict[orbfitname]['failedfits'] = {}
        
    return result_dict
    
    


# ------------------ GENERIC RESULTS ASSESSMENT  -----------------------------------------------

def assess_quality_dict(quality_dict , assessment_dict):
    """ At present this is just setting one of the following booleans in the assessment_dict ...
        'HAS_BAD_QUALITY_DICT'            : False,
        'HAS_INTERMEDIATE_QUALITY_DICT'   : False,
        'HAS_GOOD_QUALITY_DICT'           : False,
    """
    
    # Things to loop through
    expected_topline_keys = ["mid_epoch","std_epoch"]
    problems_A  = ["no orbit"]
    problems_B  = ["no CAR covariance", "no COM covariance"]

    #Default is "good"
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
    
    
def assess_result_dict(designation_dict , result_dict, assessment_dict, RESULT_DICT_ORIGIN = 'Pan'):
    """
        Assess the results in dictionaries from orbfit
        Store assessment in assessment_dict
    """
    print('assess_result_dict : result_dict=...\n', result_dict)
    
    # Setting default values
    internal = {
        'SUCCESSFUL_ORBFIT_EXECUTION'   : None,
        'INPUT_GENERATION_SUCCESS'      : None,
        'enough_obs'                    : None,
        'existing_orbit'                : None,
        'new_obs_in_db'                 : None
    }
    
    # -------- IF THE RESULT CAME FROM MARGARET'S WRAPPER, THERE IS A LOT OF PRE-POPULATED INFORMATION ----------
    if RESULT_DICT_ORIGIN == 'Pan' :
            
        # For whatever reason, the fit-wrapper returns packed designation
        packed = designation_dict['packed_provisional_designation']
        
        # There can be problems w.r.t. the input generation ...
        # If certain keys are absent, => didn't run => look for set-up failure ...
        # Expect keys like 'K15XM9X' , 'batch', 'obs_summary', 'time', 'top_level'
        internal['SUCCESSFUL_ORBFIT_EXECUTION'] = False if 'failedfits' not in result_dict else True
        if internal['SUCCESSFUL_ORBFIT_EXECUTION']:
        
            # Perhaps it ran but we get an explicit indicate of failure
            # (1) If we see something in failedfits, then this is a failure
            # (2) If we don't see the packed designation in the result then this is a failure
            # (3) If ['INPUT_GENERATION_SUCCESS'] was not successful, then this is a failure ...
            if result_dict['failedfits'] or packed not in result_dict:
                internal['SUCCESSFUL_ORBFIT_EXECUTION'] = False
            else:
                internal['SUCCESSFUL_ORBFIT_EXECUTION'] = True


        # Populate the other fields in the internal-dict using the values which will be in result_dict[packed] ...
        if packed in result_dict:
            pass # internal.update(result_dict[packed])

    # -------- IF THE RESULT CAME FROM PAYNE'S WRAPPER, THERE IS NOT MUCH PRE-POPULATED INFORMATION ----------
    elif RESULT_DICT_ORIGIN == 'Payne' :
        pass
    else:
        sys.exit('Unknown RESULT_DICT_ORIGIN:', RESULT_DICT_ORIGIN)


    # Update the assessment_dict with the internal results ...
    assessment_dict.update(internal)
    
    return
    
# ------------------ SAVE RESULTSs -----------------------------------------------

def save_results_to_database(designation_dict, assessment_dict, result_dict , destination = 'asteroid'):
    ''' Save results to table(s) ...'''
    print('*** Saving results to db ... *** ')
    if assessment_dict['SUCCESSFUL_ORBFIT_EXECUTION']:
        packed = designation_dict['packed_provisional_designation']
        if packed in result_dict:
            # Call the code to insert the results into the database
            to_db.main( [packed] , filedictlist=[result_dict[packed]] )

if __name__ == '__main__':
    check_multiple_designations(method = 'ALL' , size=1 )
