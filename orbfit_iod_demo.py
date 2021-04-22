

def direct_call_IOD( designation_dict ):
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
    

def convert_orbfit_output_to_dictionaries(designation_dict , assessment_dict, proc_dir):
    '''
    converting the comet results to dictionaries

    I think I got this code from update_wrapper.py

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
    
    '''
    
    result_dict = {}
    if assessment_dict['SUCCESSFUL_ORBFIT_EXECUTION']:
        
        # name of the directory that orbfit stores things in
        orbfitname       = designation_dict['orbfitname']
        
        # For whatever reason, the fit-wrapper returns packed designation, so I'll grudgingly use that
        packed = designation_dict['packed_provisional_designation']
        result_dict[packed] =  {}

        # loop through the comet output files that could/should exist in the processing directory
        eq_filelist      = ['eq0', 'eq1', 'eq2', 'eq3']
        rwo_file         = '.rwo'
        
        # try to read the results files ...
        try:
        
            # Read the eq* files
            for f in eq_filelist :
                filepath = os.path.join(proc_dir , orbfitname , 'epoch', orbfitname + '.' + f + '_postfit' )
                print(os.path.isfile(filepath) , ' : ', filepath)
                if os.path.isfile(filepath):
                    result_dict[packed][f + 'dict'] = o2d.fel_to_dict(filepath, allcoords=True)
                    
            # Read the rwo file
            filepath                        = os.path.join(proc_dir , orbfitname , 'mpcobs', orbfitname + rwo_file )
            print(os.path.isfile(filepath) , ' : ', filepath)
            result_dict[packed]['rwodict']   = o2d.rwo_to_dict(filepath)
            
            # Add an (empty) 'failedfits' element
            result_dict[packed]['failedfits'] = {}
            
        # if we can't read the orbit-files then I am going to label the execution as unsuccessful ...
        except Exception as e:
            print('Exception occured while reading files ...\n\t',e)
            assessment_dict['SUCCESSFUL_ORBIT_GENERATION']=False
            
        
    return result_dict
    
    
def assess_result_dict(designation_dict , result_dict, assessment_dict, RESULT_DICT_ORIGIN = 'Pan'):
    """
        Assess the results in the dictionaries that were constructed from the IOD run
        Store assessment in assessment_dict
    """
    
    packed = designation_dict['packed_provisional_designation']
    
    # Did orbfit execute successfully ?
    if  packed in result_dict and \
        'eq0dict' in result_dict[packed] and \
        'eq1dict' in result_dict[packed] and \
        'rwodict' in result_dict[packed]:
        internal['SUCCESSFUL_ORBFIT_EXECUTION'] = True
    else:
        internal['SUCCESSFUL_ORBFIT_EXECUTION'] = False
        print("\n assess_result_dict \t *** UNSUCCESSFUL EXECUTION *** \n")


    # Do the results look good according to tunable criteria
    



def run_and_assess_iod():
    assessment_dict = {}
    
    # Run the fit
    assessment_dict['SUCCESSFUL_ORBFIT_EXECUTION'] , proc_dir = direct_call_IOD( designation_dict )
    
    # Convert Flat-Files to Results
    result_dict = convert_orbfit_output_to_dictionaries(designation_dict , assessment_dict, proc_dir):
    
    # Assess
    assess_result_dict(designation_dict , result_dict , assessment_dict, RESULT_DICT_ORIGIN = 'Payne' )
