#!/usr/bin/env python3

import json
import mpc_convert as mc
import orbfit_to_dict as o2d
import psycopg2
from psycopg2.extensions import AsIs
import sys


wriDBcols= False    # change this flag depending whether to write a file for database headers


class DBConnect():
    '''
    Class to allow 
    (a) connection to the database 
    (b) table inserts & upserts
    '''
    def __init__(self, db_host='marsden.cfa.harvard.edu', db_user ='postgres', db_name='vmsops'):

        try:
            self.dbConn = psycopg2.connect(host=db_host,user=db_user, database=db_name)
            self.dbCur = self.dbConn.cursor()
            
        except (Exception, psycopg2.Error) as error :
            print ("Error while connecting to PostgreSQL", error)

    
            
    def insert(self, data_dictionary, db_table_name):
        ''' 
        Will always  *insert*, creating an ever-growing list 
        - Probably only want to use this for "archive"-type tables 

        In order for this insert to work, the data_dictionary
        "keys" need to exist as field-names in the orbfit_orbits_archive
        table in the mpcdev database 

        '''

        # Restrict the passed table to a list of pre-approved values
        # Probably only want to use this for "archive"-type tables 
        assert db_table_name in ['orbfit_results','primary_comet_orbfit_results','multiple_comet_orbfit_results'] , 'The supplied table name is not on the preapproved list for insert ...'
        
        # Get the data out of the dictionary into some lists
        columns = data_dictionary.keys() 
        values = [data_dictionary[column] for column in columns]

        # Create insert statement
        # always inserts: keeps every orbit generated
        insert_statement = f"insert into {db_table_name} (%s) values %s"

        # Do the database insert 
        self.dbCur.execute(insert_statement, (AsIs(','.join(columns)), tuple(values)))
        self.dbConn.commit()


    

    def upsert(self, data_dictionary, db_table_name):
        ''' 
        will always insert-or-replace, so only inserts if no previous entry 

        In order for this insert to work, the data_dictionary
        "keys" need to exist as field-names in the orbfit_orbits_archive
        table in the mpcdev database 


        '''
        
        # Restrict the passed table to a list of pre-approved values
        # N.B. "upsert" is *NOT* allowed for archive tables ...!
        assert db_table_name in ['orbfit_results','primary_comet_orbfit_results','multiple_comet_orbfit_results'] , 'The supplied table name is not on the preapproved list for upsert ...'

        # Get the data out of the dictionary into some lists
        columns = data_dictionary.keys() 
        values = [data_dictionary[column] for column in columns]

        # Create insert statement 
        # - will overwrite if ID matches
        insert_statement=f"""
        INSERT INTO 
             {db_table_name} (%s) values %s
        ON CONFLICT 
             (packed_primary_provisional_designation)
        DO UPDATE SET
        """
        # loop through dictionary and extend the sql statement
        # - this is the logic that is required to overwrite the existing information with the new, updated information
        for k in columns:
            insert_statement+=("     "+str(k)+"=EXCLUDED."+str(k)+' ,\n' )
        # This is just removing the space & the comma from the end of the string 
        insert_statement=insert_statement[:-3]+'\n'
        insert_statement+=("        ;")
        
        # Do the database insert 
        self.dbCur.execute(insert_statement, (AsIs(','.join(columns)), tuple(values)))
        self.dbConn.commit()


    def db_close(self):
        self.dbCur.close()
        self.dbConn.close()

        
def packeddes_to_orbfitdes(desig):

    # convert packed desig to orbfit object name

    orbfitdes = mc.packed_to_unpacked_desig(desig).replace(' ','').replace('/','').replace('(','').replace(')','')

    return orbfitdes


def add_orbitfiles(count_dict,file_list):

    # add fields to database upsert summary dictionary according to orbit files specified in file_list

    extlists = [['eq0','eq0_postfit'],['eq1','eq1_postfit'],['eq2','eq2_postfit'],['eq3','eq3_postfit']]
    extkeys = ['eq0','eq1','eq2','eq3']

    for ind in range(len(extlists)):
        extlist = extlists[ind]
        extkey = extkeys[ind]
        if set(extlist).intersection(set(file_list)):
            count_dict['no_'+extkey] = []

    if 'rwo' in file_list:
        count_dict['no_rwo'] = []

    return count_dict

    
def load_orbfit_files(desig,file_list,count_dict,feldir='neofitels/',obsdir='res/'):
    '''
    Read orbfit files in file_list and convert to dictionaries
    
    MJP: Creates one filedict: is object specific
    '''

    filedict = {}
    objname = packeddes_to_orbfitdes(desig)

    extlists    = [['eq0','eq0_postfit'],['eq1','eq1_postfit'],['eq2','eq2_postfit'],['eq3','eq3_postfit']]
    exts        = ['.eq0_postfit','.eq1_postfit','.eq2_postfit','.eq3_postfit']
    extkeys     = ['eq0','eq1','eq2','eq3']

    for ind in range(len(exts)):

        extlist = extlists[ind]
        ext = exts[ind]
        extkey = extkeys[ind]
    
        if set(extlist).intersection(set(file_list)):
            try:
                filedict[extkey+'dict'] = o2d.fel_to_dict(feldir+objname+ext,allcoords=True)
            except:
                filedict[extkey+'dict'] = {}
                cdkey = [key for key in count_dict.keys() if extkey in key][0]
                count_dict[cdkey].append(desig)

    if 'rwo' in file_list:
        try:
            filedict['rwodict'] = o2d.rwo_to_dict(obsdir+objname+'.rwo')
        except:
            filedict['rwodict'] = {}
            cdkey = [key for key,val in count_dict.keys() if 'rwo' in key][0]
            count_dict[cdkey].append(desig)

    return filedict,count_dict
    
    
def load_supplied_dict(resultdict, count_dict) :
    '''
        New routine by MJP to allow direct input of dictionaries (rather than file read)
        
        Works on a single result dict for a single object
        ( *not* a higher level dict that could have contained multiple fits)
                
        Input(s)
        --------
        resultdict: dictionary
         - Must contain keys: ['fit_status','eq0dict','eq1dict','rwodict']
         - Corresponding values must be of type [str, dict, dict, dict]
         
        Returns
        -------
        outdict : dict
            - Basically a subset of resultdict
        count_dict : dict
            - At this stage in development this is a simple pass-through of the input count_dict
         
    '''
    #
    print('load_supplied_dict')
    for k,v in resultdict.items(): print(k)
    print('...')
    # Pass the eq* & rwo dicts directly into "outdict'
    outdict = {}
    try:
        assert isinstance(resultdict, dict)
        for k in ['eq0dict','eq1dict','eq2dict','eq3dict','rwodict'] :
            if k in resultdict:
                outdict[k] = resultdict[k]
    except:
        pass
        
    return outdict, count_dict
    
    

def dict_to_insert(packed,filedict,qualitydict,addpardict=None):

    # construct dictionary to be inserted to orbfit_results

    result = {}

    result['packed_primary_provisional_designation']    = packed
    result['unpacked_primary_provisional_designation']  = mc.packed_to_unpacked_desig(desig)
    result['rwo_json']                                  = json.dumps(filedict['rwodict'])
    result['quality_json']                              = json.dumps(qualitydict)

    if 'eq0dict' in filedict.keys():
        result['mid_epoch_json'] = json.dumps(filedict['eq0dict'])
    if 'eq1dict' in filedict.keys():
        result['standard_epoch_json'] = json.dumps(filedict['eq1dict'])
    if 'eq2dict' in filedict.keys():
        result['standard_epoch_closest_to_pericenter_json'] = json.dumps(filedict['eq2dict'])
    if 'eq3dict' in filedict.keys():
        result['standard_epoch_closest_to_next_passage_json'] = json.dumps(filedict['eq3dict'])

    if addpardict:
        result['additional_parameter_json'] = json.dumps(addpardict)        
        
    return result


def check_quality(filedict,file_list):

    # generate summary dictionary of orbit quality

    qualitydict = {}
    if set(['eq0','eq0_postfit']).intersection(set(file_list)):
        qualitydict['mid_epoch'] = check_fel_quality(filedict['eq0dict'])
    if set(['eq1','eq1_postfit']).intersection(set(file_list)):
        qualitydict['std_epoch'] = check_fel_quality(filedict['eq1dict'])
    if set(['eq2','eq2_postfit']).intersection(set(file_list)):
        qualitydict['std_epoch_peri'] = check_fel_quality(filedict['eq2dict'])
    if set(['eq3','eq3_postfit']).intersection(set(file_list)):
        qualitydict['std_epoch_next'] = check_fel_quality(filedict['eq3dict'])

    return qualitydict

        
def check_fel_quality(feldict):

    # check contents of elements file

    result = ''
    
    if not feldict:
        result = 'no orbit'
    else:
        for coordtype in ['EQU','KEP','CAR','COM','COT']:
            orbparams = feldict[coordtype].keys()
            if not orbparams:
                result += ', no '+coordtype
            elif 'element0' in orbparams and 'cov00' not in orbparams:
                result += ', no '+coordtype+' covariance'
        if result:
            result = result[2:]
        else:
            result = 'ok'

    return result




######################
# Conversion function(s) ...
def orbfit_ff_to_dict( orbfitname , processing_directory):
    '''
    Convert the orbfit results in processing_directory to standardized dictionaries
    *** VERY VERY SIMILAR TO load_orbfit_files ABOVE ***
    *** (RE)WRITTEN BY MJP FOR THE SAKE OF ... *********
    *** 1) UNDERSTANDING THE CODE IN load_orbfit_files *
    *** 2) MAKING A SINGLE-OBJECT SPECIFIC VERSION *****
    
    inputs:
    -------
    orbfitname: string
    - assume Orbfit names are unpacked w/o spaces/punctuation

    processing_directory:
    - top line directory in which the orbfit output files were stored
    - expect it to contain the following structure ...
        processing_directory/orbfitname/epoch
        processing_directory/orbfitname/mpcobs

    returns:
    --------
    SUCCESS:        Boolean
    upload_dict:    Dictionary

    '''

    # dict to store results in
    result_dict     = { }

    try:

        # output files that could/should exist in the processing directory
        eq_filelist      = ['eq0', 'eq1', 'eq2', 'eq3']
        rwo_file         = '.rwo'

        # check the directory structure
        assert os.path.isdir( proc_dir )
        assert os.path.isdir( os.path.join(proc_dir , orbfitname ) )
        assert os.path.isdir( os.path.join(proc_dir , orbfitname , 'epoch') )
        assert os.path.isdir( os.path.join(proc_dir , orbfitname , 'mpcorb') )

        # Read the eq* files
        file_list = []
        for f in eq_filelist :
          filepath = os.path.join(proc_dir , orbfitname , 'epoch', orbfitname + '.' + f + '_postfit' )
          if os.path.isfile(filepath):
            result_dict[f + 'dict'] = o2d.fel_to_dict(filepath, allcoords=True)
            file_list.append(f)
                    
        # Read the rwo file
        filepath                 = os.path.join(proc_dir , orbfitname , 'mpcobs', orbfitname + rwo_file )
        if os.path.isfile(filepath):
            result_dict['rwodict']   = o2d.rwo_to_dict(filepath)
                
        # Construct quality dictionary
        qualitydict = check_quality(result_dict, file_list)

        # construct upsert dictionary
        if filedict:
            packed      = update_existing_orbits.orbfitdes_to_packeddes(orbfitname)
            upload_dict = dict_to_insert(packed, result_dict, qualitydict, addpardict=addpardict)

        SUCCESS = True

    except Exception as e:
        upload_dict['failedfits'] = {'error': e}
        SUCCESS = False

    return SUCCESS, upload_dict
    

def save_result_dict_to_db(result_dict_to_upsert, orbit_type, db=None):
    try:
        # Establish connection to the database if not passed-in
        db = DBConnect() if db is None else db
            
        # Upsert dictionaries into database
        db.upsert(result_dict_to_upsert, orbit_type)
        
        SUCCESS = True
        
    except Exception as e:
        SUCCESS = False
        print('Exception....\n', e)
        
        
    return SUCCESS,
  
def single_orbfit_directory_to_database(
            orbfitname,
            processing_directory,
            orbit_type,
            db=None):
    '''
    Generates dictionaries from orbfit output files for a single orbfit orbit-fit
    Stores in specified orbit table
    
    inputs:
    -------
    orbfitname              : string
    - assume Orbfit names are unpacked w/o spaces/punctuation
        
    processing_directory    : string
    - top line directory in which the orbfit output files were stored
    - expect it to contain the following structure ...
        processing_directory/orbfitname/epoch
        processing_directory/orbfitname/mpcobs
        
    orbit_type:             : string
    - type of orbit => destination table(s) for orbit
    - must be one of ['asteroid','comet','satellite]
    
    returns:
    --------
    SUCCESS:    Boolean
    
    '''
        
    # Convert the flat-files to dictionaries
    SUCCESS, result_dict_to_upsert = orbfit_ff_to_dict( orbfitname , processing_directory)
    
    # Load the dictionaries into the database
    if SUCCESS:
        SUCCESS = save_result_dict_to_db(result_dict_to_upsert, orbit_type, db=db)
            
    # might be useful to also return the result-dict
    return SUCCESS, result_dict_to_upsert




######################


def main(   primdesiglist,
            file_list=['eq0','eq1','rwo'],
            table_name='orbfit_results',
            feldir='neofitelss/',
            obsdir='res/',
            timestamp='',
            addpardict=None,
            filedictlist = None ):
    '''
    Generates dictionaries from orbfit output files listed in file_list for objects in primdesiglist 
    (primdesiglist = packed desigs; will assume Orbfit names are unpacked w/o spaces/punctuation)
    Checks orbit elements files for contents; generates quality summary
    Stores in specified orbit table
    '''

    # Establish connection to the database
    db = DBConnect() 

    # set up summary dictionary of info upserted
    count_dict = {
        'obj_count': 0,
        'no_upsert': [],
        'no_extract': []}
    count_dict = add_orbitfiles(count_dict,file_list)

    # for each object fitted, check fit output, construct quality dictionary, upsert results
    for n, desig in enumerate(primdesiglist):

        print('\n Attempting to insert to orbfit_results for: ', desig)
        
        # Read dictionaries direct from input
        # Not really doing anything much, as the data is mainly/totally present in the desired manner ...
        if filedictlist is not None and len(filedictlist) == len(primdesiglist) and isinstance( filedictlist[n] , dict ):
            filedict,count_dict = load_supplied_dict( filedictlist[n] , count_dict)
        else:
            # load orbfit results files into python
            try:
                filedict,count_dict = load_orbfit_files(desig,file_list,count_dict,feldir=feldir,obsdir=obsdir)
            except:
                print(desig+' : problem with file/dict load')
                filedict = {}
                count_dict['no_extract'].append(desig)
                for filename in file_list:
                    count_dict['no_'+filename].append(desig)
                continue

        # construct quality dictionary
        print('...filedict...')
        for k,v in filedict.items(): print(k,v)
        print('...')
        qualitydict = check_quality(filedict,file_list)

        # construct upsert dictionary
        if filedict:
            to_orbfit_results = dict_to_insert(desig,filedict,qualitydict,addpardict=addpardict)

        # upsert to specified table
        try:
            db.upsert(to_orbfit_results,table_name)
            count_dict['obj_count'] += 1
        except:
            print(desig+' : problem with upsert')
            count_dict['no_upsert'].append(desig)

    db.db_close()
        
    # count objects with missing orbfit results files
    missing_file_list = set([])
    filekeys = [key for key in count_dict.keys() if key not in ['obj_count','no_upsert','no_extract']]
    for key in filekeys:
        missing_file_list = missing_file_list | set(count_dict[key])
    missing_file_count = len(list(missing_file_list))

    # save summary dict of upserted info
    with open('count_dict_'+timestamp+'.json','w') as fh:
        json.dump(count_dict,fh,indent=4,sort_keys=True)
        
    summarystr = str(count_dict['obj_count'])+' object(s) saved to '+table_name+'; '+str(missing_file_count)+' object(s) missing at least one orbit file; '+str(len(count_dict['no_upsert']))+' object(s) with upsert issues'
    print(summarystr)

    return summarystr

