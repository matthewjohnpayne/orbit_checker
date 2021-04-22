

import flat_file_conversion


def save_results_to_database( expected_designation, result_dict , dbConnOrbs, destination = 'asteroid'):
    '''
    Save results to table(s) ...
    
    expected_designation: string
        - the  designation expected to be in result_dict as a key
        - expected format likely to be *orbfitformat* for IOD & comet
        - expected format likely to be *packed* for extension wrapper
        
    
    '''
        
    if expected_designation in result_dict:
        
        assert destination in ['satellite','comet','asteroid']

        try:
            # Call the code to insert the results into the database
            to_db.main( [expected_designation] , filedictlist=[result_dict[expected_designation]] )
            
        except Exception as e:
            print('An Exception occured in save_results_to_database but I am continuing ...\n\t', e)

def convert_and_save():
    ''' Demo code to convert flat-file results to dictionaries and then save into db '''
    
    # Do the conversion
    # Here I am assuming that an orbit-fit has been run in a processing_directory
    flat_file_conversion.orbfit_ff_to_dict( orbfitname , designation_key , processing_directory):

    #
