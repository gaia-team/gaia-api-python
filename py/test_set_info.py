import gaia_api
import json
import uuid

def test_set_info():
    # build the set first; so we need a type
    retval = gaia_api.do_register_type_point()            
    type_id = retval['type_id']

    #create a new set
    set_id = str(uuid.uuid1())
    retval = gaia_api.do_new_set(type_id,set_id)

    # set info
    retobj = gaia_api.do_set_info(set_id)
    print retobj
    
if __name__ == '__main__':
    test_set_info()
