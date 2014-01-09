# really just to show the gaia_response object wrapper
import gaia_api
import json
import uuid
from avro import schema, datafile, io

def test_set_info():
    # build the set first; so we need a type
    retval = gaia_api.do_register_type_point()            
    type_id = retval['type_id']

    #create a new set
    set_id = str(uuid.uuid1())
    retval = gaia_api.do_new_set(type_id,set_id)

    # now, doing some weird stuff to expose the gaia_response
    (REQ_SCHEMA,REP_SCHEMA) = gaia_api.get_schemas("set_info")
    
    datum = {} 
    datum["set_id"] = set_id
    
    encoded_datum_request  = gaia_api.write_datum(REQ_SCHEMA, datum)    
    encoded_datum_response  = gaia_api.post_to_gaia_read(encoded_datum_request, "/setinfo")

    #first parse the gaia_response message
    REP_SCHEMA_STR = open("../obj_defs/gaia_response.json","r").read()
    REP_SCHEMA = schema.parse(REP_SCHEMA_STR)
    resp = gaia_api.read_orig_datum(REP_SCHEMA, encoded_datum_response)
    
    print resp
    
if __name__ == '__main__':
    test_set_info()
