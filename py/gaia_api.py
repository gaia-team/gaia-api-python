import httplib
import urllib
import uuid
import time
import os
import subprocess
import json

from avro import schema, datafile, io
import StringIO

# local
gaia_ip = "127.0.0.1"
gaia_port  = "9191"

# reverse proxy
#gaia_ip   = "gaia.gisfederal.com"
#gaia_port = ""

# sgi-bottom
#gaia_ip = "172.30.20.4"
#gaia_port  = "9191"

#gaia_ip = "192.168.1.144"
#gaia_ip = "192.168.1.158"
#gaia_port  = "9191"

####### helpers #############
def post_to_gaia_read(jsonobj,url):
    params = jsonobj
    headers = {"Content-type": "plain/text",
           "Accept": "text/plain"}

    nurl = ""
    if len(gaia_port) > 0:
        conn = httplib.HTTPConnection(str(gaia_ip) + ":" + gaia_port)
        nurl = url
    else:
        conn = httplib.HTTPConnection(str(gaia_ip))
        nurl = "/gaia2"+str(url)

    #print nurl
    conn.request("POST",  nurl, params, headers)
    
    r1 = conn.getresponse()        
    data1 = r1.read()

    return  str(data1)

# return binary encoding of the datum
def write_datum(SCHEMA, datum):
    # build the encoder; this output is where the data will be written
    output = StringIO.StringIO()
    be = io.BinaryEncoder(output)
    
    # Create a 'record' (datum) writer
    writer = io.DatumWriter(SCHEMA)
    writer.write(datum, be)

    #print 'write_datum, size = ',len(output.getvalue()),output.tell()
    return output.getvalue() 

def read_orig_datum(SCHEMA, encoded_datum):
    # build the decoder
    output = StringIO.StringIO(encoded_datum)
    bd = io.BinaryDecoder(output)

    reader = io.DatumReader(SCHEMA)
    out = reader.read(bd) # read, give a decoder

    return out


def read_datum(SCHEMA, encoded_datum):
    
    #first parse the gaia_response message
    REP_SCHEMA_STR = open("../obj_defs/gaia_response.json","r").read()
    REP_SCHEMA = schema.parse(REP_SCHEMA_STR)
    resp = read_orig_datum(REP_SCHEMA, encoded_datum)

    #print 'read_datum, size = ',len(encoded_datum)

    #now parse the actual response if there is no error
    #NOTE: DATA_SCHEMA should be equivalent to SCHEMA but is NOT for get_set_sorted
    stype = resp['data_type']
    #print 'read_datum, embedded data_type: ',stype
    if stype == 'none':
        out = {}
    else:
        DATA_SCHEMA_STR = open("../obj_defs/%s.json"%(stype), "r").read()
        DATA_SCHEMA = schema.parse(DATA_SCHEMA_STR)
        #out = read_orig_datum(DATA_SCHEMA, resp['data'])
        out = read_orig_datum(SCHEMA, resp['data'])

        #print 'read_orig_datum, size = ',len(resp['data'])
    
    del resp['data']

    out['status_info'] = resp

    return out

def read_point(encoded_datum):
    OBJ_SCHEMA_STR = """{"type":"record","name":"point","fields":[{"name":"x","type":"double"},{"name":"y","type":"double"}]}"""
    OBJ_SCHEMA = schema.parse(OBJ_SCHEMA_STR)
    return read_orig_datum(OBJ_SCHEMA, encoded_datum)

def read_big_point(encoded_datum):
    OBJ_SCHEMA_STR = """{"type":"record","name":"point","fields":[{"name":"msg_id","type":"string"},{"name":"x","type":"double"},{"name":"y","type":"double"},{"name":"timestamp","type":"int"},{"name":"source","type":"string"},{"name":"group_id","type":"string"}]}"""
    OBJ_SCHEMA = schema.parse(OBJ_SCHEMA_STR)
    return read_orig_datum(OBJ_SCHEMA, encoded_datum)

# this point is designed to look like "Point"
def read_gis_point(encoded_datum):
    OBJ_SCHEMA_STR = """{"type":"record","name":"Point","fields":[{"name":"x","type":"double"},{"name":"y","type":"double"},{"name":"timestamp","type":"double"},{"name":"tag_id","type":"double"},{"name":"derived","type":"double"},{"name":"msg_id","type":"string"},{"name":"group_id","type":"string"},{"name":"level_one_mgrs","type":"string"},{"name":"level_two_mgrs","type":"string"},{"name":"level_three_mgrs","type":"string"},{"name":"level_final_mgrs","type":"string"}]}"""
    #OBJ_SCHEMA_STR = """{"type":"record","name":"point","fields":[{"name":"x","type":"double"},{"name":"y","type":"double"},{"name":"timestamp","type":"double"},{"name":"tag_id","type":"double"},{"name":"derived","type":"double"},{"name":"msg_id","type":"string"},{"name":"group_id","type":"string"},{"name":"level_one_mgrs","type":"string"},{"name":"level_two_mgrs","type":"string"},{"name":"level_three_mgrs","type":"string"}]}"""
    OBJ_SCHEMA = schema.parse(OBJ_SCHEMA_STR)
    return read_orig_datum(OBJ_SCHEMA, encoded_datum)


def get_schemas(base_name):
    REP_SCHEMA_STR = open("../obj_defs/"+base_name+"_response.json", "r").read()
    REP_SCHEMA = schema.parse(REP_SCHEMA_STR)
    REQ_SCHEMA_STR = open("../obj_defs/"+base_name+"_request.json", "r").read()
    REQ_SCHEMA = schema.parse(REQ_SCHEMA_STR)
    
    return (REQ_SCHEMA, REP_SCHEMA)

# NOTE: could maybe also put the get schemas in here
def post_then_get(REQ_SCHEMA,REP_SCHEMA,datum,location):
    #print REQ_SCHEMA, REP_SCHEMA, datum, location
    encoded_datum = write_datum(REQ_SCHEMA, datum)    
    retval  = post_to_gaia_read(encoded_datum, location)

    return read_datum(REP_SCHEMA, retval)


###### Main calls ##########
def do_register_type_point():
    # schema...need to pretend that its a json encoded object
    #jsonobj =  '{"type":"record","name":"register_type_request","fields":[{"type_definition":"JSON TYPE DESCRIPTION"}]}'
    #jsonobj = '{"type_definition":"{\\"type\\":\\"record\\",\\"name\\":\\"point\\",\\"fields\\":[{\\"name\\":\\"x\\",\\"type\\":\\"double\\"},{\\"name\\":\\"y\\",\\"type\\":\\"double\\"}]}"}'

    (REQ_SCHEMA,REP_SCHEMA) = get_schemas("register_type")

    datum = {}
    datum["type_definition"] = """{"type":"record","name":"point","fields":[{"name":"x","type":"double"},{"name":"y","type":"double"}]}"""
    datum["annotation"] = ""

    return post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/registertype")

def do_register_type_big_point():
    # NOTE: should we use a int or a long for the timestamp? we can't use double because that will expect a decimal value
    #jsonobj = '{"type_definition":"{\\"type\\":\\"record\\",\\"name\\":\\"point\\",\\"fields\\":[{\\"name\\":\\"msg_id\\",\\"type\\":\\"string\\"},{\\"name\\":\\"x\\",\\"type\\":\\"double\\"},{\\"name\\":\\"y\\",\\"type\\":\\"double\\"},{\\"name\\":\\"timestamp\\",\\"type\\":\\"int\\"},{\\"name\\":\\"source\\",\\"type\\":\\"string\\"},{\\"name\\":\\"group_id\\",\\"type\\":\\"string\\"}]}"}'

    (REQ_SCHEMA,REP_SCHEMA) = get_schemas("register_type")

    datum = {}
    datum["type_definition"] = """{"type":"record","name":"point","fields":[{"name":"msg_id","type":"string"},{"name":"x","type":"double"},{"name":"y","type":"double"},{"name":"timestamp","type":"int"},{"name":"source","type":"string"},{"name":"group_id","type":"string"}]}"""
    datum["annotation"] = "msg_id"

    return post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/registertype")

def do_register_type_gis_point():
    # NOTE: should we use a int or a long for the timestamp? we can't use double because that will expect a decimal value
    #jsonobj = '{"type_definition":"{\\"type\\":\\"record\\",\\"name\\":\\"point\\",\\"fields\\":[{\\"name\\":\\"msg_id\\",\\"type\\":\\"string\\"},{\\"name\\":\\"x\\",\\"type\\":\\"double\\"},{\\"name\\":\\"y\\",\\"type\\":\\"double\\"},{\\"name\\":\\"timestamp\\",\\"type\\":\\"int\\"},{\\"name\\":\\"source\\",\\"type\\":\\"string\\"},{\\"name\\":\\"group_id\\",\\"type\\":\\"string\\"}]}"}'
    REP_SCHEMA_STR = open("../obj_defs/register_type_response.json", "r").read()
    REP_SCHEMA = schema.parse(REP_SCHEMA_STR)
    SCHEMA_STR = open("../obj_defs/register_type_request.json", "r").read()
    SCHEMA = schema.parse(SCHEMA_STR)
    
    datum = {}
    datum["type_definition"] = """{"type":"record","name":"Point","fields":[{"name":"x","type":"double"},{"name":"y","type":"double"},{"name":"timestamp","type":"double"},{"name":"tag_id","type":"double"},{"name":"derived","type":"double"},{"name":"msg_id","type":"string"},{"name":"group_id","type":"string"},{"name":"level_one_mgrs","type":"string"},{"name":"level_two_mgrs","type":"string"},{"name":"level_three_mgrs","type":"string"},{"name":"level_final_mgrs","type":"string"}]}"""
    datum["annotation"] = "msg_id"

    retval  = post_to_gaia_read(write_datum(SCHEMA, datum), "/registertype")
    
    return read_datum(REP_SCHEMA,retval)

def do_register_type_bytes_point():
    # register a point that has a byte vector [NOTE: type "bytes" is a sequence (i.e. vector) of byte values]
    (REQ_SCHEMA,REP_SCHEMA) = get_schemas("register_type")
    
    # build the datum (internal, object)
    datum = {}
    datum["type_definition"] = """{"type":"record","name":"point","fields":[{"name":"msg_id","type":"string"},{"name":"x","type":"double"},{"name":"y","type":"double"},{"name":"timestamp","type":"int"},{"name":"source","type":"string"},{"name":"group_id","type":"string"},{"name":"bytes_data","type":"bytes"}]}"""
    datum["annotation"] = "msg_id"

    return post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/registertype")

#get set
def do_get_set(set_id, start, end):
    (REQ_SCHEMA,REP_SCHEMA) = get_schemas("get_set")

    datum = {}
    datum["set_id"] = set_id
    datum["start"] = start
    datum["end"] = end

    return post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/getset")

def do_new_set(type_id, set_id):
    #schema
    #jsonobj =  '{"type":"record","name":"new_set_request","fields":[{"name":"set_id","type":"string"},{"name":"type_id","type":"string"}]}'
    #jsonobj = '{"set_id":"'+set_id+'","type_id":"'+type_id+'"}'
    REP_SCHEMA_STR = open("../obj_defs/new_set_response.json", "r").read()
    REP_SCHEMA = schema.parse(REP_SCHEMA_STR)
    #SCHEMA_STR = """{"type":"record","name":"new_set_request","fields":[{"name":"set_id","type":"string"},{"name":"type_id","type":"string"}]}"""    
    SCHEMA_STR = open("../obj_defs/new_set_request.json", "r").read()
    SCHEMA = schema.parse(SCHEMA_STR)

    datum = {}
    datum["set_id"] = set_id
    datum["type_id"] = type_id

    jsonobj = write_datum(SCHEMA, datum)

    retval  = post_to_gaia_read(jsonobj, "/newset")

    return read_datum(REP_SCHEMA, retval)

# NOTE: we should remove the type_id from this, should be apparent from set_id
def do_add_point(set_id, x, y):
    #objdata =  '{\\"x\\":'+str(x)+',\\"y\\":'+str(y)+'}'
    #jsonobj = '{"object_data":"'+objdata+'","set_id":"'+set_id+'"}'
    #schema for this object/datum [outer request]
    REQ_SCHEMA_STR = """{"type":"record","name":"add_object_request","fields":[{"name":"object_data","type":"bytes"},{"name":"set_id","type":"string"}]}"""
    REQ_SCHEMA = schema.parse(REQ_SCHEMA_STR)
    REP_SCHEMA_STR = open("../obj_defs/add_object_response.json", "r").read()
    REP_SCHEMA = schema.parse(REP_SCHEMA_STR)
    OBJ_SCHEMA_STR = """{"type":"record","name":"point","fields":[{"name":"x","type":"double"},{"name":"y","type":"double"}]}"""
    OBJ_SCHEMA = schema.parse(OBJ_SCHEMA_STR)
    
    # build the datum (internal, object)
    datum = {}
    datum['x'] = x
    datum['y'] = y
    
    objdata = write_datum(OBJ_SCHEMA, datum)

    # outer datum
    req_datum = {}
    req_datum['object_data'] = objdata
    req_datum['set_id'] = set_id

    jsonobj = write_datum(REQ_SCHEMA, req_datum)

    retval  = post_to_gaia_read(jsonobj, "/add")
    
    return read_datum(REP_SCHEMA, retval)

def do_add_big_point(set_id, msg_id, x, y, timestamp, source, group_id):
    #jsonobj =  '{"type":"record","name":"add_object_request","fields":[{"name":"type_id","type":"string"},{"name":"type_id","type":"string"}]}'    
    #objdata =  '{\\"msg_id\\":\\"'+str(msg_id)+'\\",\\"x\\":'+str(x)+',\\"y\\":'+str(y)+',\\"timestamp\\":'+str(timestamp)+',\\"source\\":\\"'+str(source)+'\\",\\"group_id\\":\\"'+str(group_id)+'\\"}'
    #jsonobj = '{"object_data":"'+objdata+'","set_id":"'+set_id+'"}'
    

    #REQ_SCHEMA_STR = """{"type":"record","name":"add_object_request","fields":[{"name":"object_data","type":"bytes"},{"name":"set_id","type":"string"}]}"""
    REQ_SCHEMA_STR = open("../obj_defs/add_object_request.json", "r").read() # read the schema string
    REQ_SCHEMA = schema.parse(REQ_SCHEMA_STR)
    REP_SCHEMA_STR = open("../obj_defs/add_object_response.json", "r").read()
    REP_SCHEMA = schema.parse(REP_SCHEMA_STR)
    OBJ_SCHEMA_STR = """{"type":"record","name":"point","fields":[{"name":"msg_id","type":"string"},{"name":"x","type":"double"},{"name":"y","type":"double"},{"name":"timestamp","type":"int"},{"name":"source","type":"string"},{"name":"group_id","type":"string"}]}"""
    OBJ_SCHEMA = schema.parse(OBJ_SCHEMA_STR)
    
    # build the datum (internal, object)
    datum = {}
    datum["set_id"] = set_id
    datum["msg_id"] = msg_id
    datum["x"] = x
    datum["y"] = y
    datum["timestamp"] = timestamp
    datum["source"] = source
    datum["group_id"] = group_id
    
    objdata = write_datum(OBJ_SCHEMA, datum)
    #print "objdata", len(objdata)
    
    # outer datum
    req_datum = {}
    req_datum['object_data'] = objdata
    req_datum['set_id'] = set_id

    jsonobj = write_datum(REQ_SCHEMA, req_datum)
    #print "request object", len(jsonobj)

    retval  = post_to_gaia_read(jsonobj, "/add")
    return read_datum(REP_SCHEMA, retval)

def do_add_gis_point(set_id, msg_id, x, y, timestamp, tag_id, derived, group_id, level_one_mgrs, level_two_mgrs, level_three_mgrs, level_final_mgrs):
    REQ_SCHEMA_STR = open("../obj_defs/add_object_request.json", "r").read() # read the schema string
    REQ_SCHEMA = schema.parse(REQ_SCHEMA_STR)
    REP_SCHEMA_STR = open("../obj_defs/add_object_response.json", "r").read()
    REP_SCHEMA = schema.parse(REP_SCHEMA_STR)

    OBJ_SCHEMA_STR = """{"type":"record","name":"Point","fields":[{"name":"x","type":"double"},{"name":"y","type":"double"},{"name":"timestamp","type":"double"},{"name":"tag_id","type":"double"},{"name":"derived","type":"double"},{"name":"msg_id","type":"string"},{"name":"group_id","type":"string"},{"name":"level_one_mgrs","type":"string"},{"name":"level_two_mgrs","type":"string"},{"name":"level_three_mgrs","type":"string"},{"name":"level_final_mgrs","type":"string"}]}"""
    OBJ_SCHEMA = schema.parse(OBJ_SCHEMA_STR)
    
    # build the datum (internal, object)
    datum = {}
    datum["set_id"] = set_id
    datum["msg_id"] = msg_id
    datum["x"] = x
    datum["y"] = y
    datum["timestamp"] = timestamp
    datum["tag_id"] = tag_id
    datum["group_id"] = group_id
    datum["derived"] = derived
    datum["level_one_mgrs"] = level_one_mgrs
    datum["level_two_mgrs"] = level_two_mgrs
    datum["level_three_mgrs"] = level_three_mgrs
    datum["level_final_mgrs"] = level_final_mgrs
    
    objdata = write_datum(OBJ_SCHEMA, datum)
    
    # outer datum
    req_datum = {}
    req_datum['object_data'] = objdata
    req_datum['set_id'] = set_id

    jsonobj = write_datum(REQ_SCHEMA, req_datum)

    retval  = post_to_gaia_read(jsonobj, "/add")
    return read_datum(REP_SCHEMA, retval)

def do_add_bytes_point(set_id, msg_id, x, y, timestamp, source, group_id, bytes_data):
    (REQ_SCHEMA,REP_SCHEMA) = get_schemas("add_object")
    OBJ_SCHEMA_STR = """{"type":"record","name":"point","fields":[{"name":"msg_id","type":"string"},{"name":"x","type":"double"},{"name":"y","type":"double"},{"name":"timestamp","type":"int"},{"name":"source","type":"string"},{"name":"group_id","type":"string"},{"name":"bytes_data","type":"bytes"}]}"""
    OBJ_SCHEMA = schema.parse(OBJ_SCHEMA_STR)
    
    # build the datum (internal, object)
    datum = {}
    datum["set_id"] = set_id
    datum["msg_id"] = msg_id
    datum["x"] = x
    datum["y"] = y
    datum["timestamp"] = timestamp
    datum["source"] = source
    datum["group_id"] = group_id
    datum["bytes_data"] = bytes_data
    
    objdata = write_datum(OBJ_SCHEMA, datum)
        
    # outer datum
    req_datum = {}
    req_datum['object_data'] = objdata
    req_datum['set_id'] = set_id

    return post_then_get(REQ_SCHEMA, REP_SCHEMA, req_datum, "/add")
#get set
def do_get_set(set_id, start, end):
    #jsonobj = '{"start":'+str(start)+',"end":'+str(end)+',"set_id":"'+set_id+'"}'
    (REQ_SCHEMA,REP_SCHEMA) = get_schemas("get_set")

    datum = {}
    datum["set_id"] = set_id
    datum["start"] = start
    datum["end"] = end

    return post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/getset")

# the {x|y}_attr_name defines a mapping into x-y plane and then the min/max define the box
def do_bounding_box(min_x, max_x, min_y, max_y, x_attr_name, y_attr_name, set_id, result_set_id, user_auth=""):
    #jsonobj = '{"min_x":'+str(min_x)+',"max_x":'+str(max_x)+',"min_y":'+str(min_y)+',"max_y":'+str(max_y)+',"x_attr_name":"'+x_attr_name+'","y_attr_name":"'+y_attr_name+'","set_id":"'+set_id+'","result_set_id":"'+result_set_id+'"}'
    (REQ_SCHEMA,REP_SCHEMA) = get_schemas("bounding_box")

    datum = {}
    datum["set_id"] = set_id
    datum["min_x"] = min_x
    datum["max_x"] = max_x
    datum["min_y"] = min_y
    datum["max_y"] = max_y
    datum["x_attr_name"] = x_attr_name
    datum["y_attr_name"] = y_attr_name
    datum["result_set_id"] = result_set_id
    datum["user_auth_string"] = user_auth
    
    return post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/boundingbox")

#equi-join
def do_join(left_set, left_attr, right_set, right_attr, result_type, result_set):
    # get the schemas
    (REQ_SCHEMA,REP_SCHEMA) = get_schemas("join")

    datum = {}
    datum["left_set"] = left_set
    datum["left_attr"] = left_attr
    datum["right_set"] = right_set
    datum["right_attr"] = right_attr
    datum["result_type"] = result_type
    datum["result_set"] = result_set
    
    return post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/join")

#group by [attributes is a list]
def do_group_by(set_id, attributes, user_auth=""):    
    (REQ_SCHEMA,REP_SCHEMA) = get_schemas("group_by")
    
    datum = {}
    datum["set_id"] = set_id
    datum["attributes"] = attributes
    datum["user_auth_string"] = user_auth
    
    return post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/groupby")

#exit
def do_exit(exit_type):
    (REQ_SCHEMA,REP_SCHEMA) = get_schemas("exit")
    
    datum = {}
    datum["exit_type"] = exit_type
    
    return post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/exit")

#make bloom
def do_make_bloom(set_id, attribute):
    # get the schemas
    (REQ_SCHEMA,REP_SCHEMA) = get_schemas("make_bloom")
    
    datum = {}
    datum["set_id"] = set_id
    datum["attribute"] = attribute
    
    return post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/makebloom")
    
# This assumes that 'x' and 'y' are equal length lists
def do_bulk_add_point(set_id, x_list, y_list):    
    (REQ_SCHEMA,REP_SCHEMA) = get_schemas("bulk_add")
    
    obj_list_encoded = []
    OBJ_SCHEMA_STR = """{"type":"record","name":"point","fields":[{"name":"x","type":"double"},{"name":"y","type":"double"}]}"""
    OBJ_SCHEMA = schema.parse(OBJ_SCHEMA_STR)
    for i in range(0,len(x_list)):
        datum = {}
        datum['x'] = x_list[i]
        datum['y'] = y_list[i]
        obj_list_encoded.append(write_datum(OBJ_SCHEMA, datum))

    req_datum = {}
    req_datum["set_id"] = set_id
    req_datum["list"] = obj_list_encoded

    return post_then_get(REQ_SCHEMA, REP_SCHEMA, req_datum, "/bulkadd")    

# This assumes equal length lists
def do_bulk_add_big_point(set_id, msg_id_list, x_list, y_list, timestamp_list, source_list, group_id_list):    
    (REQ_SCHEMA,REP_SCHEMA) = get_schemas("bulk_add")
    
    obj_list_encoded = []
    OBJ_SCHEMA_STR = """{"type":"record","name":"point","fields":[{"name":"msg_id","type":"string"},{"name":"x","type":"double"},{"name":"y","type":"double"},{"name":"timestamp","type":"int"},{"name":"source","type":"string"},{"name":"group_id","type":"string"}]}"""
    OBJ_SCHEMA = schema.parse(OBJ_SCHEMA_STR)
    for msg_id,x,y,timestamp,source,group_id in zip(msg_id_list,x_list,y_list,timestamp_list,source_list,group_id_list):
        datum = {}
        datum['msg_id'] = msg_id
        datum['x'] = x
        datum['y'] = y
        datum['timestamp'] = timestamp
        datum['source'] = source
        datum['group_id'] = group_id
        obj_list_encoded.append(write_datum(OBJ_SCHEMA, datum))

    req_datum = {}
    req_datum["set_id"] = set_id
    req_datum["list"] = obj_list_encoded

    return post_then_get(REQ_SCHEMA, REP_SCHEMA, req_datum, "/bulkadd")    

#store group by [group_map is a map returned by group by
def do_store_group_by(set_id, attribute, group_map, sort, sort_attribute, user_auth=""):    
    (REQ_SCHEMA,REP_SCHEMA) = get_schemas("store_group_by")
    
    datum = {}
    datum["set_id"] = set_id
    datum["attribute"] = attribute
    datum["group_map"] = group_map
    datum["sort"] = sort
    datum["sort_attribute"] = sort_attribute
    datum["user_auth_string"] = user_auth
    
    return post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/storegroupby")

#histogram call
def do_histogram(set_id, attribute, interval, start, end, user_auth=""):
    # get the schemas
    (REQ_SCHEMA,REP_SCHEMA) = get_schemas("histogram")
        
    datum = {}
    datum["set_id"] = set_id
    datum["attribute"] = attribute
    datum["interval"] = interval
    datum["start"] = start
    datum["end"] = end
    datum["user_auth_string"] = user_auth

    return post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/histogram")

#filter then histogram call
def do_filter_then_histogram(set_id, filter_attribute, filter_values, histogram_attribute, interval, start, end, user_auth=""):
    # get the schemas
    (REQ_SCHEMA,REP_SCHEMA) = get_schemas("filter_then_histogram")
        
    datum = {}
    datum["set_id"] = set_id
    datum["filter_attribute"] = filter_attribute
    datum["histogram_attribute"] = histogram_attribute
    datum["filter"] = filter_values
    datum["interval"] = interval
    datum["start"] = start
    datum["end"] = end
    datum["user_auth_string"] = user_auth

    return post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/filterthenhistogram")

#max min call
def do_max_min(set_id, attribute, user_auth=""):
    # get the schemas
    (REQ_SCHEMA,REP_SCHEMA) = get_schemas("max_min")
    
    datum = {}
    datum["set_id"] = set_id
    datum["attribute"] = attribute
    datum["user_auth_string"] = user_auth

    return post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/maxmin")

# filter by bounds; return a count of how many objects have lower < attribute < upper
def do_filter_by_bounds(set_id, lower_bound, attribute, upper_bound, result_set_id, user_auth=""):
    # get the schemas
    (REQ_SCHEMA,REP_SCHEMA) = get_schemas("filter_by_bounds")
    
    datum = {}
    datum["set_id"] = set_id
    datum["attribute"] = attribute
    datum["lower_bound"] = lower_bound
    datum["upper_bound"] = upper_bound
    datum["result_set_id"] = result_set_id
    datum["user_auth_string"] = user_auth

    return post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/filterbybounds")

# filter by list; return a count of how many objects have their attribute value in the list
# attribute map is attribute to list of values
def do_filter_by_list(set_id, attribute_map, result_set_id, user_auth=""):
    (REQ_SCHEMA,REP_SCHEMA) = get_schemas("filter_by_list")
    
    datum = {}
    datum["set_id"] = set_id
    datum["attribute_map"] = attribute_map
    datum["result_set_id"] = result_set_id
    datum["user_auth_string"] = user_auth

    return post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/filterbylist")

# filter by nai; return a count of how many objects are within the polygon
def do_filter_by_nai(set_id, x_attribute, x_vector, y_attribute, y_vector, result_set_id, user_auth=""):
    (REQ_SCHEMA,REP_SCHEMA) = get_schemas("filter_by_nai")
    
    datum = {}
    datum["set_id"] = set_id
    datum["x_attribute"] = x_attribute
    datum["x_vector"] = x_vector
    datum["y_attribute"] = y_attribute
    datum["y_vector"] = y_vector
    datum["result_set_id"] = result_set_id
    datum["user_auth_string"] = user_auth
    
    return post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/filterbynai")

# filter by radius; return a count of how many objects are within a designated circle
def do_filter_by_radius(set_id, x_attribute, y_attribute, x_center, y_center, radius, result_set_id, user_auth=""):
    (REQ_SCHEMA,REP_SCHEMA) = get_schemas("filter_by_radius")
    
    datum = {}
    datum["set_id"] = set_id
    datum["x_attribute"] = x_attribute
    datum["y_attribute"] = y_attribute
    datum["x_center"] = x_center
    datum["y_center"] = y_center
    datum["radius"] = radius
    datum["result_set_id"] = result_set_id
    datum["user_auth_string"] = user_auth
    
    return post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/filterbyradius")

# clear
def do_clear(set_id):
    (REQ_SCHEMA,REP_SCHEMA) = get_schemas("clear")
    
    datum = {}
    datum["set_id"] = set_id

    return post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/clear")

# make a copy of a set
def do_copy_set(new_set_id, original_set_id, selector, type_id):
    (REQ_SCHEMA,REP_SCHEMA) = get_schemas("copy_set")
    
    datum = {}
    datum["new_set_id"] = new_set_id
    datum["original_set_id"] = original_set_id
    datum["selector"] = selector
    datum["type_id"] = type_id
    
    return post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/copyset")

# cluster
def do_cluster(world_set, subworld_set, result_set, shared_attribute, cluster_attribute):
    (REQ_SCHEMA,REP_SCHEMA) = get_schemas("cluster")
    
    datum = {}
    datum["world_set"] = world_set
    datum["subworld_set"] = subworld_set
    datum["result_set"] = result_set
    datum["shared_attribute"] = shared_attribute
    datum["cluster_attribute"] = cluster_attribute
    datum["list"] = []
    datum["first_pass"] = True

    return post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/cluster")

# stats
def do_stats(set_id):
    (REQ_SCHEMA,REP_SCHEMA) = get_schemas("stats")

    datum = {}
    datum["set_id"] = set_id
    
    return post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/stats")

# select
def do_select(set_id, result_set_id, expression, user_auth=""):
    (REQ_SCHEMA,REP_SCHEMA) = get_schemas("select")
    
    datum = {}
    datum["set_id"] = set_id
    datum["result_set_id"] = result_set_id
    datum["expression"] = expression
    datum["user_auth_string"] = user_auth
    
    return post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/select")

# plot2d
def do_plot2d(set_id, result_set_id, min_x, max_x, min_y, max_y, x_attr_name, y_attr_name, width, height, projection, image_attribute, user_auth=""):
    (REQ_SCHEMA,REP_SCHEMA) = get_schemas("plot2d")
    
    datum = {}    
    datum["min_x"] = min_x
    datum["max_x"] = max_x
    datum["min_y"] = min_y
    datum["max_y"] = max_y
    datum["x_attr_name"] = x_attr_name
    datum["y_attr_name"] = y_attr_name
    datum["width"] = width
    datum["height"] = height
    datum["projection"] = projection
    datum["image_attribute"] = image_attribute
    datum["result_set_id"] = result_set_id
    datum["set_id"] = set_id
    datum["user_auth_string"] = user_auth
    
        
    return post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/plot2d")

# plot2d
def do_plot2d_multiple(set_ids, colors, min_x, max_x, min_y, max_y, x_attr_name, y_attr_name, width, height, projection, user_auth=""):
    (REQ_SCHEMA,REP_SCHEMA) = get_schemas("plot2d_multiple")
    
    datum = {}    
    datum["min_x"] = min_x
    datum["max_x"] = max_x
    datum["min_y"] = min_y
    datum["max_y"] = max_y
    datum["x_attr_name"] = x_attr_name
    datum["y_attr_name"] = y_attr_name
    datum["width"] = width
    datum["height"] = height
    datum["projection"] = projection
    datum["set_ids"] = set_ids
    datum["colors"] = colors
    datum["user_auth_string"] = user_auth
    
        
    return post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/plot2dmultiple")


#initial join setup for the incremental join
def do_join_setup(left_set, left_attr, right_set, right_attr, subset_id):
    # get the schemas
    (REQ_SCHEMA,REP_SCHEMA) = get_schemas("join_setup")

    datum = {}
    datum["left_set"] = left_set
    datum["left_attr"] = left_attr
    datum["right_set"] = right_set
    datum["right_attr"] = right_attr
    datum["subset_id"] = subset_id
    
    return post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/joinsetup")

# join incremental
def do_join_incremental(left_subset, left_attr, left_index, right_set, right_attr, result_set, result_type):
    # get the schemas
    (REQ_SCHEMA,REP_SCHEMA) = get_schemas("join_incremental")
    
    datum = {}
    datum["left_subset"] = left_subset
    datum["left_attr"] = left_attr
    datum["left_index"] = left_index
    datum["right_set"] = right_set
    datum["right_attr"] = right_attr
    datum["data_map"] = {}
    datum["result_set"] = result_set
    datum["result_type"] = result_type

    return post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/joinincremental")

#set info
def do_set_info(set_id):
    (REQ_SCHEMA,REP_SCHEMA) = get_schemas("set_info")
    
    datum = {}
    datum["set_id"] = set_id
    
    return post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/setinfo")

#sort it
def do_sort(set_id, attribute):
    (REQ_SCHEMA,REP_SCHEMA) = get_schemas("sort")
    
    datum = {}
    datum["set_id"] = set_id
    datum["attribute"] = attribute
    
    return post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/sort")

#get set sorted
def do_get_set_sorted(set_id, start, end):
    (REQ_SCHEMA,REP_SCHEMA) = get_schemas("get_set_sorted")
    # this call actually returns the get_set_response
    (GS_REQ_SCHEMA,GS_REP_SCHEMA) = get_schemas("get_set")

    datum = {}
    datum["set_id"] = set_id
    datum["start"] = start
    datum["end"] = end

    return post_then_get(REQ_SCHEMA, GS_REP_SCHEMA, datum, "/getsetsorted")

#register a range trigger
def do_register_trigger_range(trigger_id, set_ids, attr, minval, maxval, id_attr):
    (REQ_SCHEMA,REP_SCHEMA) = get_schemas("register_trigger_range")

    datum = {}
    datum["request_id"] = trigger_id
    datum["set_ids"] = set_ids
    datum["attr"] = attr
    datum["lowest"] = minval
    datum["highest"] = maxval
    datum["id_attr"] = id_attr
    
    return post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/registertriggerrange")

#register a range trigger
def do_register_trigger_nai(trigger_id, set_ids, xattr, xvals, yattr, yvals, id_attr):
    (REQ_SCHEMA,REP_SCHEMA) = get_schemas("register_trigger_nai")

    datum = {}
    datum["request_id"] = trigger_id
    datum["set_ids"] = set_ids
    datum["x_attribute"] = xattr
    datum["x_vector"] = xvals
    datum["y_attribute"] = yattr
    datum["y_vector"] = yvals
    datum["id_attr"] = id_attr
    
    return post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/registertriggernai")

def do_read_trigger_msg(encoded_datum):
    REP_SCHEMA_STR = open("../obj_defs/trigger_notification.json", "r").read()
    REP_SCHEMA = schema.parse(REP_SCHEMA_STR)

    return read_orig_datum(REP_SCHEMA, encoded_datum)

def do_update_set_ttl(set_id, ttl):
    (REQ_SCHEMA,REP_SCHEMA) = get_schemas("update_set_ttl")

    datum = {}
    datum["set_id"] = set_id
    datum["ttl"] = ttl

    return post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/updatesetttl")

def do_authenticate_users(user_auth_strings,set_ids):
    (REQ_SCHEMA,REP_SCHEMA) = get_schemas("authenticate_users")

    datum = {}
    datum["user_auth_strings"] = user_auth_strings
    datum["set_ids"] = set_ids

    return post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/authenticateusers")
