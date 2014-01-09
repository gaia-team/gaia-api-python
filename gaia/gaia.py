import httplib
import uuid
import os
import json

from avro import schema, datafile, io
import StringIO

import sys

if sys.version_info >= (2, 7):
    import collections
else:
    import ordereddict as collections



class Gaia:

    def __init__(self, gaia_ip="127.0.0.1", gaia_port="9191", encoding="BINARY", connection='HTTP'):
        
        self.gaia_ip = gaia_ip
        self.gaia_port = gaia_port
        self.encoding = encoding
        self.connection = connection

    # constant
    END_OF_SET = -9999

    # schemas for helper types
    # including OBJECT_ID
    point_schema_str = """{"type":"record","name":"point","fields":[{"name":"x","type":"double"},{"name":"y","type":"double"},{"name":"OBJECT_ID","type":"string"}]}"""
    big_point_schema_str = """{"type":"record","name":"point","fields":[{"name":"msg_id","type":"string"},{"name":"x","type":"double"},{"name":"y","type":"double"},{"name":"TIMESTAMP","type":"double"},{"name":"source","type":"string"},{"name":"group_id","type":"string"},{"name":"OBJECT_ID","type":"string"}]}"""
    gis_point_schema_str = """{"type":"record","name":"Point","fields":[{"name":"x","type":"double"},{"name":"y","type":"double"},{"name":"timestamp","type":"double"},{"name":"tag_id","type":"double"},{"name":"derived","type":"double"},{"name":"msg_id","type":"string"},{"name":"group_id","type":"string"},{"name":"level_one_mgrs","type":"string"},{"name":"level_two_mgrs","type":"string"},{"name":"level_three_mgrs","type":"string"},{"name":"level_final_mgrs","type":"string"},{"name":"OBJECT_ID","type":"string"}]}"""
    bytes_point_schema_str = """{"type":"record","name":"point","fields":[{"name":"msg_id","type":"string"},{"name":"x","type":"double"},{"name":"y","type":"double"},{"name":"timestamp","type":"int"},{"name":"source","type":"string"},{"name":"group_id","type":"string"},{"name":"bytes_data","type":"bytes"},{"name":"OBJECT_ID","type":"string"}]}"""
    bigger_point_schema_str = """{"type":"record","name":"point","fields":[{"name":"ARTIFACTID","type":"string"},{"name":"x","type":"double"},{"name":"y","type":"double"},{"name":"TIMESTAMP","type":"double"},{"name":"DATASOURCE","type":"string"},{"name":"DATASOURCESUB","type":"string"},{"name":"OBJECTAUTH", "type" : "string"},{"name": "AUTHOR", "type":"string"},{"name":"DATASOURCEKEY","type":"string"},{"name":"OBJECT_ID","type":"string"}]}"""
    twitter_point_schema_str = """{"type":"record","name":"point","fields":[{"name":"ARTIFACTID","type":"string"},{"name":"x","type":"double"},{"name":"y","type":"double"},{"name":"TIMESTAMP","type":"double"},{"name":"DATASOURCE","type":"string"},{"name":"DATASOURCESUB","type":"string"},{"name":"KEYWORD","type":"string"},{"name":"OBJECTAUTH", "type" : "string"},{"name": "AUTHOR", "type":"string"},{"name":"DATASOURCEKEY","type":"string"},{"name":"OBJECT_ID","type":"string"}]}"""

    #point_schema_str = """{"type":"record","name":"point","fields":[{"name":"x","type":"double"},{"name":"y","type":"double"}]}"""
    #big_point_schema_str = """{"type":"record","name":"point","fields":[{"name":"msg_id","type":"string"},{"name":"x","type":"double"},{"name":"y","type":"double"},{"name":"timestamp","type":"int"},{"name":"source","type":"string"},{"name":"group_id","type":"string"}]}"""
    #gis_point_schema_str = """{"type":"record","name":"Point","fields":[{"name":"x","type":"double"},{"name":"y","type":"double"},{"name":"timestamp","type":"double"},{"name":"tag_id","type":"double"},{"name":"derived","type":"double"},{"name":"msg_id","type":"string"},{"name":"group_id","type":"string"},{"name":"level_one_mgrs","type":"string"},{"name":"level_two_mgrs","type":"string"},{"name":"level_three_mgrs","type":"string"},{"name":"level_final_mgrs","type":"string"}]}"""
    #bytes_point_schema_str = """{"type":"record","name":"point","fields":[{"name":"msg_id","type":"string"},{"name":"x","type":"double"},{"name":"y","type":"double"},{"name":"timestamp","type":"int"},{"name":"source","type":"string"},{"name":"group_id","type":"string"},{"name":"bytes_data","type":"bytes"}]}"""

    # Parse common schemas, others parsed on demand.
    point_schema = schema.parse(point_schema_str)
    big_point_schema = schema.parse(big_point_schema_str)
    gis_point_schema = None # schema.parse(gis_point_schema_str)
    bytes_point_schema = None # schema.parse(bytes_point_schema_str)
    bigger_point_schema = None # schema.parse(bigger_point_schema_str)
    twitter_point_schema = schema.parse(twitter_point_schema_str)

    loaded_schemas = {} # dict of previously loaded schemas, populated by get_schemas()

    ####### helpers #############
    def post_to_gaia_read(self,jsonobj,url):
        params = jsonobj

        if self.encoding == 'BINARY':
            headers = {"Content-type": "application/octet-stream",
                       "Accept": "application/octet-stream"}

        elif self.encoding == 'JSON':
            headers = {"Content-type": "application/json",
                       "Accept": "application/json"}

        # NOTE: Creating a new httplib.HTTPConnection is suprisingly just as
        #       fast as reusing a persistent one and has the advantage of
        #       fully retrying from scratch if the connection fails.

        nurl = ""
        if len(self.gaia_port) > 0:
            if (self.connection == 'HTTP'):
                conn = httplib.HTTPConnection(str(self.gaia_ip) + ":" + self.gaia_port)
            else:
                conn = httplib.HTTPSConnection(str(self.gaia_ip) + ":" + self.gaia_port)
            nurl = url
        else:
            if (self.connection == 'HTTP'):
                conn = httplib.HTTPConnection(str(self.gaia_ip))
            else:
                conn = httplib.HTTPSConnection(str(self.gaia_ip))
            nurl = "/gaia2"+str(url)

        #print nurl
        conn.request("POST", nurl, params, headers)
        #print "conn.request"

        r1 = conn.getresponse()        
        data1 = r1.read()
        #print "after read"
        #print 'response size: ',len(data1)

        return  str(data1)

    # return binary encoding of the datum
    def write_datum(self, SCHEMA, datum):
        # build the encoder; this output is where the data will be written
        if self.encoding == 'BINARY':
            output = StringIO.StringIO()
            be = io.BinaryEncoder(output)
        
            # Create a 'record' (datum) writer
            writer = io.DatumWriter(SCHEMA)
            writer.write(datum, be)

            return output.getvalue()

        elif self.encoding == 'JSON':

            data_str = json.dumps(datum)

            return data_str

    def read_orig_datum(self, SCHEMA, encoded_datum, encoding=None):
        if encoding == None:
            encoding = self.encoding

        if encoding == 'BINARY':
            output = StringIO.StringIO(encoded_datum)
            bd = io.BinaryDecoder(output)

            reader = io.DatumReader(SCHEMA)
            out = reader.read(bd) # read, give a decoder

            return out
        elif encoding == 'JSON':
            data_str = json.loads(encoded_datum.replace('\\U','\\u'))

            return data_str


    def read_datum(self, SCHEMA, encoded_datum):
        #first parse the gaia_response message
        if "gaia_response" in self.loaded_schemas:
            REP_SCHEMA = self.loaded_schemas["gaia_response"]["REP_SCHEMA"]
        else:
            REP_SCHEMA_STR = open("../obj_defs/gaia_response.json","r").read()
            REP_SCHEMA     = schema.parse(REP_SCHEMA_STR)

            self.loaded_schemas["gaia_response"] = { "REP_SCHEMA_STR" : REP_SCHEMA_STR,
                                                     "REP_SCHEMA"     : REP_SCHEMA }

        resp = self.read_orig_datum(REP_SCHEMA, encoded_datum)

        #now parse the actual response if there is no error
        #NOTE: DATA_SCHEMA should be equivalent to SCHEMA but is NOT for get_set_sorted
        stype = resp['data_type']
        #print 'read_datum, embedded data_type: ',stype
        if stype == 'none':
            out = collections.OrderedDict()
        else:
            #DATA_SCHEMA_STR = open("../obj_defs/%s.json"%(stype), "r").read()
            #DATA_SCHEMA = schema.parse(DATA_SCHEMA_STR)
            #out = read_orig_datum(DATA_SCHEMA, resp['data'])
            if self.encoding == 'JSON':
                out = self.read_orig_datum(SCHEMA, resp['data_str'])
            elif self.encoding == 'BINARY':
                out = self.read_orig_datum(SCHEMA, resp['data'])

            #print 'read_orig_datum, size = ',len(resp['data'])
        
        del resp['data']

        out['status_info'] = resp

        return out

    def read_point(self, encoded_datum):
        if self.point_schema is None:
            self.point_schema = schema.parse(self.point_schema_str)

        return self.read_orig_datum(self.point_schema, encoded_datum)

    def read_big_point(self, encoded_datum, encoding=None):
        if self.big_point_schema is None:
            self.big_point_schema = schema.parse(self.big_point_schema_str)

        return self.read_orig_datum(self.big_point_schema, encoded_datum, encoding)

    # this point is designed to look like "Point"
    def read_gis_point(self, encoded_datum):
        #OBJ_SCHEMA_STR = """{"type":"record","name":"point","fields":[{"name":"x","type":"double"},{"name":"y","type":"double"},{"name":"timestamp","type":"double"},{"name":"tag_id","type":"double"},{"name":"derived","type":"double"},{"name":"msg_id","type":"string"},{"name":"group_id","type":"string"},{"name":"level_one_mgrs","type":"string"},{"name":"level_two_mgrs","type":"string"},{"name":"level_three_mgrs","type":"string"}]}"""
        if self.gis_point_schema is None:
            self.gis_point_schema = schema.parse(self.gis_point_schema_str)

        return self.read_orig_datum(self.gis_point_schema, encoded_datum)

    def get_schemas(self, base_name):

        if base_name in self.loaded_schemas:
            REQ_SCHEMA = self.loaded_schemas[base_name]["REQ_SCHEMA"]
            REP_SCHEMA = self.loaded_schemas[base_name]["REP_SCHEMA"]
        else:
            REP_SCHEMA_STR = open("../obj_defs/"+base_name+"_response.json", "r").read()
            REQ_SCHEMA_STR = open("../obj_defs/"+base_name+"_request.json",  "r").read()
            REP_SCHEMA     = schema.parse(REP_SCHEMA_STR)
            REQ_SCHEMA     = schema.parse(REQ_SCHEMA_STR)

            self.loaded_schemas[base_name] = { "REP_SCHEMA_STR" : REP_SCHEMA_STR,
                                               "REQ_SCHEMA_STR" : REQ_SCHEMA_STR,
                                               "REP_SCHEMA"     : REP_SCHEMA,
                                               "REQ_SCHEMA"     : REQ_SCHEMA }
        return (REQ_SCHEMA, REP_SCHEMA)

    # NOTE: could maybe also put the get schemas in here
    def post_then_get(self,REQ_SCHEMA,REP_SCHEMA,datum,location):
        #print REQ_SCHEMA, REP_SCHEMA, datum, location
        encoded_datum = self.write_datum(REQ_SCHEMA, datum)    
        retval  = self.post_to_gaia_read(encoded_datum, location)

        return self.read_datum(REP_SCHEMA, retval)


    ###### Main calls ##########
    def do_register_type(self, type_definition, annotation="", label="", semantic_type=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("register_type")

        datum = collections.OrderedDict()
        datum["type_definition"] = type_definition
        datum["annotation"] = annotation
        datum["label"] = label
        datum["semantic_type"] = semantic_type

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/registertype")


    def do_register_type_point(self):
        # schema...need to pretend that its a json encoded object
        #jsonobj =  '{"type":"record","name":"register_type_request","fields":[{"type_definition":"JSON TYPE DESCRIPTION"}]}'
        #jsonobj = '{"type_definition":"{\\"type\\":\\"record\\",\\"name\\":\\"point\\",\\"fields\\":[{\\"name\\":\\"x\\",\\"type\\":\\"double\\"},{\\"name\\":\\"y\\",\\"type\\":\\"double\\"}]}"}'

        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("register_type")

        datum = collections.OrderedDict()
        datum["type_definition"] = self.point_schema_str
        datum["annotation"] = ""
        datum["label"] = "basic_point_type"
        datum["semantic_type"] = "POINT"

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/registertype")


    def do_register_type_big_point(self):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("register_type")
        
        datum = collections.OrderedDict()
        datum["type_definition"] = self.big_point_schema_str
        datum["annotation"] = "msg_id"
        datum["label"] = "big_point_type"
        datum["semantic_type"] = "POINT"

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/registertype")

    def do_register_type_bigger_point(self):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("register_type")

        datum = collections.OrderedDict()
        datum["type_definition"] = self.bigger_point_schema_str
        datum["annotation"] = "ARTIFACTID"
        datum["label"] = "bigger_point_type"
        datum["semantic_type"] = "POINT"

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/registertype")

    def do_register_type_twitter_point(self):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("register_type")

        datum = collections.OrderedDict()
        datum["type_definition"] = self.twitter_point_schema_str
        datum["annotation"] = "ARTIFACTID"
        datum["label"] = "twitter_point_type"
        datum["semantic_type"] = "POINT"

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/registertype")


    def do_register_type_gis_point(self):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("register_type")
        
        datum = collections.OrderedDict()
        datum["type_definition"] = self.gis_point_schema_str
        datum["annotation"] = "msg_id"
        datum["label"] = "gis_point_type"
        datum["semantic_type"] = "POINT"

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/registertype")

    def do_register_type_bytes_point(self):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("register_type")
        
        # build the datum (internal, object)
        datum = collections.OrderedDict()
        datum["type_definition"] = self.bytes_point_schema_str
        datum["annotation"] = "msg_id"
        datum["label"] = "bytes_point_type"
        datum["semantic_type"] = "POINT"

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/registertype")

    #get set
    def do_get_set(self, set_id, start, end, semantic_type="", user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("get_set")
        
        datum = collections.OrderedDict()
        datum["start"] = start
        datum["end"] = end
        datum["set_id"] = set_id
        datum["semantic_type"] = semantic_type
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/getset")

    def do_new_set(self, type_id, set_id, parent_set_id=None):
        if parent_set_id == None:
            parent_set_id = set_id

        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("new_set")

        datum = collections.OrderedDict()
        datum["set_id"] = set_id
        datum["parent_set_id"] = parent_set_id
        datum["type_id"] = type_id

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/newset")

    #generic version
    def do_add(self,set_id,objdata):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("add_object")

        req_datum = collections.OrderedDict()
        if (self.encoding == 'JSON'):
            req_datum['object_data'] = ''
            req_datum['object_data_str'] = objdata
        elif self.encoding == 'BINARY':
            req_datum['object_data'] = objdata
            req_datum['object_data_str'] = ''
        req_datum['object_encoding'] = self.encoding
        req_datum['set_id'] = set_id

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, req_datum, "/add")

    def encode_datum(self, schema_str, datum):
        OBJ_SCHEMA = schema.parse(schema_str)
        
        return self.write_datum(OBJ_SCHEMA, datum)

    # NOTE: we should remove the type_id from this, should be apparent from set_id
    def do_add_point(self, set_id, x, y, OBJECT_ID=''):
        if self.point_schema is None:
            self.point_schema = schema.parse(self.point_schema_str)

        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("add_object")
        
        # build the datum (internal, object)
        datum = collections.OrderedDict()
        datum['x'] = x
        datum['y'] = y
        datum['OBJECT_ID'] = OBJECT_ID
        
        objdata = self.write_datum(self.point_schema, datum)

        # outer datum
        req_datum = collections.OrderedDict()
        if (self.encoding == 'JSON'):
            req_datum['object_data'] = ''
            req_datum['object_data_str'] = objdata
        elif self.encoding == 'BINARY':
            req_datum['object_data'] = objdata
            req_datum['object_data_str'] = ''
        req_datum['object_encoding'] = self.encoding
        req_datum['set_id'] = set_id

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, req_datum, "/add")



    def do_add_bigger_point(self, set_id, artifact_id, x, y, timestamp, OBJECT_ID=''):
        if self.bigger_point_schema is None:
            self.bigger_point_schema = schema.parse(self.bigger_point_schema_str)

        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("add_object")

        # build the datum (internal, object)
        datum = collections.OrderedDict()
        datum["ARTIFACTID"] = artifact_id
        datum["x"] = x
        datum["y"] = y
        datum["TIMESTAMP"] = timestamp
        datum["DATASOURCE"] = "OSC"
        datum["DATASOURCESUB"] = "REPLICATED"
        datum["DATASOURCEKEY"] = "OSC:REPLICATED"
        datum["AUTHOR"] = "OSC"
        datum["OBJECTAUTH"] = "U"
        datum["OBJECT_ID"] = OBJECT_ID

        objdata = self.write_datum(self.bigger_point_schema, datum)
        #print "objdata", len(objdata)

        # outer datum
        req_datum = collections.OrderedDict()
        if (self.encoding == 'JSON'):
            req_datum['object_data'] = ''
            req_datum['object_data_str'] = objdata
        elif self.encoding == 'BINARY':
            req_datum['object_data'] = objdata
            req_datum['object_data_str'] = ''
        req_datum['object_encoding'] = self.encoding
        req_datum['set_id'] = set_id

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, req_datum, "/add")

    def do_add_twitter_point(self, set_id, artifact_id, x, y, timestamp,keyword,author, OBJECT_ID=''):
        if self.twitter_point_schema is None:
            self.twitter_point_schema = schema.parse(self.twitter_point_schema_str)

        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("add_object")

        # build the datum (internal, object)
        datum = collections.OrderedDict()
        datum["ARTIFACTID"] = artifact_id
        datum["x"] = x
        datum["KEYWORD"] = keyword
        datum["y"] = y
        datum["TIMESTAMP"] = timestamp
        datum["DATASOURCE"] = "TWITTER"
        datum["DATASOURCESUB"] = "KEYWORD"
        datum["DATASOURCEKEY"] = "TWITTER:KEYWORD"
        datum["AUTHOR"] = author
        datum["OBJECTAUTH"] = "U"
        datum["OBJECT_ID"] = OBJECT_ID

        objdata = self.write_datum(self.twitter_point_schema, datum)
        #print "objdata", len(objdata)

        # outer datum
        req_datum = collections.OrderedDict()
        if (self.encoding == 'JSON'):
            req_datum['object_data'] = ''
            req_datum['object_data_str'] = objdata
        elif self.encoding == 'BINARY':
            req_datum['object_data'] = objdata
            req_datum['object_data_str'] = ''
        req_datum['object_encoding'] = self.encoding
        req_datum['set_id'] = set_id

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, req_datum, "/add")


    def do_add_big_point(self, set_id, msg_id, x, y, timestamp, source, group_id, OBJECT_ID=''):
        if self.big_point_schema is None:
            self.big_point_schema = schema.parse(self.big_point_schema_str)

        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("add_object")
        
        # build the datum (internal, object)
        datum = collections.OrderedDict()
        datum["msg_id"] = msg_id
        datum["x"] = x
        datum["y"] = y
        datum["TIMESTAMP"] = timestamp
        datum["source"] = source
        datum["group_id"] = group_id
        datum["OBJECT_ID"] = OBJECT_ID
       
        objdata = self.write_datum(self.big_point_schema, datum)
        #print "objdata", len(objdata)
        
        # outer datum
        req_datum = collections.OrderedDict()
        if (self.encoding == 'JSON'):
            req_datum['object_data'] = ''
            req_datum['object_data_str'] = objdata
        elif self.encoding == 'BINARY':
            req_datum['object_data'] = objdata
            req_datum['object_data_str'] = ''
        req_datum['object_encoding'] = self.encoding
        req_datum['set_id'] = set_id

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, req_datum, "/add")

    def do_add_gis_point(self, set_id, msg_id, x, y, timestamp, tag_id, derived, group_id, level_one_mgrs, level_two_mgrs, level_three_mgrs, level_final_mgrs, OBJECT_ID=''):
        if self.gis_point_schema is None:
            self.gis_point_schema = schema.parse(self.gis_point_schema_str)

        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("add_object")
        
        # build the datum (internal, object)
        datum = collections.OrderedDict()

        datum["x"] = x
        datum["y"] = y
        datum["timestamp"] = timestamp
        datum["tag_id"] = tag_id
        datum["derived"] = derived
        datum["msg_id"] = msg_id
        datum["group_id"] = group_id
        datum["level_one_mgrs"] = level_one_mgrs
        datum["level_two_mgrs"] = level_two_mgrs
        datum["level_three_mgrs"] = level_three_mgrs
        datum["level_final_mgrs"] = level_final_mgrs
        datum["OBJECT_ID"] = OBJECT_ID
        
        objdata = self.write_datum(self.gis_point_schema, datum)
        
        # outer datum
        req_datum = collections.OrderedDict()
        if (self.encoding == 'JSON'):
            req_datum['object_data'] = ''
            req_datum['object_data_str'] = objdata
        elif self.encoding == 'BINARY':
            req_datum['object_data'] = objdata
            req_datum['object_data_str'] = ''
        req_datum['object_encoding'] = self.encoding
        req_datum['set_id'] = set_id

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, req_datum, "/add")

    def do_add_bytes_point(self, set_id, msg_id, x, y, timestamp, source, group_id, bytes_data, OBJECT_ID=''):
        if self.bytes_point_schema is None:
            self.bytes_point_schema = schema.parse(self.bytes_point_schema_str)

        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("add_object")
        
        # build the datum (internal, object)
        datum = collections.OrderedDict()
        datum["msg_id"] = msg_id
        datum["x"] = x
        datum["y"] = y
        datum["timestamp"] = timestamp
        datum["source"] = source
        datum["group_id"] = group_id
        datum["bytes_data"] = bytes_data
        datum["OBJECT_ID"] = OBJECT_ID
        
        objdata = self.write_datum(self.bytes_point_schema, datum)
            
        # outer datum
        req_datum = collections.OrderedDict()
        if (self.encoding == 'JSON'):
            req_datum['object_data'] = ''
            req_datum['object_data_str'] = objdata
        elif self.encoding == 'BINARY':
            req_datum['object_data'] = objdata
            req_datum['object_data_str'] = ''
        req_datum['object_encoding'] = self.encoding
        req_datum['set_id'] = set_id

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, req_datum, "/add")

    # the {x|y}_attr_name defines a mapping into x-y plane and then the min/max define the box
    def do_bounding_box(self, min_x, max_x, min_y, max_y, x_attr_name, y_attr_name, set_id, result_set_id, user_auth=""):
        #jsonobj = '{"min_x":'+str(min_x)+',"max_x":'+str(max_x)+',"min_y":'+str(min_y)+',"max_y":'+str(max_y)+',"x_attr_name":"'+x_attr_name+'","y_attr_name":"'+y_attr_name+'","set_id":"'+set_id+'","result_set_id":"'+result_set_id+'"}'
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("bounding_box")

        datum = collections.OrderedDict()
        
        datum["min_x"] = min_x
        datum["max_x"] = max_x
        datum["min_y"] = min_y
        datum["max_y"] = max_y
        datum["x_attr_name"] = x_attr_name
        datum["y_attr_name"] = y_attr_name
        datum["set_id"] = set_id
        datum["result_set_id"] = result_set_id
        datum["user_auth_string"] = user_auth
        
        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/boundingbox")

    #equi-join
    def do_join(self, left_set, left_attr, right_set, right_attr, result_type, result_set, user_auth=""):
        # get the schemas
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("join")

        datum = collections.OrderedDict()
        datum["left_set"] = left_set
        datum["left_attr"] = left_attr
        datum["right_set"] = right_set
        datum["right_attr"] = right_attr
        datum["result_type"] = result_type
        datum["result_set"] = result_set
        datum["user_auth_string"] = user_auth
        
        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/join")

    #group by [attributes is a list]
    def do_group_by(self, set_id, attributes, user_auth=""):    
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("group_by")
        
        datum = collections.OrderedDict()
        datum["set_id"] = set_id
        datum["attributes"] = attributes
        datum["user_auth_string"] = user_auth
        
        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/groupby")

    #exit
    def do_exit(self, exit_type, authorization):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("exit")
        
        datum = collections.OrderedDict()
        datum["exit_type"] = exit_type
        datum["authorization"] = authorization
        
        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/exit")

    #make bloom
    def do_make_bloom(self, set_id, attribute):
        # get the schemas
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("make_bloom")
        
        datum = collections.OrderedDict()
        datum["set_id"] = set_id
        datum["attribute"] = attribute
        
        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/makebloom")
        
    # This assumes that 'x' and 'y' are equal length lists
    def do_bulk_add_point(self, set_id, x_list, y_list, OBJECT_ID_list=None):    
        if self.point_schema is None:
            self.point_schema = schema.parse(self.point_schema_str)

        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("bulk_add")

        if (OBJECT_ID_list is None):
            OBJECT_ID_list = ['' for x in x_list]
        
        obj_list_encoded = []

        for i in range(0,len(x_list)):
            datum = collections.OrderedDict()
            datum['x'] = x_list[i]
            datum['y'] = y_list[i]
            datum['OBJECT_ID'] = OBJECT_ID_list[i]
            obj_list_encoded.append(self.write_datum(self.point_schema, datum))

        req_datum = collections.OrderedDict()
        req_datum["set_id"] = set_id
        if (self.encoding == 'JSON'):
            req_datum["list"] = ['']*len(x_list)
            req_datum["list_str"] = obj_list_encoded
        elif (self.encoding == 'BINARY'):
            req_datum["list"] = obj_list_encoded
            req_datum["list_str"] = ['']*len(x_list)
        req_datum["list_encoding"] = self.encoding

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, req_datum, "/bulkadd")    

    # This assumes equal length lists
    def do_bulk_add_big_point(self, set_id, msg_id_list, x_list, y_list, timestamp_list, source_list, group_id_list, OBJECT_ID_list=None):    
        if self.big_point_schema is None:
            self.big_point_schema = schema.parse(self.big_point_schema_str)

        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("bulk_add")

        if (OBJECT_ID_list is None):
            OBJECT_ID_list = ['' for x in x_list]
        
        obj_list_encoded = []

        for msg_id,x,y,timestamp,source,group_id,object_id in zip(msg_id_list,x_list,y_list,timestamp_list,source_list,group_id_list,OBJECT_ID_list):
            datum = collections.OrderedDict()
            datum['msg_id'] = msg_id
            datum['x'] = x
            datum['y'] = y
            datum['TIMESTAMP'] = timestamp
            datum['source'] = source
            datum['group_id'] = group_id
            datum['OBJECT_ID'] = object_id
            obj_list_encoded.append(self.write_datum(self.big_point_schema, datum))

        req_datum = collections.OrderedDict()
        req_datum["set_id"] = set_id
        if (self.encoding == 'JSON'):
            req_datum["list"] = ['']*len(x_list)
            req_datum["list_str"] = obj_list_encoded
        elif (self.encoding == 'BINARY'):
            req_datum["list"] = obj_list_encoded
            req_datum["list_str"] = ['']*len(x_list)
        req_datum["list_encoding"] = self.encoding

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, req_datum, "/bulkadd")    
    
    # generic version
    def do_bulk_add(self, set_id, objdatas):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("bulk_add")

        req_datum = collections.OrderedDict()
        if (self.encoding == 'JSON'):
            req_datum['list'] = ['']*len(objdatas)
            req_datum['list_str'] = objdatas
        elif self.encoding == 'BINARY':
            req_datum['list'] = objdatas
            req_datum['list_str'] = ['']*len(objdatas)
        req_datum['list_encoding'] = self.encoding
        req_datum['set_id'] = set_id

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, req_datum, "/bulkadd")

    #store group by [group_map is a map returned by group by
    def do_store_group_by(self, set_id, attribute, group_map, sort, sort_attribute, user_auth=""):    
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("store_group_by")
        
        datum = collections.OrderedDict()
        datum["attribute"] = attribute
        datum["group_map"] = group_map
        datum["sort"] = sort
        datum["sort_attribute"] = sort_attribute
        datum["set_id"] = set_id
        datum["user_auth_string"] = user_auth
        
        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/storegroupby")

    #histogram call
    def do_histogram(self, set_id, attribute, interval, start, end, user_auth=""):
        # get the schemas
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("histogram")
            
        datum = collections.OrderedDict()
        datum["attribute"] = attribute
        datum["end"] = end
        datum["interval"] = interval
        datum["set_id"] = set_id
        datum["start"] = start
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/histogram")

    #filter then histogram call
    def do_filter_then_histogram(self, set_id, filter_attribute, filter_values, histogram_attribute, interval, start, end, user_auth=""):
        # get the schemas
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("filter_then_histogram")
            
        datum = collections.OrderedDict()
        datum["histogram_attribute"] = histogram_attribute
        datum["end"] = end
        datum["interval"] = interval
        datum["set_id"] = set_id
        datum["start"] = start
        datum["filter_attribute"] = filter_attribute
        datum["filter"] = filter_values
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/filterthenhistogram")

    #max min call
    def do_max_min(self, set_id, attribute, user_auth=""):
        # get the schemas
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("max_min")
        
        datum = collections.OrderedDict()
        datum["attribute"] = attribute
        datum["set_id"] = set_id
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/maxmin")

    # filter by bounds; return a count of how many objects have lower < attribute < upper
    def do_filter_by_bounds(self, set_id, lower_bound, attribute, upper_bound, result_set_id, user_auth=""):
        # get the schemas
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("filter_by_bounds")
        
        datum = collections.OrderedDict()
        datum["attribute"] = attribute
        datum["lower_bound"] = lower_bound
        datum["result_set_id"] = result_set_id
        datum["set_id"] = set_id
        datum["upper_bound"] = upper_bound
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/filterbybounds")

    # filter by list; return a count of how many objects have their attribute value in the list
    # attribute map is attribute to list of values
    def do_filter_by_list(self, set_id, attribute_map, result_set_id, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("filter_by_list")
        
        datum = collections.OrderedDict()
        datum["attribute_map"] = attribute_map
        datum["result_set_id"] = result_set_id
        datum["set_id"] = set_id
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/filterbylist")

    # filter by nai; return a count of how many objects are within the polygon
    def do_filter_by_nai(self, set_id, x_attribute, x_vector, y_attribute, y_vector, result_set_id, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("filter_by_nai")
        
        datum = collections.OrderedDict()
        datum["result_set_id"] = result_set_id
        datum["set_id"] = set_id
        datum["x_attribute"] = x_attribute
        datum["x_vector"] = x_vector
        datum["y_attribute"] = y_attribute
        datum["y_vector"] = y_vector
        datum["user_auth_string"] = user_auth
        
        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/filterbynai")

    # filter by radius; return a count of how many objects are within a designated circle
    def do_filter_by_radius(self, set_id, x_attribute, y_attribute, x_center, y_center, radius, result_set_id, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("filter_by_radius")
        
        datum = collections.OrderedDict()
        datum["result_set_id"] = result_set_id
        datum["set_id"] = set_id
        datum["x_attribute"] = x_attribute
        datum["y_attribute"] = y_attribute
        datum["x_center"] = x_center
        datum["y_center"] = y_center
        datum["radius"] = radius
        datum["user_auth_string"] = user_auth
        
        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/filterbyradius")

    # clear
    def do_clear(self, set_id="", authorization=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("clear")
        
        datum = collections.OrderedDict()
        datum["set_id"] = set_id
        datum["authorization"] = authorization

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/clear")

    # make a copy of a set
    def do_copy_set(self, new_set_id, original_set_id, selector, type_id, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("copy_set")
        
        datum = collections.OrderedDict()
        datum["new_set_id"] = new_set_id
        datum["original_set_id"] = original_set_id
        datum["selector"] = selector
        datum["type_id"] = type_id
        datum["user_auth_string"] = user_auth
        
        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/copyset")

    # cluster
    def do_cluster(self, world_set, subworld_set, result_set, shared_attribute, cluster_attribute, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("cluster")
        
        datum = collections.OrderedDict()
        datum["cluster_attribute"] = cluster_attribute
        datum["first_pass"] = True
        datum["list"] = []
        datum["result_set"] = result_set
        datum["shared_attribute"] = shared_attribute
        datum["subworld_set"] = subworld_set
        datum["world_set"] = world_set
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/cluster")

    # stats
    def do_stats(self, set_id=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("stats")

        datum = collections.OrderedDict()
        datum["set_id"] = set_id
        
        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/stats")

    # select
    def do_select(self, set_id, result_set_id, expression, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("select")
        
        datum = collections.OrderedDict()
        datum["expression"] = expression
        datum["result_set_id"] = result_set_id
        datum["set_id"] = set_id
        datum["user_auth_string"] = user_auth
        
        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/select")

    # plot2dmultiple
    def do_plot2d_multiple(self, set_ids, colors, sizes, min_x, max_x, min_y, max_y, x_attr_name, y_attr_name, width, height, projection, bg_color, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("plot2d_multiple")
        
        datum = collections.OrderedDict()    
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
        datum["sizes"] = sizes
        datum["bg_color"] = bg_color
        datum["user_auth_string"] = user_auth
            
        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/plot2dmultiple")

    # plot2dmultiple2
    def do_plot2d_multiple_2(self, min_x, max_x, min_y, max_y, x_attr_name, y_attr_name, width, height, projection, bg_color,
                            set_ids, world_set_ids, track_ids,
                            do_points, do_shapes, do_tracks,
                            pointcolors, pointsizes, pointshapes,
                            shapelinewidths, shapelinecolors, shapefillcolors,
                            tracklinewidths, tracklinecolors, 
                            trackmarkersizes, trackmarkercolors, trackmarkershapes,
                            trackheadcolors, trackheadsizes, trackheadshapes, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("plot2d_multiple_2")
        
        datum = collections.OrderedDict()    
        datum["min_x"] = min_x
        datum["max_x"] = max_x
        datum["min_y"] = min_y
        datum["max_y"] = max_y
        datum["x_attr_name"] = x_attr_name
        datum["y_attr_name"] = y_attr_name
        datum["width"] = width
        datum["height"] = height
        datum["projection"] = projection
        datum["bg_color"] = bg_color

        datum["set_ids"] = set_ids
        datum["world_set_ids"] = world_set_ids
        datum["track_ids"] = track_ids

        datum["do_points"] = do_points
        datum["do_shapes"] = do_shapes
        datum["do_tracks"] = do_tracks

        datum["pointcolors"] = pointcolors
        datum["pointsizes"] = pointsizes
        datum["pointshapes"] = pointshapes

        datum["shapelinewidths"] = shapelinewidths
        datum["shapelinecolors"] = shapelinecolors
        datum["shapefillcolors"] = shapefillcolors

        datum["tracklinewidths"] = tracklinewidths
        datum["tracklinecolors"] = tracklinecolors
        datum["trackmarkersizes"] = trackmarkersizes
        datum["trackmarkercolors"] = trackmarkercolors
        datum["trackmarkershapes"] = trackmarkershapes
        datum["trackheadcolors"] = trackheadcolors
        datum["trackheadsizes"] = trackheadsizes
        datum["trackheadshapes"] = trackheadshapes

        datum["user_auth_string"] = user_auth
            
        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/plot2dmultiple2")

    # plot2dmultiplecb - class break
    def do_plot2d_multiple_cb(self, min_x, max_x, min_y, max_y, x_attr_name, y_attr_name, width, height, projection, bg_color,
                            set_ids, world_set_ids, track_ids,
                            cb_attr1, cb_vals1, cb_ranges1,
                            cb_attr2, cb_vals2, cb_ranges2,
                            do_points, do_shapes, do_tracks,
                            pointcolors, pointsizes, pointshapes,
                            shapelinewidths, shapelinecolors, shapefillcolors,
                            tracklinewidths, tracklinecolors, 
                            trackmarkersizes, trackmarkercolors, trackmarkershapes,
                            trackheadcolors, trackheadsizes, trackheadshapes, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("plot2d_multiple_cb")
        
        datum = collections.OrderedDict()    
        datum["min_x"] = min_x
        datum["max_x"] = max_x
        datum["min_y"] = min_y
        datum["max_y"] = max_y
        datum["x_attr_name"] = x_attr_name
        datum["y_attr_name"] = y_attr_name
        datum["width"] = width
        datum["height"] = height
        datum["projection"] = projection
        datum["bg_color"] = bg_color

        datum["set_ids"] = set_ids
        datum["world_set_ids"] = world_set_ids
        datum["track_ids"] = track_ids

        datum['cb_attr1'] = cb_attr1
        datum['cb_vals1'] = cb_vals1
        datum['cb_ranges1'] = cb_ranges1

        datum['cb_attr2'] = cb_attr2
        datum['cb_vals2'] = cb_vals2
        datum['cb_ranges2'] = cb_ranges2

        datum["do_points"] = do_points
        datum["do_shapes"] = do_shapes
        datum["do_tracks"] = do_tracks

        datum["pointcolors"] = pointcolors
        datum["pointsizes"] = pointsizes
        datum["pointshapes"] = pointshapes

        datum["shapelinewidths"] = shapelinewidths
        datum["shapelinecolors"] = shapelinecolors
        datum["shapefillcolors"] = shapefillcolors

        datum["tracklinewidths"] = tracklinewidths
        datum["tracklinecolors"] = tracklinecolors
        datum["trackmarkersizes"] = trackmarkersizes
        datum["trackmarkercolors"] = trackmarkercolors
        datum["trackmarkershapes"] = trackmarkershapes
        datum["trackheadcolors"] = trackheadcolors
        datum["trackheadsizes"] = trackheadsizes
        datum["trackheadshapes"] = trackheadshapes

        datum["user_auth_string"] = user_auth
            
        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/plot2dmultiplecb")


    # plot2dheatmap
    def do_plot2d_heatmap(self, set_ids, colormap, blur_radius, min_x, max_x, min_y, max_y, x_attr_name, y_attr_name, width, height, projection, bg_color, gradient_start_color, gradient_end_color, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("plot2d_heatmap")
        
        datum = collections.OrderedDict()    
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
        datum["colormap"] = colormap
        datum["blur_radius"] = blur_radius
        datum["bg_color"] = bg_color
        datum["gradient_start_color"] = gradient_start_color
        datum["gradient_end_color"] = gradient_end_color
        datum["user_auth_string"] = user_auth
            
        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/plot2dheatmap")

    # plot2dheatmapcb - class break
    def do_plot2d_heatmap_cb(self, min_x, max_x, min_y, max_y, x_attr_name, y_attr_name, width, height, projection, bg_color,
                             set_ids, cb_attr, cb_vals, cb_ranges,
                             colormaps, blur_radii,  gradient_start_colors, gradient_end_colors, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("plot2d_heatmap_cb")
        
        datum = collections.OrderedDict()    
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
        datum['cb_attr'] = cb_attr
        datum['cb_vals'] = cb_vals
        datum['cb_ranges'] = cb_ranges
        datum["colormaps"] = colormaps
        datum["blur_radii"] = blur_radii
        datum["bg_color"] = bg_color
        datum["gradient_start_colors"] = gradient_start_colors
        datum["gradient_end_colors"] = gradient_end_colors
        datum["user_auth_string"] = user_auth
            
        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/plot2dheatmapcb")

    #initial join setup for the incremental join
    def do_join_setup(self, left_set, left_attr, right_set, right_attr, subset_id, user_auth=""):
        # get the schemas
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("join_setup")

        datum = collections.OrderedDict()
        datum["left_set"] = left_set
        datum["left_attr"] = left_attr
        datum["right_set"] = right_set
        datum["right_attr"] = right_attr
        datum["subset_id"] = subset_id
        datum["user_auth_string"] = user_auth
        
        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/joinsetup")

    # join incremental
    def do_join_incremental(self, left_subset, left_attr, left_index, right_set, right_attr, result_set, result_type, user_auth=""):
        # get the schemas
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("join_incremental")
        
        datum = collections.OrderedDict()
        datum["left_subset"] = left_subset
        datum["left_attr"] = left_attr
        datum["left_index"] = left_index
        datum["right_set"] = right_set
        datum["right_attr"] = right_attr
        datum["result_set"] = result_set
        datum["result_type"] = result_type
        datum["data_map"] = {}
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/joinincremental")

    #set info
    def do_set_info(self, set_id):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("set_info")
        
        datum = collections.OrderedDict()
        datum["set_ids"] = [set_id]
        
        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/setinfo")

    #sort it
    def do_sort(self, set_id, attribute):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("sort")
        
        datum = collections.OrderedDict()
        datum["set_id"] = set_id
        datum["attribute"] = attribute
        
        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/sort")

    #get set sorted
    def do_get_set_sorted(self, set_id, start, end, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("get_set_sorted")
        # this call actually returns the get_set_response
        (GS_REQ_SCHEMA,GS_REP_SCHEMA) = self.get_schemas("get_set")

        datum = collections.OrderedDict()
        datum["set_id"] = set_id
        datum["start"] = start
        datum["end"] = end
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, GS_REP_SCHEMA, datum, "/getsetsorted")

    #get set sorted
    def do_get_sorted_sets(self, set_ids, attribute, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("get_sorted_sets")

        datum = collections.OrderedDict()
        datum["set_ids"] = set_ids
        datum["attribute"] = attribute
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/getsortedsets")

    #register a range trigger
    def do_register_trigger_range(self, trigger_id, set_ids, attr, minval, maxval, id_attr):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("register_trigger_range")

        datum = collections.OrderedDict()
        datum["request_id"] = trigger_id
        datum["set_ids"] = set_ids
        datum["attr"] = attr
        datum["lowest"] = minval
        datum["highest"] = maxval
        datum["id_attr"] = id_attr
        
        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/registertriggerrange")

    #register a range trigger
    def do_register_trigger_nai(self, trigger_id, set_ids, xattr, xvals, yattr, yvals, id_attr):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("register_trigger_nai")

        datum = collections.OrderedDict()
        datum["request_id"] = trigger_id
        datum["set_ids"] = set_ids
        datum["x_attribute"] = xattr
        datum["x_vector"] = xvals
        datum["y_attribute"] = yattr
        datum["y_vector"] = yvals
        datum["id_attr"] = id_attr
        
        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/registertriggernai")

    def do_read_trigger_msg(self, encoded_datum):
        REP_SCHEMA_STR = open("../obj_defs/trigger_notification.json", "r").read()
        REP_SCHEMA = schema.parse(REP_SCHEMA_STR)

        return self.read_orig_datum(REP_SCHEMA, encoded_datum, encoding='JSON')

    def do_update_set_ttl(self, set_id, ttl):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("update_set_ttl")

        datum = collections.OrderedDict()
        datum["set_id"] = set_id
        datum["ttl"] = ttl

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/updatesetttl")

    def do_authenticate_users(self, user_auth_strings,set_ids):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("authenticate_users")

        datum = collections.OrderedDict()
        datum["user_auth_strings"] = user_auth_strings
        datum["set_ids"] = set_ids

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/authenticateusers")

    def do_initialize_group_by_map(self, set_id, attribute, map_id, page_size, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("initialize_group_by_map")
        
        datum = collections.OrderedDict()
        datum["user_auth_string"] = user_auth
        datum["set_id"] = set_id
        datum["attribute"] = attribute
        datum["map_id"] = map_id
        datum["page_size"] = page_size
        datum["group_by_map"] = collections.OrderedDict() #this is set by the TW    

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/initializegroupbymap")

    def do_group_by_map_page(self, map_id, page_number, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("group_by_map_page")
        
        datum = collections.OrderedDict()
        datum["user_auth_string"] = user_auth
        datum["map_id"] = map_id
        datum["page_number"] = page_number

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/groupbymappage")

    #predicate based join
    def do_predicate_join(self, left_set, right_set, predicate, common_type, result_type, result_set, user_auth=""):
        # get the schemas
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("predicate_join")

        datum = collections.OrderedDict()
        datum["left_set"] = left_set
        datum["right_set"] = right_set
        datum["common_type"] = common_type
        datum["result_type"] = result_type
        datum["result_set"] = result_set
        datum["user_auth_string"] = user_auth
        datum["predicate"] = predicate
        
        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/predicatejoin")

    #road intersection
    def do_road_intersection(self, set_id, x_attribute, y_attribute, road_x_vector, road_y_vector, output_attribute, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("road_intersection")
        
        datum = collections.OrderedDict()
        datum["set_id"] = set_id
        datum["x_attribute"] = x_attribute
        datum["y_attribute"] = y_attribute
        datum["road_x_vector"] = road_x_vector
        datum["road_y_vector"] = road_y_vector
        datum["output_attribute"] = output_attribute
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/roadintersection")

    #type transform
    def do_register_type_transform(self, type_id, new_type_id, transform_map):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("register_type_transform")
        
        # build the datum (internal, object)
        datum = collections.OrderedDict()
        datum["type_id"] = type_id
        datum["new_type_id"] = new_type_id
        datum["transform_map"] = transform_map

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/registertypetransform")

    #get objects
    def do_get_objects(self, set_id, attribute, attr_vals, attr_str_vals, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("get_objects")
        
        # build the datum (internal, object)
        datum = collections.OrderedDict()
        datum["set_id"] = set_id
        datum["attribute"] = attribute
        datum["attr_vals"] = attr_vals
        datum["attr_str_vals"] = attr_str_vals
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/getobjects")

    #shape intersection
    def do_shape_intersection(self, set_ids, wkt_attr_name, x_vector, y_vector, geometry_type, wkt_string, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("shape_intersection")
        
        # build the datum (internal, object)
        datum = collections.OrderedDict()
        datum["set_ids"] = set_ids
        datum["wkt_attr_name"] = wkt_attr_name
        datum["x_vector"] = x_vector
        datum["y_vector"] = y_vector
        datum["geometry_type"] = geometry_type
        datum["wkt_string"] = wkt_string
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/shapeintersection")

    #shape literal intersection
    def do_shape_literal_intersection(self, x_vector_1, y_vector_1, geometry_type_1, wkt_string_1, x_vector_2, y_vector_2, geometry_type_2, wkt_string_2):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("shape_literal_intersection")
        
        # build the datum (internal, object)
        datum = collections.OrderedDict()

        datum["x_vector_1"] = x_vector_1
        datum["y_vector_1"] = y_vector_1
        datum["geometry_type_1"] = geometry_type_1
        datum["wkt_string_1"] = wkt_string_1

        datum["x_vector_2"] = x_vector_2
        datum["y_vector_2"] = y_vector_2
        datum["geometry_type_2"] = geometry_type_2
        datum["wkt_string_2"] = wkt_string_2

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/shapeliteralintersection")

    #get type info
    def do_get_type_info(self, type_id, label, semantic_type):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("get_type_info")

        datum = collections.OrderedDict()
        datum["type_id"] = type_id
        datum["label"] = label
        datum["semantic_type"] = semantic_type

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/gettypeinfo")

    #get sets by type info
    def do_get_sets_by_type_info(self, type_id, label, semantic_type):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("get_sets_by_type_info")

        datum = collections.OrderedDict()
        datum["type_id"] = type_id
        datum["label"] = label
        datum["semantic_type"] = semantic_type

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/getsetsbytypeinfo")

    #merge sets
    def do_merge_sets(self, set_ids, merged_set_id, common_type_id):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("merge_sets")

        datum = collections.OrderedDict()
        datum["set_ids"] = set_ids
        datum["merged_set_id"] = merged_set_id
        datum["common_type_id"] = common_type_id

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/mergesets")

    #turn off
    def do_turn_off(self, set_ids, group_attribute, sort_attribute, x_attribute, y_attribute, threshold, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("turn_off")
        
        datum = collections.OrderedDict()
        datum["set_ids"] = set_ids
        datum["group_attribute"] = group_attribute
        datum["sort_attribute"] = sort_attribute
        datum["x_attribute"] = x_attribute
        datum["y_attribute"] = y_attribute
        datum["threshold"] = threshold
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/turnoff")

    #get orphans
    def do_get_orphans(self, set_namespace=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("get_orphans")
        
        datum = collections.OrderedDict()
        datum["set_namespace"] = set_namespace

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/getorphans")

    #spatial query
    def do_spatial_query(self, wkt_string_1, wkt_string_2, operation):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("spatial_query")
        
        # build the datum (internal, object)
        datum = collections.OrderedDict()

        datum["wkt_string_1"] = wkt_string_1
        datum["wkt_string_2"] = wkt_string_2
        datum["operation"] = operation

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/spatialquery")

    #spatial set query
    def do_spatial_set_query(self, set_ids, wkt_attr_name, wkt_string, operation, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("spatial_set_query")
        
        # build the datum (internal, object)
        datum = collections.OrderedDict()

        datum["set_ids"] = set_ids
        datum["wkt_attr_name"] = wkt_attr_name
        datum["wkt_string"] = wkt_string
        datum["operation"] = operation
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/spatialsetquery")

    # delete object
    def do_delete_object(self, set_ids, OBJECT_ID, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("delete_object")
        
        # build the datum (internal, object)
        datum = collections.OrderedDict()

        datum["set_ids"] = set_ids
        datum["OBJECT_ID"] = OBJECT_ID
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/deleteobject")

    # filter by value; return a count of how many objects have an attribute (or any attribute) equal to the specified value
    def do_filter_by_value(self, set_id, is_string, value, value_str, attribute, result_set_id, user_auth=""):
        # get the schemas
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("filter_by_value")
        
        datum = collections.OrderedDict()
        datum["set_id"] = set_id
        datum["is_string"] = is_string
        datum["value"] = value
        datum["value_str"] = value_str
        datum["attribute"] = attribute
        datum["result_set_id"] = result_set_id
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/filterbyvalue")        

    # get tracks
    def do_get_tracks(self, set_id, world_set_id, min_x, max_x, min_y, max_y, x_attr_name, y_attr_name, do_extent, start, end, user_auth=""):
        # get the schemas
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("get_tracks")
        
        datum = collections.OrderedDict()
        datum["set_id"] = set_id
        datum["world_set_id"] = world_set_id
        datum["min_x"] = min_x
        datum["max_x"] = max_x
        datum["min_y"] = min_y
        datum["max_y"] = max_y
        datum["x_attr_name"] = x_attr_name
        datum["y_attr_name"] = y_attr_name
        datum["do_extent"] = do_extent
        datum["start"] = start
        datum["end"] = end
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/gettracks")        

    # random data generation
    def do_random(self, set_id, count, param_map):
        # get the schemas
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("random")
        
        datum = collections.OrderedDict()
        datum["set_id"] = set_id
        datum["count"] = count
        datum["param_map"] = param_map

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/random")        

    # clear auth cache
    def do_clear_auth_cache(self, set_ids=[], user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("clear_auth_cache")
        
        datum = collections.OrderedDict()
        datum["set_ids"] = set_ids
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/clearauthcache")

    # status
    def do_status(self, set_id=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("status")
        
        datum = collections.OrderedDict()
        datum["set_id"] = set_id

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/status")

     # get set sizes
    def do_get_set_sizes(self, set_ids):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("get_set_sizes")
        
        datum = collections.OrderedDict()
        datum["set_ids"] = set_ids

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/getsetsizes")

     # update object
    def do_update_object(self, set_ids, OBJECT_ID, objdata, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("update_object")
        
        # build the datum (internal, object)
        datum = collections.OrderedDict()

        datum["set_ids"] = set_ids
        datum["OBJECT_ID"] = OBJECT_ID
        if (self.encoding == 'JSON'):
            datum['object_data'] = ''
            datum['object_data_str'] = objdata
        elif self.encoding == 'BINARY':
            datum['object_data'] = objdata
            datum['object_data_str'] = ''
        datum['object_encoding'] = self.encoding
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/updateobject")

     # unique
    def do_unique(self, set_id, attribute, user_auth=""):    
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("unique")
        
        datum = collections.OrderedDict()
        datum["set_id"] = set_id
        datum["attribute"] = attribute
        datum["user_auth_string"] = user_auth
        
        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/unique")

    # generate video
    def do_generate_video(self, min_x, max_x, min_y, max_y, x_attr_name, y_attr_name, width, height, projection, bg_color,
                            set_ids, world_set_ids, track_ids,
                            do_points, do_shapes, do_tracks,
                            pointcolors, pointsizes, pointshapes,
                            shapelinewidths, shapelinecolors, shapefillcolors,
                            tracklinewidths, tracklinecolors, 
                            trackmarkersizes, trackmarkercolors, trackmarkershapes,
                            trackheadcolors, trackheadsizes, trackheadshapes, 
                            time_intervals, video_style, session_key, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("generate_video")
        
        datum = collections.OrderedDict()    
        datum["min_x"] = min_x
        datum["max_x"] = max_x
        datum["min_y"] = min_y
        datum["max_y"] = max_y
        datum["x_attr_name"] = x_attr_name
        datum["y_attr_name"] = y_attr_name
        datum["width"] = width
        datum["height"] = height
        datum["projection"] = projection
        datum["bg_color"] = bg_color

        datum["set_ids"] = set_ids
        datum["world_set_ids"] = world_set_ids
        datum["track_ids"] = track_ids

        datum["do_points"] = do_points
        datum["do_shapes"] = do_shapes
        datum["do_tracks"] = do_tracks

        datum["pointcolors"] = pointcolors
        datum["pointsizes"] = pointsizes
        datum["pointshapes"] = pointshapes

        datum["shapelinewidths"] = shapelinewidths
        datum["shapelinecolors"] = shapelinecolors
        datum["shapefillcolors"] = shapefillcolors

        datum["tracklinewidths"] = tracklinewidths
        datum["tracklinecolors"] = tracklinecolors
        datum["trackmarkersizes"] = trackmarkersizes
        datum["trackmarkercolors"] = trackmarkercolors
        datum["trackmarkershapes"] = trackmarkershapes
        datum["trackheadcolors"] = trackheadcolors
        datum["trackheadsizes"] = trackheadsizes
        datum["trackheadshapes"] = trackheadshapes

        datum["time_intervals"] = time_intervals
        datum["video_style"] = video_style
        datum["session_key"] = session_key

        datum["user_auth_string"] = user_auth
            
        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/generatevideo")

    # generate heatmap video
    def do_generate_heatmap_video(self, min_x, max_x, min_y, max_y, x_attr_name, y_attr_name, width, height, projection, bg_color,
                            set_ids, 
                            colormap, blur_radius, gradient_start_color, gradient_end_color,
                            time_intervals, video_style, session_key, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("generate_heatmap_video")
        
        datum = collections.OrderedDict()    
        datum["min_x"] = min_x
        datum["max_x"] = max_x
        datum["min_y"] = min_y
        datum["max_y"] = max_y
        datum["x_attr_name"] = x_attr_name
        datum["y_attr_name"] = y_attr_name
        datum["width"] = width
        datum["height"] = height
        datum["projection"] = projection
        datum["bg_color"] = bg_color

        datum["set_ids"] = set_ids
        

        datum["colormap"] = colormap
        datum["blur_radius"] = blur_radius
        datum["gradient_start_color"] = gradient_start_color
        datum["gradient_end_color"] = gradient_end_color

        datum["time_intervals"] = time_intervals
        datum["video_style"] = video_style
        datum["session_key"] = session_key

        datum["user_auth_string"] = user_auth
            
        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/generateheatmapvideo")


    # server status
    def do_server_status(self, option=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("server_status")
        
        datum = collections.OrderedDict()
        datum["option"] = option

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/serverstatus")


