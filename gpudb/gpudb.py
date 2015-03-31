# ---------------------------------------------------------------------------
# gpudb.py - The Python API to interact with a GPUdbDB server. 
#
# Copyright (c) 2014 GIS Federal
# ---------------------------------------------------------------------------

import cStringIO, StringIO
import base64, httplib
import os, sys
import json
import uuid

# ---------------------------------------------------------------------------
# The absolute path of this gpudb.py module for loading the obj_defs/*.json files.
gpudb_module_path = __file__
if gpudb_module_path[len(gpudb_module_path)-3:] == "pyc": # allow symlinks to gpudb.py
    gpudb_module_path = gpudb_module_path[0:len(gpudb_module_path)-1]
if os.path.islink(gpudb_module_path): # allow symlinks to gpudb.py
    gpudb_module_path = os.readlink(gpudb_module_path)
gpudb_module_path = os.path.dirname(os.path.abspath(gpudb_module_path))

# Search for our modules first, probably don't need imp or virt envs.
if not gpudb_module_path + "/packages" in sys.path :
    sys.path.insert(1, gpudb_module_path + "/packages")

# Find the path to the obj_defs/*_{request/response}.json files.
gpudb_obj_defs_path = gpudb_module_path+"/obj_defs"
assert(os.path.exists(gpudb_obj_defs_path)), "Error locating path to obj_defs, tried '%s'" % gpudb_obj_defs_path

# ---------------------------------------------------------------------------
# Local imports after adding our module search path

from avro import schema, datafile, io

if sys.version_info >= (2, 7):
    import collections
else:
    import ordereddict as collections # a separate package

have_snappy = False
try:
    import snappy
    have_snappy = True
except ImportError:
    have_snappy = False


# ---------------------------------------------------------------------------
# GPUdb - Lightweight client class to interact with a GPUdb server.
# ---------------------------------------------------------------------------

class GPUdb:

    def __init__(self, gpudb_ip="127.0.0.1", gpudb_port="9191",
                       encoding="BINARY", connection='HTTP',
                       username="", password=""):
        """
        Construct a new GPUdb client instance.

        Parameters:
            gpudb_ip    : The IP address of the GPUdb server.
            gpudb_port  : The port of the GPUdb server at the given IP address.
            encoding   : Type of Avro encoding to use, "BINARY", "JSON" or "SNAPPY".
            connection : Connection type, currently only "HTTP" or "HTTPS" supported.
            username   : An optional http username.
            password   : The http password for the username.
        """

        assert (type(gpudb_ip) is str), "Expected a string gpudb_ip address, got: '"+str(gpudb_ip)+"'"

        # gpudb_ip may take the form of :
        #  - "https://user:password@domain.com:port/path/"

        if gpudb_ip.startswith("http://") :    # Allow http://, but remove it.
            gpudb_ip = gpudb_ip[7:]
        elif gpudb_ip.startswith("https://") : # Allow https://, but remove it.
            gpudb_ip = gpudb_ip[8:]
            connection = "HTTPS" # force it

        # Parse the username and password, if supplied.
        gpudb_ip_at_sign_pos = gpudb_ip.find('@')
        if gpudb_ip_at_sign_pos != -1 :
            user_pass = gpudb_ip[:gpudb_ip_at_sign_pos]
            gpudb_ip = gpudb_ip[gpudb_ip_at_sign_pos+1:]
            user_pass_list = user_pass.split(':')
            username = user_pass_list[0]
            if len(user_pass_list) > 1 :
                password = user_pass_list[1]

        url_path = ""
        # Find the URL /path/ and remove it to get the ip address.
        gpudb_ip_path_pos = gpudb_ip.find('/')
        if gpudb_ip_path_pos != -1:
            url_path = gpudb_ip[gpudb_ip_path_pos:]
            if url_path[-1] == '/':
                url_path = url_path[:-1]
            gpudb_ip = gpudb_ip[:gpudb_ip_path_pos]

        # Override default port if specified in ip address
        gpudb_ip_port_pos = gpudb_ip.find(':')
        if gpudb_ip_port_pos != -1 :
            gpudb_port = gpudb_ip[gpudb_ip_port_pos+1:]
            gpudb_ip = gpudb_ip[:gpudb_ip_port_pos]

        # Port does not have to be provided if using standard HTTP(S) ports.
        if (gpudb_port == None) or len(str(gpudb_port)) == 0:
            if connection == 'HTTP' :
                gpudb_port = 80
            elif connection == 'HTTPS' :
                gpudb_port = 443

        # Validate gpudb_port
        try :
            port = int(gpudb_port)
        except :
            assert False, "Expected a numeric gpudb_port, got: '" + str(gpudb_port) + "'"

        assert (port > 0) and (port < 65536), "Expected a valid gpudb_port (1-65535), got: '"+str(gpudb_port)+"'"
        assert (len(gpudb_ip) > 0), "Expected a valid gpudb_ip address, got an empty string."
        assert (encoding in ["BINARY", "JSON", "SNAPPY"]), "Expected encoding to be either 'BINARY', 'JSON' or 'SNAPPY' got: '"+str(encoding)+"'"
        assert (connection in ["HTTP", "HTTPS"]), "Expected connection to be 'HTTP' or 'HTTPS', got: '"+str(connection)+"'"

        if (encoding == 'SNAPPY' and not have_snappy):
            print 'SNAPPY encoding specified but python-snappy is not installed; reverting to BINARY'
            encoding = 'BINARY'

        self.gpudb_ip       = gpudb_ip
        self.gpudb_url_path = url_path
        self.gpudb_port     = int(gpudb_port)
        self.encoding      = encoding
        self.connection    = connection
        self.username      = username
        self.password      = password

        #print("GPUdb ip: '%s', port: %d, path '%s', encoding: '%s', connection: '%s', username: '%s', password: '%s'" %
        #      (self.gpudb_ip, self.gpudb_port, self.gpudb_url_path, self.encoding, self.connection, self.username, self.password))

    # members
    gpudb_ip       = "127.0.0.1" # Input gpudb_ip with gpudb_port appended if provided.
    gpudb_url_path = ""          # Input /path (if any) that was in the gpudb_ip.
    gpudb_port     = "9191"      # Input gpudb_port, may be empty.
    encoding      = "BINARY"    # Input encoding, either 'BINARY' or 'JSON'.
    connection    = "HTTP"      # Input connection type, either 'HTTP' or 'HTTPS'.
    username      = ""          # Input username or empty string for none.
    password      = ""          # Input password or empty string for none.

    # constants
    END_OF_SET = -9999

    # schemas for common data types
    point_schema_str = """{"type":"record","name":"point","fields":[{"name":"x","type":"double"},{"name":"y","type":"double"},{"name":"OBJECT_ID","type":"string"}]}"""
    big_point_schema_str = """{"type":"record","name":"point","fields":[{"name":"msg_id","type":"string"},{"name":"x","type":"double"},{"name":"y","type":"double"},{"name":"TIMESTAMP","type":"double"},{"name":"source","type":"string"},{"name":"group_id","type":"string"},{"name":"OBJECT_ID","type":"string"}]}"""
    gis_point_schema_str = """{"type":"record","name":"Point","fields":[{"name":"x","type":"double"},{"name":"y","type":"double"},{"name":"timestamp","type":"double"},{"name":"tag_id","type":"double"},{"name":"derived","type":"double"},{"name":"msg_id","type":"string"},{"name":"group_id","type":"string"},{"name":"level_one_mgrs","type":"string"},{"name":"level_two_mgrs","type":"string"},{"name":"level_three_mgrs","type":"string"},{"name":"level_final_mgrs","type":"string"},{"name":"OBJECT_ID","type":"string"}]}"""
    bytes_point_schema_str = """{"type":"record","name":"point","fields":[{"name":"msg_id","type":"string"},{"name":"x","type":"double"},{"name":"y","type":"double"},{"name":"timestamp","type":"int"},{"name":"source","type":"string"},{"name":"group_id","type":"string"},{"name":"bytes_data","type":"bytes"},{"name":"OBJECT_ID","type":"string"}]}"""
    bigger_point_schema_str = """{"type":"record","name":"point","fields":[{"name":"ARTIFACTID","type":"string"},{"name":"x","type":"double"},{"name":"y","type":"double"},{"name":"TIMESTAMP","type":"double"},{"name":"DATASOURCE","type":"string"},{"name":"DATASOURCESUB","type":"string"},{"name":"OBJECTAUTH", "type" : "string"},{"name": "AUTHOR", "type":"string"},{"name":"DATASOURCEKEY","type":"string"},{"name":"OBJECT_ID","type":"string"}]}"""
    twitter_point_schema_str = """{"type":"record","name":"point","fields":[{"name":"ARTIFACTID","type":"string"},{"name":"x","type":"double"},{"name":"y","type":"double"},{"name":"TIMESTAMP","type":"double"},{"name":"DATASOURCE","type":"string"},{"name":"DATASOURCESUB","type":"string"},{"name":"KEYWORD","type":"string"},{"name":"OBJECTAUTH", "type" : "string"},{"name": "AUTHOR", "type":"string"},{"name":"DATASOURCEKEY","type":"string"},{"name":"OBJECT_ID","type":"string"}]}"""

    # Parse common schemas, others parsed on demand.
    point_schema = schema.parse(point_schema_str)
    big_point_schema = schema.parse(big_point_schema_str)
    gis_point_schema = None # schema.parse(gis_point_schema_str)
    bytes_point_schema = None # schema.parse(bytes_point_schema_str)
    bigger_point_schema = None # schema.parse(bigger_point_schema_str)
    twitter_point_schema = schema.parse(twitter_point_schema_str)

    loaded_schemas = {} # dict of previously loaded schemas, populated by get_schemas()

    # -----------------------------------------------------------------------
    # Helper functions
    # -----------------------------------------------------------------------

    def post_to_gpudb_read(self, body_data, endpoint):
        """
        Create a HTTP connection and POST then get GET, returning the server response.

        Parameters:
            body_data : Data to POST to GPUdb server.
            endpoint  : Server path to POST to, e.g. "/add".
        """

        if self.encoding == 'BINARY':
            headers = {"Content-type": "application/octet-stream",
                       "Accept": "application/octet-stream"}
        elif self.encoding == 'JSON':
            headers = {"Content-type": "application/json",
                       "Accept": "application/json"}
        elif self.encoding == 'SNAPPY':
            headers = {"Content-type": "application/x-snappy",
                       "Accept": "application/x-snappy"}
            body_data = snappy.compress(body_data)

        if len(self.username) != 0:
            # base64 encode the username and password
            auth = base64.encodestring('%s:%s' % (self.username, self.password)).replace('\n', '')
            headers["Authorization"] = ("Basic %s" % auth)

        # NOTE: Creating a new httplib.HTTPConnection is suprisingly just as
        #       fast as reusing a persistent one and has the advantage of
        #       fully retrying from scratch if the connection fails.

        try:
            if (self.connection == 'HTTP'):
                conn = httplib.HTTPConnection(host=self.gpudb_ip, port=self.gpudb_port)
            elif (self.connection == 'HTTPS'):
                conn = httplib.HTTPSConnection(host=self.gpudb_ip, port=self.gpudb_port)
            else:
                assert False, "Unknown connection type, should be 'HTTP' or 'HTTPS'"
        except:
            print("Error connecting to: '%s' on port %d" % (self.gpudb_ip, self.gpudb_port))
            raise

        try:
            conn.request("POST", self.gpudb_url_path+endpoint, body_data, headers)
        except:
            print("Error posting to: '%s:%d%s'" % (self.gpudb_ip, self.gpudb_port, self.gpudb_url_path+endpoint))
            raise

        try:
            resp = conn.getresponse()
            resp_data = resp.read()
        except: # some error occurred; return a message
            error_resp = collections.OrderedDict()
            error_resp["status"] = "ERROR"
            error_resp["message"] = "Timeout Error: No response received from %s" % self.gpudb_ip
            error_resp["data_type"] = "none"
            error_resp["data"] = ""
            error_resp["data_str"] = ""
            gpudb_resp_schema = self.loaded_schemas["gaia_response"]["REP_SCHEMA"]
            encoded_error_resp = self.write_datum( gpudb_resp_schema, error_resp )
            return encoded_error_resp
        # end except

        # resp = conn.getresponse()
        # #Print resp.status,resp.reason
        # resp_data = resp.read() # TODO: comment this out
        # #print("response size: %d"   % (len(resp_data)))
        # #print("response     : '%s'" % (resp_data))

        return  str(resp_data)

    def write_datum(self, SCHEMA, datum):
        """
        Returns an avro binary or JSON encoded dataum dict using its schema.

        Parameters:
            SCHEMA : A parsed schema from avro.schema.parse().
            datum  : A dict of key-value pairs matching the schema.
        """

        # build the encoder; this output is where the data will be written
        if self.encoding == 'BINARY' or self.encoding == 'SNAPPY':
            output = cStringIO.StringIO()
            be = io.BinaryEncoder(output)

            # Create a 'record' (datum) writer
            writer = io.DatumWriter(SCHEMA)
            writer.write(datum, be)

            return output.getvalue()

        elif self.encoding == 'JSON':

            data_str = json.dumps(datum)

            return data_str

    def encode_datum(self, schema_str, datum):
        OBJ_SCHEMA = schema.parse(schema_str)

        return self.write_datum(OBJ_SCHEMA, datum)

    def read_orig_datum(self, SCHEMA, encoded_datum, encoding=None):
        """
        Decode the binary or JSON encoded datum using the avro schema and return a dict.

        Parameters:
            SCHEMA        : A parsed schema from avro.schema.parse().
            encoded_datum : Binary or JSON encoded data.
            encoding      : Type of avro encoding, either "BINARY" or "JSON",
                            None uses the encoding this class was initialized with.
        """
        if encoding == None:
            encoding = self.encoding

        if (encoding == 'BINARY') or (encoding == 'SNAPPY'):
            output = cStringIO.StringIO(encoded_datum)
            bd = io.BinaryDecoder(output)
            reader = io.DatumReader(SCHEMA)
            out = reader.read(bd) # read, give a decoder

            return out
        elif encoding == 'JSON':
            data_str = json.loads(encoded_datum.replace('\\U','\\u'))

            return data_str


    def read_datum(self, SCHEMA, encoded_datum, encoding=None):
        """
        Decode a gaia_response and decode the contained message too.

        Parameters:
            SCHEMA : The parsed schema from avro.schema.parse() that the gaia_response contains.
            encoded_datum : A BINARY or JSON encoded gaia_response message.
        Returns:
            An OrderedDict of the decoded gaia_response message's data with the
            gaia_response put into the "status_info" field.
        """

        #first parse the gaia_response message
        if "gaia_response" in self.loaded_schemas:
            REP_SCHEMA = self.loaded_schemas["gaia_response"]["REP_SCHEMA"]
        else:
            REP_SCHEMA_STR = open(gpudb_obj_defs_path+"/gaia_response.json","r").read()
            REP_SCHEMA     = schema.parse(REP_SCHEMA_STR)

            self.loaded_schemas["gaia_response"] = { "REP_SCHEMA_STR" : REP_SCHEMA_STR,
                                                     "REP_SCHEMA"     : REP_SCHEMA }

        resp = self.read_orig_datum(REP_SCHEMA, encoded_datum, encoding)

        #now parse the actual response if there is no error
        #NOTE: DATA_SCHEMA should be equivalent to SCHEMA but is NOT for get_set_sorted
        stype = resp['data_type']
        #print 'read_datum, embedded data_type: ',stype
        if stype == 'none':
            out = collections.OrderedDict()
        else:
            #DATA_SCHEMA_STR = open(gpudb_obj_defs_path+"/%s.json"%(stype), "r").read()
            #DATA_SCHEMA = schema.parse(DATA_SCHEMA_STR)
            #out = read_orig_datum(DATA_SCHEMA, resp['data'])
            if self.encoding == 'JSON':
                out = self.read_orig_datum(SCHEMA, resp['data_str'], 'JSON')
            elif (self.encoding == 'BINARY') or (self.encoding == 'SNAPPY'):
                out = self.read_orig_datum(SCHEMA, resp['data'], 'BINARY')

            #print 'read_orig_datum, size = ',len(resp['data'])

        del resp['data']

        out['status_info'] = resp

        return out

    def read_point(self, encoded_datum, encoding=None):
        if self.point_schema is None:
            self.point_schema = schema.parse(self.point_schema_str)

        return self.read_orig_datum(self.point_schema, encoded_datum, encoding)

    def read_big_point(self, encoded_datum, encoding=None):
        if self.big_point_schema is None:
            self.big_point_schema = schema.parse(self.big_point_schema_str)

        return self.read_orig_datum(self.big_point_schema, encoded_datum, encoding)

    def read_gis_point(self, encoded_datum, encoding=None):
        # this point is designed to look like "Point"

        if self.gis_point_schema is None:
            self.gis_point_schema = schema.parse(self.gis_point_schema_str)

        return self.read_orig_datum(self.gis_point_schema, encoded_datum, encoding)

    def get_schemas(self, base_name):
        """
        Get a tuple of parsed and cached request and reply schemas.

        Parameters:
            base_name : Schema name, e.g. "base_name"+"_request.json" or "_response.json"
        """

        if base_name in self.loaded_schemas:
            REQ_SCHEMA = self.loaded_schemas[base_name]["REQ_SCHEMA"]
            REP_SCHEMA = self.loaded_schemas[base_name]["REP_SCHEMA"]
        else:
            REP_SCHEMA_STR = open(gpudb_obj_defs_path+"/"+base_name+"_response.json", "r").read()
            REQ_SCHEMA_STR = open(gpudb_obj_defs_path+"/"+base_name+"_request.json",  "r").read()
            REP_SCHEMA     = schema.parse(REP_SCHEMA_STR)
            REQ_SCHEMA     = schema.parse(REQ_SCHEMA_STR)

            self.loaded_schemas[base_name] = { "REP_SCHEMA_STR" : REP_SCHEMA_STR,
                                               "REQ_SCHEMA_STR" : REQ_SCHEMA_STR,
                                               "REP_SCHEMA"     : REP_SCHEMA,
                                               "REQ_SCHEMA"     : REQ_SCHEMA }
        return (REQ_SCHEMA, REP_SCHEMA)

    def post_then_get(self, REQ_SCHEMA, REP_SCHEMA, datum, endpoint):
        """
        Encode the datum dict using the REQ_SCHEMA, POST to GPUdb server and
        decode the reply using the REP_SCHEMA.

        Parameters:
            REQ_SCHEMA : The parsed schema from avro.schema.parse() of the request.
            REP_SCHEMA : The parsed schema from avro.schema.parse() of the reply.
            datum      : Request dict matching the REQ_SCHEMA.
            endpoint   : Server path to POST to, e.g. "/add".
        """
        #print REQ_SCHEMA, REP_SCHEMA, datum, endpoint
        encoded_datum = self.write_datum(REQ_SCHEMA, datum)
        retval  = self.post_to_gpudb_read(encoded_datum, endpoint)

        return self.read_datum(REP_SCHEMA, retval)


    def do_read_trigger_msg(self, encoded_datum):
        REP_SCHEMA_STR = open(gpudb_obj_defs_path+"/trigger_notification.json", "r").read()
        REP_SCHEMA = schema.parse(REP_SCHEMA_STR)

        return self.read_orig_datum(REP_SCHEMA, encoded_datum)

    # -----------------------------------------------------------------------
    # Endpoint calls
    # -----------------------------------------------------------------------

    # -----------------------------------------------------------------------
    # add_object -> /add

    def do_add(self,set_id,objdata,params={}):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("add_object")

        req_datum = collections.OrderedDict()

        req_datum['set_id'] = set_id
        if (self.encoding == 'BINARY') or (self.encoding == 'SNAPPY'):
            req_datum['object_data'] = objdata
            req_datum['object_data_str'] = ""
            req_datum['object_encoding'] = 'BINARY'
        elif self.encoding == 'JSON':
            req_datum['object_data'] = ""
            req_datum['object_data_str'] = objdata
            req_datum['object_encoding'] = 'JSON'
        req_datum['params'] = params

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, req_datum, "/add")


    def do_add_big_point(self, set_id, msg_id, x, y, timestamp, source, group_id, OBJECT_ID=''):
        if self.big_point_schema is None:
            self.big_point_schema = schema.parse(self.big_point_schema_str)

        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("add_object")

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

        return self.do_add(set_id, objdata)

    def do_add_bigger_point(self, set_id, artifact_id, x, y, timestamp, OBJECT_ID=''):
        if self.bigger_point_schema is None:
            self.bigger_point_schema = schema.parse(self.bigger_point_schema_str)

        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("add_object")

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

        return self.do_add(set_id, objdata)


    def do_add_bytes_point(self, set_id, msg_id, x, y, timestamp, source, group_id, bytes_data, OBJECT_ID=''):
        if self.bytes_point_schema is None:
            self.bytes_point_schema = schema.parse(self.bytes_point_schema_str)

        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("add_object")

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

        return self.do_add(set_id, objdata)


    def do_add_gis_point(self, set_id, msg_id, x, y, timestamp, tag_id, derived, group_id,
                         level_one_mgrs, level_two_mgrs, level_three_mgrs, level_final_mgrs, OBJECT_ID=''):
        if self.gis_point_schema is None:
            self.gis_point_schema = schema.parse(self.gis_point_schema_str)

        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("add_object")

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

        return self.do_add(set_id, objdata)


    def do_add_point(self, set_id, x, y, OBJECT_ID=''):
        if self.point_schema is None:
            self.point_schema = schema.parse(self.point_schema_str)

        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("add_object")

        datum = collections.OrderedDict()
        datum['x'] = x
        datum['y'] = y
        datum['OBJECT_ID'] = OBJECT_ID

        objdata = self.write_datum(self.point_schema, datum)

        return self.do_add(set_id, objdata)

    # -----------------------------------------------------------------------
    # add_symbol -> /addsymbol

    def do_add_symbol(self, symbol_id, symbol_format, symbol_data, params = {}):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("add_symbol")

        datum = collections.OrderedDict()
        datum["symbol_id"]      = symbol_id
        datum["symbol_format"]  = symbol_format
        datum["symbol_data"]    = symbol_data
        datum["params"]         = params

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/addsymbol")


    def do_add_twitter_point(self, set_id, artifact_id, x, y, timestamp,keyword,author, OBJECT_ID=''):
        if self.twitter_point_schema is None:
            self.twitter_point_schema = schema.parse(self.twitter_point_schema_str)

        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("add_object")

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

        return self.do_add(set_id, objdata)

    # -----------------------------------------------------------------------
    # authenticate_users -> /authenticateusers

    def do_authenticate_users(self, user_auth_strings,set_ids):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("authenticate_users")

        datum = collections.OrderedDict()
        datum["user_auth_strings"] = user_auth_strings
        datum["set_ids"] = set_ids

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/authenticateusers")

    # -----------------------------------------------------------------------
    # bounding_box -> /boundingbox

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

    # -----------------------------------------------------------------------
    # bulk_add -> /bulkadd

    # generic version
    def do_bulk_add(self, set_id, objdatas, params={}):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("bulk_add")

        req_datum = collections.OrderedDict()

        req_datum['set_id'] = set_id
        if (self.encoding == 'BINARY') or (self.encoding == 'SNAPPY'):
            req_datum['list'] = objdatas
            req_datum['list_str'] = ['']*len(objdatas)
            req_datum['list_encoding'] = 'BINARY'
        elif self.encoding == 'JSON':
            req_datum['list'] = ['']*len(objdatas)
            req_datum['list_str'] = objdatas
            req_datum['list_encoding'] = 'JSON'   
        req_datum['params'] = params

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, req_datum, "/bulkadd")

    # This assumes equal length lists
    def do_bulk_add_big_point(self, set_id, msg_id_list, x_list, y_list, timestamp_list, source_list, group_id_list, OBJECT_ID_list=None):
        if self.big_point_schema is None:
            self.big_point_schema = schema.parse(self.big_point_schema_str)

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

        return self.do_bulk_add(set_id, obj_list_encoded)

    # This assumes that 'x' and 'y' are equal length lists
    def do_bulk_add_point(self, set_id, x_list, y_list, OBJECT_ID_list=None):
        if self.point_schema is None:
            self.point_schema = schema.parse(self.point_schema_str)

        if (OBJECT_ID_list is None):
            OBJECT_ID_list = ['' for x in x_list]

        obj_list_encoded = []

        for i in range(0,len(x_list)):
            datum = collections.OrderedDict()
            datum['x'] = x_list[i]
            datum['y'] = y_list[i]
            datum['OBJECT_ID'] = OBJECT_ID_list[i]
            obj_list_encoded.append(self.write_datum(self.point_schema, datum))

        return self.do_bulk_add(set_id, obj_list_encoded)

    # -----------------------------------------------------------------------
    # bulk_delete -> /bulkdelete

    def do_bulk_delete(self, set_id, global_expression, expressions, params = {}, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("bulk_delete")

        datum = collections.OrderedDict()
        datum["set_id"] = set_id
        datum["global_expression"] = global_expression
        datum["expressions"] = expressions
        datum["params"] = params
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/bulkdelete")

    # -----------------------------------------------------------------------
    # bulk_select -> /bulkselect

    def do_bulk_select(self, set_id, global_expression, expressions, params = {}, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("bulk_select")

        datum = collections.OrderedDict()
        datum["set_id"] = set_id
        datum["global_expression"] = global_expression
        datum["expressions"] = expressions
        datum["params"] = params
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/bulkselect")

    # -----------------------------------------------------------------------
    # bulk_selectvalues -> /bulkselectvalues

    def do_bulk_select_values(self, set_id, attributes, global_expression, expressions, params = {}, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("bulk_select_values")

        datum = collections.OrderedDict()
        datum["set_id"] = set_id
        datum["attributes"] = attributes
        datum["global_expression"] = global_expression
        datum["expressions"] = expressions
        datum["params"] = params
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/bulkselectvalues")

    # -----------------------------------------------------------------------
    # bulk_update -> /bulkupdate

    def do_bulk_update(self, set_id, global_expression, expressions, new_values_map, objects_to_insert = [], 
                       objects_to_insert_str = [], object_encoding = 'BINARY', params = {}, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("bulk_update")

        datum = collections.OrderedDict()
        datum["set_id"] = set_id
        datum["global_expression"] = global_expression
        datum["expressions"] = expressions
        datum["new_values_maps"] = new_values_map
        datum["objects_to_insert"] = objects_to_insert
        datum["objects_to_insert_str"] = objects_to_insert_str
        datum["object_encoding"] = object_encoding
        datum["params"] = params
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/bulkupdate")

    # -----------------------------------------------------------------------

    # -----------------------------------------------------------------------
    # clear -> /clear

    def do_clear(self, set_id="", authorization=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("clear")

        datum = collections.OrderedDict()
        datum["set_id"] = set_id
        datum["authorization"] = authorization

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/clear")

    # -----------------------------------------------------------------------
    # clear_trigger -> /clear_trigger

    def do_clear_trigger(self, trigger_id):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("clear_trigger")

        datum = collections.OrderedDict()
        datum["trigger_id"] = trigger_id

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/cleartrigger")

    # -----------------------------------------------------------------------
    # clear_auth_cache -> /clearauthcache

    def do_clear_auth_cache(self, set_ids=[], user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("clear_auth_cache")

        datum = collections.OrderedDict()
        datum["set_ids"] = set_ids
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/clearauthcache")

    # -----------------------------------------------------------------------
    # convex_hull -> /convexhull

    def do_convex_hull(self, x_attr_name, y_attr_name, set_id, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("convex_hull")

        datum = collections.OrderedDict()
        datum["x_attr_name"] = x_attr_name
        datum["y_attr_name"] = y_attr_name
        datum["set_id"] = set_id
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/convexhull")

    # -----------------------------------------------------------------------
    # copy_set -> /copyset

    def do_copy_set(self, new_set_id, original_set_id, selector, type_id, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("copy_set")

        datum = collections.OrderedDict()
        datum["new_set_id"] = new_set_id
        datum["original_set_id"] = original_set_id
        datum["selector"] = selector
        datum["type_id"] = type_id
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/copyset")

    # -----------------------------------------------------------------------
    # delete_object -> /deleteobject

    def do_delete_object(self, set_ids, OBJECT_ID, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("delete_object")

        datum = collections.OrderedDict()

        datum["set_ids"] = set_ids
        datum["OBJECT_ID"] = OBJECT_ID
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/deleteobject")

    # -----------------------------------------------------------------------
    # execute -> /execute

    def do_execute(self, statement):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("execute")

        datum = collections.OrderedDict()
        datum["statement"] = statement

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/execute")

    # -----------------------------------------------------------------------
    # exit -> /exit

    def do_exit(self, exit_type, authorization):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("exit")

        datum = collections.OrderedDict()
        datum["exit_type"] = exit_type
        datum["authorization"] = authorization

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/exit")

    # -----------------------------------------------------------------------
    # filter_by_bounds -> /filterbybounds

    # filter by bounds; return a count of how many objects have lower < attribute < upper
    def do_filter_by_bounds(self, set_id, lower_bound, attribute, upper_bound, result_set_id, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("filter_by_bounds")

        datum = collections.OrderedDict()
        datum["attribute"] = attribute
        datum["lower_bound"] = lower_bound
        datum["result_set_id"] = result_set_id
        datum["set_id"] = set_id
        datum["upper_bound"] = upper_bound
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/filterbybounds")

    # -----------------------------------------------------------------------
    # filter_by_list -> /filterbylist

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

    # -----------------------------------------------------------------------
    # filter_by_nai -> /filterbynai

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

    # -----------------------------------------------------------------------
    # filter_by_radius -> /filterbyradius

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

    # -----------------------------------------------------------------------
    # filter_by_set -> /filterbyset

    # filter by set; like filter by list except using data contained in another set
    def do_filter_by_set(self, set_id, result_set_id, attribute, source_set_id, source_set_attribute, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("filter_by_set")

        datum = collections.OrderedDict()
        datum["set_id"] = set_id
        datum["result_set_id"] = result_set_id
        datum["attribute"] = attribute
        datum["source_set_id"] = source_set_id
        datum["source_set_attribute"] = source_set_attribute
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/filterbyset")

    # -----------------------------------------------------------------------
    # filter_by_string -> /filterbystring

    # filter by string; return a count of how many objects have an attribute (or any attribute) matching the expression
    def do_filter_by_string(self, expression, mode, options, set_id, attributes, result_set_id, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("filter_by_string")

        datum = collections.OrderedDict()
        datum["expression"] = expression
        datum["mode"] = mode
        datum["options"] = options
        datum["set_id"] = set_id
        datum["attributes"] = attributes
        datum["result_set_id"] = result_set_id
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/filterbystring")

    # -----------------------------------------------------------------------
    # filter_by_track -> /filterbytrack

    # filter by track; return a count of how many points are with a spatio-temporal radius of a given track
    def do_filter_by_track(self, set_id, track_id, target_track_ids, params, result_set_id, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("filter_by_track")

        datum = collections.OrderedDict()
        datum["set_id"] = set_id
        datum["track_id"] = track_id
        datum["target_track_ids"] = target_track_ids
        datum["params"] = params
        datum["result_set_id"] = result_set_id
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/filterbytrack")

    # -----------------------------------------------------------------------
    # filter_by_track_values -> /filterbytrackvalues

    # filter by track values; return a count of how many points are with a spatio-temporal radius of a given track
    def do_filter_by_track_values(self, set_id, x_vals, y_vals, t_vals, track_ids_to_ignore, target_track_ids, params, result_set_id, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("filter_by_track_values")

        datum = collections.OrderedDict()
        datum["set_id"] = set_id
        datum["x_vals"] = x_vals
        datum["y_vals"] = y_vals
        datum["t_vals"] = t_vals
        datum["track_ids_to_ignore"] = track_ids_to_ignore
        datum["target_track_ids"] = target_track_ids
        datum["params"] = params
        datum["result_set_id"] = result_set_id
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/filterbytrackvalues")

    # -----------------------------------------------------------------------
    # filter_by_value -> /filterbyvalue

    # filter by value; return a count of how many objects have an attribute (or any attribute) equal to the specified value
    def do_filter_by_value(self, set_id, is_string, value, value_str, attribute, result_set_id, user_auth=""):
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

    # -----------------------------------------------------------------------
    # filter_then_histogram -> /filterthenhistogram

    def do_filter_then_histogram(self, set_id, filter_attribute, filter_values, histogram_attribute, interval, start, end, user_auth=""):
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

    # -----------------------------------------------------------------------
    # generate_heatmap_video -> /generateheatmapvideo

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

    # -----------------------------------------------------------------------
    # generate_video -> /generatevideo

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

    # -----------------------------------------------------------------------
    # get_objects -> /getobjects

    def do_get_objects(self, set_id, attribute, attr_vals, attr_str_vals, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("get_objects")

        datum = collections.OrderedDict()
        datum["set_id"] = set_id
        datum["attribute"] = attribute
        datum["attr_vals"] = attr_vals
        datum["attr_str_vals"] = attr_str_vals
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/getobjects")

    # -----------------------------------------------------------------------
    # get_orphans -> /getorphans

    def do_get_orphans(self, set_namespace=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("get_orphans")

        datum = collections.OrderedDict()
        datum["set_namespace"] = set_namespace

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/getorphans")

    # -----------------------------------------------------------------------
    # get_set -> /getset

    def do_get_set(self, set_id, start, end, semantic_type="", user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("get_set")

        datum = collections.OrderedDict()
        datum["start"] = start
        datum["end"] = end
        datum["set_id"] = set_id
        datum["semantic_type"] = semantic_type
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/getset")

    # -----------------------------------------------------------------------
    # get_set_metdata -> /getsetmetadata

    def do_get_set_metadata(self, set_ids):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("get_set_metadata")

        datum = collections.OrderedDict()
        datum["set_ids"] = set_ids

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/getsetmetadata")

    # -----------------------------------------------------------------------
    # get_set_objects -> /getsetobjects

    def do_get_set_objects(self, set_id, start, end, encoding="binary", user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("get_set_objects")

        datum = collections.OrderedDict()
        datum["set_id"] = set_id
        datum["start"] = start
        datum["end"] = end
        datum["encoding"] = encoding
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/getsetobjects")

    # -----------------------------------------------------------------------
    # get_set_objects -> /getvalues

    def do_get_values(self, set_id, attributes, start, end, params = {}, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("get_values")

        datum = collections.OrderedDict()
        datum["set_id"] = set_id
        datum["attributes"] = attributes
        datum["start"] = start
        datum["end"] = end
        datum["params"] = params;
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/getvalues")

    # -----------------------------------------------------------------------
    # get_set_properties -> /getsetproperties

    def do_get_set_properties(self, set_ids):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("get_set_properties")

        datum = collections.OrderedDict()
        datum["set_ids"] = set_ids

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/getsetproperties")

    # -----------------------------------------------------------------------
    # get_set_sizes -> /getsetsizesc

    def do_get_set_sizes(self, set_ids):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("get_set_sizes")

        datum = collections.OrderedDict()
        datum["set_ids"] = set_ids

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/getsetsizes")

    # -----------------------------------------------------------------------
    # get_sets_by_type_info -> /getsetsbytypeinfo

    def do_get_sets_by_type_info(self, type_id, label, semantic_type):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("get_sets_by_type_info")

        datum = collections.OrderedDict()
        datum["type_id"] = type_id
        datum["label"] = label
        datum["semantic_type"] = semantic_type

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/getsetsbytypeinfo")

    # -----------------------------------------------------------------------
    # get_tracks -> /gettracks

    def do_get_tracks(self, set_id, world_set_id, min_x, max_x, min_y, max_y, x_attr_name, y_attr_name, do_extent, start, end, user_auth=""):
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

    # -----------------------------------------------------------------------
    # get_tracks2 -> /gettracks2

    def do_get_tracks2(self, set_id, world_set_id, min_x, max_x, min_y, max_y, x_attr_name, y_attr_name, do_extent, start, end, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("get_tracks2")

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

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/gettracks2")

    # -----------------------------------------------------------------------
    # get_get_sorted_set -> /getsortedset

    def do_get_sorted_set(self, set_id, attribute, start, end, encoding="binary", params = {}, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("get_sorted_set")

        datum = collections.OrderedDict()
        datum["set_id"] = set_id
        datum["attribute"] = attribute
        datum["start"]  = start
        datum["end"]    = end
        datum["encoding"] = encoding;
        datum["params"] = params;
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/getsortedset")

    # -----------------------------------------------------------------------
    # get_type_info -> /gettypeinfo

    def do_get_type_info(self, type_id, label, semantic_type):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("get_type_info")

        datum = collections.OrderedDict()
        datum["type_id"] = type_id
        datum["label"] = label
        datum["semantic_type"] = semantic_type

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/gettypeinfo")

    # -----------------------------------------------------------------------
    # get_sorted_sets -> /getsortedsets

    def do_get_sorted_sets(self, set_ids, attribute, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("get_sorted_sets")

        datum = collections.OrderedDict()
        datum["set_ids"] = set_ids
        datum["attribute"] = attribute
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/getsortedsets")

    # -----------------------------------------------------------------------
    # group_by -> /groupby

    #group by [attributes is a list]
    def do_group_by(self, set_id, attributes, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("group_by")

        datum = collections.OrderedDict()
        datum["set_id"] = set_id
        datum["attributes"] = attributes
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/groupby")

    # -----------------------------------------------------------------------
    # group_by_value -> /groupbyvalue

    #group by value [attributes is a list]
    def do_group_by_value(self, set_id, attributes, value_attribute = "", params = {}, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("group_by_value")

        datum = collections.OrderedDict()
        datum["set_id"] = set_id
        datum["attributes"] = attributes
        datum["value_attribute"] = value_attribute
        datum["params"] = params
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/groupbyvalue")

    # -----------------------------------------------------------------------
    # histogram -> /histogram

    def do_histogram(self, set_id, attribute, interval, start, end, params={}, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("histogram")

        datum = collections.OrderedDict()
        datum["set_id"] = set_id
        datum["attribute"] = attribute
        datum["start"] = start
        datum["end"] = end
        datum["interval"] = interval
        datum["params"] = params
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/histogram")

    # -----------------------------------------------------------------------
    # index -> /index

    def do_index(self, set_id, attribute, action, params={}):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("index")

        datum = collections.OrderedDict()
        datum["set_id"] = set_id
        datum["attribute"] = attribute
        datum["action"] = action
        datum["params"] = params

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/index")

    # -----------------------------------------------------------------------
    # join -> /join

    def do_join(self, left_set, left_attr, right_set, right_attr, result_type, result_set, user_auth=""):
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

    # -----------------------------------------------------------------------
    # join_incremental -> /joinincremental

    def do_join_incremental(self, left_subset, left_attr, left_index, right_set, right_attr, result_set, result_type, user_auth=""):
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

    # -----------------------------------------------------------------------
    # join_setup -> /joinsetup

    #initial join setup for the incremental join
    def do_join_setup(self, left_set, left_attr, right_set, right_attr, subset_id, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("join_setup")

        datum = collections.OrderedDict()
        datum["left_set"] = left_set
        datum["left_attr"] = left_attr
        datum["right_set"] = right_set
        datum["right_attr"] = right_attr
        datum["subset_id"] = subset_id
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/joinsetup")

    # -----------------------------------------------------------------------
    # make_bloom -> /makebloom

    def do_make_bloom(self, set_id, attribute):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("make_bloom")

        datum = collections.OrderedDict()
        datum["set_id"] = set_id
        datum["attribute"] = attribute

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/makebloom")

    # -----------------------------------------------------------------------
    # max_min -> /maxmin

    def do_max_min(self, set_id, attribute, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("max_min")

        datum = collections.OrderedDict()
        datum["attribute"] = attribute
        datum["set_id"] = set_id
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/maxmin")

    # -----------------------------------------------------------------------
    # merge_sets -> /mergesets

    def do_merge_sets(self, set_ids, merged_set_id, common_type_id):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("merge_sets")

        datum = collections.OrderedDict()
        datum["set_ids"] = set_ids
        datum["merged_set_id"] = merged_set_id
        datum["common_type_id"] = common_type_id

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/mergesets")

    # -----------------------------------------------------------------------
    # new_set -> /newset

    def do_new_set(self, type_id, set_id, parent_set_id=None):
        if parent_set_id == None:
            parent_set_id = set_id

        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("new_set")

        datum = collections.OrderedDict()
        datum["set_id"] = set_id
        datum["parent_set_id"] = parent_set_id
        datum["type_id"] = type_id

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/newset")

    # -----------------------------------------------------------------------
    # plot2d_heatmap -> /plot2dheatmap

    def do_plot2d_heatmap(self, set_ids, colormap, blur_radius, min_x, max_x, min_y, max_y, 
                          x_attr_name, y_attr_name, value_attr_name,
                          width, height, projection, bg_color, gradient_start_color, gradient_end_color, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("plot2d_heatmap")

        datum = collections.OrderedDict()
        datum["min_x"] = min_x
        datum["max_x"] = max_x
        datum["min_y"] = min_y
        datum["max_y"] = max_y
        datum["x_attr_name"] = x_attr_name
        datum["y_attr_name"] = y_attr_name
        datum["value_attr_name"] = value_attr_name
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

    # -----------------------------------------------------------------------
    # plot2d_heatmap_cb -> /plot2dheatmapcb - class break

    def do_plot2d_heatmap_cb(self, min_x, max_x, min_y, max_y, x_attr_name, y_attr_name, width, height, projection,
                             set_ids, cb_attr, cb_vals, cb_ranges,
                             colormaps, blur_radii, bg_color, gradient_start_colors, gradient_end_colors, user_auth=""):
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

    # -----------------------------------------------------------------------
    # plot2d_multiple_2 -> /plot2dmultiple2

    def do_plot2d_multiple_2(self, min_x, max_x, min_y, max_y, x_attr_name, y_attr_name, width, height, projection, bg_color,
                             set_ids, world_set_ids, track_ids,
                             do_points, do_shapes, do_tracks, do_symbology,
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
        datum["do_symbology"] = do_symbology

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

    # -----------------------------------------------------------------------
    # plot2d_multiple_cb -> /plot2dmultiplecb - class break

    def do_plot2d_multiple_cb(self, min_x, max_x, min_y, max_y, x_attr_name, y_attr_name, width, height, projection, bg_color,
                              set_ids, world_set_ids, track_ids,
                              cb_attr1, cb_vals1,
                              cb_attr2, cb_vals2,
                              do_points, do_shapes, do_tracks, do_symbology,
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

        datum['cb_attr2'] = cb_attr2
        datum['cb_vals2'] = cb_vals2

        datum["do_points"] = do_points
        datum["do_shapes"] = do_shapes
        datum["do_tracks"] = do_tracks
        datum["do_symbology"] = do_symbology

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

    # -----------------------------------------------------------------------
    # populate_full_tracks -> /populatefulltracks

    def do_populate_full_tracks(self, set_id, world_set_id, result_set_id, reserved = [], user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("populate_full_tracks")

        datum = collections.OrderedDict()
        datum["set_id"] = set_id
        datum["world_set_id"] = world_set_id
        datum["result_set_id"] = result_set_id
        datum["reserved"] = reserved
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/populatefulltracks")

    # -----------------------------------------------------------------------
    # predicate_join -> /predicatejoin

    def do_predicate_join(self, left_set, right_set, predicate, common_type, result_type, result_set, user_auth=""):
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

    # -----------------------------------------------------------------------
    # random -> /random

    def do_random(self, set_id, count, param_map={}):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("random")

        datum = collections.OrderedDict()
        datum["set_id"] = set_id
        datum["count"] = count
        datum["param_map"] = param_map

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/random")

    # -----------------------------------------------------------------------
    # register_parent_set -> /registerparentset

    def do_register_parent_set(self, set_id, allow_duplicate_children=True):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("register_parent_set")

        datum = collections.OrderedDict()
        datum["set_id"] = set_id
        datum["allow_duplicate_children"] = allow_duplicate_children

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/registerparentset")

    # -----------------------------------------------------------------------
    # register_trigger_nai -> /registertriggernai

    def do_register_trigger_nai(self, trigger_id, set_ids, xattr, xvals, yattr, yvals):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("register_trigger_nai")

        datum = collections.OrderedDict()
        datum["request_id"] = trigger_id
        datum["set_ids"] = set_ids
        datum["x_attribute"] = xattr
        datum["x_vector"] = xvals
        datum["y_attribute"] = yattr
        datum["y_vector"] = yvals

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/registertriggernai")

    # -----------------------------------------------------------------------
    # register_trigger_range -> /registertriggerrange

    def do_register_trigger_range(self, trigger_id, set_ids, attr, minval, maxval):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("register_trigger_range")

        datum = collections.OrderedDict()
        datum["request_id"] = trigger_id
        datum["set_ids"] = set_ids
        datum["attr"] = attr
        datum["lowest"] = minval
        datum["highest"] = maxval

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/registertriggerrange")

    # -----------------------------------------------------------------------
    # get_trigger_info -> /gettriggerinfo

    def do_get_trigger_info(self, trigger_ids):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("get_trigger_info")

        datum = collections.OrderedDict()
        datum["trigger_ids"] = trigger_ids

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/gettriggerinfo")

    # -----------------------------------------------------------------------
    # register_type -> /registertype

    def do_register_type(self, type_definition, annotation="", label="", semantic_type=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("register_type")

        datum = collections.OrderedDict()
        datum["type_definition"] = type_definition
        datum["annotation"] = annotation
        datum["label"] = label
        datum["semantic_type"] = semantic_type

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/registertype")

    def do_register_type_with_annotations(self, type_definition, label="", semantic_type="", annotations = {}):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("register_type_with_annotations")

        datum = collections.OrderedDict()
        datum["type_definition"] = type_definition
        datum["label"] = label
        datum["semantic_type"] = semantic_type
        datum["annotations"] = annotations

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/registertypewithannotations")

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

    def do_register_type_bytes_point(self):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("register_type")

        datum = collections.OrderedDict()
        datum["type_definition"] = self.bytes_point_schema_str
        datum["annotation"] = "msg_id"
        datum["label"] = "bytes_point_type"
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

    def do_register_type_point(self):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("register_type")

        datum = collections.OrderedDict()
        datum["type_definition"] = self.point_schema_str
        datum["annotation"] = ""
        datum["label"] = "basic_point_type"
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

    # -----------------------------------------------------------------------
    # register_type_transform -> /registertypetransform

    def do_register_type_transform(self, type_id, new_type_id, transform_map):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("register_type_transform")

        datum = collections.OrderedDict()
        datum["type_id"] = type_id
        datum["new_type_id"] = new_type_id
        datum["transform_map"] = transform_map

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/registertypetransform")

    # -----------------------------------------------------------------------
    # road_intersection -> /roadintersection

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

    # -----------------------------------------------------------------------
    # select -> /select

    def do_select(self, set_id, result_set_id, expression, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("select")

        datum = collections.OrderedDict()
        datum["expression"] = expression
        datum["result_set_id"] = result_set_id
        datum["set_id"] = set_id
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/select")

    # -----------------------------------------------------------------------
    # select_delete -> /selectdelete

    def do_select_delete(self, set_id, expression, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("select_delete")

        datum = collections.OrderedDict()
        datum["set_id"] = set_id
        datum["expression"] = expression
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/selectdelete")

    # -----------------------------------------------------------------------
    # select_update -> /select_update

    def do_select_update(self, set_id, expression, new_values_map, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("select_update")

        datum = collections.OrderedDict()
        datum["set_id"] = set_id
        datum["expression"] = expression
        datum["new_values_map"] = new_values_map
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/selectupdate")

    # -----------------------------------------------------------------------
    # server_status -> /serverstatus

    def do_server_status(self, option=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("server_status")

        datum = collections.OrderedDict()
        datum["option"] = option

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/serverstatus")

    # -----------------------------------------------------------------------
    # server_timing -> /servertiming

    def do_server_timing(self, option=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("server_timing")

        datum = collections.OrderedDict()
        datum["option"] = option

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/servertiming")

    # -----------------------------------------------------------------------
    # set_info -> /setinfo

    def do_set_info(self, set_id):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("set_info")

        datum = collections.OrderedDict()
        datum["set_ids"] = [set_id]

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/setinfo")

    # -----------------------------------------------------------------------
    # sort -> /sort

    def do_sort(self, set_id, attribute):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("sort")

        datum = collections.OrderedDict()
        datum["set_id"] = set_id
        datum["attribute"] = attribute

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/sort")

    # -----------------------------------------------------------------------
    # spatial_query -> /spatialquery

    def do_spatial_query(self, wkt_string_1, wkt_string_2, operation):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("spatial_query")

        datum = collections.OrderedDict()

        datum["wkt_string_1"] = wkt_string_1
        datum["wkt_string_2"] = wkt_string_2
        datum["operation"] = operation

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/spatialquery")

    # -----------------------------------------------------------------------
    # spatial_set_query -> /spatialsetquery

    def do_spatial_set_query(self, set_ids, wkt_attr_name, wkt_string, operation, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("spatial_set_query")

        datum = collections.OrderedDict()

        datum["set_ids"] = set_ids
        datum["wkt_attr_name"] = wkt_attr_name
        datum["wkt_string"] = wkt_string
        datum["operation"] = operation
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/spatialsetquery")

    # -----------------------------------------------------------------------
    # statistics -> /statistics

    def do_statistics(self, set_id, attribute, stats, params = {}, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("statistics")

        datum = collections.OrderedDict()
        datum["stats"] = stats
        datum["attribute"] = attribute
        datum["set_id"] = set_id
        datum["params"] = params;
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/statistics")

    # -----------------------------------------------------------------------
    # ranged_statistics -> /rangedstatistics

    def do_ranged_statistics(self, set_id, attribute, value_attribute, stats, interval, start, end, select_expression = "", params = {}, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("ranged_statistics")

        datum = collections.OrderedDict()
        datum["set_id"] = set_id
        datum["attribute"] = attribute
        datum["value_attribute"] = value_attribute
        datum["stats"] = stats
        datum["start"] = start
        datum["end"] = end
        datum["interval"] = interval
        datum["select_expression"] = select_expression
        datum["params"] = params;
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/rangedstatistics")

    # -----------------------------------------------------------------------
    # kmeans -> /kmeans

    def do_k_means(self, set_id, k, attributes, tolerance, params = {}, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("k_means")

        datum = collections.OrderedDict()
        datum["set_id"] = set_id
        datum["attributes"] = attributes
        datum["k"] = k
        datum["tolerance"] = tolerance
        datum["params"] = params;
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/kmeans")

    # -----------------------------------------------------------------------
    # stats -> /stats

    def do_stats(self, set_id=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("stats")

        datum = collections.OrderedDict()
        datum["set_id"] = set_id

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/stats")

    # -----------------------------------------------------------------------
    # status -> /status

    def do_status(self, set_id=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("status")

        datum = collections.OrderedDict()
        datum["set_id"] = set_id

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/status")

    # -----------------------------------------------------------------------
    # store_group_by -> /storegroupby

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

    # -----------------------------------------------------------------------
    # unique -> /unique

    def do_unique(self, set_id, attribute, params = {}, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("unique")

        datum = collections.OrderedDict()
        datum["set_id"] = set_id
        datum["attribute"] = attribute
        datum["params"] = params
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/unique")

    # -----------------------------------------------------------------------
    # update_object -> /updateobject

    def do_update_object(self, set_ids, OBJECT_ID, objdata, user_auth=""):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("update_object")

        datum = collections.OrderedDict()

        datum["set_ids"] = set_ids
        datum["OBJECT_ID"] = OBJECT_ID
        datum['object_data'] = objdata if (self.encoding == 'BINARY' or self.encoding == 'SNAPPY') else ""
        datum['object_data_str'] = objdata if (self.encoding == 'JSON') else ""
        datum['object_encoding'] = self.encoding
        datum["user_auth_string"] = user_auth

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/updateobject")

    # -----------------------------------------------------------------------
    # update_set_metdata -> /updatesetmetadata

    def do_update_set_metadata(self, set_ids, metadata_map):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("update_set_metadata")

        datum = collections.OrderedDict()
        datum["set_ids"] = set_ids
        datum["metadata_map"] = metadata_map

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/updatesetmetadata")

    # -----------------------------------------------------------------------
    # update_set_properties -> /updatesetproperties

    def do_update_set_properties(self, set_ids, properties_map):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("update_set_properties")

        datum = collections.OrderedDict()
        datum["set_ids"] = set_ids
        datum["properties_map"] = properties_map

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/updatesetproperties")

    # -----------------------------------------------------------------------
    # update_set_ttl -> /updatesetttl

    def do_update_set_ttl(self, set_id, ttl):
        (REQ_SCHEMA,REP_SCHEMA) = self.get_schemas("update_set_ttl")

        datum = collections.OrderedDict()
        datum["set_id"] = set_id
        datum["ttl"] = ttl

        return self.post_then_get(REQ_SCHEMA, REP_SCHEMA, datum, "/updatesetttl")

