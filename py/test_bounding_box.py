import gaia_api_ispatial
import uuid
import json
import random

def test_bounding_box():
    # need a type first
    retval = gaia_api_ispatial.do_register_type_point()    
    type_id = retval['type_id']

    # get id, create a new set
    set_id = str(uuid.uuid1())
    retval = gaia_api_ispatial.do_new_set(type_id,set_id)
    
    # add points that will be within the box
    my_points = [] # list of points; can't guarrantee order back will be the same
    points_inside = 4    
    for i in range(0,points_inside):
        point = {}
        point["x"] = round(random.uniform(1, 10), 6)
        point["y"] = round(random.uniform(1, 10), 6)
        gaia_api_ispatial.do_add_point(set_id, point["x"], point["y"])
        my_points.append(point)

    # add points that will be outside the box
    for i in range(0,10):
        gaia_api_ispatial.do_add_point(set_id, round(random.uniform(12, 20), 6), round(random.uniform(12, 20), 6))
    
    # outside; just another test
    gaia_api_ispatial.do_add_point(set_id, 11.11, 10.11)
    
    # do bounding box, only capture the points in the first for loop (1,10) and (1,10)
    # NOTE: uniform(a,b) is inclusive for the endpoints but the BB is exlusive i.e. < not <=

    result_set_id = str(uuid.uuid1())
    retval = gaia_api_ispatial.do_bounding_box(0.1,11.1,0.1,11.1,'x','y',set_id,result_set_id)
    count = retval['count']
    
    print count
    assert points_inside == count
    
    #####
    # check all the points
    retobj = gaia_api_ispatial.do_get_set(result_set_id, 0, count-1)
    byte_list = retobj['list']
    print byte_list
    assert len(byte_list) == points_inside
    
    # convert byte list (i.e. list of byte encoded objects into proper datums)
    obj_list = []
    for encoded_datum in byte_list:
        print encoded_datum
        print gaia_api_ispatial.read_point(encoded_datum)
        obj_list.append(gaia_api_ispatial.read_point(encoded_datum))

    for point in obj_list:
        my_points.remove(point)
        
    assert len(my_points) == 0
    
if __name__ == '__main__':
    test_bounding_box()
