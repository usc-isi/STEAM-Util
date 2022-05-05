import sys
import math
import glob
import subprocess
import os
from os.path import exists
import numpy
import re



def usage():
    print("python event2htsim.py <event file> <total ranks>")
    sys.exit("exit")

# covert alist to a list
# if a == [-1], make a list of range(total_ranks)
#
def handle_list(alist):
    if alist[0] == '[': # list
        t = alist.strip().strip('][').split(', ')
        rt = [int(i) for i in t]
        return(rt)
    else:
        v = int(alist)
        if (v == -1): v = -999999999
        return([v])

def handle_ranks (alist):
    if alist == [-1]: # all
        a = []
        for i in range(total_ranks):
            a.append(i)
        return a
    if alist == [-999999999]:
        return([-1])
    if (isinstance(alist, list)):
        return alist
    assert alist < 0
    return []

# argv[1]: event file
if len(sys.argv) != 3:
    usage()

# single file for now
event_input = open(sys.argv[1], 'r')
total_ranks = int(sys.argv[2])

events = []
comment_marker = 99999

for line in event_input:
    list_line = line.split(';')
    rank = int(list_line[0].strip())
    op = list_line[1].strip()
    _targets = list_line[2].strip()
    _comm_size = list_line[3].strip()
    _requests = list_line[4].strip()
    time = float(list_line[5].strip())
    org_op = list_line[6].strip()

#    print(line)
    comm_size = handle_list(_comm_size)
    requests = handle_list(_requests)
#    print(_targets)
    targets = handle_list(_targets)

    # target format
    # src, dest, bytes, start time nanoseconds, request
    comm_time = int(time * 1000000000)
    if (op == 'send'):
        # src(rank), dest(targets), bytes(comm_size), start time (time), request
        operation = 2
        src = rank
        target_list = handle_ranks(targets)
        request_list = handle_ranks(requests)
        assert request_list == [-1]
        for i in range(len(target_list)):
            if (src == target_list[i]): continue
            if (len(comm_size) == 1 and len(target_list) != 1):
                events.append([src, target_list[i], comm_size[0], comm_time, comment_marker, rank, operation, -1, org_op]) 
            else:
                events.append([src, target_list[i], comm_size[i], comm_time, comment_marker, rank, operation, -1, org_op]) 
            #comm_time = -i * 2
            comm_time = 0;
    elif (op == 'isend'):
        # src(rank), dest(targets), bytes(comm_size), start time (time), request
        operation = 1
        src = rank
        target_list = handle_ranks(targets)
        request_list = handle_ranks(requests)
        assert request_list != [-1]
        assert len(target_list) == 1
        for i in range(len(target_list)):
#            if (src == target_list[i]): continue
            if (len(comm_size) == 1 and len(target_list) != 1):
                events.append([src, target_list[i], comm_size[0], comm_time, comment_marker, rank, operation, request_list[0], org_op]) 
            else:
                events.append([src, target_list[i], comm_size[i], comm_time, comment_marker, rank, operation, request_list[0], org_op]) 
            #comm_time = -i * 2
            comm_time = 0;
    elif (op == 'receive'):
        operation = 0
        # src(-rank), dest(targets), bytes(comm_size), start time (time), request
        dest = rank
        src_list = handle_ranks(targets)
        request_list = handle_ranks(requests)
        assert request_list == [-1]
        for i in range(len(src_list)):
            if (src_list[i] == dest): continue
            if (len(comm_size) == 1 and len(src_list) != 1):
                events.append([src_list[i], dest, comm_size[0], comm_time, comment_marker, rank, operation, -1, org_op]) 
            else:
                events.append([src_list[i], dest, comm_size[i], comm_time, comment_marker, rank, operation, -1, org_op]) 
            #comm_time = -i * 2
            comm_time = 0;
    elif (op == 'ireceive'):
        # src(-rank), dest(targets), bytes(comm_size), start time (time), request
        operation = -1
        dest = rank
        src_list = handle_ranks(targets)
        request_list = handle_ranks(requests)
        assert request_list != [-1]
        assert len(src_list) == 1
#        if (src_list[0] == dest): continue
        events.append([src_list[0], dest, comm_size[0], comm_time, comment_marker, rank, operation, request_list[0], org_op]) 
    elif (op == 'wait'):
        operation = -2
        # src(-rank), dest(targets), bytes(comm_size), start time (time), request
        src = -1
        dest = rank
        request_list = handle_ranks(requests)
        invalid = False
        # wait_some is treated as wait_all
        # only the first wait_some is counted
        for i in range(len(request_list)):
            if (request_list[i] == 1):
                invalid = True
                break
        if (invalid == False):
            for i in range(len(request_list)):
                if (request_list[i] > 1):
                    events.append([src, dest, comm_size[0], comm_time, comment_marker, rank, operation, request_list[i], org_op]) 
                #comm_time = -i * 2
                comm_time = 0;
    else:
        sys.exit("Unknown op = %s" % op)

for i in range(len(events)):
    event_str = ' '.join(str(e) for e in events[i])
    print(event_str)
