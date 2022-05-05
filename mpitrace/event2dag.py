import sys
import math
import glob
import subprocess
import os
from os.path import exists
import re
import json
import time as time_pkg

dbg_flag = 0

def usage():
    print("python event2dag.py <event file> <total ranks> <JSON task graph>")
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

def taskid_to_rank(taskid):
    global tasks
    global task_index
   
    for i, v in enumerate(task_index):
        if (taskid < v): break
    return i - 1

def get_range_tasks_unused(rank):
    global tasks
    global task_index
    global task_index_unused
    global total_ranks

    from_index = task_index_unused[rank]
    to_index = task_index[rank+1]
    if (rank == total_ranks - 1 and from_index >= 0):
        to_index = len(tasks)
    return (from_index, to_index)

def get_range_tasks(rank):
    global tasks
    global task_index
    global total_ranks

    from_index = task_index[rank]
    to_index = task_index[rank+1]
    if (rank == total_ranks - 1 and from_index >= 0):
        to_index = len(tasks)
    return (from_index, to_index)

# search and find the first unused (src, dest, comm_size) task in the tasks[rank]
def search_task(rank, key_string, src, dest, comm_size, mark=False):
    global tasks
    global task_index
    global task_index_unused
    global dbg_flag

    (from_index, to_index) = get_range_tasks(rank)
    if (from_index < 0): return -1

    if (dbg_flag == 1):
        print("search_task: rank(%d), key_string(%s), src(%d), dest(%d), comm_size(%d)" % (rank, key_string, src, dest, comm_size))
        print("search_task: task_index = %s" % str(task_index))
        print("search_task: rank(%d), from_index(%d), to_index(%d)" % (rank, from_index, to_index))
    no_used_so_far = 0
    for t in range(from_index, to_index):
#        if ("receive" in tasks[t]["type"] or "communication" in tasks[t]["type"]):
#            comm_l = tasks[t]["comm_list"]
#            for c in comm_l:
#                if (c["used"] == 0): 
#                    no_used_so_far = no_used_so_far + 1
#                    break;
        if (key_string in tasks[t]["type"]):
            comm_l = tasks[t]["comm_list"]
            if (dbg_flag == 1):
                print("search_task: task_id(%d): com_l = %s" % (t, str(comm_l)))
            for c in comm_l:
                if (c["used"] == 1): continue
                if (c["src"] == src and c["dest"] == dest and c["comm_size"] == comm_size):
                    if (dbg_flag == 1):
                        print("search_task: return %d" % tasks[t]["guid"])
#                    if (no_used_so_far == 1): # we can move forward task_index_unused
#                        task_index_unused[rank] = t
                    if (mark): c["used"] = 1
                    return tasks[t]["guid"]
    if (dbg_flag == 1):
        print("search_task: return -2")
        sys.exit("Exit")
    return -2
           
def mark_task(task_id_from, task_id_to, src, dest, comm_size):
    global tasks

#    (s_from_index, s_to_index) = get_range_tasks(src)
#    if (s_from_index < 0): return -1
#    (d_from_index, d_to_index) = get_range_tasks(dest)
#    if (d_from_index < 0): return -1

    for c in tasks[task_id_from]["comm_list"]:
        if (c["src"] == src and c["dest"] == dest and c["comm_size"] == comm_size and c["used"] == 0):
            c["used"] = 1
            for d in tasks[task_id_to]["comm_list"]:
                if (d["src"] == src and d["dest"] == dest and d["comm_size"] == comm_size and d["used"] == 0):
                    d["used"] = 1
                    return 0
            sys.exit("mark_task[task_id_from(%d), task_id_to(%d), src(%d), dest(%d), comm_size(%d)]: cannot find destination task" % (task_id_from, task_id_to, src, dest, comm_size))
    sys.exit("mark_task[task_id_from(%d), task_id_to(%d), src(%d), dest(%d), comm_size(%d)]: cannot find source task" % (task_id_from, task_id_to, src, dest, comm_size))
    return -1

def add_edge(task_id, rank, src, dest, comm_size):
    global tasks, edges
    global dbg_flag

    res_task_id = -1
    task_id_to = -2
    task_id_from = -2
    flag = 0
    if (rank == src): # send
        # look for task "receive"
#        if (rank > dest): dbg_flag = 1
        task_id_to = search_task(dest, "receive", src, dest, comm_size)
        task_id_from = task_id
    elif (rank == dest): # receive
        # look for task "*communication"
#        if (rank > src): dbg_flag = 1
        task_id_from = search_task(src, "communication", src, dest, comm_size)
        task_id_to = task_id
    else: sys.exit("add_edge: rank must be either src or dest")
    dbf_flag = 0

    if (task_id_from < -1 or task_id_to < -1): # 
        print("Warning: rank(%d), src(%d), dest(%d), comm_size(%d) NOT exist\n" % (rank, src, dest, comm_size))
        return 0
    elif (task_id_from == -1 or task_id_to == -1): # the corresponding task is not created, yet
        return 0
    else:
        edges.append([task_id_from, task_id_to])
        mark_task(task_id_from, task_id_to, src, dest, comm_size)

def get_src_dest_from_request(request):
    global unresolved_requests
    index = -1
    _src = -1
    _dest = -1
    _comm = -1
    _receive = -1
    # unresolved_requests([rank, task_id, request, src, dest, _comm_size, receive])
#    print("unresolved_requests = %s" % str(unresolved_requests))
    for i, r in enumerate(unresolved_requests):
        if (r[2] == request):
            _src = r[3]
            _dest = r[4]
            _comm = r[5]
            _receive = r[6]
            index = i
            break
    if (index >= 0):
        del unresolved_requests[index]
    else:
        sys.exit("no such request [%d] exists" % request)
    return (_src, _dest, _comm, _receive)

def sanity_check():
    global tasks, edges
    global g_task_id

    in_edges = [0] * g_task_id
    out_edges = [0] * g_task_id

    for e in edges:
        src = e[0]
        dest = e[1]
        out_edges[src] = out_edges[src] + 1
        in_edges[dest] = in_edges[dest] + 1

    for i in range(g_task_id):
        if (in_edges[i] == 0 and out_edges[i] == 0):
            print("Error: dangling task (%d) on rank (%d)" % (i, taskid_to_rank(i)))
            print(tasks[i])
        elif (in_edges[i] == 0):
            print("Start task (%d) on rank (%d)" % (i, taskid_to_rank(i)))
            print(tasks[i])
        elif (out_edges[i] == 0):
            print("End task (%d) on rank (%d)" % (i, taskid_to_rank(i)))
            print(tasks[i])
        if ("communication" in tasks[i]["type"]):
            for c in tasks[i]["comm_list"]:
                if c["used"] == 0:
                    print("Error: Unused comm_list found")
                    print(tasks[i])

def cycle_detect():
    global task_index
    # traverse to check if there is a cycle
    in_edges = [0] * g_task_id
    s_edges = sorted(edges, key=lambda x: x[1])
    task_id = -1
    task_index = [-1] * g_task_id
    for i, e in enumerate(s_edges):
        e.append(0)
        if (e[0] != task_id):
            task_index[e[0]] = i
            task_id = e[0]
    # traverse
#    for i, e in enumerate(s_edges):
#        if (e[2] != 0): continue

def output_graphs(json_file_name):
    global tasks, edges

    fp = open(json_file_name, 'w')
    for task in tasks:
        if "receive" in task["type"]:
            task["type"] = "compute"
        # to distribute CrystalRouter 10 nodes to different racks
        if "communica" in task["type"]:
            task["fromNode"] = task["fromNode"] * 60
            task["toNode"] = task["toNode"] * 60
            task["fromTask"] = task["fromTask"] * 60
            task["toTask"] = task["toTask"] * 60
#        task.pop("comm_list", None)

    gtasks = {"tasks": tasks, "edges": edges}
    fp.write(json.dumps(gtasks))
    fp.close()


def create_comp_task(rank, readytime, starttime, compute_time):
    global g_task_id
    global tasks

    task_exec = {"type": "compute", "fromTask": -1, "toTask": -1, "fromWorker": -1, "toWorker": -1, "fromNode": -1, "toNode": -1, "xferSize": 1.0, "guid": g_task_id, "workerId": rank, "readyTime": readytime, "startTime": starttime, "computeTime": float(compute_time), "comm_list": []}
    g_task_id = g_task_id + 1
    tasks.append(task_exec)
    return g_task_id - 1

def create_comm_task(rank, src, dest, comm_size)   :
    global g_task_id
    global tasks

    task_c = {"type": "inter-communication", "fromTask": src, "toTask": dest, "fromWorker": -1, "toWorker": -1, "fromNode": src, "toNode": dest, "xferSize": comm_size * WEIGHT, "guid": g_task_id, "workerId": rank, "readyTime": 0.0, "startTime": 0.0, "computeTime": -1.0, "comm_list": []}
    g_task_id = g_task_id + 1
    tasks.append(task_c)
    return g_task_id - 1

def create_recv_task(rank, src, dest, comm_size)   :
    global g_task_id
    global tasks

    task_c = {"type": "receive", "fromTask": src, "toTask": dest, "fromWorker": -1, "toWorker": -1, "fromNode": src, "toNode": dest, "xferSize": comm_size * WEIGHT, "guid": g_task_id, "workerId": rank, "readyTime": 0.0, "startTime": 0.0, "computeTime": 1.0, "comm_list": []}
    g_task_id = g_task_id + 1
    tasks.append(task_c)
    return g_task_id - 1

# argv[1]: event file
if len(sys.argv) != 4:
    usage()

# single file for now
prev_time = time_pkg.time()
event_input = open(sys.argv[1], 'r')
total_ranks = int(sys.argv[2])

#events = []
comment_marker = 99999

g_task_id = 0
tasks = []  # total tasks, list of rtasks
task_index = [-1] * (total_ranks + 1) # start task_id of ranks
task_index_unused = [-1] * (total_ranks + 1) # start task_id of ranks which has unused comm_list
edges = []  # (src, dest)
rtasks = [] # per rank tasks, list of task dictionary

WEIGHT = 1000
prev_rank = -1
prev_compute = -1
curr_compute = -1
prev_comm = -1
curr_comm = -1
prev_comm = []  # list of comm whose dependency within the same node is not resolved
unresolved_requests = []
prev_no_comm = False
prev_receive = False
curr_time = time_pkg.time()    # measure the elapsed time
print("start at %s, diff(%s)" % (str(curr_time), str(curr_time - prev_time)))
prev_time = curr_time
for line in event_input:
    list_line = line.split(';')
    rank = int(list_line[0].strip())
    op = list_line[1].strip()
    _targets = list_line[2].strip()
    _comm_size = list_line[3].strip()
    _requests = list_line[4].strip()
    time = float(list_line[5].strip())
    wait_time = float(list_line[5].strip())
    org_op = list_line[6].strip()

    comm_size = handle_list(_comm_size)
    requests = handle_list(_requests)
    targets = handle_list(_targets)

    # create computation task
    if (prev_rank != rank):
        curr_time = time_pkg.time()    # measure the elapsed time
        print("============== Rank %d ================ time taken(%s)" % (rank, str(curr_time - prev_time)), flush=True)
        prev_time = curr_time
        prev_no_comm = False
        prev_receive = False
        assert(time >= 0)
        prev_rank = rank
        task_index[rank] = g_task_id
        task_index_unused[rank] = g_task_id
        curr_compute = -1
        curr_comm = -1
        assert(len(unresolved_requests) == 0)
    else:
        time = time * -1
    prev_compute = curr_compute
    prev_comm = curr_comm
    curr_compute = create_comp_task(rank, 0.0, 0.0, time)   
    if (prev_no_comm == True):
        edges.append([prev_compute, curr_compute])
    if (prev_receive == True):
        edges.append([prev_comm, curr_compute])
    prev_no_comm = False
    prev_receive = False
    # create communication task if needed
    if (op == 'send'):
        # task creation
        # no dependency
        # src(rank), dest(targets), bytes(comm_size), start time (time), request
        operation = 2
        src = rank
        target_list = handle_ranks(targets)
        request_list = handle_ranks(requests)
        assert request_list == [-1]
        for i in range(len(target_list)):
            if (src == target_list[i]): continue
            _comm_size = 0
            if (len(comm_size) == 1 and len(target_list) != 1):
                _comm_size = comm_size[0]
#                events.append([src, target_list[i], comm_size[0], comm_time, comment_marker, rank, operation, -1, org_op]) 
            else:
                _comm_size = comm_size[i]
#                events.append([src, target_list[i], comm_size[i], comm_time, comment_marker, rank, operation, -1, org_op]) 
            #comm_time = -i * 2
            task_id = create_comm_task(rank, src, target_list[i], _comm_size)   
            curr_comm = task_id
            edges.append([curr_compute, task_id])
            comm_list = {"src":src, "dest": target_list[i], "comm_size":_comm_size, "used":0}
            tasks[task_id]["comm_list"].append(comm_list)
            add_edge(task_id, rank, src, target_list[i], _comm_size)
            comm_time = 0
        prev_no_comm = True 
    elif (op == 'isend'):
        # task creation
        # src(rank), dest(targets), bytes(comm_size), start time (time), request
        operation = 1
        src = rank
        target_list = handle_ranks(targets)
        request_list = handle_ranks(requests)
        assert request_list != [-1]
        assert len(target_list) == 1
        for i in range(len(target_list)):
            _comm_size = 0
            if (len(comm_size) == 1 and len(target_list) != 1):
                _comm_size = comm_size[0]
#                events.append([src, target_list[i], comm_size[0], comm_time, comment_marker, rank, operation, request_list[0], org_op]) 
            else:
                _comm_size = comm_size[i]
#                events.append([src, target_list[i], comm_size[i], comm_time, comment_marker, rank, operation, request_list[0], org_op]) 
            #comm_time = -i * 2
            task_id = create_comm_task(rank, src, target_list[i], _comm_size)
            curr_comm = task_id
            edges.append([curr_compute, task_id])
            comm_list = {"src":src, "dest": target_list[i], "comm_size":_comm_size, "used":0}
#            print(task_id)
#            print(tasks)
            tasks[task_id]["comm_list"].append(comm_list)
            add_edge(task_id, rank, src, target_list[i], _comm_size)
            unresolved_requests.append([rank, task_id, requests[i], src, dest, _comm_size, 0])
            comm_time = 0
        prev_no_comm = True 
    elif (op == 'receive'):
        # task creation
        # blocking and has strong dependency
        operation = 0
        # src(-rank), dest(targets), bytes(comm_size), start time (time), request
        dest = rank
        src_list = handle_ranks(targets)
        request_list = handle_ranks(requests)
        assert request_list == [-1]
        l = []
        task_id = -1
        for i in range(len(src_list)): # build (src, dest, comm_size, fulfilled) list
            if (src_list[i] == dest): continue
            _comm_size = comm_size[0]
            if (len(comm_size) == 1 and len(src_list) != 1):
                _comm_size = comm_size[0]
#                events.append([src_list[i], dest, comm_size[0], comm_time, comment_marker, rank, operation, -1, org_op]) 
            else:
                _comm_size = comm_size[i]
#                events.append([src_list[i], dest, comm_size[i], comm_time, comment_marker, rank, operation, -1, org_op]) 
            #comm_time = -i * 2
            if (task_id == -1):
                task_id = create_recv_task(rank, src_list[i], rank, _comm_size)   
                curr_comm = task_id
                prev_receive = True
                edges.append([curr_compute, task_id])
            comm_list = {"src":src_list[i], "dest": dest, "comm_size":_comm_size, "used":0}
            tasks[task_id]["comm_list"].append(comm_list)
            add_edge(task_id, rank, src_list[i], rank, _comm_size)
        comm_time = 0
    elif (op == 'ireceive'):
        # no task creation
        # src(-rank), dest(targets), bytes(comm_size), start time (time), request
        operation = -1
        dest = rank
        src_list = handle_ranks(targets)
        request_list = handle_ranks(requests)
        assert request_list != [-1]
        assert len(src_list) == 1
#        if (src_list[0] == dest): continue
        # task_id = create_comm_task(src, target_list[i], _comm_size)   
#        events.append([src_list[0], dest, comm_size[0], comm_time, comment_marker, rank, operation, request_list[0], org_op]) 
        unresolved_requests.append([rank, -1, request_list[0], src_list[0], dest, comm_size[0], 1])
        prev_no_comm = True 
    elif (op == 'wait'):
        # task creation only when it waits for requests of ireceive
        operation = -2
        # src(-rank), dest(targets), bytes(comm_size), start time (time), request
        src = -1
        dest = rank
        request_list = handle_ranks(requests)
        invalid = False
        # wait_some is treated as wait_all
        # only the first wait_some is counted
        for i in range(len(request_list)):
            if (request_list[i] == 1):  # some are already fulfilled - waitsome()
                invalid = True
                break
        if (invalid == False):
            task_id = -1
#            print("wait: request_list = %s" % (str(request_list)))
            for i in range(len(request_list)):
                if (request_list[i] > 1):
                    (src, dest, comm, receive) = get_src_dest_from_request(request_list[i])
                    if (receive == 0): continue
                    if (task_id == -1):
                        task_id = create_recv_task(rank, src, dest, comm)
                        edges.append([curr_compute, task_id])
                        curr_comm = task_id
                        prev_receive = True
                    comm_list = {"src":src, "dest":dest, "comm_size":comm, "used":0}
                    tasks[task_id]["comm_list"].append(comm_list)
                    add_edge(task_id, rank, src, dest, comm)
#                    events.append([src, dest, comm_size[0], comm_time, wait_time, comment_marker, rank, operation, request_list[i], org_op]) 
                comm_time = 0
            if (task_id == -1): prev_no_comm = True
       #     print("wait: task[%d] = %s" % (task_id, str(tasks[task_id])))
        else:   # no request to wait
            prev_no_comm = True 

    else:
        sys.exit("Unknown op = %s" % op)

# final sanity check if all edges are fulfilled
curr_time = time_pkg.time()    # measure the elapsed time
print("-- task_index -- at %s, time taken(%s)" % (str(curr_time), str(curr_time - prev_time)))
prev_time = curr_time
print(task_index, flush=True)

for t in tasks:
   if (t["type"] == "inter-communication" or t["type"] == "receive"):
       l = t["comm_list"]
       for c in l:
           if (c["used"] == 0):
               print("rank %d: task_id %d: Error: Un-matched communications (%d --> %d, %d)" % (taskid_to_rank(t["guid"]), t["guid"], c["src"], c["dest"], c["comm_size"]))
               print(t)
               sys.exit("Exit")
curr_time = time_pkg.time()    # measure the elapsed time
print("-- sanity check -- at %s, time taken(%s)" % (str(curr_time), str(curr_time - prev_time)), flush=True)
prev_time = curr_time
sanity_check()
curr_time = time_pkg.time()    # measure the elapsed time
print("-- output graphs -- %s, time taken(%s)" % (str(curr_time), str(curr_time - prev_time)), flush=True)
prev_time = curr_time
output_graphs(sys.argv[3])
print("-- end -- %s, time taken(%s)" % (str(curr_time), str(curr_time - prev_time)), flush=True)
prev_time = curr_time

#for i in range(len(events)):
#    event_str = ' '.join(str(e) for e in events[i])
#    print(event_str)
