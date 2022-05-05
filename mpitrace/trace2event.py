import sys
import math
import glob
import subprocess
import os
from os.path import exists
import numpy
import re

mpi_ops = { 
        'MPI_Initialized': 0,    # enter, return
        'MPI_Init': 1,          # enter, return
        'MPI_Init_thread': 2,          # enter, return
        'MPI_Finalize' : 3
        }

mpi_group = {
        'MPI_Group_incl': 0,    # enter, return
        'MPI_Comm_dup': 1,    # enter, return
        'MPI_Comm_size': 2,    # enter, return
        'MPI_Comm_rank': 3,    # enter, return
        'MPI_Comm_split': 5,    # enter, return
        'MPI_Comm_create': 6,    # enter, return
        'MPI_Group_free': 7,    # enter, return
        'MPI_Comm_group': 8,    # enter, return
        }

mpi_wait_ops = {
        'MPI_Wait': 0,      # enter, count, outcount, count --> wait time.
        'MPI_Waitsome': 1,      # enter, count, outcount, count --> wait time.
        'MPI_Waitall': 2,      # enter, count, outcount, count --> wait time.
        'MPI_Waitany': 3,      # enter, count, outcount, count --> wait time.
        'MPI_Barrier': 4,       # enter time, return time --> wait time.
        'MPI_Test': 5,      # enter, count, outcount, count --> wait time.
        }

mpi_comm_sendcount = {
        'MPI_Allgather': 6,     # enter time, sendcount, sendtype, recvcount[], recvtype, return time
        'MPI_Gather': 10,       # sendcount
        'MPI_Allgatherv': 18,    # enter time, sendcount, sendtype, recvcount[], recvtype, return time
        'MPI_Gatherv': 11,      # sendcount
        'MPI_Scatter': 12,       # sendcount
        'MPI_Alltoall': 7,      # commsize, sendcount
        }

mpi_comm_sendcount_list = {
        'MPI_Alltoallv': 8,     # commsize, sendcounts[]
        'MPI_Scatterv': 13,      # sendcount
        }

mpi_comm_receive = {
        'MPI_Irecv': 2,         # enter time, count, datatype, source, return time --> comm. time
        'MPI_Recv': 14,         # enter time, count, datatype, source, return time --> comm. time
        }

mpi_comm_ops = {
        'MPI_Send': 0,          # buffered, enter, count, datatype, dest, return time --> comm. time
        'MPI_Irecv': 2,         # Nonblocking, enter time, count, datatype, source, return time --> comm. time
        'MPI_Reduce': 3,        # blocking, enter time, count, datatype, root, return time
        'MPI_Allreduce': 4,     # enter time, count, datatype, return time
        'MPI_Allgather': 6,     # enter time, sendcount, sendtype, recvcount[], recvtype, return time
        'MPI_Alltoall': 7,      # commsize, sendcounts[]
        'MPI_Alltoallv': 8,     # commsize, sendcounts[]
        'MPI_Bcast': 9,         # count, datatype, comm, root
        'MPI_Gather': 10,       # commrank, sendcount, sendtype, root
        'MPI_Gatherv': 11,      # commrank, commsize, sendcount, sendtype, root
        'MPI_Scatter': 12,       # sendcount
        'MPI_Scatterv': 13,      # sendcount
        'MPI_Recv': 14,         # blocking, enter time, count, datatype, source, return time --> comm. time
        'MPI_Sendrecv_replace': 15,         # enter time, count, datatype, source, return time --> comm. time
        'MPI_Isend': 16,         # Nonblocking, enter time, count, datatype, dest, return time --> comm. time
        'MPI_Rsend': 17,         # blocking, enter time, count, datatype, source, return time --> comm. time
        'MPI_Allgatherv': 18,    # enter time, sendcount, sendtype, recvcount[], recvtype, return time
        }

mpi_data = {
        'MPI_Wtime': 8,         # enter time, return time
        'MPI_Type_vector': 9,         # enter time, return time
        'MPI_Pack': 10,         # enter time, return time
        'MPI_Pack_size': 11,         # enter time, return time
        }

mpi_sub_comm_mode = {
        'Receive': 0,
        'Send': 1,
        'Sends': 2,
        'Others': 3,
        }
undefined = { }

mpi_blocking_events = {
        'MPI_Alltoall': 1,      # commsize, sendcounts[]
        'MPI_Allreduce': 2,     # enter time, count, datatype, return time
        'MPI_Allgather': 3,     # enter time, sendcount, sendtype, recvcount[], recvtype, return time
        'MPI_Barrier': 4,       # enter time, return time --> wait time.
        'MPI_Bcast': 5,         # count, datatype, comm, root
        'MPI_Gather': 6,       # commrank, sendcount, sendtype, root
        'MPI_Recv': 7,         # blocking, enter time, count, datatype, source, return time --> comm. time
        'MPI_Rsend': 8,         # blocking, enter time, count, datatype, source, return time --> comm. time
        'MPI_Scatter': 9,       # sendcount
        'MPI_Test': 10,      # enter, count, outcount, count --> wait time.
        'MPI_Wait': 11,      # enter, count, outcount, count --> wait time.
        'MPI_Waitsome': 12,      # enter, count, outcount, count --> wait time.
        'MPI_Waitall': 13,      # enter, count, outcount, count --> wait time.
        'MPI_Waitany': 14,      # enter, count, outcount, count --> wait time.
        }

mpi_nonblocking_events = {
        'MPI_Allgatherv': 1,    # enter time, sendcount, sendtype, recvcount[], recvtype, return time
        'MPI_Alltoallv': 2,     # commsize, sendcounts[]
        'MPI_Gatherv': 3,      # commrank, commsize, sendcount, sendtype, root
        'MPI_Irecv': 4,         # enter time, count, datatype, source, return time --> comm. time
        'MPI_Isend': 5,         # Nonblocking, enter time, count, datatype, dest, return time --> comm. time
        'MPI_Scatterv': 6,      # sendcount
        'MPI_Send': 7,      # sendcount
        }
MIN_TIME = 0.000000010 # 10ns

mpi_total = {}
mpi_total.update(mpi_comm_ops)
mpi_total.update(mpi_wait_ops)
mpi_total.update(mpi_group)
mpi_total.update(mpi_ops)
mpi_total.update(mpi_data)

comm_dict = {"rank": 0, "thread": 1, "op": 2, "ranks": 3,
            "count": 4, "size": 5,
            "dest": 6, "sendcounts": 7, "sendtype": 8,
            "source": 9, "recvcounts": 10, "recvtype": 11,
            "datatype": 12, "datasize": 13,
            "wall_start": 14, "wall_end": 15, "cpu_start": 16, "cpu_end": 17,
            "comm": 18, "newgroup": 19, "oldcomm": 20, "newcomm": 21, 
            "group" : 22, "root": 23, "color": 24, "key": 25,
            "commsize": 26, "blocklength": 27, "sendcount": 28, "oldtype": 29, "newtype": 30, "requests": 31, "recvcount": 32, "request":33}
comm_fields = ['rank', 'thread', 'ranks', 'count', 'size', 'dest', 'sendcounts', 'sendtype', 'source', 'recvcounts', 'recvtype', 'datatype', 'datasize', 'comm', 'newgroup', 'oldcomm', 'newcomm', 'group', 'root', 'color', 'key', 'commsize', 'blocklength', 'sendcount', 'oldtype', 'newtype', 'requests', 'recvcount', 'request']

# comm_dir:
#     10 elements: id, oldcomm, rank, count, group, ranks, color, key, seq, myranks
comm_directory = []
comm_dir_dict = {"id": 0, "oldcomm": 1, "rank": 2, "count":3, "group":4, "ranks":5, "color":6, "key":7, "seq":8, "myranks": 9}
# group_directory: id, group_from, count, ranks, seq
group_directory = []
group_dir_dict = {"id": 0, "oldgroup": 1, "count":2, "ranks":3, "seq": 4}

summary_label = "rank, walltime, walltime(w/o mpi setup/finalize), mpi_time(wall), other_time(wall), mpi_time(wall) percentage, cputime, cputime(w/o mpi setup/finalize), mpi_time(cpu), other_time(cpu), mpi_time(cpu) percentage, total_comm_amount, total_comm_freq, all_to_all_comm_amount, all_to_all_comm_amount_ratio, all_to_all_comm_freq, all_to_all_comm_freq_ratio"
summary = [] # list of list

comm_histogram_label = "below 1k, 1k+, 2k+, 4k+, 8k+, 16k+, 32k+, 64k+, 128k+, 256k+, 512k+, 1M or above"
comm_histogram_label_list = comm_histogram_label.split(',')

# histogram table (3 tables): 3 lists
g_comm_cmount = []
g_comm_freq = []
g_comm_percentage = []
comm_matrix = []

g_comm_list = []

g_group_comm_dict = []

# events    (MPI_func, start, end, src, dest, amount, 
# data  (comm ratio, comp ratio, wait ratio, comm amount, ...)

def isfloat(num):
    try:
        float(num)
        return True
    except ValueError:
        return False


def get_field_value(line, alist, field):
    indices = [i for i, s in enumerate(alist) if field in s]
    if (indices != []): 
        if (field == 'rank' and 'ranks' in alist[indices[0]]):
            return "-1"
        for i in indices:
            if (alist[i].find(field) == 0):
                if ('=' in alist[i]):
                    blist = alist[i].split('=')
                    field_id = blist[0]
                    if (blist[0][-1] == ']'):
                        new_list = blist[0].split('[')
                        field_id = new_list[0]
                    if (field_id != field): continue
                    if (blist[1][0] == '['): # list
                        new_list = line.split('=')
                        res = new_list[1].strip().strip('][')
                        res = res.split(', ')
#                        if (field == 'sendcounts'):
#                            print("res list = %s" % str(res))
#                        if (field == 'sendcounts'):
#                            print("result = %s" % str(res))
                    else: # no list
                        res = blist[1].strip(',').strip('.')
                        #.strip().strip('][')
#                        if (field == 'sendcounts'):
#                            print("result no list = %s" % str(res))
                else: res = alist[i+1].strip(',').strip('.')

                if (isinstance(res, list)):
                    if (res != []):
                        if (res[0].isnumeric()):
                            new_res = [int(x) for x in res]
                        elif (isfloat(res[0])):
                            new_res = [float(x) for x in res]
                        res = new_res
                else:
                    if (res.isnumeric()): res = int(res)
                    elif (isfloat(res)): res = float(res)
                return res
    return "-1"

def add_field_value(clist, clist_dic, line, list_line, field, fname):
    if (not(field in line)): return
        
    f = get_field_value(line, list_line, field)
    if (f != '-1'):
        clist[clist_dic[fname]] = f

def add_field(clist, clist_dic, line, list_line, field, fname):
    if (not(field in line)): return
        
    clist[clist_dic[fname]] = field

def init_comm_prop(comm_dict):
    a = []
    for key in comm_dict:
        a.append(-1)
    return a

def which_comm_mode(mpi_op):
    if mpi_op in mpi_comm_receive:
        mode = mpi_sub_comm_mode['Receive']
    elif mpi_op in mpi_comm_sendcount:
        mode = mpi_sub_comm_mode['Send']
    elif mpi_op in mpi_comm_sendcount_list:
        mode = mpi_sub_comm_mode['Sends']
    else: mode = mpi_sub_comm_mode['Others']
    return mode

def usage():
    print("python trace_gen.py <trace directory> <trace output file> <# of ranks>")

total_ranks = 0
my_rank = -1
t_comm_histogram = []
t_total_data = []
no_header = 0

def parse(asciifile):
    print("parse: %s" % asciifile)
    tlist = asciifile.split('/')
    tstr = tlist[-1]
    tlist = tstr.split('.')
    t_rank = int(tlist[0]) 

    input_trace = open(asciifile, 'r')

    collect_info = False
    g_comm_prop = []
    comm_prop = []
    for line in input_trace:
      list_line = line.split()
      if (list_line[0] in mpi_total):
          if (list_line[1] == 'entering' or list_line[1] == 'returning'):
              if (list_line[1] == 'entering'):
                  if (collect_info == True): sys.exit("nested MPI op entering")
                  collect_info = True
                  comm_prop = init_comm_prop(comm_dict)
                  add_field(comm_prop, comm_dict, line, list_line, list_line[0], "op")
                  add_field_value(comm_prop, comm_dict, line, list_line, "walltime", "wall_start")
                  add_field_value(comm_prop, comm_dict, line, list_line, "cputime", "cpu_start")
                  add_field_value(comm_prop, comm_dict, line, list_line, "thread", "thread")
              if (list_line[1] == 'returning'):
                  if (collect_info == False): sys.exit("nested MPI op returning")
                  collect_info = False
                  add_field_value(comm_prop, comm_dict, line, list_line, "walltime", "wall_end")
                  add_field_value(comm_prop, comm_dict, line, list_line, "cputime", "cpu_end")
                  g_comm_prop.append(comm_prop)
      elif (collect_info == True): # collect information
          for f in comm_fields:
              add_field_value(comm_prop, comm_dict, line, list_line, f, f)
#    for i in g_comm_prop:
#        print(i)
    return g_comm_prop

def get_comm_dict_field(v, field):
    return v[comm_dict[field]]

def update_histogram(hist, amount):
    if (amount <= 0): return
    hist_index = (math.log(amount,2))
    hist_index = int(hist_index - 9) # start with 0.5k
    if (hist_index < 0): hist_index = 0
    if (hist_index >= len(hist)): hist_index = len(hist) - 1 
    hist[hist_index] = hist[hist_index] + 1

type_dict = {}
unknown_data_type = {}
# FIXME

# unit_type: integer
def type_to_size(unit_type):
    unit_size = 1 
    unknown_type = 0
    if (unit_type < 0):
        return 8    # MPI_Bcast, ... implicitly defined.
    if (unit_type < 8):
        unit_size = 1
        if (unit_type != 4): # MPI_UNSIGNED_CHAR
            unknown_type = 1
    elif (unit_type < 20):
        unit_size = 8
        if (unit_type != 9 and unit_type != 11 and unit_type != 14): # MPI_INT, MPI_LONG, MPI_DOUBLE
            unknown_type = 1
        if (unit_type == 19): # MPI_PACKED
            unit_size = 1
    else: # user-defined-type
        if (str(unit_type) in type_dict):
            unit_size = type_dict[unit_type]
        else: unit_size = 8

    if (unknown_type == 1):
        if (unit_type not in unknown_data_type):
            unknown_data_type[unit_type] = 1
            print("unknown datatype %d" % unit_type)

    return unit_size

def get_unit_size(v):
    unittype = -1
#    print(v)
#    print(get_comm_dict_field(v, "datatype"))
#    print("type_dict = %s" % str(type_dict))
    datatype = (get_comm_dict_field(v, "datatype"))
    sendtype = (get_comm_dict_field(v, "sendtype"))
    recvtype = (get_comm_dict_field(v, "recvtype"))
    if (datatype >= 0):
        unittype = datatype
    elif (sendtype >= 0):
        unittype = sendtype
    elif (recvtype >= 0):
        unittype = recvtype

    return type_to_size(unittype)


# return full list
def get_dest_list_from_group(rank, group_id): # FIXME: seq
    global group_directory
    global current_comm_seq

    seq = 0
    if (group_id in current_group_seq):
        seq = current_group_seq[group_id]

    dest_list = -1
    for l in group_directory[rank]:
        if l[group_dir_dict['id']] == group_id: # gave up seq of a group 
            dest_list = l[group_dir_dict['ranks']]
            break
    if (dest_list == [-1]):
        dest_list = []
        for i in range(total_ranks): 
            dest_list.append(i)
    return dest_list

# return full list
def get_dest_list_from_comm(rank, comm_id): # FIXME: seq
    global comm_directory
    global current_comm_seq

    seq = 0
    if (comm_id in current_comm_seq):
        seq = current_comm_seq[comm_id]
    print("get_dest_list_from_comm(%d)" % comm_id)
    group_id = -1
    size = 0

    dest_list = []
    if (comm_id == 2): # MPI_WORLD
        for i in range(total_ranks): 
            dest_list.append(i)
        return dest_list

    dest_list = -1
    print("comm_directory[%d]" % rank)
    print(comm_directory[rank])
    for l in comm_directory[rank]:
        if (l[comm_dir_dict['id']] == comm_id and l[comm_dir_dict['seq']] == seq):
            group_id = l[comm_dir_dict['group']]
            size = l[comm_dir_dict['count']]
            dest_list = l[comm_dir_dict['ranks']]
            break
    if (dest_list == -1 and group_id != -1):
        dest_list = get_dest_list_from_group(rank, group_id)
        if (dest_list != -1): return dest_list

    if (size > 0 or dest_list == [-1]):
        dest_list = []
        if (size == total_ranks or dest_list == [-1]):
            for i in range(total_ranks): 
                dest_list.append(i)
        else:
            for i in range(size):
                dest_list.append((my_rank + i) % total_ranks) # FIXME
            print("return fake %s" % str(dest_list))
    print("dest_list = %s" % str(dest_list))
    return dest_list

def ugly_hacking(my_rank, commid, commsize, seq):
    dest_list = []
    if (commsize == total_ranks):
        for i in range(total_ranks):
            dest_list.append(i)
        return dest_list
    sqrt_ranks = int(math.sqrt(total_ranks))
    cbrt_ranks = int(numpy.cbrt(total_ranks))
    print("Ugly hacking: commid(%d), commsize(%d)" % (commid, commsize))
    # 2D, row
    if (sqrt_ranks * sqrt_ranks == total_ranks and commsize == sqrt_ranks):
        # return row where my_rank exists
        row = int(my_rank / sqrt_ranks)
        for i in range(row * sqrt_ranks, (row + 1) * sqrt_ranks):
            dest_list.append(i)
    # 3D
    elif (cbrt_ranks * cbrt_ranks * cbrt_ranks == total_ranks):
        cbrt_ranks2 = cbrt_ranks * cbrt_ranks
        if (commsize == cbrt_ranks): # row
            # return row where my_rank exists
            row = int(my_rank / cbrt_ranks)
            for i in range(row * cbrt_ranks, (row * cbrt_ranks + cbrt_ranks)):
                dest_list.append(i)
        elif (commsize == cbrt_ranks * cbrt_ranks): # y,z
             # return row where my_rank exists
            row = int(my_rank % cbrt_ranks)
            for i in range(row , total_ranks, cbrt_ranks):
                dest_list.append(i)
            if (len(dest_list) != cbrt_ranks2):
                print("Ugly hacking fails: lenth of dest_list %d is not %d\n" % (len(dest_list), cbrt_ranks2))
    else:
        if (seq == 0): # row (x)
            row = int(my_rank / commsize)
            for i in range(row*commsize, (row+1)*commsize):
                dest_list.append(i)
        elif (seq == 1): # the other
            rowsize = int(total_ranks / commsize)
            row = int(my_rank % rowsize)
            assert rowsize * commsize == total_ranks
            for i in range(row, total_ranks, rowsize):
                dest_list.append(i)
        else:
            sys.exit("seq can be as large as 1")
    print("ugly_hacking returns %s" % str(dest_list)) 
    return dest_list

def get_seq_from_comm(comm):
    global current_comm_seq

    if (comm not in current_comm_seq):
        return 0
    else:  return current_comm_seq[comm]

def event_sanity_check(event):
    ranks = event[2]
    sizes = event[3]
    success = True 
    if (not isinstance(ranks, list)):
        success = False
        print("ranks must be a list: %s" % str(ranks))
    if (not isinstance(sizes, list)):
        success = False
        print("sizes must be a list: %s" % str(sizes))
    if (success == True):
        if (len(ranks) != len(sizes)):
            if (len(ranks) == 1 and ranks[0] == -1):
                if (len(sizes) != 1 and len(sizes) != total_ranks):
                    success = False
                    print("ranks(%s) and sizes(%s) mismatch" % (str(ranks), str(sizes)))
    return success
        
def generate_event(comm_list, event_list):
    global last_event_start_time
    global last_event_end_time
    global MIN_TIME 
    global no_event_dict

    comm_op = get_comm_dict_field(comm_list, "op")
    comm = (get_comm_dict_field(comm_list, "comm"))
    src = (get_comm_dict_field(comm_list, "source"))
    dest = (get_comm_dict_field(comm_list, "dest"))
    count = (get_comm_dict_field(comm_list, "count"))
#    print("count = %d" % count)
#    print("sendcount = %s" % str(get_comm_dict_field(comm_list, "sendcount")))
    sendcount = (get_comm_dict_field(comm_list, "sendcount"))
#    print("recvcount = %s" % str(get_comm_dict_field(comm_list, "recvcount")))
    recvcount = (get_comm_dict_field(comm_list, "recvcount"))
    sendcounts = get_comm_dict_field(comm_list, "sendcounts")
    recvcounts = get_comm_dict_field(comm_list, "recvcounts")
    root = (get_comm_dict_field(comm_list, "root"))
    start_time = (get_comm_dict_field(comm_list, "cpu_start"))
    end_time = (get_comm_dict_field(comm_list, "cpu_end"))
    requests = get_comm_dict_field(comm_list, "requests")
    exec_time = end_time - start_time
    if isinstance(requests, str):
        if ("IGNORED" in requests):   # requests[0] = <IGNORED>
            requests = [-1]
    request = get_comm_dict_field(comm_list, "request")
    unit_size = get_unit_size(comm_list)
    etime = start_time 
#    print("this must be fine: generate_event: comm_list = %s" % str(comm_list))
    if (last_event_start_time == 0):
        last_event_start_time = start_time
        last_event_end_time = end_time
    # op [ myrank, op, [src/dest], [amount], req ]
    op1 = []
    op2 = []
    comm_root = 0
    new_src = src
    new_dest = dest
    new_ranks = []
#    print("comm_list = %s" % str(comm_list))
#    print("%s, src(%d), dest(%d), comm(%d)" % (comm_op, src, dest, comm))
    if (comm != -1):
        seq = 0
        if (comm != 2):
            seq = get_seq_from_comm(comm)
        comm_root = get_root(my_rank, comm, seq)
        new_ranks = get_ranks_from_comm(my_rank, comm, seq)
        if src != -1:
#            print("comm_list = %s" % str(comm_list))
#            print("%s, src(%d), dest(%d), comm(%d)" % (comm_op, src, dest, comm))
            new_src = get_absrank(my_rank, src, comm, seq)
            if (new_src == -1):
                sys.exit("Error: new_src(-1)")
        if dest != -1:
#            print("comm_list = %s" % str(comm_list))
#            print("%s, src(%d), dest(%d), comm(%d)" % (comm_op, src, dest, comm))
            new_dest = get_absrank(my_rank, dest, comm, seq)
            if (new_dest == -1):
                sys.exit("Error: new_dest(-1)")
    if (etime != last_event_start_time):
        etime = - (start_time - last_event_end_time)
    etime2 = - MIN_TIME
    last_event_start_time = start_time
    last_event_end_time = end_time
    if count >= 0: _count = count
    if sendcount >= 0: _count = sendcount
    if comm_op == 'MPI_Recv' or comm_op == 'MPI_Irecv':
        assert _count >= 0
        if (new_src == -1):
            new_src = -9
        comm_str = 'receive'
        if (comm_op == 'MPI_Irecv'):
            comm_str = 'i' + comm_str
        op1 = [my_rank, comm_str, [new_src], [_count * unit_size], request, etime]
    elif comm_op == 'MPI_Send' or comm_op == 'MPI_Isend':
        assert _count >= 0
        if (new_dest == -1):
            sys.exit("Wrong dest[-1]")
        comm_str = 'send'
        if (comm_op == 'MPI_Isend'):
            comm_str = 'i' + comm_str
        op1 = [my_rank, comm_str, [new_dest], [_count * unit_size], request, etime]
    elif 'MPI_Barrier' in comm_op:
        data_size = 8
        if (my_rank == comm_root):
            # receive from all
            op1 = [my_rank, 'receive', new_ranks, [data_size], request, etime]
            # send to all
            op2 = [my_rank, 'send', new_ranks, [data_size], request, etime2]
        else:
            # send to root
            op1 = [my_rank, 'send', [comm_root], [data_size], request, etime]
            # receive from root
            op2 = [my_rank, 'receive', [comm_root], [data_size], request, etime2]
    elif 'MPI_Bcast' in comm_op:
        assert _count > 0
        if (my_rank == root):
            # send to all
            op1 = [my_rank, 'send', new_ranks, [_count * unit_size], request, etime]
        else:
            # receive from root
            op1 = [my_rank, 'receive', [comm_root], [_count * unit_size], request, etime]
    elif comm_op == 'MPI_Alltoall' or comm_op == 'MPI_Allgather':
        assert sendcount > 0
        assert recvcount > 0
        # send to all
        op1 = [my_rank, 'send', new_ranks, [sendcount * unit_size], request, etime]
        # receive from all
        op2 = [my_rank, 'receive', new_ranks, [recvcount * unit_size], request, etime2]
    elif 'MPI_Allreduce' in comm_op:    # no MPI_Allreducev
        assert _count > 0
        # send to all
        op1 = [my_rank, 'send', new_ranks, [_count * unit_size], request, etime]
        # receive from all
        op2 = [my_rank, 'receive', new_ranks, [_count * unit_size], request, etime2]
    elif comm_op == 'MPI_Alltoallv' or comm_op == 'MPI_Allgatherv':
        # send to all
        assert recvcounts != -1
        if comm_op == 'MPI_Alltoallv':
            assert sendcounts != -1
            for i, s in enumerate(sendcounts):
                sendcounts[i]= sendcounts[i] * unit_size
            op1 = [my_rank, 'send', new_ranks, sendcounts, request, etime]
        else: # MPI_Allgatherv
            assert sendcount != -1
            sendcount = sendcount * unit_size
            op1 = [my_rank, 'send', new_ranks, [sendcount], request, etime]
        for i, s in enumerate(recvcounts):
            recvcounts[i]= recvcounts[i] * unit_size
        # receive from all
        op2 = [my_rank, 'receive', new_ranks, recvcounts, request, etime2]
    elif 'MPI_Reduce' in comm_op:
        assert count >= 0
        if (my_rank == comm_root):
            op1 = [my_rank, 'receive', new_ranks, [count * unit_size], request, etime]
        else:
            op1 = [my_rank, 'send', [comm_root], [count * unit_size], request, etime]
    elif 'MPI_Gather' in comm_op:
        if (my_rank == comm_root): # receive from all
            if (comm_op == 'MPI_Gatherv'):
                assert recvcounts != -1
                for i, s in enumerate(recvcounts):
                    recvcounts[i]= recvcounts[i] * unit_size
                op1 = [my_rank, 'receive', new_ranks, recvcounts, request, etime]
            else:
                assert sendcount != -1
                op1 = [my_rank, 'send', new_ranks, [sendcount * unit_size], request, etime]
        else:
           # send to root
            assert sendcount != -1
            op1 = [my_rank, 'send', [comm_root], [sendcount * unit_size], request, etime]
    elif 'MPI_Scatter' in comm_op:
        if (my_rank == comm_root): # send
            if comm_op == 'MPI_Scatterv':
                assert sendcounts != -1
                for i, s in enumerate(sendcounts):
                    sendcounts[i]= sendcounts[i] * unit_size
                op1 = [my_rank, 'send', new_ranks, sendcounts, request, etime]
            else:
                assert sendcount != -1
                op1 = [my_rank, 'send', new_ranks, [sendcount * unit_size], request, etime]
        else: # receive from root
            assert recvcount != -1
            op1 = [my_rank, 'receive', [comm_root], [recvcount * unit_size], request, etime]
    elif 'MPI_Test' in comm_op or 'MPI_Wait' in comm_op:
        # FIXME: can be problemetic
        # take care of all MPI_Wait* as MPI_Waitall 
        # may need to look forward
        # extrace dependency for future MPI_Wait* and MPI_Test
#        for d in dep_list:
        new_requests = []
        if (comm_op == 'MPI_Test' or comm_op == 'MPI_Wait'):
            if (isinstance(request, list)):
                new_requests = request
            else:
                new_requests = [request]
        else: # some or all
            new_requests = requests
        if (new_requests[0] < 1): 
            print("%s has problem! NO code is generated." % comm_op)
            print(comm_list)
        else:
            op1 = [my_rank, 'wait', [-1], [-1], new_requests, etime]
    else:
        if (comm_op not in no_event_dict):
            no_event_dict[comm_op] = 1
            print("Warning: the following MPI trasaction is not taken care of by the event generator!")
            print(comm_list)
        else:
            no_event_dict[comm_op] = no_event_dict[comm_op] + 1
 
    if (op1 != []): 
        op1.append(exec_time)
        op1.append(comm_op)
        event_list.append(op1)
        success = event_sanity_check(op1)
        if (success == False):
            print(comm_list)
            print(op1)
            sys.exit("Failed to event sanity check")
    if (op2 != []):
        op2.append(exec_time)
        op2.append(comm_op)
        event_list.append(op2)
        success = event_sanity_check(op2)
        if (success == False):
            print(comm_list)
            print(op1)
            sys.exit("Failed to event sanity check")

taken_dict = {}
# analysis_comm (comm_list, comm_matrix)
#  return (comm_time[wall, cpu], comm_amount)
def analysis_comm(comm_list, comm_matrix_row):
    global total_ranks
    global taken_dict
    if (total_ranks == 0): sys.exit("total_ranks is not known")
    # get time, return (wall, cpu)
    comm_time = [0, 0]
    comm_op = get_comm_dict_field(comm_list, "op")
    comm_time[0] = (get_comm_dict_field(comm_list, "wall_end")) - (get_comm_dict_field(comm_list, "wall_start"))
    comm_time[1] = (get_comm_dict_field(comm_list, "cpu_end")) - (get_comm_dict_field(comm_list, "cpu_start"))
    # get comm amount, return
    unit_size = get_unit_size(comm_list)
    comm_amount = 0
    seq = 0
    comm = (get_comm_dict_field(comm_list, "comm"))
    if (comm == -1): comm = 2
    if (comm != 2):
        seq = get_seq_from_comm(comm)
    new_ranks = get_ranks_from_comm(my_rank, comm, seq)
    dest_list = new_ranks
    if (new_ranks == -1): 
        commsize = get_comm_dict_field(comm_list, "commsize")
        dest_list = ugly_hacking(my_rank, comm, commsize, 0)
    if (new_ranks == [-1]):
        dest_list = []
        for i in range(total_ranks):
            dest_list.append(i)
    dest = (get_comm_dict_field(comm_list, "dest"))
    root = (get_comm_dict_field(comm_list, "root"))
    if (dest >= 0):
        dest = get_absrank(my_rank, (get_comm_dict_field(comm_list, "dest")), comm, seq)
        if (dest == -1):
            print(comm_list)
            print("original dest = %d" % get_comm_dict_field(comm_list, "dest"))
            print("abs dest == -1")
            sys.exit("wrong")
    if (root >= 0):
        root = get_absrank(my_rank, (get_comm_dict_field(comm_list, "root")), comm, seq)
        if (root == -1):
            print(comm_list)
            print("original root = %d" % get_comm_dict_field(comm_list, "root"))
            print("abs root == -1")
            sys.exit("wrong")
    if (comm_op in mpi_comm_sendcount_list):   # sendcounts[]
        send_list = get_comm_dict_field(comm_list, "sendcounts")
        if (not isinstance(send_list, list)):
            print("sendcounts is not a list")
            print(comm_list)
            print("send_list = %s" % str(send_list))
        for i, v  in enumerate(send_list):
            comm_amount = comm_amount + int(v)
            assert dest_list != []
            if (dest_list != []):
                comm_matrix_row[int(dest_list[i])] = comm_matrix_row[int(dest_list[i])] + int(v) * unit_size
    elif (comm_op in mpi_comm_sendcount):  # sendcount
        comm_amount = (get_comm_dict_field(comm_list, "sendcount"))
        comm_size = (get_comm_dict_field(comm_list, "commsize"))
        if (comm_op == 'MPI_Alltoall' or comm_op == 'MPI_Allgather'):
            if comm_op not in taken_dict:
                taken_dict[comm_op] = 1
                print("%s is taken care" % comm_op)
            num_dest = len(dest_list)
            if comm_size > 0 and comm_size != num_dest:
                print("Error: comm_size(%d) != num_dest(%d)" % (comm_size, num_dest))
            if (dest_list != []):
                for i in dest_list:
                    comm_matrix_row[i] = comm_matrix_row[i] + comm_amount * unit_size
            comm_amount = comm_amount * num_dest
        else:
            if (dest == -1):
                dest = root
                print("Warning: dest is -1")
                print(comm_list)
            if (dest != my_rank):
                comm_matrix_row[dest] = comm_matrix_row[dest] + comm_amount * unit_size
    elif (comm_op in mpi_comm_receive): # receive, don't count it since it is counted at the sender.
        comm_amount = 0
    else: # count
        comm_amount = (get_comm_dict_field(comm_list, "count"))
#        comm = (get_comm_dict_field(comm_list, "comm"))
        if (dest == -1):
            if (comm_op == 'MPI_Bcast'):
                if (root == my_rank):
                    if (dest_list != []):
                        dest_num = len(dest_list)
                        for d in dest_list:
                            if (int(d) != root):
                                comm_matrix_row[int(d)] = comm_matrix_row[int(d)] + comm_amount * unit_size
                    else:
                        dest_num = total_ranks
                        for i in range(total_ranks):
                            if (i != root):
                                comm_matrix_row[i] = comm_matrix_row[i] + comm_amount * unit_size
                    comm_amount =  (dest_num - 1) * comm_amount
            elif (comm_op == 'MPI_Barrier'): # FIXME
                comm_amount = 0
            else:
                comm_amount = 0
        else:
            comm_matrix_row[dest] = comm_matrix_row[dest] + comm_amount * unit_size
#            print("comm_op(%s): dest(%d), count(%d), comm_amount(%d)" % (comm_op, dest, comm_amount, comm_amount * unit_size))

    if comm_amount == -1: # MPI_Barrier, 
        if (comm_op not in mpi_wait_ops):
            print(comm_list)
            print("Warning: negative comm amount!")
        comm_amount = 1

    # get comm pattern, global
    # return (comm_time, comm_amount)
#    print("comm_amount = %d, unit_size = %d" % (comm_amount, unit_size))
    return (comm_time, comm_amount * unit_size)

def update_comm_rank(rank, l_comm_dir, comm_id, new_rank):
    # backward
    for c in reversed(l_comm_dir):
        if (c[comm_dir_dict['id']] == comm_id):
            c[comm_dir_dict['rank']] = new_rank
            break

def update_comm_size(rank, l_comm_dir, comm_id, comm_size):
    # backward
    for c in reversed(l_comm_dir):
        if (c[comm_dir_dict['id']] == comm_id):
            c[comm_dir_dict['count']] = comm_size
            if (comm_size == total_ranks and c[comm_dir_dict['ranks']] == -1):
                c[comm_dir_dict['ranks']] = [-1]
                return

def add_unique_overwrite(a_list, entry):
    if a_list != []:
        index = -1
        for i, v in enumerate(a_list):
            if (v[0] == entry[0]):
                index = i
                break
        if (index >= 0): a_list.pop(index)
    a_list.append(entry)


def get_comm_group_commands(t_rank, clist):
    global g_comm_list
    global g_group_comm_dict

    t_comm_list = []
    group_comm_dict = g_group_comm_dict[t_rank]
    for i, l in enumerate(clist):
        if l[comm_dict['op']] in mpi_group:
            t_comm_list.append(l)
        elif "MPI_All" in l[comm_dict['op']]:
            comm = (get_comm_dict_field(l, "comm"))
            count = (get_comm_dict_field(l, "commsize"))
            if comm not in group_comm_dict and count > 0:
                group_comm_dict[comm] = count
    
    g_comm_list.append([t_rank, t_comm_list])

def get_ranks_from_group(rank, group_id, seq):
    # look for seq-th occurrence of group_id of rank
#    print("get_ranks_from_group(%d, %d, %d)" % (rank, group_id, seq))
    g = group_directory[rank]
    ranks = []
    count = 0
    for i, l in enumerate(g):
        if (l[group_dir_dict["id"]] == group_id):
            if (count == seq):
                if l[group_dir_dict["ranks"]] == -1 and l[group_dir_dict["oldgroup"]] == 3: # MPI_COMM_WORLD
                    ranks = [-1]
                    break
                else:
                    ranks = l[group_dir_dict["ranks"]]
                    break
            count = count + 1
#    print("ranks = %s" % ranks)
    return ranks

def get_ranks_from_comm(rank, comm_id, seq):
    global warning_comm_dict
    global comm_directory
    if rank == -1: return rank
    if (comm_id == 2): # MPI_COMM_WORLD
        return [-1]
    # look for seq-th occurrence of comm_id of rank
    c = comm_directory[rank]
    comm_d = []
    for l in c:
        if (l[comm_dir_dict['id']] == comm_id and l[comm_dir_dict['seq']] == seq):
            comm_d = l
            break;
    if (comm_d == []):
        if (comm_id in warning_comm_dict[rank]):
            warning_comm_dict[rank][comm_id] = seq
            print("Warning: empty for rank(%d) commd_id(%d) and seq(%d)" % (rank, comm_id, seq))
        assert seq == 0
        return [-1]
    if (comm_d[comm_dir_dict["ranks"]] != -1): # comm has ranks defined
#        print("get_ranks_from_comm: comm_d = %s" % str(comm_d))
#        print("get_ranks_from_comm: comm_d[ranks] = %s" % str(comm_d[comm_dir_dict["ranks"]]))
        return comm_d[comm_dir_dict["ranks"]]
    elif (comm_d[comm_dir_dict['group']] != -1): # comm has group
        group_id = comm_d[comm_dir_dict['group']]
        new_ranks = get_ranks_from_group(rank, group_id, 0)  # FIXME
#        print("get_ranks_from_comm: group_id = %s" % str(group_id))
#        print("get_ranks_from_comm: new_ranks = %s" % str(new_ranks))
        return new_ranks 
    print("Error: cannot find ranks from comm (%d) of rank (%d)" % (comm_id, rank))
    sys.exit("")
    return -1

def get_root(rank, comm, seq):
    if (comm == 2): return 0
    ranks = get_ranks_from_comm(rank, comm, seq)
    if ranks == -1: return 0
    if (ranks == [-1]): return 0
    if (not isinstance(ranks, list) or ranks == []):
        print("Wrong: get_root(%d, %d, %d) - %s" % (rank, comm, seq, str(ranks)))
    return ranks[0]

def get_absrank(myrank, rank, comm, seq):
    if (comm == 2): # MPI_COMM_WORLD
        return rank
    new_ranks = get_ranks_from_comm(myrank, comm, seq)
    if new_ranks == -1 or new_ranks == [-1] : return rank
    if (new_ranks[rank] == -1):
        print("get_absrank(%d, %d, %d, %d)" % (myrank, rank, comm, seq))
        print("new_ranks = %s" % str(new_ranks))
    return new_ranks[rank]

# add new comm
def add_new_comm(rank, comm_dir, l):
    count = 0
    for i, c in enumerate(comm_dir):
        if c[comm_dir_dict['id']] == l[0]:
            count = count + 1
    l[comm_dir_dict['seq']] = count
    comm_dir.append(l)

# update most recent comm with group
# l[comm, group]
def update_comm_group(rank, comm_dir, l):
    for c in comm_dir:
        if (c[comm_dir_dict['id']] == l[0]):
            c[comm_dir_dict['group']] = l[1]

# add new group
# l[id, group_from, count, ranks, -1]
def add_new_group(rank, group_dir, l):
    # convert l[ranks] to list of integer

    #ranks_list = l[3].strip().strip('][').split(', ')
    ranks = [int(i) for i in l[3]]
    if (l[4] == -1):
        count = 0
        for i, c in enumerate(group_dir):
            if c[group_dir_dict['id']] == l[0]:
                count = count + 1
        l[group_dir_dict['seq']] = count
    # if rank is in ranks, add
    if (rank in ranks):
        new_group = [int(l[0]), int(l[1]), int(l[2]), ranks, l[4]]
        if (new_group[group_dir_dict['count']] != len(ranks)):
            sys.exit("add_new_group: count(%d) != len(ranks)" % (new_group[2], len(ranks)))
        group_dir.append(new_group)
    # else do nothing

# assume that 'pos' are the same for all ranks
def get_color_ranks(rank, newcomm, color, key, seq, pos):
    global comm_directory
    tlist = []
    newrank = rank
    for i, l in enumerate(comm_directory): # rank
        for j, l2 in enumerate(l): # entries
#            if (l2[comm_dir_dict['id']] == newcomm and l2[comm_dir_dict['color']] == color and j == pos):
#            if (l2[comm_dir_dict['id']] == newcomm and l2[comm_dir_dict['color']] == color and l2[comm_dir_dict['seq']] == seq):
            if (l2[comm_dir_dict['color']] == color and j == pos):
                tlist.append([i, l2[comm_dir_dict['key']]])
    # sort according to key field
    tlist_s = sorted(tlist, key=lambda x:x[1])
    # return the first entries 
    result = []
    for i, l in enumerate(tlist_s):
        result.append(l[0])
        if (l[0] == rank): newrank = i
#    print("get_color_ranks(%d, %d, %d, %d, %d, %d) returns (newrank: %d, new ranks = %s)" % (rank, newcomm, color, key, seq, pos, newrank, str(result)))
    return (newrank, result)

def analysis_comm_group():
    global g_comm_list
    global comm_directory
    global group_directory
    global g_group_comm_dict

    # first phase: collect each information
    print("First phase ---> ")
    for i, ltop in enumerate(g_comm_list): # for each rank
        print("--- rank %d" % i)
        comm_directory.append([])
        group_directory.append([])
        local_comm_d = comm_directory[i]
        local_group_d = group_directory[i]
        my_rank = ltop[0]
        for j, l in enumerate(ltop[1]): # for each comm
            op_index = comm_dict['op']
            if l[op_index] not in mpi_group: continue
            if l[op_index] == "MPI_Comm_rank": 
                comm = (get_comm_dict_field(l, "comm"))
                rank = (get_comm_dict_field(l, "rank"))
                update_comm_rank(my_rank, local_comm_d, comm, rank)
            elif l[op_index] == "MPI_Comm_size":
                comm = (get_comm_dict_field(l, "comm"))
                comm_size = (get_comm_dict_field(l, "size"))
#                print(l)
                update_comm_size(my_rank, local_comm_d, comm, comm_size)
            elif l[op_index] == "MPI_Comm_split": # oldcomm, color, key, newcomm
                oldcomm = (get_comm_dict_field(l, "oldcomm"))
                color = (get_comm_dict_field(l, "color"))
                key = (get_comm_dict_field(l, "key"))
                newcomm = (get_comm_dict_field(l, "newcomm"))
#                print(l)
                if (newcomm != 1):
#                    print("new com is added: %s" % str(newcomm))
                    add_new_comm(my_rank, local_comm_d, [newcomm, oldcomm, -1, -1, -1, -1, color, key, -1, -1])
            elif l[op_index] == "MPI_Comm_create": # oldcomm, group, newcomm
                oldcomm = (get_comm_dict_field(l, "oldcomm"))
                group = (get_comm_dict_field(l, "group"))
                newcomm = (get_comm_dict_field(l, "newcomm"))
                if (newcomm != 1):
                    add_new_comm(my_rank, local_comm_d, [newcomm, oldcomm, -1, -1, group, -1, -1, -1, -1, -1])
            elif l[op_index] == "MPI_Comm_group": # comm, group
                comm = (get_comm_dict_field(l, "comm"))
                group = (get_comm_dict_field(l, "group"))
                update_comm_group(my_rank, local_comm_d, [comm, group])
            elif l[op_index] == "MPI_Comm_dup": # comm, group
                oldcomm = (get_comm_dict_field(l, "oldcomm"))
                newcomm = (get_comm_dict_field(l, "newcomm"))
                if (newcomm != 1):
                    add_new_comm(my_rank, local_comm_d, [newcomm, oldcomm, -1, -1, -1, -1, -1, -1, -1, -1])
            elif l[op_index] == "MPI_Group_incl": # comm, group # valid only my_rank is in ranks
                group = (get_comm_dict_field(l, "group"))
                count = (get_comm_dict_field(l, "count"))
                newgroup = (get_comm_dict_field(l, "newgroup"))
                ranks = get_comm_dict_field(l, "ranks") # list
#                print("add new group: %s" % str(l))
                add_new_group(my_rank, local_group_d, [newgroup, group, count, ranks, -1])
            elif l[op_index] == "MPI_Group_free": # comm, group # valid only my_rank is in ranks
                count = 0
            else:
                sys.exit("no such Op(%s) in mpi_goup" % l[op_index])

        # take care of undefined comm's
        group_comm_dict = g_group_comm_dict[i]
        print("group_comm_dict = %s" % str(group_comm_dict))
        for c in local_comm_d:
            commid = c[comm_dir_dict['id']]
            if (commid in group_comm_dict):
                group_comm_dict.pop(commid)
        print("after removing exisiting commid: group_comm_dict = %s" % str(group_comm_dict))
        m = 0
        for k, e in group_comm_dict.items():
            dest_list = ugly_hacking(my_rank, k, e, m)
            m = m + 1
            print("dest_list = %s" % str(dest_list))
            idx = dest_list.index(my_rank)
            add_new_comm(my_rank, local_comm_d, [k, 2, idx, e, -1, dest_list, -1, -1, -1, -1])

    print("comm_directory: before filling ranks")
    for i, c in enumerate(comm_directory):
        print("===== rank %d =======" % i)
        if (len(c) != 0):
            for j, cd in enumerate(c):
                print("%d:  %s " % (j, str(cd)))
    print("group_directory")
    for i, c in enumerate(group_directory):
        print("===== rank %d =======" % i)
        if (len(c) != 0):
            for j, cd in enumerate(c):
                print("%d:  %s " % (j, str(cd)))
    # 2nd phase. fill ranks 
    print(" Second Phase ----> ")
    for i, c in enumerate(comm_directory): # for each rank
        print("--- rank %d" % i)
        trank = i
        for j, d in enumerate(c): # j-th comm entry
            entry = j
            newcomm = d[comm_dir_dict['id']]
            oldcomm = d[comm_dir_dict['oldcomm']]
            rank = d[comm_dir_dict['rank']]
            count = d[comm_dir_dict['count']]
            group = d[comm_dir_dict['group']]
            ranks = d[comm_dir_dict['ranks']]
            color = d[comm_dir_dict['color']]
            key = d[comm_dir_dict['key']]
            seq = d[comm_dir_dict['seq']]
            # inherit oldcomm
            oldseq = -1
            for k in range(entry-1, -1, -1):
                if (c[k][comm_dir_dict['id']] == oldcomm):
                        oldseq = c[k][comm_dir_dict['seq']]
            old_ranks = get_ranks_from_comm(trank, oldcomm, oldseq)
#            print("old_ranks = %s" % str(old_ranks))
            if (ranks != -1): continue  # already done
            if (color >= 0): # non-negative color and ranks are not initialized yet
                # match with j-th comm_id from all the other ranks
                # collect ranks in the same color
                # FIXIT optimize 
                (newrank, newranks) = get_color_ranks(trank, newcomm, color, key, seq, entry)
                d[comm_dir_dict['rank']] = newrank
                d[comm_dir_dict['ranks']] = newranks
                if (newranks == -1):
                    d[comm_dir_dict['count']] = total_ranks
                else: 
                    d[comm_dir_dict['count']] = len(newranks)

#                for k, e in enumerate(comm_directory):
#                    if (len(e) <= entry): 
#                        print("rank(%d) has less entry in comm_directory than that" % k)
#                        continue
#                    comm_entry = e[entry]
#                    if comm_entry[comm_dir_dict['id']] == newcomm:
#                        if comm_entry[comm_dir_dict['color']] == color:
#                            if comm_entry[comm_dir_dict['ranks']] == -1:
##                                print("Bingo! set new ranks")
#                                count = count
#                                comm_entry[comm_dir_dict['ranks']] = newranks
#                            else:
#                                count = count
#                        else:
#                            count = count
#                    else: 
#                        count = count
                    
            elif (group != -1):
                d[comm_dir_dict['ranks']] = get_ranks_from_group(trank, group, 0) # FIXME
                d[comm_dir_dict['count']] = len(d[comm_dir_dict['ranks']])
            elif (oldcomm == 2): # old comm == MPI_COMM_WORLD and no group associated
                d[comm_dir_dict['ranks']] = [-1]
                d[comm_dir_dict['count']] = total_ranks;
            elif (oldcomm != -1): # duplication
                d[comm_dir_dict['ranks']] = old_ranks
                if (old_ranks == [-1]):
                    d[comm_dir_dict['count']] = total_ranks
                else:
                    d[comm_dir_dict['count']] = len(old_ranks)
            else:
                print("Wrong: ranks are not initialized")
                print("source entry :")
                print(d)
                sys.exit("Fail")
#            print("newcomm(%d), old_comm_id(%d), newranks(%s), oldranks(%s)" % (newcomm, oldcomm, str(d[comm_dir_dict['ranks']]), str( str(old_ranks))))
    print("comm_directory - after filling ranks")
    for i, c in enumerate(comm_directory):
        print("===== rank %d =======" % i)
        if (len(c) != 0):
            for cd in c:
                print("   " + str(cd))
    print("group_directory")
    for i, c in enumerate(group_directory):
        print("===== rank %d =======" % i)
        if (len(c) != 0):
            for cd in c:
                print("   " + str(cd))

def analysis(clist, matrix_file, fp_event):
    global no_header
    global type_dict
    global total_ranks
    global t_comm_histogram
    global t_total_data
    global my_rank
    global current_comm_seq
    global current_group_seq
    global last_event_start_time
    global last_event_end_time
    
    start_measure = -1
    last_event_start_time = 0

    end_measure = len(clist) -1
    main_start_measure = -1
    main_end_measure = end_measure
    comm_histogram = [ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 ] 
    t_comm_amount = 0
    t_comm_freq = 0
    t_comm_time = [0, 0]
    t_wait_time = [0, 0]
    all_to_all_comm_amount = 0
    all_to_all_comm_freq = 0
    vector_size = 0
    comm_matrix_row = [0] * total_ranks
    group_info = []
    dbg_comp_time = 0.0
    prev_return = 0
    event_list = []

    op_index = 2
#    my_rank = -1
    no_header = 0
    for i, l in enumerate(clist):
        comm_amount = 0
        flag_unsupported = 0
        if start_measure < 0:
            if l[op_index] in mpi_comm_ops or l[op_index] == 'MPI_Barrier':
                start_measure = i   # end-time
                main_start_measure = i + 1 # start-time
                if (i == 0 and l[op_index] != 'MPI_Barrier'):
                    no_header = 1
                    start_measure = 0   # end-time
                    main_start_measure = 0 # start-time

            prev_return = (get_comm_dict_field(l, "cpu_end"))
        else:
            current_enter = (get_comm_dict_field(l, "cpu_start"))
            gap_time = current_enter - prev_return
            dbg_comp_time = dbg_comp_time + gap_time
            prev_return = (get_comm_dict_field(l, "cpu_end"))
           
        if (start_measure >= 0): generate_event(l, event_list)
        if l[op_index] in mpi_wait_ops: # FIXME how to consider its communication amount? Minimum?
#            print(l[op_index])
            (wait_time, comm_amount) = analysis_comm(l, comm_matrix_row)
            if (start_measure >= 0):
                for j in range(len(t_wait_time)):
                    t_wait_time[j] = t_wait_time[j] + wait_time[j]
        elif l[op_index] in mpi_comm_ops: # FIXME blocking time needs to be considered for some command (Recv)
#            print(l[op_index])
            comm_time = []
            (comm_time, comm_amount) = analysis_comm(l, comm_matrix_row)
            for j in range(len(t_comm_time)):
                    t_comm_time[j] = t_comm_time[j] + comm_time[j]
            if (comm_amount > 0):
                t_comm_amount = t_comm_amount + comm_amount
                t_comm_freq = t_comm_freq + 1
            update_histogram(comm_histogram, comm_amount)
            if l[op_index] in mpi_comm_sendcount_list or l[op_index] == "MPI_Alltoall":
                all_to_all_comm_amount = all_to_all_comm_amount + comm_amount
                all_to_all_comm_freq = all_to_all_comm_freq + 1
            # get comm amount and matrix
        elif l[op_index] in mpi_group:
            if l[op_index] == "MPI_Comm_rank": 
                if (my_rank < 0):
                    my_rank = (get_comm_dict_field(l, "rank"))
            elif (l[op_index] == "MPI_Comm_dup" or l[op_index] == "MPI_Comm_create" or l[op_index] == "MPI_Comm_split"): 
                # keep track of the current 'seq' of the newcomm
                newcomm = (get_comm_dict_field(l, "newcomm"))
                if (newcomm in current_comm_seq):
                    current_comm_seq[newcomm] = current_comm_seq[newcomm] + 1
                else:
                    current_comm_seq[newcomm] = 0
            elif l[op_index] == "MPI_Group_incl": 
                # keep track of the current 'seq' of the newgroup
                newgroup = (get_comm_dict_field(l, "newgroup"))
                if (newgroup in current_group_seq):
                    current_group_seq[newgroup] = current_group_seq[newgroup] + 1
                else:
                    current_group_seq[newgroup] = 0
            elif l[op_index] == "MPI_Comm_size": 
                comm_id = (get_comm_dict_field(l, "comm"))
                comm_size = (get_comm_dict_field(l, "size"))
            else: 
                flag_unsupported = 1
        elif l[op_index] in mpi_ops:
            if l[op_index] == "MPI_Finalize": 
                end_measure = i # start_time
                main_end_measure = i - 1 # end_time
            elif l[op_index] == "MPI_Init": i = i
            else: 
                flag_unsupported = 1
        elif l[op_index] in mpi_data:
            if l[op_index] == "MPI_Type_vector": 
                vector_size = (get_comm_dict_field(l, "blocklength"))
                count = (get_comm_dict_field(l, "count"))
                new_type = (get_comm_dict_field(l, "newtype"))
                old_type = (get_comm_dict_field(l, "oldtype"))
                base_size = type_to_size(old_type)
#                print("vector: base_size(%d), vector_size(%d), count(%d)" % (base_size, vector_size, count))
                type_dict[new_type] = vector_size * base_size * count
#                print("new type_dict = %s" % str(type_dict))
            else: 
                flag_unsupported = 1
        else:
            flag_unsupported = 1

#        print(l)
#        print(comm_matrix_row)

    if (flag_unsupported == 1):
        if (l[op_index] not in undefined):
            print("Unsupported %s" % l[op_index])
            undefined[l[op_index]] = 1

    l_total_comm = sum(comm_matrix_row)
    comm_matrix_row.insert(0, my_rank)
    comm_matrix_row.insert(1, l_total_comm)

#    print("++++++ Event list (rank: %d) ++++++ " % my_rank)
    for i in range(len(event_list)):
#        print(event_list[i])
        out_str = '; '.join(str(e) for e in event_list[i])
        fp_event.write(out_str + '\n')


    print("no_header(%d), start_measure(%d), end_measure(%d)" % (no_header, start_measure, end_measure))
    print("t_comm_time(%f), t_wait_time(%f), dbg_comp_time(%f)" % (t_comm_time[1], t_wait_time[1], dbg_comp_time))
    if (no_header == 0):
            t1 = (get_comm_dict_field(clist[end_measure], "wall_start")) - (get_comm_dict_field(clist[start_measure], "wall_end"))
            t2 = (get_comm_dict_field(clist[main_end_measure], "wall_end")) - (get_comm_dict_field(clist[main_start_measure], "wall_start"))
            t3 = (get_comm_dict_field(clist[end_measure], "cpu_start")) - (get_comm_dict_field(clist[start_measure], "cpu_end"))
            t4 = (get_comm_dict_field(clist[main_end_measure], "cpu_end")) - (get_comm_dict_field(clist[main_start_measure], "cpu_start"))
    else:
            t1 = (get_comm_dict_field(clist[end_measure], "wall_start")) - (get_comm_dict_field(clist[start_measure], "wall_end"))
            t2 = (get_comm_dict_field(clist[main_end_measure], "wall_start")) - (get_comm_dict_field(clist[main_start_measure], "wall_end"))
            t3 = (get_comm_dict_field(clist[end_measure], "cpu_start")) - (get_comm_dict_field(clist[start_measure], "cpu_end"))
            t4 = (get_comm_dict_field(clist[main_end_measure], "cpu_start")) - (get_comm_dict_field(clist[main_start_measure], "cpu_end"))

    list_wall = [my_rank, 
            # total duration [after MPI_Initialization, before start of MPI_Finalize]
            t1, 
            # total duration [start of the first MPI call, end of the last MPI all]
            t2,
            # total mpi time
            t_comm_time[0] + t_wait_time[0]]
    # total time other than mpi time
    list_wall.append(list_wall[2] - list_wall[3])
    # mpi time ratio over total time
    list_wall.append(100 * list_wall[3] / float(list_wall[2]))

    list_cpu = [
            # total duration [after MPI_Initialization, before start of MPI_Finalize]
            t3,
            # total duration [start of the first MPI call, end of the last MPI all]
            t4,
            # total mpi time
            t_comm_time[1] + t_wait_time[1]]
    # total time other than mpi time
    list_cpu.append(list_cpu[1] - list_cpu[2])
    # mpi time ratio over total time
    list_cpu.append(100 * list_cpu[2] / float(list_cpu[1]))
            
    list_comm = [t_comm_amount, t_comm_freq]
    all2all_amount_ratio = all_to_all_comm_amount/float(t_comm_amount)
    all2all_freq_ratio = all_to_all_comm_freq/float(t_comm_freq)
    list_all_to_all_comm = [all_to_all_comm_amount, all2all_amount_ratio, all_to_all_comm_freq, all2all_freq_ratio]
    t_list = list_wall + list_cpu + list_comm + list_all_to_all_comm
    comm_h1 = []
    comm_h2 = []
#    print(t_list)
    for i in range(len(comm_histogram)):
        comm_h1.append(comm_histogram[i])
        comm_ratio = 0
        if (t_comm_freq != 0):
            comm_ratio = comm_histogram[i]/float(t_comm_freq)*100
        comm_h2.append(comm_ratio)
        print("%s, %d, %2.2f " % (comm_histogram_label_list[i], comm_histogram[i], comm_ratio))

#    comm_matrix.append(comm_matrix_row)
    amount_str = ','.join(str(e) for e in comm_matrix_row)
    matrix_file.write(amount_str + "\n")

    return (t_list, comm_h1, comm_h2)

def __analysis(asciifile):
    global comm_matrix

    print("__analysis: %s" % asciifile)

    # [rank, to_0, to_1, ..., to_N-1]
    tlist = asciifile.split('/')
    tstr = tlist[-1]
    tlist = tstr.split('.')
    t_rank = int(tlist[0]) 
    l_comm_matrix = []
    l_comm_matrix.append(t_rank)

    # [thread, MPI_Comm command, dest[], send_count[], datatype, wall_start, wall_end, cpu_start, cpu_end]
    # if MPI_Bcast: dest[] becomes all except root
    # if MPI_Gather, MPI_Gatherv, MPI_Reduce: dest[] becomes [root]
    comm_dict = {"thread": 0, "op": 1, 
            "dest": 2, "sendcounts": 3, "source": 4, "rcvcounts": 5,
            "datatype": 6, "datasize": 7, 
            "wall_start": 8, "wall_end": 9, "cpu_start": 10, "cpu_end": 11 }
    comm_prop = [-1, '', [], 0, [], 0, 0, 0, 0, 0, 0, 0]

    input_trace = open(asciifile, 'r')
    
    rank = 0
    start_time = -0.1
    end_time = -0.1
    main_start_time = -0.1  # except prologue and epilogue
    main_end_time = -0.1    # except prologue and epilogue
    mpi_time = 0.0
    other_time = 0.0
    cpu_start_time = -0.1
    cpu_end_time = -0.1
    cpu_main_start_time = -0.1  # except prologue and epilogue
    cpu_main_end_time = -0.1    # except prologue and epilogue
    cpu_mpi_time = 0.0
    cpu_other_time = 0.0
    in_prologue = True
    in_epilogue = False
    count = 0
    other_tmp_start_time = 0.0
    cpu_other_tmp_start_time = 0.0
    comm_amount = 0
    comm_freq = 0
    comm_histogram = [ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 ] 
    comm_mode = False
    amount = 0
    unit_size = 8
    valid_count = False
    vector_size = 8
    vector_mode = False
    sub_comm_mode = 0
    all_to_all_comm_amount = 0
    all_to_all_comm_freq = 0
    last_walltime = 0
    last_cputime = 0
    group_mode = False
    group_list = [] # MPI_Group

    line_no = 0
    for line in input_trace:
      list_line = line.split()
      if (line_no == 0 and not (list_line[0] in mpi_ops)):
          in_prologue = False
          start_time = float(list_line[4].rstrip(','))
          cpu_start_time = float(list_line[6].rstrip(','))
          main_start_time = float(list_line[4].rstrip(','))
          cpu_main_start_time = float(list_line[6].rstrip(','))
          other_tmp_start_time = main_start_time
          cpu_other_tmp_start_time = cpu_main_start_time
      line_no = line_no + 1

      # MPI_Group_incl: we don't include time taken for this for mpi_time
      if (group_mode == True or (list_line[0] in mpi_group and list_line[1] == 'entering')):
          group_mode = True
          if (list_line[0] in mpi_group and list_line[1] == "returning"):
              group_mode = False
          if ('ranks' in list_line[1]):
              tlist = line.split('=')
#              print(tlist)
              t = tlist[1].strip().strip('][').split(', ')
#              print(t)
              rank_list = [int(i) for i in t]
              group_list.append(rank_list)
          continue
      
      if ('walltime' in line):
          last_walltime = float(list_line[4].rstrip(','))
      if ('cputime' in line):
          last_cputime = float(list_line[6].rstrip(','))
      if (" rank=" in list_line[1]):
          tlist = list_line[1].split('=')
          rank = int(tlist[1])
          if (t_rank != rank): 
              print("t_rank %d is different from rank %d" % (t_rank, rank))
              sys.exit("rank conflict")
      if (list_line[0] in mpi_ops):
          if (mpi_ops[list_line[0]] == 0): # MPI_Initialized
              if (list_line[1] == "entering"):
                  start_time = float(list_line[4].rstrip(','))
                  cpu_start_time = float(list_line[6].rstrip(','))
          elif (mpi_ops[list_line[0]] == 3): # MPI_Finalize
              if (list_line[1] == "entering"):
                  main_end_time = float(list_line[4].rstrip(','))
                  cpu_main_end_time = float(list_line[6].rstrip(','))
                  in_epilogue = True
              if (list_line[1] == "returning"):
                  end_time = float(list_line[4].rstrip(','))
                  cpu_end_time = float(list_line[6].rstrip(','))
      elif (list_line[0] in mpi_data):
          if (list_line[0] == "MPI_Type_vector"):
              if (list_line[1] == 'entering'):
                  vector_mode = True
              elif (list_line[1] == 'returning'):
                  vector_mode = False 
          elif ('MPI_Datatype datatype' in line and 'MPI_PACKED' in line):
              unit_size = 1
          elif (comm_mode == True and 'MPI_Datatype datatype=32 (user-defined-datatype)' in line):
#              print("vector_size is use: %d" % vector_size)
              unit_size = vector_size
          elif (comm_mode == True and 'MPI_Datatype datatype=31 (user-defined-datatype)' in line):
              unit_size = 1 
          elif (('MPI_Datatype datatype' in line) and ('MPI_' in list_line[2])):
              unit_size = 8
      elif (list_line[0] in mpi_comm_ops or list_line[0] in mpi_wait_ops):
          if (in_prologue == False and in_epilogue == False):
              if (list_line[1] == 'entering'):
                  if (list_line[0] in mpi_comm_ops):
                      # print(list_line)
                      comm_mode = True
                      valid_count = False
                      comm_str = list_line[0]
#                      print(list_line)
                      sub_comm_mode = which_comm_mode(list_line[0])
#                      if (sub_comm_mode == 1 or sub_comm_mode == 2):
#                          print(list_line)
                  comm_prop[comm_dict["wall_start"]] = float(list_line[4].rstrip(','))
                  comm_prop[comm_dict["cpu_start"]] = float(list_line[6].rstrip(','))
                  comm_prop[comm_dict["op"]] = list_line[0]
                  other_time = other_time + (comm_prop[comm_dict["wall_start"]] - other_tmp_start_time)
                  cpu_other_time = cpu_other_time + (comm_prop[comm_dict["cpu_start"]] - cpu_other_tmp_start_time)
                  thread_id = list_line[len(list_line) - 1].split('.')
                  comm_prop[comm_dict["thread"]] = int(thread_id[0])
              if (list_line[1] == 'returning'):
                  if (list_line[0] in mpi_comm_ops):
                      comm_mode = False 
                  amount = amount * unit_size
                  comm_prop[comm_dict["wall_end"]] = float(list_line[4].rstrip(','))
                  comm_prop[comm_dict["cpu_end"]] = float(list_line[6].rstrip(','))
                  other_tmp_start_time = comm_prop[comm_dict["wall_end"]]
                  cpu_other_tmp_start_time = comm_prop[comm_dict["cpu_end"]]
                  if (comm_prop[comm_dict["op"]] != list_line[0]):
                      print("mismatch: %s vs %s" % (comm_prop[comm_dict["op"]], list_line[0]))
                  if (comm_prop[comm_dict["wall_end"]] < comm_prop[comm_dict["wall_start"]]): 
                      print("Error: end time of comm (%f) is smaller than start of comm (%f)" % (comm_prop[comm_dict["wall_start"]], comm_prop[comm_dict["wall_end"]]))
                  mpi_time = mpi_time + (comm_prop[comm_dict["wall_end"]] - comm_prop[comm_dict["wall_start"]])
                  cpu_mpi_time = cpu_mpi_time + (comm_prop[comm_dict["cpu_end"]] - comm_prop[comm_dict["cpu_start"]])
                  if (valid_count == True and amount > 0):
                      valid_count = False
#                      print("%s: amount = %d" % (comm_str, amount))
                      hist_index = (math.log(amount,2))
                      hist_index = int(hist_index - 9) # start with 0.5k
                      if (hist_index < 0): hist_index = 0
                      if (hist_index >= len(comm_histogram)): hist_index = len(comm_histogram) - 1 
#                      print("amount %d, hist_index %f" % (amount, hist_index))
                      comm_amount = comm_amount + amount
                      if (sub_comm_mode == mpi_sub_comm_mode['Sends']):
                          all_to_all_comm_amount = all_to_all_comm_amount + amount
                          all_to_all_comm_freq = all_to_all_comm_freq + 1
                      comm_freq = comm_freq + 1
                      comm_histogram[hist_index] = comm_histogram[hist_index] + 1
          if ((list_line[0] == 'MPI_Barrier' and in_prologue == True and list_line[1] == "returning") \
                  or (list_line[0] in mpi_comm_ops and list_line[1] == "entering")): # exit prologue
                  in_prologue = False
                  main_start_time = float(list_line[4].rstrip(','))
                  cpu_main_start_time = float(list_line[6].rstrip(','))
                  other_tmp_start_time = main_start_time
                  cpu_other_tmp_start_time = cpu_main_start_time
    #          elif (list_line[1] == "entering"):
    #          elif (list_line[1] == "returning"):
      else:
          if ("rank=" in list_line[1]):
              tlist = list_line[1].split('=')
              rank = int(tlist[1])
          elif ("MPI_" in list_line[0]):
              if (list_line[0] not in undefined):
                  print("Unknown MPI_ keywork %s" % list_line[0])         
                  undefined[list_line[0]] = 1
          else:
              if (vector_mode == True):
                  if ('blocklength' in list_line[1]):
                      tlist = list_line[1].split('=')
                      vector_size = int(tlist[1])
#                      print("blocklength = %d" % unit_size)
              if (comm_mode == True):
                  tlist = list_line[1].split('=')
#                  print(list_line)
#                  print(tlist)

                  if (sub_comm_mode >= mpi_sub_comm_mode['Send']): 
#                      print("sub_comm_mode = %d" % sub_comm_mode)
                      if (sub_comm_mode == mpi_sub_comm_mode['Send']):
                          if (tlist[0] == 'sendcount'):
#                             print("DBG: sendcount")
#                             print(tlist[1])
                             amount = int(tlist[1])
#                             print("%s: sendcount: amount = %d\n" % (comm_str, amount))
                             valid_count = True
                      elif (sub_comm_mode == mpi_sub_comm_mode['Sends']):
#                          print(tlist)
                          if ('sendcounts' in tlist[0] or 'recvcounts' in tlist[0]):
                             tlist2 = line.split('=')
#                             print("DBG: sendcounts")
#                             print(line)
#                             print(tlist2[1])
                             amount_list = tlist2[1].strip()
                             amount_list = amount_list.strip('][').split(', ')
                             amount = 0
#                             print(amount_list)
                             if ('sendcounts' in tlist[0]):
                                 for i in amount_list:
                                    amount = amount + int(i)
                             tamount_list = [int(i) for i in amount_list]
                             dict_key = 'sendcounts'
                             if ('recvcounts' in tlist[0]):
                                 dict_key = 'recvcounts'
                             comm_prop[comm_dict[dict_key]] = tamount_list

#                             print("%s: sendcounts: amount = %d\n" % (comm_str, amount))
                             valid_count = True
                      else: # Others    Bcast, ...
                          if (tlist[0] == 'count'):
                             amount = int(tlist[1])
                             tamount_list = []
                             tamount_list.append(amount)
                             comm_prop['sendcounts'] = tamount_list
                  else: # Receive
                             comm_prop['sendcounts'] = tamount_list

#                             print("%s: others: amount = %d\n" % (comm_str, amount))
                             valid_count = True
#                  if (valid_count == True):
#                      if (amount == 0):
#                          print(line)

    if (in_epilogue == False): # no epilogue
        # use the last timestamp
        end_time = last_walltime
        main_end_time = end_time 
        cpu_end_time = last_cputime
        cpu_main_end_time = cpu_end_time

    mpi_time_ratio = 0
    cpu_mpi_time_ratio = 0
    if (other_time != 0):
            mpi_time_ratio = float(100.0 * mpi_time/(mpi_time + other_time))
    if (cpu_other_time != 0):
            cpu_mpi_time_ratio = float(100.0 * cpu_mpi_time/(cpu_mpi_time + cpu_other_time))
    if (other_time == 0 or cpu_other_time == 0): return ([], [], [])
    list_wall = [rank, 
            end_time - start_time,
            main_end_time - main_start_time, 
            mpi_time, 
            other_time, 
            mpi_time_ratio]
    list_cpu = [cpu_end_time - cpu_start_time, 
            cpu_main_end_time - cpu_main_start_time, 
            cpu_mpi_time, 
            cpu_other_time, 
            cpu_mpi_time_ratio]
    list_comm = [comm_amount, comm_freq]
    all2all_amount_ratio = all_to_all_comm_amount/float(comm_amount)
    all2all_freq_ratio = all_to_all_comm_freq/float(comm_freq)
    list_all_to_all_comm = [all_to_all_comm_amount, all2all_amount_ratio, all_to_all_comm_freq, all2all_freq_ratio]
    t_list = list_wall + list_cpu + list_comm + list_all_to_all_comm
    comm_h1 = []
    comm_h2 = []
#    print(t_list)
    for i in range(len(comm_histogram)):
        comm_h1.append(comm_histogram[i])
        comm_ratio = 0
        if (comm_freq != 0):
            comm_ratio = comm_histogram[i]/float(comm_freq)*100
        comm_h2.append(comm_ratio)
        print("%s, %d, %2.2f " % (comm_histogram_label_list[i], comm_histogram[i], comm_ratio))
    comm_matrix.append(l_comm_matrix)
    return (t_list, comm_h1, comm_h2)

# main
if len(sys.argv) != 4:
    usage()
if len(sys.argv) == 4:
    total_ranks = int(sys.argv[3])

no_event_dict = {}
last_event_start_time = 0
last_event_end_time = 0
output_trace = open(sys.argv[1] + '/' + sys.argv[2], 'w')
output_trace_matrix = open(sys.argv[1] + '/comm_matrix_' + sys.argv[2], 'w')
fp_event = open(sys.argv[1] + '/event_' + sys.argv[2], 'w')

output_trace.write(summary_label + "\n")
output_trace_matrix.write("\n=== Comm matrix : rank, total comm amount, comm_amount to others\n")
a_str = ','.join(str(e) for e in range(total_ranks))
output_trace_matrix.write("rank, total comm amount, " + a_str + "\n")

total_output = []
filename_list = []
filenamestr = sys.argv[1] + "/*.bin"
for filename in glob.glob(filenamestr):
    filename_list.append(filename)
sorted_filenames = sorted(filename_list, key=lambda x: int(x.split('/')[-1].split('-')[-1].split('.')[0]))

for i in range(total_ranks):
    g_group_comm_dict.append({})

asciifile_list = []
for filename in sorted_filenames:
    tstr0 = filename.split('/')
    tstr1 = tstr0[len(tstr0) - 1]
    tstr = tstr1.split('-')
    tlist = tstr[len(tstr) - 1].split('.')
    t_rank = int(tlist[0])
    asciifile = sys.argv[1] + "/" + tlist[0] + ".ascii"
    asciifile_list.append(asciifile)
    if (not exists(asciifile)):
        print("dumpi2ascii %s > %s" % (filename, asciifile))
        f = open(asciifile, "w") 
        # This is the way you'd run "ls -al > ls.output.txt" in the background
        p = subprocess.Popen(["dumpi2ascii", filename], stdout=f) # This will run in the background
        p.wait()
        f.close()
        output = p.communicate()[0]
        if p.returncode != 0:
            print("file open failed %d %s" % (p.returncode, output))
            os.system("rm " + asciifile)
            continue
    else:
        print("Skipped: dmpi2ascii for %s" % filename)
    p_list = parse(asciifile)
#    (t_list, comm_h1, comm_h2) = analysis(p_list, output_trace_matrix)
    get_comm_group_commands(t_rank, p_list)

analysis_comm_group()
g_comm_list = [] # release memory

print(comm_directory)
print(group_directory)
#sys.exit("debug")
current_comm_seq = {}
current_group_seq = {}

warning_comm_dict = []
for i in range(total_ranks):
    warning_comm_dict.append({})

for asciifile in asciifile_list: 

    tstr0 = asciifile.split('/')
    tstr1 = tstr0[len(tstr0) - 1]
    tstr = tstr1.split('-')
    tlist = tstr[len(tstr) - 1].split('.')
    my_rank = int(tlist[0])

    current_comm_seq = {}
    current_group_seq = {}
    l_list = []
    p_list = parse(asciifile)
    print("=== analyze %s (my_rank = %d) ===" % (asciifile, my_rank))
    (t_list, comm_h1, comm_h2) = analysis(p_list, output_trace_matrix, fp_event)

#    print(t_list)
#    print(comm_h1)
#    print(comm_h2)

    if (t_list == []): continue
    l_list.append(t_list)
    l_list.append(comm_h1)
    l_list.append(comm_h2)
    total_output.append(l_list)

num_entries = len(total_output)
if (num_entries == 0):
    output_trace.write("=== Histogram : comm. size, comm. freq\n")
    output_trace.write("=== Histogram : comm. size, comm. freq\n")
    sys.exit("No meaningful communication at all")

# collect average communication properties
average_comm_prop = []
for i in range(len(total_output)):
    print(total_output[i][0])
    if (i == 0): average_comm_prop = total_output[i][0].copy()
    else:
       for j in range(len(total_output[i][0])):
           average_comm_prop[j] = average_comm_prop[j] + total_output[i][0][j]

for j in range(0, len(average_comm_prop)):
    average_comm_prop[j] = average_comm_prop[j] / num_entries
average_comm_prop.pop(0)
average_comm_prop_str = ', '.join(str(e) for e in average_comm_prop)
output_trace.write("Average, " + average_comm_prop_str + '\n')

# collect average histogram properties
output_trace.write("=== Histogram : comm. size, comm. precentage\n")
output_trace.write("rank, " + comm_histogram_label + "\n")
average_hist_percentage = []
for i in range(len(total_output)):
    if i == 0: average_hist_percentage = total_output[i][2].copy()
    else:
       for j in range(len(total_output[i][2])):
           average_hist_percentage[j] = average_hist_percentage[j] + total_output[i][2][j]

for j in range(len(total_output[0][1])):
    average_hist_percentage[j] = average_hist_percentage[j] / num_entries
average_hist_percentage_str = ', '.join(str(e) for e in average_hist_percentage)
output_trace.write("Average, " + average_hist_percentage_str + '\n')

output_trace.write("=== Histogram : comm. size, comm. freq\n")
output_trace.write("rank, " + comm_histogram_label + "\n")

average_hist_freq = []
for i in range(len(total_output)):
    if (i == 0): average_hist_freq = total_output[i][1].copy()
    else:
       for j in range(len(total_output[i][1])):
           average_hist_freq[j] = average_hist_freq[j] + total_output[i][1][j]

for j in range(len(total_output[0][1])):
    average_hist_freq[j] = average_hist_freq[j] / num_entries
average_hist_freq_str = ', '.join(str(e) for e in average_hist_freq)
output_trace.write("Average, " + average_hist_freq_str + '\n')

# Raw data output
output_trace.write("=== Raw data of comm properties ==\n")
sorted_total_output = sorted(total_output, key=lambda x: x[0][0])
output_trace.write(summary_label + "\n")
for i in range(len(total_output)):
    amount_str = ','.join(str(e) for e in sorted_total_output[i][0])
    output_trace.write(amount_str + "\n")

output_trace.write("=== Histogram : comm. size, comm. freq\n")
output_trace.write("rank, " + comm_histogram_label + "\n")
for i in range(len(total_output)):
    amount_str = ','.join(str(e) for e in sorted_total_output[i][1])
    output_trace.write(str(sorted_total_output[i][0][0]) + ', ' + amount_str + "\n")

output_trace.write("\n=== Histogram : comm. size, comm. precentage\n")
output_trace.write("rank, " + comm_histogram_label + "\n")
for i in range(len(total_output)):
    amount_str = ','.join(str(e) for e in sorted_total_output[i][2])
    output_trace.write(str(sorted_total_output[i][0][0]) + ', ' + amount_str + "\n")

# sort comm_matrix with rank
#sorted_comm_matrix = sorted(comm_matrix, key=lambda x: x[0])
# output comm_matrix
# output_trace_matrix.write("\n=== Comm matrix : rank, total comm amount, comm_amount to others\n")
#print("\n=== Comm matrix : rank, total comm amount, comm_amount to others\n")
#a_str = ','.join(str(e) for e in range(total_ranks))
#print("rank, total comm amount, %s" % a_str)
#output_trace_matrix.write("rank, total comm amount, " + a_str + "\n")
#for l in sorted_comm_matrix:
#    amount_str = ','.join(str(e) for e in l)
##    print(amount_str)
#    output_trace_matrix.write(amount_str + "\n")

output_trace.close()
output_trace_matrix.close()
fp_event.close()
