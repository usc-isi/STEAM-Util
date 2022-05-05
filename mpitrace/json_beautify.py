import sys
import os
import json

fp = open(sys.argv[1], 'r')
data = json.load(fp)

print("-------- tasks -------- ")
for i in data["tasks"]:
   print(i)

print("-------- edges -------- ")
j = 0
edge_str = ""
for i in data["edges"]:
    if (i[0] != j):
        print(edge_str)
        edge_str = ""
    edge_str += str(i) + ", "

    
