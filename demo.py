#!/usr/bin/env python

# 整体为两级并行
# 固定数量MPI进程 --一级并行
# 一个MPI进程内部使用 MPI.COMM_SELF.Spawn  --二级并行

from __future__ import print_function
from mpi4py import MPI
from parutils import pprint
import sys
import Pool
import common
import time
import copy
tags = common.tags

# usage 
# mpirun -n 2 demo.py 1 1

# global parameters
SCENE_NUM = 0
FIRESYS_NUM_PER_SCENE = 0
TOTAL_RES_LIST = []

def fire_sys():
    print ("Fire system id: %d"%(comm.rank))
    Total_frame = 51
    Max_process = 1 + 20
    Target_Q = []
    Res_list = []
    # read target parameters
    # 1: increase target
    # 0: decrease target

    # target = ["No.", position, meet_flag, timestamp]
    target_map = {
        5:[["01",0,0,0]]*30,
        20:[["04",0,0,0]]*50,
        40:[["05",0,0,0]]*40,
        50:[["07",0,0,0]]*10,
    }

    # Process Pool
    pool = Pool.MyPool(Max_process,"simulator.py")
    # 从雷达发现目标群,到对目标群完成进行计算，为一帧
    frame = 0

    while frame < Total_frame or len(Target_Q):
        print("-"*78)
        print("Fire system %d: cur frame is %d" %(comm.rank,frame))
        # increase new target
        if frame in target_map.keys():
            target_map[frame][0][3] = time.time()
            Target_Q.extend(target_map[frame])
        
        if len(Target_Q):
            Target_Q = pool.create(Target_Q)
            Target_Q = update(Target_Q,Res_list)
        frame += 1
    # time.sleep(5)
    print("Pool exit")
    pool.exit()
    
    print("final res",len(Res_list))
    comm.send(Res_list,0,tag=tags.DONE)

def update(Target_Q,Res_list):
    tmp = list()
    for target in Target_Q:
        if target[2] == 0:
            target[1] += 1
            tmp.append(target)
        else:
            target[3] = round(time.time()-target[3],2)
            Res_list.append(target)
    return copy.deepcopy(tmp)

def scene_process():
    print ("Scene id: %d"%(comm.rank))
    cnt = 0
    result_list = []
    
    while cnt < FIRESYS_NUM_PER_SCENE:
        data = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
        source = status.Get_source()
        print("Fire System id:%d"%(source))
        tag = status.Get_tag()
        if tag == tags.DONE:
            result_list.append(data)
            cnt += 1

    return result_list


def main(argv):
    global SCENE_NUM,FIRESYS_NUM_PER_SCENE,TOTAL_RES_LIST
    SCENE_NUM = int(argv[0])
    FIRESYS_NUM_PER_SCENE = int(argv[1])
    print("-"*78)
    print("main MPI Rank: %d"%(comm.rank))
    # scene process
    if comm.rank % (FIRESYS_NUM_PER_SCENE+1) == 0:
        TOTAL_RES_LIST = scene_process()
        print(TOTAL_RES_LIST)
    # fire system process
    else:
        fire_sys()
    comm.Barrier()
    print("main MPI Rank: %d Done"%(comm.rank))
        

if __name__ == '__main__':
    # 一级并行
    global comm,status
    comm = MPI.COMM_WORLD
    status = MPI.Status()

    pprint("-"*78)
    pprint(" Running on %d cores" % comm.size)
    pprint("-"*78)

    t = MPI.Wtime()
    main(sys.argv[1:])
    print(MPI.Wtime() - t)
