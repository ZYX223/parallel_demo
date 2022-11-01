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

tags = common.tags

# global parameters
SCENE_NUM = 0
FIRESYS_NUM_PER_SCENE = 0
TOTAL_RES_LIST = []

def fire_sys():
    print ("Fire system id: %d"%(comm.rank))
    Total_frame = 6
    FireCar_NUM = 4
    Max_process = 1 + 1 + FireCar_NUM
    Target_Q = []
    Res_list = []
    
    # target = ["No.", position, [car_id, missile_id],meet_flag, timestamp]
    target_map = {
        5:[[str(x),100,[-1,-1],0,0] for x in range(30)],
        # 20:[[str(x),90,[-1,-1],0,0] for x in range(30,30+50)],
        # 40:[[str(x),120,[-1,-1],0,0] for x in range(80,80+40)],
        # 50:[[str(x),70,[-1,-1],0,0] for x in range(120,120+10)],
    }

    # Process Pool
    pool = Pool.MyPool(Max_process,FireCar_NUM,'simulator.py')
    # 从雷达发现目标群,到对目标群完成进行计算，为一帧
    frame = 0
    while frame < Total_frame or len(Target_Q):
        print("-"*78)
        print("Fire system %d: cur frame is %d" %(comm.rank,frame))
        start = MPI.Wtime()
        # increase new target
        if frame in target_map.keys():
            for i in range(len(target_map[frame])):
                target_map[frame][i][4] = MPI.Wtime()
            Target_Q.extend(target_map[frame])
        
        if len(Target_Q):
            Target_Q = pool.create(Target_Q)
            Target_Q = update(Target_Q,Res_list)
        
        frame += 1
        print("Frame %d time is :%f"%(frame,round(MPI.Wtime()-start,2)))

    print("Pool exit")
    pool.exit()
    
    print("final res",len(Res_list))
    comm.send(Res_list,0,tag=tags.DONE)

def update(Target_Q,Res_list):
    tmp = list()
    for target in Target_Q:
        if target[3] == 0 and target[1] > 0:
            target[1] -= 5
            tmp.append(target)
        else:
            target[4] = round(MPI.Wtime()-target[4],2)
            Res_list.append(target)
    return tmp

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
    global comm,status
    comm = MPI.COMM_WORLD
    status = MPI.Status()

    pprint("-"*78)
    pprint(" Running on %d cores" % comm.size)
    pprint("-"*78)

    t = MPI.Wtime()
    main(sys.argv[1:])
    print(MPI.Wtime() - t)
