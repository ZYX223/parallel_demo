#!/usr/bin/env python

# 整体为两级并行
# 固定数量MPI进程 --一级并行
# 一个MPI进程内部使用 MPIPoolExecutor  --二级并行

from __future__ import print_function
from mpi4py import MPI
from parutils import pprint
from MPIPool import MyPool
import os
import cuda_text2
import random
# 一级并行
comm = MPI.COMM_WORLD

pprint("-"*78)
pprint(" Running on %d cores" % comm.size)
pprint("-"*78)

target_map = {
    5:(1,["01"]),
    20:(1,["04"]),
    25:(1,["05","06"]),
    50:(1,["07"]),
    200:(0,["01","02"]),
    250:(0,["03"]),
    500:(0,["04"]),
    700:(0,["05","06"]),
    900:(0,["07"]),
}
Total_frame = 20
TarQueue = []

Max_Process = os.cpu_count()

def main():
    # 初始化进程池
    max_workers = (Max_Process - comm.size)//comm.size
    Pool = MyPool(comm.rank,1)
    # 从雷达发现目标群,到对目标群完成进行计算，为一帧
    for frame in range(Total_frame):
        print("-"*78)
        print("MPI rank %d: cur frame is %d" %(comm.rank,frame))
        if frame in target_map.keys():
            if target_map[frame][0] == 1:
                TarQueue.extend(target_map[frame][1])
            else:
                for tar in target_map[frame][1]:
                    TarQueue.remove(tar)
        if len(TarQueue):
            # MPIPool 计算,二级并行
            #Pool.calculate(TarQueue)
            cuda_text2.main(700,500,3)
        
if __name__ == '__main__':
    t = MPI.Wtime()
    main()
    print(MPI.Wtime() - t)
