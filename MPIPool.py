from mpi4py import MPI
from mpi4py.futures import MPIPoolExecutor
import time

class MyPool:
    def __init__(self,rank,max_workers=1):
        self.MPI_Rank = rank
        self.max_workers = max_workers
        self.executor = MPIPoolExecutor(max_workers = self.max_workers)
    
    def calculate(self,TarQueue):
        res_list = self.executor.map(simulator,TarQueue)
        for res in res_list:
            print("MPI Rank: %d, %s" %(self.MPI_Rank,res))
        
        
# 相关仿真计算
def simulator(tar):
    # 包含诸元计算，生成发射令，导弹位置估计
    # 诸元计算4个发射车，需要并行,可考虑cuda 加速
    for i in range(4):
        time.sleep(1)
    return [tar,1]


if __name__ == '__main__':
    pass
