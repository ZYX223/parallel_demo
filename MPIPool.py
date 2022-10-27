from mpi4py.futures import MPIPoolExecutor
import cuda_text2

class MyPool:
    def __init__(self,rank,max_workers=1):
        self.MPI_Rank = rank
        self.max_workers = max_workers
        self.executor = MPIPoolExecutor(max_workers = self.max_workers)
    
    def calculate(self,TarQueue):
        res_list = self.executor.map(simulator,TarQueue)
        for res in res_list:
            print("MPI Rank: %d, %s" %(self.MPI_Rank,res))
        
        
def simulator(tar):
    # 包含诸元计算，生成发射令，导弹位置估计
    # 诸元计算4个发射车，需要并行,可考虑 cuda 加速
    
    cuda_text2.calculator(3)
    return [tar,1]


if __name__ == '__main__':
    pass
