from mpi4py import MPI
import time
import threading
import common
import copy


if __name__ == "__main__":
    try:
        comm = MPI.Comm.Get_parent()
        rank = comm.Get_rank()
        size = comm.Get_size()
        car_num = 4

        car_list = [x+1 for x in range(car_num)]
        worker_list = [x for x in range(size) if x !=0 and x not in car_list]
        
    except:
        raise ValueError('Could not connect to parent - ')
    
    if rank in car_list:
        print ("Process Connect Success %d car_num is %d"% (rank,car_num))
        print("launch car script")
