from mpi4py import MPI
import time
import threading
import common
import copy

tags = common.tags
status = MPI.Status()

def calculator(tar):
    # target = ["No.", position, meet_flag, timestamp]
    # update pos for every calculator
    print ("Worker Rank :%d",comm.rank)
    res = copy.copy(tar)
    # res[1] += 1
    print(res)
    if res[1] == 10:
        print("Worker %d send HIT msg"%(comm.rank))
        res[2] = 1
        comm.send(res,dest=0,tag=tags.HIT)
    else:
        print("Worker %d send FLY msg"%(comm.rank))
        comm.send(res,dest=0,tag=tags.FLY)

def recv_running():
    while True:
        msg = comm.recv(source=MPI.ANY_SOURCE,tag=MPI.ANY_TAG,status=status)
        tag = status.Get_tag()
        if tag == tags.EXEC:
            print("Worker %d  wiil calculator"%(comm.rank))
            calculator(msg)
        elif tag == tags.EXIT:
            print("Worker %d  wiil shutdown"%(comm.rank))
            comm.send(None,dest=0,tag=tags.EXIT)
            break
        
    print ("Process %d Done"%(rank))     
    

if __name__ == '__main__':
    try:
        comm = MPI.Comm.Get_parent()
        rank = comm.Get_rank()
        print ("Process Connect Success %d"% (rank))
    except:
        raise ValueError('Could not connect to parent - ')
    
    if rank != 0:
        recv_t = threading.Thread(target=recv_running)
        recv_t.start()
        recv_t.join()
        comm.Disconnect()


