from mpi4py import MPI
import time
import threading
import common
import copy

# target = ["No.", position, [car_id,missile_id],meet_flag, timestamp]

tags = common.tags
status = MPI.Status()

# worker or car send to eachother
def Send(argvs,des,tag):
    comm.send([argvs,des],dest=0,tag=tag)


def calculator(tar):
    # print ("Worker Rank :%d"%comm.rank,tar)
    res_ls = list()
    launch_flag = int()
    missile_pos = int()
    # launch_car calculater every frame
    for car in car_list:
        Send(tar,car,tags.CAR_EXEC)
        [tmp_res,sour] = comm.recv(source=0,tag=tags.CAR_RES,status=status)
        res_ls.append([car,tmp_res])

    # missile launch require
    if tar[2] == [-1,-1]:
        # max policy
        launch_res = max(res_ls,key= lambda x:x[1])
        car_id = launch_res[0]

        Send(tar,car_id,tags.LAUNCH_REQUIRE)
        [[launch_flag,missile_id],sour] = comm.recv(source=0,tag=tags.LAUNCH_INFO,status=status)

        if launch_flag == 1:
            tar[2] = [car_id,missile_id]
            Send(tar,tar[2][0],tags.UPDATE_MISSILE)
            [pos,sour] = comm.recv(source=0,tag=tags.MISSILE_POS)
            missile_pos = pos

    # missile has launched
    # need to update missile position
    else:
        Send(tar,tar[2][0],tags.UPDATE_MISSILE)
        [pos,sour] = comm.recv(source=0,tag=tags.MISSILE_POS)
        missile_pos = pos

    # judge if target and missile meet
    # print("Worker %d ,tar id :%s,pos :%d, missile pos :%d"%(comm.rank,tar[0],tar[1],missile_pos))
    if tar[2] != [-1,-1] and abs(tar[1] - missile_pos) <= 10:
        # print("Worker %d send HIT msg"%(comm.rank))
        tar[3] = 1
        comm.send(tar,dest=0,tag=tags.HIT)
    else:
        # print("Worker %d send FLY msg"%(comm.rank))
        comm.send(tar,dest=0,tag=tags.FLY)
    return

def recv_running():
    while True:
        msg = comm.recv(source=0,tag=MPI.ANY_TAG,status=status)
        tag = status.Get_tag()
        source = status.Get_source()
        # print("worker %d recv msg"%(source))
        if tag == tags.EXEC:
            # print("Worker %d  wiil calculator"%(source))
            calculator(msg)
        
        elif tag == tags.EXIT:
            # print("Worker %d  wiil shutdown"%(comm.rank))
            comm.send(None,dest=0,tag=tags.EXIT)
            break
        
    print ("Worker Process %d Done"%(rank))     
    
def car_exec(argvs):
    res = 0.0
    # car has not missile
    if sum([1 for x in missile_list if x[0] == -1]) == 0:
        return -1.0

    tar_pos = argvs[1]
    if tar_pos <= 70 and tar_pos > 40:
        res = 0.3
    elif tar_pos <= 40 and tar_pos > 20:
        res = 0.5
    elif tar_pos <= 20 and tar_pos > 10:
        res = 0.8
    elif tar_pos <= 10:
        res = 1.0
    return res

def car_launch(argvs):
    tar_id = argvs[0]
    flag = 0
    missile_id = int()

    # find the missile not launched
    for i,missile in enumerate(missile_list):
        # find out
        if missile[0] == -1:
            missile_id = i
            flag = 1
            break

    if flag:
        missile_list[missile_id][0] = tar_id
        undate_missilePos(missile_id)

    return [flag,missile_id]

def undate_missilePos(missile_id):
    missile_list[missile_id][1] += 5
    return

def car_recv_running():
    while True:
        # print("car %d process recv msg"%(comm.rank))
        msg = comm.recv(source=0,tag=MPI.ANY_TAG,status=status)
        tag = status.Get_tag()
        source = status.Get_source()
        if tag == tags.CAR_EXEC:
            # print("recv msg  CAR_EXEC from %d"%(source))
            res = car_exec(msg[0])
            Send(res,msg[1],tags.CAR_RES)
            # comm.send(res,dest=source,tag=tags.CAR_RES)
        
        elif tag == tags.LAUNCH_REQUIRE:
            launch_info = car_launch(msg[0])
            Send(launch_info,msg[1],tag=tags.LAUNCH_INFO)
            # comm.send(launch_info,dest=source,tag=tags.LAUNCH_INFO)

        elif tag == tags.UPDATE_MISSILE:
            dst = msg[1]
            msg = msg[0]
            missile_id = msg[2][1]
            missile_pos = missile_list[missile_id][1]
            undate_missilePos(missile_id)
            Send(missile_pos,dst,tags.MISSILE_POS)
            # comm.send(missile_pos,dest=source,tag=tags.MISSILE_POS)

        elif tag == tags.MISSILE_REPLACE:
            if msg[0] == comm.rank:
                missile_list[msg[1]] = [-1,-1]

        elif tag == tags.EXIT:
            comm.send(None,dest=0,tag=tags.EXIT)
            break
        
    print ("Car Process %d Done"%(rank))     

if __name__ == '__main__':
    try:
        comm = MPI.Comm.Get_parent()
        rank = comm.Get_rank()
        size = comm.Get_size()
        car_num = 4
        
        # comm.bcast(car_num,root=0) 
        car_list = [x+1 for x in range(car_num)]
        worker_list = [x for x in range(size) if x != 0 and x not in car_list]

    except:
        raise ValueError('Could not connect to parent - ')
    # worker process
    if rank in worker_list:
        # print ("Worker Process Connect Success %d"% (rank))
        recv_t = threading.Thread(target=recv_running)
        recv_t.start()
        recv_t.join()
        comm.Disconnect()
    
    # launch car process
    elif rank in car_list:
        # [[target_id,pos],[],[]]
        missile_list = [[-1,0] for x in range(4)]
        # print ("Car Process Connect Success %d"% (rank))
        car_recv = threading.Thread(target=car_recv_running)
        car_recv.start()
        car_recv.join()
        comm.Disconnect()



