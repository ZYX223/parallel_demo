from mpi4py import MPI
import sys
import threading
import common

tags = common.tags
status = MPI.Status()

class MyPool():
    def __init__(self,max_worker,car_num,target_file):
        self.max_worker = max_worker
        self.car_num = car_num
        self.worker_num = self.max_worker - self.car_num -1
        self.closed_worker = 0
        
        self.comm = MPI.COMM_SELF.Spawn(
                sys.executable,
                args=[target_file],
                maxprocs=self.max_worker)
        print("master node :%d"%self.comm.rank)
        self.common_comm = self.comm.Merge()
        print("Merge master node :%d"%self.common_comm.rank)
        # self.comm.bcast(self.car_num,root=0)
        # content master node and launch car node [0,1,2,3,4]
        # worker node list [5,6,7,8,...,max_worker]
        self.Car_list = [x+1 for x in range(self.car_num)]
        self.Process_queue = [x + self.car_num +1 for x in range(self.worker_num)]
        self.Task_queue = []
        self.result = []

        self.recv_t = threading.Thread(target=self.recv_running)
        self.send_t = threading.Thread(target=self.send_running)
        self.recv_t.start()
        self.send_t.start()

    def Send(self,argvs,des,tag):
        self.comm.send([argvs,des],dest=0,tag=tag)

    def recv_running(self):
        while self.closed_worker < self.worker_num + self.car_num:
            msg = self.common_comm.recv(source=MPI.ANY_SOURCE,tag=MPI.ANY_TAG,status=status)
            tag = status.Get_tag()
            source = status.Get_source()
            # print ("Recv msg from %d, msg is :%d"%(source,tag))
            if tag == tags.FLY:
                self.Process_queue.append(source)
                self.result.append(msg)
            
            elif tag == tags.HIT:
                self.Process_queue.append(source)
                self.result.append(msg)
                self.common_comm.send(msg[2],dest=msg[2][0],tag=tags.MISSILE_REPLACE)

            elif tag == tags.EXIT:
                self.closed_worker += 1
        print("running thread is shutdown")

    def submit_task(self,argvs):
        if type(argvs[0]) != list:
            self.Task_queue.append(argvs)
        else:
            self.Task_queue.extend(argvs)

    def send_running(self):
        while self.closed_worker < self.worker_num + self.car_num:
            if len(self.Task_queue):
                for task in self.Task_queue:
                    self.exec(task)
                self.Task_queue.clear()
                
        print("exec running is shutdown")
    
    def exec(self,argvs):
        while len(self.Process_queue) == 0:
            continue
        pro_id = self.Process_queue[0]
        self.Process_queue = self.Process_queue[1:]
        self.common_comm.send(argvs,dest=pro_id,tag=tags.EXEC)

    def create(self,argvs):
        self.result.clear()
        if type(argvs[0]) != list:
            self.Task_queue.append(argvs)
        else:
            self.Task_queue.extend(argvs)
        while len(self.Process_queue) != self.worker_num or len(self.result) != len(argvs):
            continue
        return self.result

    def exit(self):
        while len(self.Process_queue)  != self.worker_num or len(self.Task_queue) != 0:
            continue

        for process in self.Process_queue:
            self.common_comm.send(None,dest=process,tag=tags.EXIT)
        for car in self.Car_list:
            self.common_comm.send(None,dest=car,tag=tags.EXIT)
        self.recv_t.join()
        self.send_t.join()
        self.comm.Disconnect()
        return self.result

if __name__ == '__main__':
    pass