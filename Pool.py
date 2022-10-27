from mpi4py import MPI
import sys
import threading
import common

tags = common.tags
status = MPI.Status()

class MyPool():
    def __init__(self,max_worker,target_file):
        self.max_worker = max_worker
        self.closed_worker = 0
        
        self.comm = MPI.COMM_SELF.Spawn(
                sys.executable,
                args=[target_file],
                maxprocs=self.max_worker)

        self.Process_queue = [x for x in range(self.max_worker) if x != 0]
        self.Task_queue = []
        self.result = []

        self.recv_t = threading.Thread(target=self.recv_running)
        self.send_t = threading.Thread(target=self.send_running)
        self.recv_t.start()
        self.send_t.start()

    def recv_running(self):
        while self.closed_worker < self.max_worker-1:
            msg = self.comm.recv(source=MPI.ANY_SOURCE,tag=MPI.ANY_TAG,status=status)
            tag = status.Get_tag()
            source = status.Get_source()
            print ("Recv msg from %d, msg is :%d"%(source,tag))
            if tag == tags.FLY:
                self.Task_queue.append(msg)
                self.Process_queue.append(source)

            elif tag == tags.HIT:
                self.Process_queue.append(source)
                self.result.append(msg)

            elif tag == tags.EXIT:
                self.closed_worker += 1
        print("running thread is shutdown")

    def submit_task(self,agrvs):
        if type(agrvs[0]) != list:
            self.Task_queue.append(agrvs)
        else:
            self.Task_queue.extend(agrvs)

    def send_running(self):
        while self.closed_worker < self.max_worker-1:
            if len(self.Task_queue):
                for task in self.Task_queue:
                    self.exec(task)
                self.Task_queue.clear()
                
        print("exec running is shutdown")
    
    def exec(self,argvs):
        while len(self.Process_queue) == 0:
            # print("waiting........")
            continue
        pro_id = self.Process_queue[0]
        self.Process_queue = self.Process_queue[1:]
        self.comm.send(argvs,dest=pro_id,tag=tags.EXEC)

    def exit(self):
        while len(self.Process_queue) +1 != self.max_worker or len(self.Task_queue) != 0:
            continue
        # print(len(self.Process_queue))
        # print(len(self.Task_queue))
        for process in self.Process_queue:
            self.comm.send(None,dest=process,tag=tags.EXIT)
        self.recv_t.join()
        self.send_t.join()
        self.comm.Disconnect()
        return self.result

if __name__ == '__main__':
    pass