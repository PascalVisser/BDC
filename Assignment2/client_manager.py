import queue
import time
import multiprocessing as mp
from multiprocessing.managers import BaseManager
from phredscore_calc import read_fastq_file, make_chunks, calc_quality_score



def make_client_manager(ip, port, authkey):
    """ Create a manager for a client. This manager connects to a server on the
        given address and exposes the get_job_q and get_result_q methods for
        accessing the shared queues from the server.
        Return a manager object.
    """

    class ServerQueueManager(BaseManager):
        pass

    ServerQueueManager.register('get_job_q')
    ServerQueueManager.register('get_result_q')

    manager = ServerQueueManager(address=(ip, port), authkey=authkey)
    manager.connect()

    print('Client connected to %s:%s' % (ip, port))
    return manager


def runclient(num_processes, IP, PORTNUM, AUTHKEY, POISONPILL):
    manager = make_client_manager(IP, PORTNUM, AUTHKEY)
    job_q = manager.get_job_q()
    result_q = manager.get_result_q()
    run_workers(job_q, result_q, num_processes, POISONPILL)


def run_workers(job_q, result_q, num_processes, POISONPILL):
    processes = []
    for p in range(num_processes):
        temP = mp.Process(target=peon, args=(job_q, result_q, POISONPILL))
        processes.append(temP)
        temP.start()
    print("Started %s workers!" % len(processes))
    for temP in processes:
        temP.join()


def peon(job_q, result_q, POISONPILL):
    my_name = mp.current_process().name
    while True:
        try:
            job = job_q.get_nowait()
            if job == POISONPILL:
                job_q.put(POISONPILL)
                # print("Aaaaaaargh", my_name)
                return job
            else:
                try:
                    qualities = read_fastq_file(job['arg'])
                    scores = calc_quality_score(qualities)
                    phredscores_avg = [sum(i) / len(qualities) for i in zip(scores)]
                    result = phredscores_avg
                    # print("Peon %s Workwork on %s!" % (my_name, job['arg']))
                    result_q.put({'job': job, 'result': result})
                except NameError:
                    print("Can't find yer fun Bob!")
                    result_q.put({'job': job, 'result': "DOH"})

        except queue.Empty:
            # print("sleepytime for", my_name)
            time.sleep(1)
