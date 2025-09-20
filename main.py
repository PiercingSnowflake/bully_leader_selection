import sys
import random
import multiprocessing as mp
import threading
import time
import zmq

def leader(proc_id, alive_ids, is_starter, ready_count, ready_lock):
    context = zmq.Context()

    #creating publisher and subscriber sockets and connecting them
    pub = context.socket(zmq.PUB)
    pub.bind(f"tcp://127.0.0.1:{5550 + proc_id}")
    sub = context.socket(zmq.SUB)
    
    for other in alive_ids:
        if other != proc_id:
            sub.connect(f"tcp://127.0.0.1:{5550 + other}")
    sub.setsockopt_string(zmq.SUBSCRIBE, "LEADER:")
    sub.setsockopt_string(zmq.SUBSCRIBE, "RESPONSE:")
    sub.setsockopt_string(zmq.SUBSCRIBE, "TERMINATE:")

    #barrier implementation to wait for each process to be ready
    with ready_lock:
        ready_count.value += 1
    while True:
        with ready_lock:
            if ready_count.value >= len(alive_ids):
                break
        time.sleep(0.01)

    print(f"PROCESS STARTS: {mp.current_process().pid} {proc_id} {is_starter}")
    election_start  = threading.Event()
    response_received = threading.Event()
    terminate_evt = threading.Event()

    poller = zmq.Poller()
    poller.register(sub, zmq.POLLIN)

    
   #when a message is received, this func is called and it does those:
   #if the received message is a leader message and has lower ID, then we reply as it can't be the leader
   #if we receive a response message, we ACK that we can't be the leader as it is sent by someone bullying us
   #and on termintate, we terminate
    def responder():
        print(f"RESPONDER STARTS: {proc_id}")
        while not terminate_evt.is_set():
            socks = dict(poller.poll(1000))
            if sub in socks and socks[sub] == zmq.POLLIN:
                msg = sub.recv_string()

                if msg.startswith("LEADER:"):
                    sender = int(msg.split(":", 1)[1])
                    if sender < proc_id:
                        print(f"RESPONDER RESPONDS {proc_id} {sender}")
                        pub.send_string(f"RESPONSE:{proc_id}:{sender}")
                        if not election_start.is_set() and not response_received.is_set():
                            election_start.set()

                elif msg.startswith("RESPONSE:"):
                    _, resp_from, resp_to = msg.split(":", 2)
                    resp_from, resp_to = int(resp_from), int(resp_to)
                    if resp_to == proc_id and resp_from > proc_id:
                        response_received.set()

                elif msg.startswith("TERMINATE:"):
                    terminate_evt.set()
                    break

    listener = threading.Thread(target=responder, daemon=True)
    listener.start()

    if is_starter:
        time.sleep(0.5)
        election_start.set()

    while not terminate_evt.is_set():
        if election_start.is_set():
            election_start.clear()
            response_received.clear()
            print(f"PROCESS MULTICASTS LEADER MSG: {proc_id}")
            pub.send_string(f"LEADER:{proc_id}")

            #Highest ID process is the bulliest
            if not response_received.wait(timeout=2.0):
                print(f"PROCESS BROADCASTS TERMINATE MSG: {proc_id}")
                pub.send_string(f"TERMINATE:{proc_id}")
                terminate_evt.set()
                break

        time.sleep(0.1)

    listener.join()

def main():
    if len(sys.argv) != 4:
        print("python bully.py <total_procs> <num_alive> <num_starters>")
        sys.exit(1)

    total      = int(sys.argv[1])
    num_alive  = int(sys.argv[2])
    num_starters = int(sys.argv[3])

    if not (0 < num_alive <= total) or not (0 < num_starters <= num_alive):
        print("Must be 0 < num_alive <= total_procs and 0 < num_starters <= num_alive")
        sys.exit(1)

    all_ids     = list(range(total))
    alive_ids   = random.sample(all_ids, num_alive)
    starter_ids = random.sample(alive_ids, num_starters)

    print("Alives :")
    print(f" [ {', '.join(str(i) for i in alive_ids)} ]")
    print("Starters :")
    print(f" [ {', '.join(str(i) for i in starter_ids)} ]")

    manager     = mp.Manager()
    ready_count = manager.Value('i', 0)
    ready_lock  = manager.Lock()

    processes = []
    for pid in alive_ids:
        is_starter = pid in starter_ids
        p = mp.Process(
            target=leader,
            args=(pid, alive_ids, is_starter, ready_count, ready_lock)
        )
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

if __name__ == "__main__":
    main()
