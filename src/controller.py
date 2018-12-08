from multiprocessing import Process, Manager
import os
import sys
import logging

sys.path.append(os.path.join(os.path.dirname(__file__), os.pardir))
import src
from src.paxos_node import Node


def run_paxos(num_paxos_nodes=3):

    with Manager() as manager:

        node_map = {}

        processes = []

        for i in range(1, num_paxos_nodes + 1):
            node_map[i] = Node(_id=i, manager=manager)

        for i in range(1, num_paxos_nodes + 1):
            node_map[i].set_majority({j: node for j, node in node_map.items()})

        for i in range(1, num_paxos_nodes + 1):
            processes.append(Process(target=node_map[i].send_prepares, args=()))
            processes[-1].start()

        for i in range(num_paxos_nodes):
            processes[i].join()


if __name__ == '__main__':
    run_paxos()
