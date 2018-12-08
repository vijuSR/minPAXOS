from multiprocessing import Process, Manager
import os
import sys
import logging
import argparse

sys.path.append(os.path.join(os.path.dirname(__file__), os.pardir))
import src
from src.paxos_node import Node

logger = logging.getLogger('paxos.controller')


def run_paxos(num_paxos_nodes):
    """
    * runs a single paxos-run
    * consensus should be achieved but there may
      be a few cases of <//contention//>

    :param num_paxos_nodes: integer value,
    controls the number of process nodes to create
    """

    with Manager() as manager:

        node_map = {}

        processes = []

        # create the required number of nodes
        for i in range(1, num_paxos_nodes + 1):
            node_map[i] = Node(_id=i, manager=manager)

        # pass all the nodes to every-other node for their
        # communication via 'manager' proxy objects
        for i in range(1, num_paxos_nodes + 1):
            node_map[i].set_majority({j: node for j, node in node_map.items()})

        # create a single process for every single existing node
        # and start them
        for i in range(1, num_paxos_nodes + 1):
            processes.append(Process(target=node_map[i].send_prepares, args=()))
            processes[-1].start()
            logger.info('node {} with process-id {} started ...'.format(node_map[i].id.value, processes[-1].pid))

        # wait for every processes to finish
        for i in range(num_paxos_nodes):
            processes[i].join()


if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-npn',
        '--num_of_paxos_nodes',
        type=int,
        default=3,
        help='number of paxos-nodes to create, DEFAULT[3]'
    )

    args = parser.parse_args()

    run_paxos(args.num_of_paxos_nodes)
