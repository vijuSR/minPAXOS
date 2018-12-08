from multiprocessing import Pool, Pipe, Process, Value, Array, Manager
import time
import logging
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), os.pardir))
import src

logger = logging.getLogger('paxos.node_id_{}'.format(os.getpid()))


class Node(object):

    def __init__(self, _id):
        self.majority = manager.Value('i', None)
        self.id = manager.Value('i', _id)
        self.nodes = manager.dict()
        self.ops = ('+', 2)
        self.data_store = {0: {0: 0}}

        self.paxos_proposed_id = manager.Value('i', None)
        self.paxos_promised_id = manager.Value('i', -1)
        self.paxos_proposed_value = manager.Value('i', None)
        self.paxos_majority = manager.Value('i', None)
        self.paxos_accepted_value = manager.Value('i', None)
        self.paxos_accepted_id = manager.Value('i', -1)
        self.num_nodes = manager.Value('i', None)

    def set_majority(self, nodes):
        for k, v in nodes.items():
            self.nodes[k] = v
        self.majority.value = int((len(self.nodes)) / 2) + 1    # int((len(self.nodes) + 1) / 2) + 1
        self.num_nodes.value = len(self.nodes)    # + 1

    def generate_next_paxos_id(self, upto=1):
        for i in range(upto):
            if self.paxos_proposed_id.value is None:
                self.paxos_proposed_id.value = self.id.value
            else:
                self.paxos_proposed_id.value = \
                self.paxos_proposed_id.value + self.num_nodes.value

    def prepare(self, proposed_id):
        if proposed_id.value <= self.paxos_promised_id.value:
            return None
        else:
            self.paxos_promised_id.value = proposed_id.value

            if self.paxos_accepted_id.value is not None and \
                            self.paxos_accepted_id.value != -1:
                return self.paxos_promised_id.value, \
                       self.paxos_accepted_id.value, \
                       self.paxos_accepted_value.value
            else:
                return self.paxos_promised_id.value

    # call the thread with a timeout
    def send_prepares(self):
        responses = []
        self.generate_next_paxos_id()
        # self.paxos_promised_id.value = self.paxos_proposed_id.value
        for _id, node in self.nodes.items():
            logger.debug('node {}: sending PREPARE(id {}) to node id {}'.format(
                self.id.value, self.paxos_proposed_id.value, node.id.value
            ))
            responses.append(node.prepare(self.paxos_proposed_id))

        logger.debug('''node {}: checking for majority for proposed-id {}, responses {}'''.format(
            self.id.value, self.paxos_proposed_id.value, responses
        ))
        if self.check_majority(responses):
            logger.debug('''node {}: majority for proposed-id {} achieved, sending accept-requests'''.format(
                self.id.value, self.paxos_proposed_id.value
            ))
            self.send_accept_requests(responses)
        else:
            self.send_prepares()

    def accept_request(self, paxos_proposed_id, paxos_proposed_value):
        if paxos_proposed_id.value < self.paxos_promised_id.value:
            return None
        else:
            self.paxos_accepted_id.value = paxos_proposed_id.value
            self.paxos_accepted_value.value = paxos_proposed_value.value

            # self.send_to_learners() ??

            return self.paxos_accepted_id.value, self.paxos_accepted_value.value

    def send_accept_requests(self, responses):
        accept_request_responses = []

        temp_id = -1
        for response in responses:
            if type(response) is tuple:
                if response[1] > temp_id:
                    self.paxos_proposed_value.value = response[2]
                    temp_id = response[1]

        if not self.paxos_proposed_value.value:
            self.paxos_proposed_value.value = sorted(self.data_store.keys())[-1] + 1

        for _id, node in self.nodes.items():
            logger.debug('node {}: sending ACCEPT-REQUEST(id {}, value {}) to node id {}'.format(
                self.id.value, 
                self.paxos_proposed_id.value, 
                self.paxos_proposed_value.value, 
                node.id.value
            ))
            accept_request_responses.append(
                node.accept_request(self.paxos_proposed_id,
                                    self.paxos_proposed_value)
            )

        logger.debug('''node {}: checking for majority for proposed-id {}, accept-request-responses {}'''.format(
            self.id.value, self.paxos_proposed_id.value, accept_request_responses
        ))

        if self.check_majority(accept_request_responses):
            # logger consensus reached
            logger.info('NODE {} , PROCESS-ID {}: CONSENSUS IS REACHED ON VALUE: {}'.format(
                self.id.value, os.getpid(), self.paxos_accepted_value.value
            ))

            self.data_store[self.paxos_accepted_value.value] = {
                0: self.data_store[sorted(self.data_store.keys())[-1]][0] + 2
            }

            logger.info('node-id {}: current state of data-store: {}'.format(self.id.value, self.data_store))
        else:
            self.send_prepares()

    def send_to_learners(self):
        pass

    def check_majority(self, responses):

        response_count = len(responses) - responses.count(None)

        if response_count >= self.majority.value:
            return True
        else:
            return False


if __name__ == '__main__':

    num_paxos_nodes = 3
    with Manager() as manager:

        node_map = {}

        processes = []

        for i in range(1, num_paxos_nodes + 1):
            node_map[i] = Node(_id=i)

        for i in range(1, num_paxos_nodes + 1):
            node_map[i].set_majority({j: node for j, node in node_map.items()})

        for i in range(1, num_paxos_nodes + 1):
            processes.append(Process(target=node_map[i].send_prepares, args=()))
            processes[-1].start()

        for i in range(num_paxos_nodes):
            processes[i].join()
