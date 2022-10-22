import grpc
import bank_pb2
import bank_pb2_grpc
import time
import json

from utilities import get_operation, configure_logger, get_result_name
from concurrent import futures

logger = configure_logger("Customer")

class Customer:
    def __init__(self, id, events):
        # unique ID of the Customer
        self.id = id
        # events from the input
        self.events = events
        # a list of received messages used for debugging purpose
        self.recvMsg = list()
        # pointer for the stub
        self.stub = None

    # TODO: students are expected to create the Customer stub
    def createStub(self, branch_bind_address):
        self.stub = bank_pb2_grpc.BankStub(grpc.insecure_channel(branch_bind_address))

    # TODO: students are expected to send out the events to the Bank
    def executeEvents(self, output_file):
        result_outout = {}
        if self.id not in result_outout:
            result_outout["id"] = self.id
            result_outout["recv"] = []
        for event in self.events:
            operation = get_operation(event["interface"])
            logger.info("Customer {} sending request with operation {}, amout {}".format(self.id, event["interface"], event["money"]))
            customer_request = bank_pb2.MsgDelivery_request(
                operation_type = operation,
                id = self.id,
                amount = event["money"],
                source_type = bank_pb2.Source.customer,
            )
            response = self.stub.MsgDelivery(customer_request)
            self.recvMsg.append(response)
            event_result = {}
            event_result["interface"] = event["interface"]
            event_result["result"] = get_result_name(response.operation_result)
            event_result["money"] = response.amount
            result_outout["recv"].append(event_result)
            logger.info("Customer {} has recevied the response {}".format(self.id, event_result))
        with open(output_file, "a") as file:
            json.dump(result_outout, file)
            file.write("\n")
        


def execute_customer_request(id, branch_bind_address, events, output_file):
    cust = Customer(id, events)
    cust.createStub(branch_bind_address)
    cust.executeEvents(output_file)


if __name__ == '__main__':
    execute_customer_request(1, "[::]:32987", "deposit", 1000, "output.json")
