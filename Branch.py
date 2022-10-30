import grpc
import bank_pb2
import bank_pb2_grpc
import time
from utilities import configure_logger, get_operation_name, get_result_name, get_source_type_name
from concurrent import futures

logger = configure_logger("Branch")

class Branch(bank_pb2_grpc.BankServicer):

    def __init__(self, id, balance, branches, bind_addresses):
        # unique ID of the Branch
        self.id = id
        # replica of the Branch's balance
        self.balance = balance
        # the list of process IDs of the branches
        self.branches = branches
        # the list of Client stubs to communicate with the branches
        self.stubList = list()
        # a list of received messages used for debugging purpose
        self.recvMsg = list()
        # iterate the processID of the branches
        self.branches_bind_addresses = bind_addresses

    # TODO: students are expected to process requests from both Client and Branch
    def MsgDelivery(self, request, context):
        self.recvMsg.append(request)
        logger.info(
            "Branch {} has received {} request from {} {}".format(
                self.id, 
                get_operation_name(request.operation_type), 
                get_source_type_name(request.source_type), 
                request.id
            )
        )
        new_balance = 0
        op_result = bank_pb2.Result.failure
        #if request is a query, sleep 3 seconds and make sure all of propagate completed
        if request.operation_type == bank_pb2.Operation.query:
            time.sleep(3)
            new_balance = self.balance
            op_result = bank_pb2.Result.success
        #if request from customer, run propagate
        elif request.operation_type == bank_pb2.Operation.withdraw and request.source_type == bank_pb2.Source.customer:
            op_result, new_balance = self.WithDraw(request.amount)
            if op_result == bank_pb2.Result.success:
                propagate_result = self.Branch_Propagate(request.operation_type, request.amount)
                if not all(propagate_result):
                    op_result = bank_pb2.Result.error
        #if request from customer, run propagate
        elif request.operation_type == bank_pb2.Operation.deposit and request.source_type == bank_pb2.Source.customer:
            op_result, new_balance = self.Deposit(request.amount)
            if op_result == bank_pb2.Result.success:
                propagate_result = self.Branch_Propagate(request.operation_type, request.amount)
                if not all(propagate_result):
                    op_result = bank_pb2.Result.error
        #if request from branch, no propagate
        elif request.operation_type == bank_pb2.Operation.deposit and request.source_type == bank_pb2.Source.branch:
            op_result, new_balance = self.Deposit(request.amount)
        #if request from branch, no propagate
        elif request.operation_type == bank_pb2.Operation.withdraw and request.source_type == bank_pb2.Source.branch:
            op_result, new_balance = self.WithDraw(request.amount)
        #construct response
        response = bank_pb2.MsgDelivery_response(
            operation_result = op_result,
            id = self.id,
            amount = new_balance,
            source_type = bank_pb2.Source.branch,
        )
        logger.info(
            "Branch id {}, After received {} id {} {}, Current balance is: {}".format(
                self.id, 
                get_source_type_name(request.source_type), 
                request.id, 
                get_operation_name(request.operation_type), 
                self.balance
            )
        )
        return response

    def Deposit(self, amount):
        if amount < 0:
            return bank_pb2.Result.error, amount
        self.balance += amount
        return bank_pb2.Result.success, self.balance

    def WithDraw(self, amount):
        if amount > self.balance:
            return bank_pb2.Result.failure, amount
        self.balance -= amount
        return bank_pb2.Result.success, self.balance
    
    def Create_propagate_request(self, operation_type, amount):
        """
        Build the branch propagate request context
        """
        logger.info("Creating propagate request....")
        request = bank_pb2.MsgDelivery_request(
            operation_type = operation_type,
            id = self.id,
            amount = amount,
            source_type = bank_pb2.Source.branch,
        )
        return request

    def Create_branches_stub(self):
        """
        Create branches stub
        """
        logger.info("Creating branches stub....")
        for branch in self.branches:
            if branch != self.id:
                bind_address = self.branches_bind_addresses[branch]
                stub = bank_pb2_grpc.BankStub(grpc.insecure_channel(bind_address))
                self.stubList.append(stub)

    def Branch_Propagate(self, operation_type, amount):
        """
        Run branches propagate
        If all of branches propagate return success, return list of branches propagate result(True/False)
        """
        result = []
        if len(self.stubList) == 0:
            self.Create_branches_stub()
        for stub in self.stubList:
            propagate_request = self.Create_propagate_request(operation_type, amount)
            response = stub.MsgDelivery(propagate_request)
            logger.info("Propagate {} response from branch {}".format(
                get_result_name(response.operation_result), response.id
                )
            )
            result.append(response.operation_result)
        return [True if check == bank_pb2.Result.success else False for check in result]


def branch_service(branch_id, balance, branches, bind_addresses):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1,),)
    bank_pb2_grpc.add_BankServicer_to_server(
        Branch(id=branch_id, balance=balance, branches=branches, bind_addresses=bind_addresses), 
        server
    )
    server.add_insecure_port(bind_addresses[branch_id])
    server.start()
    logger.info("Branch {} started with balance {} and is litsening on TCP port {}".format(
        branch_id, balance, 
        bind_addresses[branch_id].split(":")[-1]
        )
    )
    server.wait_for_termination()
