import bank_pb2
import socketserver
import sys
import json
import logging

def get_operation(operation):
    if operation.lower() == "query":
        return bank_pb2.Operation.query
    if operation.lower() == "withdraw":
        return bank_pb2.Operation.withdraw
    if operation.lower() == "deposit":
        return bank_pb2.Operation.deposit

def get_operation_name(operation_type):
    if operation_type == bank_pb2.Operation.query:
        return "query"
    if operation_type == bank_pb2.Operation.withdraw:
        return "withdraw"
    if operation_type == bank_pb2.Operation.deposit:
        return "deposit"

def get_result_name(result):
    if result == bank_pb2.Result.success:
        return "success"
    if result == bank_pb2.Result.failure:
        return "failure"
    if result == bank_pb2.Result.error:
        return "error"

def get_source_type_name(source_type):
    if source_type == bank_pb2.Source.customer:
        return "customer"
    if source_type == bank_pb2.Source.branch:
        return "branch"

def get_system_free_tcp_port():
    with socketserver.TCPServer(("localhost", 0), None) as server:
        available_port = server.server_address[1]
    return available_port

def configure_logger(name):
    logger = logging.getLogger(name)
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(process)d - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger

def get_json_data(json_file):
    with open(json_file, 'r') as file:
        json_data = json.load(file)
    return json_data