import argparse
import multiprocessing
import pdb
import time

from Branch import branch_service
from Customer import execute_customer_request
from utilities import configure_logger, get_operation_name, get_result_name, get_source_type_name, get_system_free_tcp_port, get_json_data

logger = configure_logger("main")

# argparse refer https://docs.python.org/3/howto/argparse.html
def get_args():
    parser = argparse.ArgumentParser(description="Input and output files")
    parser.add_argument("-i", "--input", required=False, type=str, default="input.json", help="Input file name wiht json format")
    parser.add_argument("-o", "--output", required=False, type=str, default="output.json",help="Output file name wiht json format")
    args = parser.parse_args()
    return args.input.strip(), args.output.strip()

def create_branch_input_data_collection(input_file):
    branches_data = []
    customers_events_data = []
    input_json_data = get_json_data(input_file)
    for data in input_json_data:
        if data["type"] == "branch":
            branch = {}
            branch["id"] = data["id"]
            branch["balance"] = data["balance"]
            port = get_system_free_tcp_port()
            bind_address = "[::]:{}".format(port)
            branch["bind_address"] = bind_address
            branches_data.append(branch)
        if data["type"] == "customer":
            customers_events_data.append(data)
    return branches_data, customers_events_data

def main():
    input_file, output_file = get_args()
    branches_data, customers_data = create_branch_input_data_collection(input_file)
    list_of_branches_id = [branch["id"] for branch in branches_data]
    branches_bind_addresses = {branch["id"]:branch["bind_address"] for branch in branches_data}
    workers = []
    for branch in branches_data:
        branch_worker = multiprocessing.Process(
            name="Branch-{}".format(branch["id"]),
            target=branch_service,
            args=(branch["id"], branch["balance"], list_of_branches_id, branches_bind_addresses),
        )
        branch_worker.start()
        workers.append(branch_worker)
    #Add delay to let branch to be ready for serve customer
    time.sleep(1)
    for customer in customers_data:
        customer_id = customer["id"]
        logger.info("Customer {} starting to execute events".format(customer_id))
        execute_customer_request(customer_id, branches_bind_addresses[customer_id], customer["events"], output_file)

    #Add delay before terminate the branch processes
    time.sleep(3)
    for worker in workers:
        worker.terminate()

if __name__ == "__main__":
    main()
