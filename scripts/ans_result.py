import json
import sys

filename = sys.argv[1]
with open(filename, 'r') as f:
    data_list = json.load(f)
trans_list = ["delivery","new_order","order_status","payment","stock_level"]
tpcc_result = {
    "success_count": {
        "delivery": 0,
        "new_order": 0,
        "order_status": 0,
        "payment": 0,
        "stock_level": 0,
    },
    "failed_count": {
        "delivery": 0,
        "new_order": 0,
        "order_status": 0,
        "payment": 0,
        "stock_level": 0
    },
    "exec_count": {
        "delivery": 0,
        "new_order": 0,
        "order_status": 0,
        "payment": 0,
        "stock_level": 0
    },
    "exec_time": {
        "delivery": 0,
        "new_order": 0,
        "order_status": 0,
        "payment": 0,
        "stock_level": 0
    },
    "tps": 0,
    "success_rate": 0
}
tpch_result = {
    "success_count": [0]*22,
    "failed_count": [0]*22,
    "exec_count": [0]*22,
    "exec_time": [0]*22,
}
for data in data_list:
    if data['type'] == 'tpcc':
        for trans in trans_list:
            tpcc_result['success_count'][trans] += data['success_count'][trans]
            tpcc_result['failed_count'][trans] += data['failed_count'][trans]
            tpcc_result['exec_count'][trans] += data['exec_count'][trans]
            tpcc_result['exec_time'][trans] += data['exec_time'][trans]
    elif data['type'] == 'tpch':
        for i in range(22):
            tpch_result['success_count'][i] += data['success_count'][i]
            tpch_result['failed_count'][i] += data['failed_count'][i]
            tpch_result['exec_count'][i] += data['exec_count'][i]
            tpch_result['exec_time'][i] += data['exec_time'][i]
total_success_count, total_failed_count, total_exec_count, total_exec_time = 0, 0, 0, 0
for trans in trans_list:
    total_success_count += tpcc_result['success_count'][trans]
    total_failed_count += tpcc_result['failed_count'][trans]
    total_exec_count += tpcc_result['exec_count'][trans]
    total_exec_time += tpcc_result['exec_time'][trans]
tpcc_result['tps'] = total_success_count / total_exec_time
tpcc_result['success_rate'] = total_success_count / total_exec_count
print("tpcc:", tpcc_result)
print("tpch:", tpch_result)