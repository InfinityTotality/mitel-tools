import re
import sys
import operator
import smdrreader
from time import sleep
from collections import defaultdict


def read_all_data(data_dirs, start_date, end_date):
    all_data = []
    for dir in data_dirs:
        reader = smdrreader.SMDRReader(dir, start_date, end_date)
        file_gen = reader.file_reader()
        for file in file_gen:
            for line in file:
                line = line.decode(sys.stdout.encoding)
                try:
                    event = smdrreader.SMDREvent(line)
                    all_data.append(event)
                except smdrreader.InvalidInputException as e:
                    if e.severity > 0:
                        print(e)
    return all_data


def merge_ids(id_one, id_two, events_by_id):
    if events_by_id[id_one] is not events_by_id[id_two]:
        existing_events = events_by_id[id_one]
        events_by_id[id_one] = events_by_id[id_two]
        events_by_id[id_two].extend(existing_events) 


def associate_id(call_id, associated_id, events_by_id):
    if call_id in events_by_id:
        merge_ids(call_id, associated_id, events_by_id)
    else:
        events_by_id[call_id] = events_by_id[associated_id]


def group_calls_by_id(call_ids, all_data, events_by_id):
    if len(call_ids) == 0:
        return
    assoc_ids = set()
    my_data = all_data[:]
    for event in all_data:
        if event.call_id in call_ids:
            events_by_id[event.call_id].append(event)
            my_data.remove(event)
            if event.associated_id != '':
                associate_id(event.associated_id, event.call_id, events_by_id)
                if event.associated_id not in call_ids:
                    assoc_ids.add(event.associated_id)
        elif event.associated_id in call_ids:
            associate_id(event.call_id, event.associated_id, events_by_id)
            events_by_id[event.call_id].append(event)
            my_data.remove(event)
            if event.call_id not in call_ids:
                assoc_ids.add(event.call_id)
    if len(assoc_ids) > 0:
        group_calls_by_id(assoc_ids, my_data, events_by_id)


def group_all_calls(all_data, events_by_id):
    for event in all_data:
        events_by_id[event.call_id].append(event)
        if event.associated_id != '':
            associate_id(event.associated_id, event.call_id, events_by_id)


def print_unique_calls(events_by_id):
    printed_ids = set()
    for events in events_by_id.values():
        if id(events) not in printed_ids:
            printed_ids.add(id(events))
            print('\n'.join([str(event) for event in events]))
            print()


def sort_calls(events_by_id):
    for event_list in events_by_id.values():
        event_list.sort(key=operator.attrgetter('sequence_id'))
        event_list.sort(key=operator.attrgetter('call_id'))
        event_list.sort(key=operator.attrgetter('time'))


call_ids = set()
data_dir_args = []

while '-c' in sys.argv:
    argindex = sys.argv.index('-c')
    parameter = sys.argv[argindex + 1]
    if re.match('[A-Z][0-9]{7}', parameter) is not None:
        call_ids.add(parameter)
    else:
        raise smdrreader.InvalidInputException('Invalid call id: ' + parameter)
    sys.argv.remove('-c')
    sys.argv.remove(parameter)

start_date = sys.argv[1]
end_date = sys.argv[2]
data_dir_args.extend(sys.argv[3:])

events_by_id = defaultdict(list)
all_data = read_all_data(data_dir_args, start_date, end_date)

if len(call_ids) > 0:
    group_calls_by_id(call_ids, all_data, events_by_id)
else:
    group_all_calls(all_data, events_by_id)
sort_calls(events_by_id)
print_unique_calls(events_by_id)
