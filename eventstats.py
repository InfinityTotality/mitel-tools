import re
import os
import sys
import smdrreader
from collections import defaultdict


def debug_print(message, file=sys.stderr):
    if debug_mode is True:
        print(message, file=file)


def get_node_dirs(data_dir):
    node_dirs = []
    if not os.path.isdir(data_dir):
        raise smdrreader.InvalidInputException('{} is not a valid directory'
                .format(data_dir), severity=2)
    for dir in os.listdir(data_dir):
        if re.match('Node_\d\d', dir, re.I) is not None:
            node_dirs.append(os.path.join(data_dir, dir))
    return node_dirs


def read_all_data(data_dirs, start_date, end_date):
    all_data = []
    for dir in data_dirs:
        try:
            reader = smdrreader.SMDRReader(dir, start_date, end_date)
        except smdrreader.InvalidInputException as e:
            print('Error while creating SMDR reader for directory {}:'
                  .format(dir))
            print(e, file=sys.stderr)
            continue
        for file in reader.file_reader():
            for line in file:
                line = line.decode(sys.stdout.encoding)
                try:
                    event = smdrreader.SMDREvent(line)
                    all_data.append(event)
                except smdrreader.InvalidInputException as e:
                    if e.severity > 0:
                        print(e)
                    else:
                        debug_print(e)
    return all_data


def get_events_by_filter(all_data, filter_condition):
    found_events = []
    for event in all_data:
        try:
            result = eval(filter_condition) 
        except:
            debug_print('Failure evaluating filter condition "{}"'
                        .format(filter_condition))
            break
        if result is True:
            debug_print('Found event {} matching filter'
                        .format(event.call_id))
            found_events.append(event)
    return found_events


def print_events(events):
    for event in events:
        print(event)
    print('\n{} total events processed in event mode'.format(len(events)),
          file=sys.stderr)


def print_agent_stats(events):
    agents = defaultdict(int)
    for event in events:
        agents[event.called_party] += 1
    for agent,count in agents.items():
        print('{}\t{}'.format(agent, count))
    print('\n{} total events processed in agent mode'.format(len(events)),
          file=sys.stderr)


def print_group_stats(events):
    groups = defaultdict(int)
    for event in events:
        match = re.match('[^ ]+ [0-9]{3} ([0-9]{3})+', event.dialed_digits)
        if not match:
            debug_print('Event does not appear to be a path call:'
                        '\n{}'.format(event))
        else:
            groups[event.dialed_digits[-3:]] += 1
    for group,count in groups.items():
        print('{}\t{}'.format(group, count))
    print('\n{} total events processed in group mode'.format(len(events)),
          file=sys.stderr)


filter_conditions = set()
debug_mode = False
mode = None

while '-a' in sys.argv:
    mode = 'agent'
    sys.argv.remove('-a')

while '-g' in sys.argv:
    mode = 'group'
    sys.argv.remove('-g')

while '-e' in sys.argv:
    mode = 'event'
    sys.argv.remove('-e')

if mode is None:
    print('Please select a mode using one of -a -e -g')
    exit()

while '-v' in sys.argv:
    debug_mode = True
    sys.argv.remove('-v')
    
while '-f' in sys.argv:
    argindex = sys.argv.index('-f')
    parameter = sys.argv[argindex + 1]
    filter_conditions.add(parameter)
    sys.argv.remove('-f')
    sys.argv.remove(parameter)

start_date = sys.argv[1]
end_date = sys.argv[2]
data_dir = sys.argv[3]

try:
    data_dirs = get_node_dirs(data_dir)
except smdrreader.InvalidInputException as e:
    print(e)
    exit()
except PermissionError as e:
    print('You do not have permission to access {}'.format(data_dir))
    exit()

all_data = read_all_data(data_dirs, start_date, end_date)

filtered_events = []

for condition in filter_conditions:
    filtered_events.extend(get_events_by_filter(all_data, condition))

if mode == 'agent':
    print_agent_stats(filtered_events)
elif mode == 'group':
    print_group_stats(filtered_events)
elif mode == 'event':
    print_events(filtered_events)
else:
    print('Please select a mode using one of -a -e -g')
