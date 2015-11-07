import re
import os
import sys
import smdrreader
from collections import defaultdict


def debug_print(message, file=sys.stderr):
    if debug_mode is True:
        print(message, file=file)


def data_reader(reader):
    for file_dict in reader.date_reader():
        debug_print('Retrieved file dictionary for date {}'.format(
                    reader.current_date.strftime('%Y-%m-%d'))
                    + ' from reader containing {} files'.format(
                    len(file_dict)))
        for dir,file in file_dict.items():
            if file is None:
                print('No data file found for {} in {}'.format(
                      reader.current_date.strftime('%Y-%m-%d'),dir),
                      file=sys.stderr)
                continue
            file_events = []
            debug_print('{} lines read from file for {}'.format(len(file),
                        os.path.basename(dir)))
            for line in file:
                line = line.decode('UTF-8-SIG')
                try:
                    event = smdrreader.SMDREvent(line)
                    file_events.append(event)
                except smdrreader.InvalidInputException as e:
                    if e.severity > 0:
                        print(str(e) + ': ' + line.rstrip(), file=sys.stderr)
                    else:
                        debug_print(str(e) + ': ' + line.rstrip())
            debug_print('{} events processed from file'.format(len(file_events)))
            yield file_events


def get_events_by_filter(data, filter_condition):
    debug_print('Processing filter condition "{}"'.format(filter_condition))
    found_events = []
    for event in data:
        try:
            result = eval(filter_condition)
        except:
            print('Failure evaluating filter condition "{}"'
                        .format(filter_condition), file=sys.stderr)
            break
        if result is True:
            found_events.append(event)
    debug_print('{} events found matching filter condition'.format(
                len(found_events)))
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
    smdr_reader = smdrreader.SMDRReader(data_dir, start_date, end_date)
    debug_print('SMDRReader created successfully')
except smdrreader.InvalidInputException as e:
    print('Error: ' + str(e))
    exit()

filtered_events = []

for events_list in data_reader(smdr_reader):
    for condition in filter_conditions:
            filtered_events.extend(get_events_by_filter(events_list, condition))

if mode == 'agent':
    print_agent_stats(filtered_events)
elif mode == 'group':
    print_group_stats(filtered_events)
elif mode == 'event':
    print_events(filtered_events)
else:
    print('Please select a mode using one of -a -e -g')
