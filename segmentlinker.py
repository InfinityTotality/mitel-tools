import re
import os
import sys
import operator
import smdrreader
from collections import defaultdict


def debug_print(message, file=sys.stderr):
    if debug_mode is True:
        print(message, file=file)


def read_all_data(file_dict, date):
    all_data = []
    for dir,file in file_dict.items():
        if file is None:
            print('No data file found for {} in {}'.format(
                  date.strftime('%Y-%m-%d'),dir), file=sys.stderr)
            continue
        debug_print('{} lines read from file for {}'.format(len(file),
                    os.path.basename(dir)))
        for line in file:
            line = line.decode('UTF-8-SIG')
            try:
                event = smdrreader.SMDREvent(line)
                all_data.append(event)
            except smdrreader.InvalidInputException as e:
                if e.severity > 0:
                    print(str(e) + ': ' + line.rstrip(), file=sys.stderr)
                else:
                    debug_print(str(e) + ': ' + line.rstrip())
    debug_print('{} events processed from dict'.format(len(all_data)))
    return all_data


def add_event(call_id, event, events_by_id, ids_by_events):
    events_by_id[call_id].append(event)
    if id(events_by_id[call_id]) not in ids_by_events:
        ids_by_events[id(events_by_id[call_id])].append(call_id)


def merge_ids(id_one, id_two, events_by_id, ids_by_events):
    debug_print('Merging IDs {} and {}'.format(id_one, id_two))
    if events_by_id[id_one] is not events_by_id[id_two]:
        debug_print('Not already merged')
        events_one = events_by_id[id_one]
        events_two = events_by_id[id_two]
        events_references = []
        events_references.extend(ids_by_events.pop(id(events_one)))
        events_references.extend(ids_by_events.pop(id(events_two)))
        new_events = []
        new_events.extend(events_one)
        new_events.extend(events_two)
        for call_id in events_references:
            events_by_id[call_id] = new_events
        ids_by_events[id(new_events)] = events_references
    else:
        debug_print('Already merged')


def associate_new_id(existing_id, new_id, events_by_id, ids_by_events):
    debug_print('Creating association for new ID {} with existing ID {}'
              .format(new_id, existing_id))
    events_by_id[new_id] = events_by_id[existing_id]
    existing_events_obj_id = id(events_by_id[existing_id])
    ids_by_events[existing_events_obj_id].append(new_id)


def associate_ids(id_one, id_two, events_by_id, ids_by_events):
    if id_one in events_by_id and id_two in events_by_id:
        merge_ids(id_one, id_two, events_by_id, ids_by_events)
    elif id_one in events_by_id and id_two not in events_by_id:
        associate_new_id(id_one, id_two, events_by_id, ids_by_events)
    elif id_two in events_by_id and id_one not in events_by_id:
        associate_new_id(id_two, id_one, events_by_id, ids_by_events)
    else:
        debug_print('Warning: tried to associate two new IDs: {} and {}'
                    .format(id_one, id_two))


def group_calls_by_id(call_ids, all_data):
    events_by_id = defaultdict(list)
    ids_by_events = defaultdict(list)
    while len(call_ids) > 0:
        assoc_ids = set()
        my_data = all_data[:]
        for event in all_data:
            if event.call_id in call_ids:
                debug_print('Found call ID {} in call_ids'
                            .format(event.call_id))
                add_event(event.call_id, event, events_by_id, ids_by_events)
                my_data.remove(event)
                if event.associated_id != '':
                    associate_ids(event.associated_id, event.call_id,
                                  events_by_id, ids_by_events)
                    if event.associated_id not in call_ids:
                        assoc_ids.add(event.associated_id)
            elif event.associated_id in call_ids:
                debug_print('Found associated ID {} in call_ids'
                            .format(event.associated_id))
                associate_ids(event.call_id, event.associated_id,
                              events_by_id, ids_by_events)
                add_event(event.call_id, event, events_by_id, ids_by_events)
                my_data.remove(event)
                if event.call_id not in call_ids:
                    assoc_ids.add(event.call_id)
        call_ids = assoc_ids
        all_data = my_data
    return events_by_id


def group_all_calls(all_data):
    events_by_id = defaultdict(list)
    ids_by_events = defaultdict(list)
    for event in all_data:
        if event.call_id == '        ':
            continue
        add_event(event.call_id, event, events_by_id, ids_by_events)
        if event.associated_id != '':
            associate_ids(event.associated_id, event.call_id, events_by_id,
                          ids_by_events)
    return events_by_id


def get_unique_calls(events_by_id):
    printed_ids = set()
    unique_event_lists = []
    for list in events_by_id.values():
        if id(list) not in printed_ids:
            printed_ids.add(id(list))
            unique_event_lists.append(list)
    return unique_event_lists


def print_calls(unique_event_lists):
    unique_event_lists.sort(key=lambda x: x[0].time)
    unique_event_lists.sort(key=lambda x: x[0].date)
    for list in unique_event_lists:
            print()
            debug_print('Event list id {}:'.format(id(list)), file=sys.stdout)
            print('\n'.join([str(event) for event in list]))
    return len(unique_event_lists)


def print_anis(unique_event_lists):
    unique_event_lists.sort(key=lambda x: x[0].ani)
    for events in unique_event_lists:
        anis = set()
        for event in events:
            anis.add(event.ani)
        print('\n'.join(anis))


def sort_calls(call_event_lists):
    debug_print('Sorting calls')
    sorted_event_lists = []
    for event_list in call_event_lists:
        event_list.sort(key=operator.attrgetter('sequence_id'))
        event_list.sort(key=operator.attrgetter('call_id'))
        sorted_event_lists.append(event_list)
        continue
        call_id_lists = {}
        last_call_ids = (None, None)
        last_assoc_id = ''
        for event in event_list:
            if event.associated_id not in [last_assoc_id, '', event.call_id]:
                call_id_lists[(event.call_id, event.associated_id)] = \
                        (last_assoc_id, [event])
                last_assoc_id = event.associated_id
            call_id_lists[(event.call_id, last_assoc_id)].append(event)
        sorted_list = recombine_call_events(call_id_lists)
        if sorted_list is None:
            print('Warning: failed to sort call', file=sys.stderr)
            sorted_event_lists.append(event_list)
        elif len(sorted_list) != len(event_list):
            print('Warning: call events lost during sorting, '
                  'falling back to unsorted', file=sys.stderr)
            sorted_event_lists.append(event_list)
        else:
            sorted_event_lists.append(sorted_list)
    return sorted_event_lists


def recombine_call_events(call_id_lists):
    first_id = None
    for call_id,assoc_id in call_id_lists:
        if assoc_id == '':
            first_id = (call_id, assoc_id)
            break
    if first_id is None:
        return None
    insertions_to_make = []
    for call_id,assoc_id in call_id_lists:
        if assoc_id != '':
            insertions_to_make.append((call_id,assoc_id))
    while len(insertions_to_make) > 0:
        ids_with_insertions = [insertion[1] for insertion
                               in insertions_to_make]
        insertions = 0
        for insertion in insertions_to_make:
            if insertion[0] not in ids_with_insertions:
                insert_events(call_id_lists, insertion)
                insertions_to_make.remove(insertion)
                insertions += 1
        if insertions == 0:
            return None
    return call_id_lists[first_id]


def insert_events(call_id_lists, insertion):
    insert_id = insertion[0]
    insert_into_id = insertion[1]
    insert_list = call_id_lists[insertion]
    insert_into_list = call_id_lists[insert_into_id]
    insert_index = 0
    while insert_index < len(insert_into_list):
        if insert_list[0].time < insert_into_list[insert_index].time\
           or insert_into_list[insert_index].associated_id == insert_id:
            break
        insert_index += 1
    insert_into_list[insert_index:insert_index] = insert_list


def get_call_ids_by_filter(all_data, filter_condition):
    debug_print('Processing filter "{}"'.format(filter_condition))
    call_ids = set()
    for event in all_data:
        if event.call_id == '        ':
            continue
        try:
            result = eval(filter_condition)
        except:
            print('Failure evaluating filter condition "{}"'
                        .format(filter_condition), file=sys.stderr)
            break
        if result is True:
            debug_print('Found call id {} matching filter'
                        .format(event.call_id))
            call_ids.add(event.call_id)
            if event.associated_id != '':
                call_ids.add(event.associated_id)
    return call_ids


def get_no_id_events_by_filter(all_data, filter_condition):
    event_lists = []
    for event in all_data:
        if event.call_id == '        ':
            try:
                result = eval(filter_condition)
            except:
                print('Failure evaluating filter condition "{}"'
                            .format(filter_condition), file=sys.stderr)
                break
            if result is True:
                event_lists.append([event])
    return event_lists


def process_days(reader, filter_conditions, call_ids):
    unique_call_count = 0
    for file_dict in reader.date_reader():
        no_id_events = []
        debug_print('Retrieved file dictionary for date {}'.format(
                    reader.current_date.strftime('%Y-%m-%d'))
                    + ' from reader containing {} files'.format(
                    len(file_dict)))
        all_data = read_all_data(file_dict, reader.current_date)

        current_call_ids = call_ids
        for condition in filter_conditions:
            current_call_ids = current_call_ids.union(get_call_ids_by_filter(
                                                      all_data, condition))
            no_id_events.extend(get_no_id_events_by_filter(all_data,
                                                           condition))

        debug_print('{} call IDs selected for {}:'.format(len(current_call_ids),
                    reader.current_date.strftime('%Y-%m-%d')))
        debug_print(current_call_ids)

        if len(current_call_ids) > 0:
            events = group_calls_by_id(current_call_ids, all_data)
        elif len(filter_conditions) == 0:
            events = group_all_calls(all_data)
        else:
            events = {}
        unique_events = get_unique_calls(events)
        unique_events.extend(no_id_events)
        unique_call_count += len(unique_events)
        sorted_calls = sort_calls(unique_events)
        print_calls(sorted_calls)
    print('\n{} unique calls processed'.format(unique_call_count), file=sys.stderr)



call_ids = set()
filter_conditions = set()
debug_mode = False

while '-v' in sys.argv:
    debug_mode = True
    sys.argv.remove('-v')

while '-f' in sys.argv:
    argindex = sys.argv.index('-f')
    parameter = sys.argv[argindex + 1]
    filter_conditions.add(parameter)
    sys.argv.remove('-f')
    sys.argv.remove(parameter)

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
data_dir = sys.argv[3]

try:
    smdr_reader = smdrreader.SMDRReader(data_dir, start_date, end_date)
    debug_print('SMDRReader created successfully')
except smdrreader.InvalidInputException as e:
    print('Error: ' + str(e), file=sys.stderr)
    sys.exit(1)

process_days(smdr_reader,filter_conditions,call_ids)
