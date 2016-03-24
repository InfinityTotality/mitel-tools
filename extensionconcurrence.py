import sys
import time
import collections
import smdrreader
from os import path


def debug_print(message, file=sys.stderr):
    if debug_mode is True:
        print(message, file=file)


def duration_to_seconds(duration):
    duration_parts = duration.split(':')
    hours = int(duration_parts[0])
    minutes = int(duration_parts[1])
    seconds = int(duration_parts[2])
    seconds += minutes * 60
    seconds += hours * 60 * 60
    return seconds


def parse_lines(smdr_lines, extension_data):
    for line in smdr_lines:
        line = line.decode('UTF-8-SIG')
        try:
            event = smdrreader.SMDREvent(line)
        except smdrreader.InvalidInputException as e:
            if e.severity > 0:
                print(str(e) + ': ' + line.strip(), file=sys.stderr)
            else:
                debug_print(str(e) + ': ' + line.strip())
            continue
        extension = None
        if event.called_party in extension_data:
            extension = event.called_party
        elif outbound_mode is True and event.calling_party in extension_data:
            extension = event.calling_party
        if extension is not None:
            try:
                event_start_time = time.strptime('{} {}'.format(event.date,
                                                 event.time), '%m/%d %H:%M:%S')
                event_duration = duration_to_seconds(event.duration)
            except Exception as e:
                debug_print('Error processing event time or duration: {}'
                            .format(str(e)))
            current_timestamp = time.mktime(event_start_time)
            end_timestamp = current_timestamp + event_duration
            while current_timestamp < end_timestamp:
                if extension_data[extension][current_timestamp] == True:
                    print('Duplicate call events detected for extension ' +
                          '{} on {} at {}'.format(
                          extension,event.date,event.time), file=sys.stderr)
                extension_data[extension][current_timestamp] = True
                current_timestamp += 1


def combine_extensions(extension_data):
    combined_data = collections.defaultdict(int)
    for extension in extension_data:
        for timestamp in extension_data[extension]:
            combined_data[timestamp] += 1
    return combined_data


def split_args(args):
    split_args = []
    for arg in args:
        words = arg.split(' ')
        for word in words:
            if '-' in word:
                range_parts = word.split('-')
                int_list = range(int(range_parts[0]), int(range_parts[1]) + 1)
                split_args.extend([str(int) for int in int_list])
            else:
                split_args.append(word)
    args_dict = {arg: None for arg in split_args}
    return args_dict


def print_results_with_zeroes(data, number_of_extensions):
    sorted_data = sorted(data)
    current_timestamp = sorted_data[0]
    end_timestamp = sorted_data[-1]
    all_in_use_events = 0
    high_use_events = 0

    while current_timestamp <= end_timestamp:
        time_string = time.strftime('%m/%d %H:%M:%S', time.localtime(current_timestamp))
        count = data[current_timestamp]
        if count == number_of_extensions:
            all_in_use_events += 1
            high_use_events += 1
        elif count >= number_of_extensions * 3 / 4 or\
                number_of_extensions - count == 1:
            high_use_events += 1
        print('{}\t{}'.format(time_string, count))
        current_timestamp += 1
    print('{} total seconds of high usage'.format(high_use_events), file=sys.stderr)
    print('{} total seconds of all in use'.format(all_in_use_events), file=sys.stderr)


def print_results(data, number_of_extensions):
    sorted_data = sorted(data)
    all_in_use_events = 0
    high_use_events = 0

    for timestamp in sorted_data:
        time_string = time.strftime('%m/%d %H:%M:%S', time.localtime(timestamp))
        count = data[timestamp]
        if count == number_of_extensions:
            all_in_use_events += 1
            high_use_events += 1
        elif count >= number_of_extensions * 3 / 4 or\
                number_of_extensions - count == 1:
            high_use_events += 1
        print('{}\t{}'.format(time_string, count))
    print('{} total seconds of high usage'.format(high_use_events), file=sys.stderr)
    print('{} total seconds of all in use'.format(all_in_use_events), file=sys.stderr)


def summarize(smdr_reader, extensions, zero_mode):
    extension_data = dict((extension, collections.defaultdict(int)) for extension in extensions)

    for file in smdr_reader.file_reader():
        parse_lines(file, extension_data)
    combined_data = combine_extensions(extension_data)
    if zero_mode is True:
        print_results_with_zeroes(combined_data, len(extensions))
    else:
        print_results(combined_data, len(extensions))


outbound_mode = False
while '-i' in sys.argv:
    outbound_mode = True
    sys.argv.remove('-i')

zero_mode = False
while '-0' in sys.argv:
    zero_mode = True
    sys.argv.remove('-0')

debug_mode = False
while '-v' in sys.argv:
    debug_mode = True
    sys.argv.remove('-v')

start_date = sys.argv[1]
end_date = sys.argv[2]
data_directory = sys.argv[3]

try:
    smdr_reader = smdrreader.SMDRReader(data_directory, start_date, end_date)
except smdrreader.InvalidInputException as e:
    print('Error: ' + str(e), file=sys.stderr)
    sys.exit(1)

extensions = split_args(sys.argv[4:])

for extension in extensions:
    if not extension.isnumeric():
        raise smdrreader.InvalidInputException('Invalid extension: "{}"'.format(extension))

summarize(smdr_reader, extensions, zero_mode)

