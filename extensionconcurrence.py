import sys
import datetime
import collections
import smdrreader
from os import path


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
        try:
            event = smdrreader.SMDREvent(line.decode('UTF-8-SIG'))
        except smdrreader.InvalidInputException as e:
            print(str(e), file=sys.stderr)
            print(line)
            continue
        extension = event.called_party
        if extension in extension_data:
            event_start_time = datetime.datetime.strptime(event.time, '%H:%M:%S')
            event_duration = duration_to_seconds(event.duration)
            i = 0
            while i < event_duration:
                event_current_time = event_start_time + datetime.timedelta(seconds=i)
                event_timedate_string = '{} {}'.format(event.date,
                        event_current_time.strftime('%H:%M:%S'))
                if extension_data[extension][event_timedate_string] == 1:
                    print('Duplicate call events detected for extension' +
                          '{} on {} at {}'.format(extension,event.date,event.time))
                extension_data[extension][event_timedate_string] = 1
                i += 1


def combine_extensions(extension_data):
    output_lines = []
    combined_data = collections.defaultdict(int)
    for extension in extension_data:
        for datetime in extension_data[extension]:
            combined_data[datetime] += 1
    return combined_data


def split_args(args):
    split_args = []
    for arg in args:
        words = arg.split(' ')
        for word in words:
            if '-' in word:
                range_parts = word.split('-')
                split_args.extend(range(range_parts[0], range_parts[1]))
            else:
                split_args.append(word)
    return split_args


def print_results_with_zeroes(data, number_of_extensions):
    sorted_data = sorted(data)
    current_datetime = datetime.datetime.strptime(sorted_data[0], '%m/%d %H:%M:%S')
    end_datetime = datetime.datetime.strptime(sorted_data[-1], '%m/%d %H:%M:%S')
    all_in_use_events = 0
    high_use_events = 0

    while current_datetime <= end_datetime:
        time = current_datetime.strftime('%m/%d %H:%M:%S')
        count = data[time]
        if count == number_of_extensions:
            all_in_use_events += 1
            high_use_events += 1
        elif count > number_of_extensions * 3 / 4:
            high_use_events += 1
        print('{}\t{}'.format(time, count))
        current_datetime += datetime.timedelta(seconds=1)
    print('{} total seconds of high usage:'.format(high_use_events), file=sys.stderr)
    print('{} total seconds of all in use:'.format(all_in_use_events), file=sys.stderr)


def print_results(data, number_of_extensions):
    sorted_data = sorted(data.items(), key=lambda item: item[0])
    all_in_use_events = 0
    high_use_events = 0

    for time,count in sorted_data:
        if count == number_of_extensions:
            all_in_use_events += 1
            high_use_events += 1
        elif count > number_of_extensions * 3 / 4:
            high_use_events += 1
        print('{}\t{}'.format(time, count))
    print('{} total seconds of high usage:'.format(high_use_events), file=sys.stderr)
    print('{} total seconds of all in use:'.format(all_in_use_events), file=sys.stderr)


def summarize(smdr_reader, extensions, zero_mode):
    extension_data = dict((extension, collections.defaultdict(int)) for extension in extensions)

    for file in smdr_reader.file_reader():
        parse_lines(file, extension_data)
    combined_data = combine_extensions(extension_data)
    if zero_mode is True:
        print_results_with_zeroes(combined_data, len(extensions))
    else:
        print_results(combined_data, len(extensions))



zero_mode = False
while '-0' in sys.argv:
    zero_mode = True
    sys.argv.remove('-0')

start_date = sys.argv[1]
end_date = sys.argv[2]
data_directory = sys.argv[3]

try:
    smdr_reader = smdrreader.SMDRReader(data_directory, start_date, end_date)
except smdrreader.InvalidInputException as e:
    print('Error: ' + str(e), file=sys.stderr)
    exit(1)

extensions = split_args(sys.argv[4:])

for extension in extensions:
    if not extension.isnumeric():
        raise smdrreader.InvalidInputException('Invalid extension: "{}"'.format(extension))

summarize(smdr_reader, extensions, zero_mode)
