import re
import os
import sys
import zipfile
import datetime

class SMDRReader(object):
    def __init__(self, data_directory, start_date, end_date):
        try:
            os.listdir(data_directory)
            self.data_directory = data_directory
        except FileNotFoundError:
            raise InvalidInputException('The specified path does not exist')
        except PermissionError:
            raise InvalidInputException('You do not have permission to access'
                                        ' this directory')

        try:
            self.start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        except:
            raise InvalidInputException('Invalid start date')

        try:
            self.end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
        except:
            raise InvalidInputException('Invalid end date')


    def get_node_dirs(self, data_dir):
        node_dirs = []
        for dir in os.listdir(data_dir):
            if re.match('Node_\d\d+', dir, re.I) is not None:
                node_dirs.append(os.path.join(data_dir, dir))
        return node_dirs


    def get_file_by_date(self, path, date):
            basename = None
            datestring = date.strftime('%Y%m%d')
            filelist = os.listdir(path)
            for file in filelist:
                if re.match('s' + datestring + '\.(zip|txt)',
                            file, re.I) is not None:
                    basename = file
                    break
            if basename is None:
                return None

            filename,fileextension = os.path.splitext(basename)
            filefullpath = os.path.join(path, basename)

            if fileextension.lower() == '.zip':
                myzip = zipfile.ZipFile(filefullpath)
                with myzip.open('{}.txt'.format(filename)) as smdr_file:
                    file_lines = smdr_file.readlines()
                myzip.close()
            elif fileextension.lower() == '.txt':
                with open(filefullpath, 'rb') as smdr_file:
                    file_lines = smdr_file.readlines()
            else:
                return None
            return file_lines


    def file_reader(self):
        self.current_date = self.start_date
        while self.current_date <= self.end_date:
            file = self.get_file_by_date(self.data_directory, self.current_date)
            if file is not None:
                yield file
            else:
                yield []
            self.current_date += datetime.timedelta(days=1)


    def date_reader(self):
        self.current_date = self.start_date
        node_dirs = self.get_node_dirs(self.data_directory)
        file_dict = dict((os.path.basename(dir), None) for dir in node_dirs)
        while self.current_date <= self.end_date:
            for dir in node_dirs:
                file = self.get_file_by_date(dir, self.current_date)
                file_dict[os.path.basename(dir)] = file
            yield file_dict
            self.current_date += datetime.timedelta(days=1)


    def change_directory(self, new_directory):
        if os.path.isdir(new_directory):
            self.data_directory = new_directory
            return True
        else:
            return False


class InvalidInputException(Exception):
    def __init__(self, value, severity=0):
        self.value = value
        self.severity = severity

    def __str__(self):
        return str(self.value)

    def __repr__(self):
        return self.__str__()


class SMDREvent(object):
    def __init__(self, smdr_string):
        if type(smdr_string) != str:
            raise InvalidInputException('Input is not a string', severity=1)
        if smdr_string[0] not in (' ', '-', '%', '+'):
            raise InvalidInputException('Input string is not an SMDR event')
        smdr_string = smdr_string.rstrip('\r\n')
        self.smdr_string = smdr_string
        event_length = len(smdr_string)

        if event_length == 207:
            self.validate_207(smdr_string)
            self.init_207(smdr_string)
        elif event_length == 204:
            self.validate_204(smdr_string)
            self.init_204(smdr_string)
        elif event_length == 113:
            self.validate_113(smdr_String)
            self.init_113(smdr_string)
        else:
            raise InvalidInputException('Unknown SMDR event encountered', severity=1)


    def __str__(self):
        return self.smdr_string


    def __repr__(self):
        return self.__str__()


    def raise_validation_exception(self, length, smdr_string):
        raise InvalidInputException('SMDR event with length '
                                    '{} does not match expected format: {}'
                                    .format(length, smdr_string), severity=1)


    def init_non_event(self):
        self.length_flag = ''
        self.date = ''
        self.time = ''
        self.duration = ''
        self.calling_party = ''
        self.time_to_answer = ''
        self.dialed_digits = ''
        self.completion_flag = ''
        self.speed_call_flag = ''
        self.called_party = ''
        self.trans_conf_flag = ''
        self.third_party = ''
        self.system_id = ''
        self.ani = ''
        self.dnis = ''
        self.call_id = ''
        self.sequence_id = ''
        self.associated_id = ''


    def validate_207(self, smdr_string):
        try:
            datetime.datetime.strptime(smdr_string[7:15], '%H:%M:%S')
        except ValueError:
            self.raise_validation_exception(207, smdr_string)
        for char in (smdr_string[6],
                     smdr_string[15],
                     smdr_string[27],
                     smdr_string[40],
                     smdr_string[85],
                     smdr_string[110]):
            if char != ' ':
                self.raise_validation_exception(207, smdr_string)


    def init_207(self, smdr_string):
        self.length_flag = smdr_string[0]
        self.date = smdr_string[1:6]
        self.time = smdr_string[7:15]
        self.duration = smdr_string[17:27]
        self.calling_party = smdr_string[28:35].strip()
        self.time_to_answer = smdr_string[36:40].strip()
        self.dialed_digits = smdr_string[41:67].strip()
        self.completion_flag = smdr_string[67].strip()
        self.speed_call_flag = smdr_string[68].strip()
        self.called_party = smdr_string[69:76].strip()
        self.trans_conf_flag = smdr_string[84].strip()
        self.third_party = smdr_string[86:92].strip()
        self.system_id = smdr_string[107:110]
        self.ani = smdr_string[113:123].strip()
        self.dnis = smdr_string[134:144].strip()
        self.call_id = smdr_string[153:161]
        self.sequence_id = smdr_string[162]
        self.associated_id = smdr_string[164:172].strip()


    def validate_204(self, smdr_string):
        try:
            datetime.datetime.strptime(smdr_string[7:12], '%H:%M')
        except ValueError:
            self.raise_validation_exception(204, smdr_string)
        for char in (smdr_string[6],
                     smdr_string[12],
                     smdr_string[24],
                     smdr_string[37],
                     smdr_string[82],
                     smdr_string[107]):
            if char != ' ':
                self.raise_validation_exception(204, smdr_string)


    def init_204(self, smdr_string):
        self.length_flag = smdr_string[0]
        self.date = smdr_string[1:6]
        self.time = smdr_string[7:12] + ':00'
        self.duration = smdr_string[14:24]
        self.calling_party = smdr_string[25:32].strip()
        self.time_to_answer = smdr_string[33:37].strip()
        self.dialed_digits = smdr_string[38:64].strip()
        self.completion_flag = smdr_string[64].strip()
        self.speed_call_flag = smdr_string[65].strip()
        self.called_party = smdr_string[66:73].strip()
        self.trans_conf_flag = smdr_string[81].strip()
        self.third_party = smdr_string[83:90].strip()
        self.system_id = smdr_string[104:107]
        self.ani = smdr_string[110:120].strip()
        self.dnis = smdr_string[131:141].strip()
        self.call_id = smdr_string[150:158]
        self.sequence_id = smdr_string[159]
        self.associated_id = smdr_string[161:169].strip()


    def validate_113(self, smdr_string):
        try:
            datetime.datetime.strptime(smdr_string[7:13] + 'M', '%I:%M%p')
        except ValueError:
            self.raise_validation_exception(113, smdr_string)
        for char in (smdr_string[6],
                     smdr_string[13],
                     smdr_string[32],
                     smdr_string[66],
                     smdr_string[101]):
            if char != ' ':
                self.raise_validation_exception(113, smdr_string)


    def init_113(self, smdr_string):
        self.length_flag = smdr_string[0]
        self.date = smdr_string[1:6]
        time = datetime.datetime.strptime(smdr_string[7:13] + 'M', '%I:%M%p')
        self.time = time.strftime('%H:%M:%S')
        self.duration = smdr_string[14:22]
        self.calling_party = smdr_string[23:28].strip()
        self.time_to_answer = smdr_string[29:32].strip()
        self.dialed_digits = smdr_string[33:59].strip()
        self.completion_flag = smdr_string[59].strip()
        self.speed_call_flag = smdr_string[60].strip()
        self.called_party = smdr_string[61:65].strip()
        self.trans_conf_flag = smdr_string[65].strip()
        self.third_party = smdr_string[67:71].strip()
        self.system_id = ''
        self.ani = smdr_string[91:101].strip()
        self.dnis = smdr_string[102:112].strip()
        self.call_id = ''
        self.sequence_id = ''
        self.associated_id = ''

