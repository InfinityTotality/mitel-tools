import zipfile, datetime
from os import path

class ACDReader(object):
    def __init__(self, data_directory, start_date, end_date):
        if path.isdir(data_directory):
            self.data_directory = data_directory
        else:
            raise InvalidInputException('Invalid path for ACD files')

        try:
            self.start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        except:
            raise InvalidInputException('Invalid start date')

        try:
            self.end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
        except:
            raise InvalidInputException('Invalid end date')

        self.current_date = self.start_date


    def file_reader(self):
        while self.current_date <= self.end_date:
            print('Analyzing file for {}'.format(self.current_date.strftime('%Y-%m-%d')))

            basename = 'a{}'.format(self.current_date.strftime('%Y%m%d'))
            filename = path.join(self.data_directory, basename)

            if path.isfile('{}.zip'.format(filename)):
                myzip = zipfile.ZipFile('{}.zip'.format(filename))
                with myzip.open('{}.txt'.format(basename)) as acd_file:
                    myzip.close()
                    yield acd_file.readlines()
            elif path.isfile('{}.txt'.format(filename)):
                with open('{}.txt'.format(filename), 'rb') as acd_file:
                    yield acd_file.readlines()
            else:
                print('failed to locate file {}'.format(filename))
                print('No records for {}'.format(self.current_date.strftime('%Y-%m-%d')))

            self.current_date += datetime.timedelta(days=1)


class InvalidInputException(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)
