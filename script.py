from optparse import OptionParser
import gzip
import json
import bz2
import os
from values import values, tags
from pathlib import Path
import re

parser = OptionParser()
parser.add_option("-r", "--region", dest="reg",
                  help="enter region", metavar="PATH")
parser.add_option("-f", "--flow", dest="fl",
                  help="enter flow", metavar="PATH")
parser.add_option("-p", "--path", dest="zip",
                  help="zip file path", metavar="PATH")
parser.add_option("-o", "--output", dest="json",
                  help="json file path", metavar="OUTPUT")
parser.add_option("-q", "--quiet",
                  action="store_false", dest="verbose", default=True,
                  help="don't print status messages to stdout")

(options, args) = parser.parse_args()

res = options.json

try:
    region, system = options.reg, options.fl
except TypeError:
    print('Param -p is empty')
    exit()
except ValueError:
    # print('Not enouth params')
    # exit()
    region = system = ''

try:
    os.mkdir(options.json)
except FileExistsError:
    pass
except TypeError:
    print('Param -o is empty')
    exit()

names = dict(zip(tags, values))


def read_file(directory):
    try:
        files = os.listdir(directory)
    except FileNotFoundError:
        files = []
        print('"{}" directory Not Found (search)'.format(directory))
        exit()
    if not files:
        yield directory
    for each_file in files:
        suf = ''.join(Path(each_file).suffixes)
        if suf == '.gz':
            open_func = gzip.open
        elif suf == '.bz2':
            open_func = bz2.open
        else:
            try:
                os.mkdir(res)
            except FileExistsError:
                pass
            open_func = open
        try:
            file_data = open_func('{}/{}'.format(directory, each_file), 'rt', encoding='utf-8', errors='ignore')
        except FileNotFoundError:
            file_data = []
            print('File Not Found')
            exit()
        fix_row = file_data.read()
        if isinstance(fix_row, bytes):
            fix_row = fix_row.decode('utf-8')
        file_data.close()
        yield fix_row, '{}/{}'.format(res, each_file.split('.')[0])


def write_key_value(data):
    data = list(data)
    for each_data in data:
        if each_data[0]:
            if isinstance(each_data, str):
                print('"{}" directory is empty'.format(data[0]))
                break
            try:
                d = each_data[0].strip().split('\n')
                for line, message in enumerate(d, start=1):
                    fix_split = message.strip().replace('\x02', '').split('\x01')
                    dict_data = dict(map(lambda items:
                                         (names.get(items.split('=')[0], items.split('=')[0]),
                                          items.split('=')[1] if '=' in items else ''),
                                         filter(None, fix_split)))

                    new_dict_data = dict()
                    new_dict_data['region'] = region
                    new_dict_data['flow'] = system
                    # new_dict_data['order_id'] = dict_data.get('OrderID', '')
                    # new_dict_data['order_version'] = dict_data.get('10240', '')
                    # new_dict_data['trd_date'] = dict_data.get('TradeDate', '')
                    new_dict_data['message'] = ''.join(fix_split)
                    # new_list_data = [
                    #     {'region': new_dict_data['region']},
                    #     {'system': new_dict_data['system']},
                    #     {'order_id': new_dict_data['order_id']},
                    #     {'order_version': new_dict_data['order_version']},
                    #     {'trd_date': new_dict_data['trd_date']},
                    #     {'message': new_dict_data['message']}
                    # ]
                    new_dict_data.update(dict_data)

                    filename = '%s%s' % (each_data[1], line) if len(d) > 1 else each_data[1]
                    with open('%s.json' % filename, 'w') as json_data:
                        json.dump(
                            # new_list_data,
                            new_dict_data,
                            json_data
                        )
            except FileNotFoundError:
                print('File Not Found (record)')


write_key_value(read_file('{r}/{s}/{p}'.format(r=options.reg, s=options.fl, p=options.zip)))
