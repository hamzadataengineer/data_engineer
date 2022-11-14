import ipaddress
import re
from pytz import timezone
import pytz
import re
import time
import json
import random
import string
import math
import socket
import requests
import os
from urllib.parse import urlparse


def hasAllElements(arr1, arr2):
    n = len(arr1)
    m = len(arr2)

    # If lengths of array are not
    # equal means array are not equal
    if (n != m):
        return False

    # Sort both arrays
    arr1.sort()
    arr2.sort()

    # Linearly compare elements
    for i in range(0, n - 1):
        if (arr1[i] != arr2[i]):
            return False

    # If all elements were same.
    return True


def listToString(s):
    # initialize an empty string
    str1 = " "

    # return string
    return (str1.join(s))


def currentUTC():
    raw_date = datetime.datetime.now(pytz.timezone('US/Pacific'))
    created_at = raw_date.strftime('%Y-%m-%d %H:%M:%S')
    return created_at


def getCurrentUTCTime():
    raw_date = datetime.datetime.now(pytz.timezone('US/Pacific'))
    created_at = raw_date.strftime('%Y-%m-%d %H:%M:%S')
    return created_at


def KB(x):
    K = x * 1000
    return K


def MB(x):
    M = x * 1000 * 1000
    return M


def GB(x):
    G = MB(x) * 1000
    return G


def TB(x):
    T = GB(x) * 1000
    return T


def PB(x):
    P = TB(x) * 1000
    return P


def toBytes(human_size):
    if len(human_size) > 1:
        unit = human_size[-1]
        raw = str(human_size[:-1])
        raw = raw.strip()

        number = float(raw)
        if unit == "P":
            return PB(number)
        elif unit == "T":
            return TB(number)
        elif unit == "G":
            return GB(number)
        elif unit == "M":
            return MB(number)
        elif unit == "K":
            return KB(number)
    else:
        return human_size


def toHumanNumber(n):
    millnames = ['', ' Thousand', ' Million', ' Billion', ' Trillion']
    n = float(n)
    millidx = max(0, min(len(millnames) - 1,
                         int(math.floor(0 if n == 0 else math.log10(abs(n)) / 3))))

    return '{:.0f}{}'.format(n / 10 ** (3 * millidx), millnames[millidx])


def convertSizeToBytes(size_str):
    multipliers = {
        'kilobyte':  1024,
        'megabyte':  1024 ** 2,
        'gigabyte':  1024 ** 3,
        'terabyte':  1024 ** 4,
        'petabyte':  1024 ** 5,
        'exabyte':   1024 ** 6,
        'zetabyte':  1024 ** 7,
        'yottabyte': 1024 ** 8,
        'kb': 1024,
        'mb': 1024**2,
        'gb': 1024**3,
        'tb': 1024**4,
        'pb': 1024**5,
        'eb': 1024**6,
        'zb': 1024**7,
        'yb': 1024**8,
    }

    for suffix in multipliers:
        size_str = size_str.lower().strip().strip('s')
        if size_str.lower().endswith(suffix):
            return int(float(size_str[0:-len(suffix)]) * multipliers[suffix])
    else:
        if size_str.endswith('b'):
            size_str = size_str[0:-1]
        elif size_str.endswith('byte'):
            size_str = size_str[0:-4]
    return int(size_str)

# Old code to src valid domain

# def isValidDomain(domain_name):
#     domain_regex = r'(([\da-zA-Z])([_\w-]{,62})\.){,127}(([\da-zA-Z])[_\w-]{,61})?([\da-zA-Z]\.((xn\-\-[a-zA-Z\d]+)|([a-zA-Z\d]{2,})))'
#
#     # Python
#     domain_regex = '{0}$'.format(domain_regex)
#     valid_domain_name_regex = re.compile(domain_regex, re.IGNORECASE)
#     domain_name = domain_name.lower().strip()
#     if re.match(valid_domain_name_regex, domain_name):
#         return True
#     else:
#         return False

def isValidDomain(domain_name):
    # Regex to src valid
    # domain name.
    regex = "^((?!-)[A-Za-z0-9-]" + "{1,63}(?<!-)\\.)" + "+[A-Za-z]{2,6}"

    # Compile the ReGex
    p = re.compile(regex)

    # If the string is empty
    # return false
    if (str == None):
        return False

    # Return if the string
    # matched the ReGex
    if (re.search(p, domain_name)):
        return True
    else:
        return False

def generateRandomString(length):
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str


def getKeys(results):
    keys = list()
    if results is not None:
        for key in results:
            keys.append(key)
        return keys
    return None


def readDataFromFile(file_name):
    with open(file_name) as json_file:
        data = json.load(json_file)
    return data


def writeDataToFile(file_name, data, searlizer=None):
    try:
        with open(file_name, 'a+') as outfile:
            # print(f"Writing date to {file_name}")
            # print("data:", data)
            if searlizer is None:
                json.dump(data, outfile)
            else:
                json.dump(data, outfile, default=searlizer)
            outfile.write('\n\n\n')
    except IOError:
        with open(file_name, 'w') as outfile:
            json.dump(data, outfile)
            outfile.write('\n\n\n')
    # print(f"Wrote data to {file_name}")


def overwriteDataToNewFile(file_name, data):
    try:
        if os.path.exists(file_name):
            os.remove(file_name)
            print(f"Removed older {file_name}")
        with open(file_name, 'w') as outfile:
            json.dump(data, outfile)
    except IOError:
        with open(file_name, 'w') as outfile:
            json.dump(data, outfile)
    print(f"Wrote {file_name}")


def readLicenseKey(file_name):
    f = open(file_name, "rb")
    return f.read()


def writeLicenseKey(file_name, data):
    try:
        f = open(file_name, "wb")
        f.write(data)
        f.close()
    except IOError:
        print("Writing File Failed")
    print(f"Wrote {file_name}")


start_time = None


def startDebug():
    global start_time
    start_time = time.time()


def endDebug():
    global start_time
    print("Took %s seconds" % (time.time() - start_time))


def convertTimestampToSQLDateTime(value):
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(value))


def convertSQLDateTimeToTimestamp(value):
    return time.mktime(time.strptime(value, '%Y-%m-%d %H:%M:%S'))


def extractIPAddress(raw_string):
    ip = re.sub('[^0123456789\.]', '', raw_string)
    return ip


def getIPFromCidrs(ip_cidrs):
    return [str(ip) for ip in ipaddress.IPv4Network(ip_cidrs)]


def convertToUTC(date_str, format='%Y-%m-%d %H:%M:%S'):
    local = pytz.timezone("Asia/Karachi")
    naive = datetime.datetime.strptime(str(date_str).split(".")[0], format)
    local_dt = local.localize(naive, is_dst=None)
    utc_dt = local_dt.astimezone(pytz.utc)
    utd = datetime.datetime.strftime(utc_dt, '%Y-%m-%d %H:%M:%S')
    return (utd, utc_dt)




def hasString(string_to_find, arr):
    return any(string_to_find in s for s in arr)


def covertDTtoUTC(dt, format='%Y-%m-%d %H:%M:%S'):
    local = pytz.timezone("Asia/Karachi")
    local_dt = local.localize(dt, is_dst=None)
    utc_dt = local_dt.astimezone(pytz.utc)
    utd = datetime.datetime.strftime(utc_dt, format)
    return (utd, utc_dt)


def convertToUTCMySQL(date_str, format='%Y-%m-%dT%H:%M:%S'):
    local = pytz.timezone("Asia/Karachi")
    naive = datetime.datetime.strptime(str(date_str).split(".")[0], format)
    local_dt = local.localize(naive, is_dst=None)
    utc_dt = local_dt.astimezone(pytz.utc)
    utd = datetime.datetime.strftime(utc_dt, '%Y-%m-%d %H:%M:%S')
    return (utd, utc_dt)


def convertTimeStampToLocal(date_time_value):
    from datetime import datetime
    local_tz = timezone('Asia/Karachi')
    local_dt = datetime.strptime(str(date_time_value).split(".")[0], '%Y-%m-%dT%H:%M:%S').replace(
        tzinfo=pytz.utc).astimezone(local_tz)

    # return local_dt.strftime('%Y-%m-%d %H:%M:%S %Z%z')
    return local_dt.strftime('%Y-%m-%d %H:%M:%S')


def convertTimeStampToLocalTime(date_time_value):
    from datetime import datetime
    local_tz = timezone('Asia/Karachi')
    local_dt = datetime.strptime(str(date_time_value).split(".")[0], '%Y-%m-%dT%H:%M:%S').replace(
        tzinfo=pytz.utc).astimezone(local_tz)

    # return local_dt.strftime('%Y-%m-%d %H:%M:%S %Z%z')
    return local_dt.strftime('%H:%M:%S')


def convertTimeStampToLocalDate(date_time_value):
    from datetime import datetime
    local_tz = timezone('Asia/Karachi')
    local_dt = datetime.strptime(str(date_time_value).split(".")[0], '%Y-%m-%dT%H:%M:%S').replace(
        tzinfo=pytz.utc).astimezone(local_tz)

    # return local_dt.strftime('%Y-%m-%d %H:%M:%S %Z%z')
    return local_dt.strftime('%Y-%m-%d')


def convertToMySQLFormat(date_time_obj):
    date_time_obj = datetime.datetime.strptime(str(date_time_obj).split(".")[0], '%Y-%m-%dT%H:%M:%S')
    return date_time_obj.strftime('%Y-%m-%d %H:%M:%S')


def convertTimeStampToLCMySQLFormat(date_time_value):
    from datetime import datetime
    local_tz = timezone('Asia/Karachi')
    local_dt = datetime.strptime(str(date_time_value).split(".")[0], '%Y-%m-%dT%H:%M:%S').replace(
        tzinfo=pytz.utc).astimezone(local_tz)

    return local_dt.strftime('%Y-%m-%d %H:%M:%S')

def humanizeDateTime(date_time_value):
    from datetime import datetime
    local_tz = timezone('Asia/Karachi')
    local_dt = datetime.strptime(str(date_time_value).split(".")[0], '%Y-%m-%dT%H:%M:%S').replace(
        tzinfo=pytz.utc).astimezone(local_tz)

    return local_dt.strftime('%d-%b-%Y, %I:%M:%S %p')

def humanizeDateTimeDefault(date_time_value):
    from datetime import datetime
    print("ORIGNAL Date")
    print(date_time_value)
    local_dt = datetime.strptime(str(date_time_value).split(".")[0], '%Y-%m-%dT%H:%M:%S')
    print("DATE Testings:")
    print(local_dt)
    return local_dt.strftime('%d-%b-%Y, %I:%M:%S %p')


def convertTimeStampToLocalDateObject(date_time_value, format='%Y-%m-%dT%H:%M:%S'):
    from datetime import datetime
    local_tz = timezone('Asia/Karachi')
    local_dt = datetime.strptime(str(date_time_value).split(".")[0], format).replace(
        tzinfo=pytz.utc).astimezone(local_tz)

    # return local_dt.strftime('%Y-%m-%d %H:%M:%S %Z%z')
    return local_dt


def is_valid_macaddr802(value):
    allowed = re.compile(r"""
                         (
                             ^([0-9A-F]{2}[-]){5}([0-9A-F]{2})$
                            |^([0-9A-F]{2}[:]){5}([0-9A-F]{2})$
                         )
                         """,
                         re.VERBOSE | re.IGNORECASE)

    if allowed.match(value) is None:
        return False
    else:
        return True


def findIPAddress(text):
    return re.findall(r'[0-9]+(?:\.[0-9]+){3}', text)


def findEmail(text):
    emails = re.findall(r"[a-z0-9\.\-+_]+@[a-z0-9\.\-+_]+\.[a-z]+", text)
    return emails


def findUrl(text):
    URL_REGEX = r"""(?i)\b((?:https?:(?:/{1,3}|[a-z0-9%])|[a-z0-9.\-]+[.](?:com|net|org|edu|gov|mil|aero|asia|biz|cat|coop|info|int|jobs|mobi|museum|name|post|pro|tel|travel|xxx|ac|ad|ae|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cs|cu|cv|cx|cy|cz|dd|de|dj|dk|dm|do|dz|ec|ee|eg|eh|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mm|mn|mo|mp|mq|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|nz|om|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw|py|qa|re|ro|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|Ja|sk|sl|sm|sn|so|sr|ss|st|su|sv|sx|sy|sz|tc|td|tf|tg|th|tj|tk|tl|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|yu|za|zm|zw)/)(?:[^\s()<>{}\[\]]+|\([^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\))+(?:\([^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\)|[^\s`!()\[\]{};:'".,<>?«»“”‘’])|(?:(?<!@)[a-z0-9]+(?:[.\-][a-z0-9]+)*[.](?:com|net|org|edu|gov|mil|aero|asia|biz|cat|coop|info|int|jobs|mobi|museum|name|post|pro|tel|travel|xxx|ac|ad|ae|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cs|cu|cv|cx|cy|cz|dd|de|dj|dk|dm|do|dz|ec|ee|eg|eh|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mm|mn|mo|mp|mq|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|nz|om|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw|py|qa|re|ro|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|Ja|sk|sl|sm|sn|so|sr|ss|st|su|sv|sx|sy|sz|tc|td|tf|tg|th|tj|tk|tl|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|yu|za|zm|zw)\b/?(?!@)))"""
    urls = re.findall(URL_REGEX, text)
    return urls


def removeTextInsideBrackets(text, brackets="()[]"):
    count = [0] * (len(brackets) // 2)  # count open/close brackets
    saved_chars = []
    for character in text:
        for i, b in enumerate(brackets):
            if character == b:  # found bracket
                kind, is_close = divmod(i, 2)
                count[kind] += (-1) ** is_close  # `+1`: open, `-1`: close
                if count[kind] < 0:  # unbalanced bracket
                    count[kind] = 0  # keep it
                else:  # found bracket to remove
                    break
        else:  # character is not a [balanced] bracket
            if not any(count):  # outside brackets
                saved_chars.append(character)
    return ''.join(saved_chars)


def is_valid_ip(ip):
    """Validates IP addresses.
    """
    return is_valid_ipv4(ip) or is_valid_ipv6(ip)


def is_valid_ipv4(ip):
    """Validates IPv4 addresses.
    """
    pattern = re.compile(r"""
        ^
        (?:
          # Dotted variants:
          (?:
            # Decimal 1-255 (no leading 0's)
            [3-9]\d?|2(?:5[0-5]|[0-4]?\d)?|1\d{0,2}
          |
            0x0*[0-9a-f]{1,2}  # Hexadecimal 0x0 - 0xFF (possible leading 0's)
          |
            0+[1-3]?[0-7]{0,2} # Octal 0 - 0377 (possible leading 0's)
          )
          (?:                  # Repeat 0-3 times, separated by a dot
            \.
            (?:
              [3-9]\d?|2(?:5[0-5]|[0-4]?\d)?|1\d{0,2}
            |
              0x0*[0-9a-f]{1,2}
            |
              0+[1-3]?[0-7]{0,2}
            )
          ){0,3}
        |
          0x0*[0-9a-f]{1,8}    # Hexadecimal notation, 0x0 - 0xffffffff
        |
          0+[0-3]?[0-7]{0,10}  # Octal notation, 0 - 037777777777
        |
          # Decimal notation, 1-4294967295:
          429496729[0-5]|42949672[0-8]\d|4294967[01]\d\d|429496[0-6]\d{3}|
          42949[0-5]\d{4}|4294[0-8]\d{5}|429[0-3]\d{6}|42[0-8]\d{7}|
          4[01]\d{8}|[1-3]\d{0,9}|[4-9]\d{0,8}
        )
        $
    """, re.VERBOSE | re.IGNORECASE)
    return pattern.match(ip) is not None


def is_valid_ipv6(ip):
    """Validates IPv6 addresses.
    """
    pattern = re.compile(r"""
        ^
        \s*                         # Leading whitespace
        (?!.*::.*::)                # Only a single whildcard allowed
        (?:(?!:)|:(?=:))            # Colon iff it would be part of a wildcard
        (?:                         # Repeat 6 times:
            [0-9a-f]{0,4}           #   A group of at most four hexadecimal digits
            (?:(?<=::)|(?<!::):)    #   Colon unless preceeded by wildcard
        ){6}                        #
        (?:                         # Either
            [0-9a-f]{0,4}           #   Another group
            (?:(?<=::)|(?<!::):)    #   Colon unless preceeded by wildcard
            [0-9a-f]{0,4}           #   Last group
            (?: (?<=::)             #   Colon iff preceeded by exacly one colon
             |  (?<!:)              #
             |  (?<=:) (?<!::) :    #
             )                      # OR
         |                          #   A v4 address with NO leading zeros 
            (?:25[0-4]|2[0-4]\d|1\d\d|[1-9]?\d)
            (?: \.
                (?:25[0-4]|2[0-4]\d|1\d\d|[1-9]?\d)
            ){3}
        )
        \s*                         # Trailing whitespace
        $
    """, re.VERBOSE | re.IGNORECASE | re.DOTALL)
    return pattern.match(ip) is not None


def keys_exists(element, *keys):
    '''
    Check if *keys (nested) exists in `element` (dict).
    '''
    if type(element) is not dict:
        raise AttributeError('keys_exists() expects dict as first argument.')
    if len(keys) == 0:
        raise AttributeError('keys_exists() expects at least two arguments, one given.')

    _element = element
    for key in keys:
        try:
            _element = _element[key]
        except KeyError:
            return False
    return True


import datetime


def round_time(dt=None, date_delta=datetime.timedelta(minutes=1), to='average'):
    """
    Round a datetime object to a multiple of a timedelta
    dt : datetime.datetime object, default now.
    dateDelta : timedelta object, we round to a multiple of this, default 1 minute.
    from:  http://stackoverflow.com/questions/3463930/how-to-round-the-minute-of-a-datetime-object-python
    """
    round_to = date_delta.total_seconds()
    if dt is None:
        dt = datetime.now()
    seconds = (dt - dt.min).seconds

    if seconds % round_to == 0:
        rounding = (seconds + round_to / 2) // round_to * round_to
    else:
        if to == 'up':
            # // is a floor division, not a comment on following line (like in javascript):
            rounding = (seconds + round_to) // round_to * round_to
        elif to == 'down':
            rounding = seconds // round_to * round_to
        else:
            rounding = (seconds + round_to / 2) // round_to * round_to

    return dt + datetime.timedelta(0, rounding - seconds, -dt.microsecond)


def calcEpochSec(dt):
    epochZero = datetime.datetime(1970, 1, 1, tzinfo=dt.tzinfo)
    return (dt - epochZero).total_seconds()


def convertEpochToDate(epoch_time):
    epoch_time = epoch_time / 1000
    datetime_time = datetime.datetime.fromtimestamp(epoch_time)
    return datetime_time


def convertStringToUTCDate(str_date, format):
    local = pytz.timezone("Asia/Karachi")
    naive = datetime.datetime.strptime(str(str_date), format)
    local_dt = local.localize(naive, is_dst=None)
    utc_dt = local_dt.astimezone(pytz.utc)
    return utc_dt


def unixTimeMillis(dt):
    epoch = datetime.datetime.utcfromtimestamp(0)
    return (dt - epoch).total_seconds() * 1000.0


# src data
def testTimeRound():
    print(round_time(datetime.datetime(2012, 12, 31, 23, 44, 59)))
    print(round_time(datetime.datetime(2012, 12, 31, 23, 44, 59), date_delta=datetime.timedelta(hours=1), to='down'))
    print(round_time(datetime.datetime(2012, 12, 31, 23, 44, 59), date_delta=datetime.timedelta(hours=1), to='up'))
    print(round_time(datetime.datetime(2012, 12, 31, 23, 44, 59), date_delta=datetime.timedelta(hours=1)))
    print(round_time(datetime.datetime(2012, 12, 31, 23, 00, 00), date_delta=datetime.timedelta(hours=1), to='down'))
    print(round_time(datetime.datetime(2012, 12, 31, 23, 00, 00), date_delta=datetime.timedelta(hours=1), to='up'))
    print(round_time(datetime.datetime(2012, 12, 31, 23, 00, 00), date_delta=datetime.timedelta(hours=1)))


def isStringExists(find_in, desired_str):
    if (find_in.find(desired_str) != -1):
        return True
    else:
        return False


def writeTextFile(file_path_name, content):
    myfile = file_path_name
    with open(myfile, "w") as f:
        f.seek(0)
        f.write(content)
        f.truncate()
        f.close()


# def isInternetAvailable(host="8.8.8.8", port=53, timeout=3):
#     try:
#         import httplib
#     except:
#         import http.client as httplib
#
#     conn = httplib.HTTPConnection(LICENSE_SERVER_URI, timeout=5)
#     try:
#         conn.request("HEAD", "/")
#         conn.close()
#         return True
#     except:
#         conn.close()
#         return False


# Function For Download File From Given Url

# def DownloadFile(url, file_path):
#    if url == "https://www.dan.me.uk/torlist/":
#        filename = url.split('/')
#        file_name = filename[2] + ".txt"
#    else:
#        file_name = os.path.basename(url)
#    if os.path.exists(file_path + file_name):
#        os.remove(file_path + file_name)
#        file = requests.get(url)
#        with open(file_path + file_name, "wb") as local_file:
#            local_file.write(file.content)
#            return file_name
#    else:
#        file = requests.get(url)
#        with open(file_path + file_name, "wb") as local_file:
#            local_file.write(file.content)
#            return file_name


def downloadFile(url, file_path, file_name):
    if os.path.exists(file_path + file_name):
        os.remove(file_path + file_name)
    file = requests.get(url)
    with open(file_path + file_name, "wb") as local_file:
        local_file.write(file.content)
        return "Downloaded Successfully!!"


def isURL(url):
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False
