import DBconnection as DB
import query as Q
import pandas as pd
import psycopg2
from datetime import datetime



def postgresql_to_dataframe(connect,query, column_names):
    """
    Convert SELECT query into a pandas dataframe
    """
    cursor = connect.cursor()
    try:
        cursor.execute(query)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        cursor.close()
        return 1

    # get a list of tupples
    tupples = cursor.fetchall()
    # turn into a pandas dataframe
    df = pd.DataFrame(tupples, columns=column_names)
    cursor.close()
    return df

def parsebinaryflag(flag): #parsing binary flags to actual namelist
    flaglist = tuple(flag)
    namelist = ['leak', 'reverse flow', 'burst', 'extreme temp.', 'absence of flow', 'dry', 'battery low', 'tamper']
    actualflag = []
    i = 0
    for n in flaglist:
        if n == '1':
            actualflag.append(namelist[i]) #append active flag to list
        i = i + 1
    return actualflag

def getallserialnumbers():
    select_query_serial_numbers = f"SELECT distinct device_up.device_name FROM public.device_up"
    dff = postgresql_to_dataframe(connect=DB.conn, query =select_query_serial_numbers, column_names= ["device_name"])
    rawnumbers = dff['device_name'].values.tolist()
    return rawnumbers
def dehex2bytes(byte2,byte1):
    vol = byte2 + byte1
    return (int(vol, base = 16)) / 1000

class Meter:
    def __init__(self, SN):
        self.SN = SN
    def getsn(self):
        return self.SN

    def getdatarawbytes(self):
        select_query = f"SELECT device_up.device_name, encode(data, 'hex')::text FROM public.device_up where device_name like '{self.SN}' ORDER BY received_at DESC LIMIT 1 "
        dff = postgresql_to_dataframe(connect = DB.conn, query = select_query, column_names= Q.column_names)
        rawbytes = dff['data'] #where(dff['device_name'] == SN)   pandas dataframe (with index 0)
        rawbyteslist = rawbytes.loc[0]  # list of bytes converted from pandas dataframe
        # stringlist = [x.decode('utf-8') for x in rawbyteslist]
        # strData = codecs.decode(rawbytes.loc[0], 'UTF-8')
        return rawbyteslist
    def getdeveui(self):
        select_query = f"SELECT encode(dev_eui, 'hex') FROM public.device_up where device_name like '{self.SN}' ORDER BY id DESC LIMIT 1 "
        dff = postgresql_to_dataframe(connect=DB.conn, query=select_query, column_names=['dev_eui'])
        dev_eui = dff['dev_eui'].loc[0]
        return dev_eui

    def payloadtolist(self, rawbyteslist): #instead of doing rawbyteslist -> self.getrawbytes
        n = 2
        payloadtolist = [rawbyteslist[i:i + n] for i in range(0, len(rawbyteslist), n)]
        return payloadtolist

    def getmeterunixtime(self, payloadtolist):    #get last log time from payload
        time = payloadtolist[4] + payloadtolist[3] + payloadtolist[2] + payloadtolist[1]
        # print(DT.datetime.utcfromtimestamp(float(int(str(time), 16)) / 16 ** 4))
        #print(hex(int(time.mktime(time.strptime(time, '%Y-%m-%d %H:%M:%S')))))
        timed = int(time, base= 16)
        datetime_object = datetime.fromtimestamp(timed).strftime('%d-%m-%Y  %H:%M:%S')
        return datetime_object
    def getlastvolume(self, payloadtolist):       #get last volume log from payload
        volume1 = payloadtolist[19] + payloadtolist[18] + payloadtolist[17] + payloadtolist[16]
        volume1dec = (int(volume1, base = 16)) / 1000
        return volume1dec

    def get13volume(self, payloadtolist, volume1dec):
        #volume13 = payloadtolist[21] + payloadtolist[20]
        #volume13dec =  (int(volume13, base = 16)) / 1000
        return [(volume1dec - dehex2bytes(payloadtolist[21],payloadtolist[20])),
                ((volume1dec - dehex2bytes(payloadtolist[23],payloadtolist[22]))),
                ((volume1dec - dehex2bytes(payloadtolist[25],payloadtolist[24]))),
                ((volume1dec - dehex2bytes(payloadtolist[27],payloadtolist[26]))),
                ((volume1dec - dehex2bytes(payloadtolist[29],payloadtolist[28]))),
                ((volume1dec - dehex2bytes(payloadtolist[31],payloadtolist[30]))),
                ((volume1dec - dehex2bytes(payloadtolist[33],payloadtolist[32]))),
                ((volume1dec - dehex2bytes(payloadtolist[35],payloadtolist[34]))),
                ((volume1dec - dehex2bytes(payloadtolist[37],payloadtolist[36]))),
                ((volume1dec - dehex2bytes(payloadtolist[39],payloadtolist[38]))),
                ((volume1dec - dehex2bytes(payloadtolist[41],payloadtolist[40]))),
                ((volume1dec - dehex2bytes(payloadtolist[43],payloadtolist[42]))),
                ((volume1dec - dehex2bytes(payloadtolist[45],payloadtolist[44])))]

    def getreversevol(self, payloadtolist):
        reversevol = payloadtolist[12] + payloadtolist[11] + payloadtolist[10] + payloadtolist[9]
        reversevoldec = (int(reversevol, base = 16)) / 1000
        return reversevoldec

    def getlastmonthfag(self, payloadtolist):
        flagbyte = payloadtolist[13]
        flagbin0 = bin(int(flagbyte, 16))[2:].zfill(8)
        return parsebinaryflag(flagbin0)
    def getthismonthflag(self, payloadtolist):
        flagbyte = payloadtolist[14]
        flagbin1 = bin(int(flagbyte, 16))[2:].zfill(8)
        return parsebinaryflag(flagbin1)
    def getcurrentflag(self, payloadtolist):
        flagbyte = payloadtolist[15]
        flagbin2 = bin(int(flagbyte, 16))[2:].zfill(8)
        return parsebinaryflag(flagbin2)
