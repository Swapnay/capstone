from datetime import datetime
import json



# 04/25/2014,T,A,36177024,C,04/25/2014 09:33:11.529,L,2.43,4
def parse_csv(line: str):
    rec_type_pos = 1
    csv_list = line.split(',')
    if csv_list[rec_type_pos] == 'T':
        partition = 'T'
        trade_dt = datetime.strptime(csv_list[0], '%m/%d/%Y')
        rec_type = csv_list[1]
        symbol = csv_list[2]
        event_seq_num = int(csv_list[3])
        event_time = datetime.strptime(csv_list[5], '%m/%d/%Y %H:%M:%S.%f')
        exchange = csv_list[4]
        trade_price = float(csv_list[7])
        trade_size = int(csv_list[8])
        bid_price = 0.0
        bid_size = 0
        ask_price = 0.0
        ask_size = 0
        return CommonEvent(trade_dt, rec_type, symbol, exchange, event_time, event_seq_num, datetime.now(),
                           trade_price, trade_size, bid_price, bid_size, ask_price, ask_size,
                           partition)

    elif csv_list[rec_type_pos] == 'Q':
        # 09/13/2019,Q,A,09/13/2019 09:30:02.446,36163,C,0.5,4421,0.650000,4939
        partition = 'Q'
        trade_dt = datetime.strptime(csv_list[0], '%m/%d/%Y')
        rec_type = csv_list[1]
        symbol = csv_list[2]
        event_seq_num = int(csv_list[3])
        event_time = datetime.strptime(csv_list[5], '%m/%d/%Y %H:%M:%S.%f')

        exchange = csv_list[4]
        trade_price = 0.0
        trade_size = 0
        bid_price = float(csv_list[6])
        bid_size = int(csv_list[7])
        ask_price = float(csv_list[8])
        ask_size = int(csv_list[9])
        return CommonEvent(trade_dt, rec_type, symbol, exchange, event_time, event_seq_num,
                           datetime.now(), trade_price, trade_size, bid_price, bid_size, ask_price,
                           ask_size, partition)
        return event
    else:
        print('invalid data ', line)
        return CommonEvent(None, None,None,None,None,None,None,None,None,None,None,None,None,'B')



def parse_json(line: str):

    json_line = line
    if line.endswith(","):
        json_line = line[:-1]

    try :
        record = json.loads(json_line)

        if record['record_type'] == 'T':

            name_index = [ 'trade_date', 'record_type', 'symbol','event_seq_num','exchange','event_time', 'execution_id','trade_price', 'trade_size']
            partition = 'T'
            trade_dt = datetime.strptime(record[name_index[0]], '%m/%d/%Y')
            rec_type = record[name_index[1]]
            symbol = record[name_index[2]]
            event_seq_num = int(record[name_index[3]])
            event_time = datetime.strptime(record[name_index[5]], '%m/%d/%Y %H:%M:%S.%f')
            exchange = record[name_index[6]]
            arrival_time = datetime.now()
            trade_price = float(record[name_index[7]])
            trade_size = int(record[name_index[8]])
            bid_price = 0.0
            bid_size = 0
            ask_price = 0.0
            ask_size = 0

            return CommonEvent(trade_dt, rec_type, symbol, exchange, event_time, event_seq_num,
                               arrival_time, trade_price, trade_size, bid_price, bid_size, ask_price, ask_size,
                               partition)

        elif record['record_type'] == 'Q':

            name_index = ['trade_date', 'record_type', 'symbol','event_seq_num','exchange','event_time',
                           'bid_price', 'bid_size', 'ask_price', 'ask_size']
            partition = 'Q'
            trade_dt = datetime.strptime(record[name_index[0]], '%m/%d/%Y')
            rec_type = record[name_index[1]]
            symbol = record[name_index[2]]
            event_seq_num = int(record[name_index[3]])
            event_time = datetime.strptime(record[name_index[5]], '%m/%d/%Y %H:%M:%S.%f')
            exchange = record[name_index[4]]
            arrival_time = datetime.now()
            trade_price = 0.0
            trade_size = 0
            bid_price = float(record[name_index[6]])
            bid_size = int(record[name_index[7]])
            ask_price = float(record[name_index[8]])
            ask_size = int(record[name_index[9]])


            return CommonEvent(trade_dt, rec_type, symbol, exchange, event_time, event_seq_num,
                               arrival_time, trade_price, trade_size, bid_price, bid_size, ask_price, ask_size,
                               partition)
        else:
            print("Error "+line)
            return CommonEvent(None, None,None,None,None,None,None,None,None,None,None,None,None,'B')
    except Exception as ex:
        print(ex)
        print(line)
        return CommonEvent(None, None,None,None,None,None,None,None,None,None,None,None,None,'B')


class CommonEvent:

    def __init__(self, trade_dt, rec_type, symbol, exchange, event_tm, event_seq_nb, arrival_tm, trade_pr, trade_size, bid_pr,
                 bid_size, ask_pr, ask_size, partition):
        self.trade_dt = trade_dt
        self.rec_type = rec_type
        self.symbol = symbol
        self.event_time = event_tm
        self.event_seq_num = event_seq_nb
        self.exchange = exchange
        self.arrival_time = arrival_tm
        self.trade_price = trade_pr
        self.bid_price = bid_pr
        self.bid_size = bid_size
        self.ask_price = ask_pr
        self.ask_size = ask_size
        self.trade_size = trade_size
        self.partition = partition

    def __str__(self):
        return f"({self.trade_dt.strftime('%m/%d/%Y')}, {self.rec_type}, {self.symbol}, {self.event_time.strftime('%m/%d/%Y %H:%M:%S.%f')}, \
                        {self.exchange}, {self.event_seq_num}, {self.arrival_time.strftime('%Y-%m-%d')}, {self.trade_price}, \
                        {self.bid_price}, {self.bid_size}, {self.ask_price}, {self.ask_size} "
