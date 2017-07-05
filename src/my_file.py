

#import section



import sys
import os
import json
import datetime
import operator
import math
#########################################

# taking arguments form the script

args_1 = sys.argv[1][1:]   # batch_log
args_2 = sys.argv[2][1:]   # stream_log
args_3 =  sys.argv[3][1:]  # flagged_purchases


# user records
user_record = dict()

'''
user_record : = is dictionary holds user_id and the connections.
'''

# ledger

ledger = dict()

'''
ledger keeps a record of all the transaction. it is a dictionary containing dictionary of history transactions of users
in a list. I have created variable  transaction_id to understand the order of transaction's, it will be incremented for
every financial transactions.
'''
transaction_id = 0

# loading file paths

path = os.getcwd()
path_sample_batch_log = path + args_1
path_sample_stream_log = path + args_2
output_file_path = path + args_3

batch_log = open(path_sample_batch_log)
stream_log = open(path_sample_stream_log)


# when reading the batch long for the first time we need to take D AND T AS INPUTS

def get_D_T(batch_log):
    file = str(batch_log.readline())
    temp = str(file)
    temp = temp[:-1]
    json_acceptable_string = temp.replace("'", "\"")
    jason_line_dict = json.loads(json_acceptable_string)
    D = int(jason_line_dict['D'])
    T = int(jason_line_dict['T'])
    return D,T, batch_log

#  this is to read each line from stream_log.
def read_line_steam_log(stream_log):
    file = str(stream_log.readline())
    temp = str(file)
    temp = temp[:-1]
    json_acceptable_string = temp.replace("'", "\"")
    dict_temp = json.loads(json_acceptable_string)
    return dict_temp

D, T, batch_log= get_D_T(batch_log) # get values of D,T, batch_log


# we are reading each line  we are do iteratively until we reach the end of the file.

def log_read_line(log):
    file = str(log.readline())
    temp = str(file)
    return temp


def decode_string(temp):
    json_acceptable_string = temp.replace("'", "\"")
    dict_temp = json.loads(json_acceptable_string)
    return dict_temp

'''
files are read as a string and they have to decoded, for that reason we have decode_string(), it will return a
dictonary.
'''

'''
rise_event() is to classify what kind of even has be arised form the batch_log and stream files.
p: purchase
b: befriend
u: unfriend
'''

def rise_event(dict_temp):
    if dict_temp['event_type'][0] == 'p':
        return 'p'
    if dict_temp['event_type'][0] == 'b':
        return 'b'
    if dict_temp['event_type'][0] == 'u':
        return 'u'


'''
befriend(), adds friends, it is made sure friendships are bidirectional
measure is taken that we add someone as friend only when one is not a friend.
'''


def befriend(dict_temp):
    friends_list = []
    try:
        friends_list = list(user_record[dict_temp['id1']])
        if not dict_temp['id2'] in friends_list:
            friends_list.append(dict_temp['id2'])
            user_record.setdefault(dict_temp['id1'],[]).append(dict_temp['id2'])
    except KeyError:
        friends_list.append(dict_temp['id2'])
        user_record.setdefault(dict_temp['id1'],[]).append(dict_temp['id2'])
    try:
        friends_list = list(user_record[dict_temp['id2']])
        if not dict_temp['id1'] in friends_list:
            friends_list.append(dict_temp['id1'])
            user_record.setdefault(dict_temp['id2'],[]).append(dict_temp['id1'])
    except KeyError:
        friends_list.append(dict_temp['id1'])
        user_record.setdefault(dict_temp['id2'],[]).append(dict_temp['id1'])

    return None

'''
opposite of befriend(), removes friends from list, again its bidirectional and should be perfomed only when people
are already friend's.
'''

def unfriend(dict_temp):
    friends_list = list(user_record[dict_temp['id1']])
    if dict_temp['id2'] in friends_list:
        friends_list.remove(dict_temp['id2'])
        user_record.setdefault(dict_temp['id1'],[]).remove(dict_temp['id2'])
    friends_list= list(user_record[dict_temp['id2']])
    if dict_temp['id1'] in friends_list:
        friends_list.remove(dict_temp['id1'])
        user_record.setdefault(dict_temp['id2'],[]).remove(dict_temp['id1'])
    return None


'''
purchase() function adds transactions to the ledger, ledger has records of every user.
each  transaction amount  is coupled with transaction id.

example of structure of ledger:

ledger : =  {'1':{'2017-06-14 18:46:40': [[68.92, 13000], [106.23, 30145], [77.3, 32143]]},
'2':{'2017-06-15 11:46:40': [[62.91, 13023], [1012.22, 30145], [77.3, 32143]]}}
'''

def purchase(dict_temp,transaction_id):
    billing_history = dict()
    date_purchase = dict_temp['timestamp']
    date_purchase =datetime.datetime.strptime( date_purchase,'%Y-%m-%d %H:%M:%S') # convert to Date-time
    date_purchase = datetime.datetime.strftime( date_purchase, '%Y-%m-%d %H:%M:%S') # convert to string
    amount = float( dict_temp['amount'])

    try:
        billing_history = list(ledger[dict_temp['id']])
        if date_purchase  in billing_history:
            billing_history = ledger[dict_temp['id']]
            bill_for_the_day = billing_history[date_purchase]
            billing_history.setdefault(date_purchase,bill_for_the_day).append([amount,transaction_id])
            ledger[dict_temp['id']] = billing_history
        else:
            billing_history = ledger[dict_temp['id']]
            billing_history.setdefault(date_purchase,[]).append([amount,transaction_id])
            ledger[dict_temp['id']] = billing_history

    except KeyError:
        billing_history.setdefault(date_purchase,[]).append([amount,transaction_id])
        ledger[dict_temp['id']] = billing_history
    return transaction_id


# wish function is what will  pulls all  everything to build inital state of the network.
def read_every_line(transaction_id):
    temp = 'not-empty'
    while temp != '' :
        temp = log_read_line(batch_log)
        if temp == '':
            break
        dict_temp = decode_string(temp)
        event_type = rise_event(dict_temp)
        if event_type == 'b':
            befriend(dict_temp)
        if event_type == 'u':
            unfriend(dict_temp)
        if event_type == 'p':
            transaction_id +=1
            purchase(dict_temp,transaction_id)
    return transaction_id




# calling the function to read every line form bath_log.json.
transaction_id = read_every_line(transaction_id)


# from here we will have functions that will handle stream of data.


'''
get_network() will return all the connections with specified degree.
'''

def get_network(D,user_id):
    d_temp = 2
    network_list = list(user_record[user_id])
    network_list_dup=list()
    visited_list = list( )
    while(d_temp< D ):
        d_temp += 1
        for user in network_list:
            if user not in visited_list:
                network_list_dup += user_record[user]
                visited_list.append( user)
        network_list = list(set(network_list + network_list_dup))
        network_list_dup = []

    if user_id in network_list:
        network_list.remove(user_id)
    return network_list


'''
will return the transaction history of a particular network.
the goal is to get last T number transactions in a network. If we dont have 50 transactions in a network,
will take the maximum number of transations that took place in that network.
'''

def get_network_history(network_list, date_purchase, T ):
    network_transaction_history = []
    net_transactions = []
    temp_T = T
    for user_id in network_list:
        if user_id in ledger:
            net_transactions = net_transactions + list( ledger[user_id].values())
    net_flat_list = [item for sublist in net_transactions for item in sublist]
    if (len(net_flat_list) < T):
        T = len(net_flat_list) - 1
    while( len(network_transaction_history) < (T+1) ):
        for user in network_list:
            user_transaction_history = ledger[user]
            if date_purchase in user_transaction_history:
                network_transaction_history = network_transaction_history + \
                                               user_transaction_history[date_purchase]
        date_purchase = datetime.datetime.strptime(date_purchase, '%Y-%m-%d %H:%M:%S')
        date_purchase = (date_purchase - datetime.timedelta(seconds = 1 ))
        date_purchase = datetime.datetime.strftime(date_purchase, '%Y-%m-%d %H:%M:%S')
    T = temp_T
    return network_transaction_history


# function to calculate mean and standard deviation.

def calcualtion(T_records):
    mean =  sum(T_records)/ len(T_records)
    sq_diff = list(map(lambda x: (x - mean)**2, T_records))
    sd =  math.sqrt( sum(sq_diff)/ len(T_records))
    return mean, sd


'''
this function does everything the purchase() function does while building initial state of the system and on top
that they are other operations it will perform to handle stream of transactions that is to declare anomaly.

this is the section that will perform the intented goal of this challenge.

I faced issue while saving the data in json file, the test throwed an error because my responses are not in right order
even if they are correct, so I used OrderedDict() to preserve the the order of data in dict and later saved to Json file.
'''

def purchase_stream(dict_temp, transaction_id):
    import collections
    dict_temp_o = collections.OrderedDict()
    date_purchase = dict_temp['timestamp']
    user_id = dict_temp['id']
    date_purchase = datetime.datetime.strptime( date_purchase,'%Y-%m-%d %H:%M:%S') # convert to Date-time
    date_purchase = datetime.datetime.strftime( date_purchase, '%Y-%m-%d %H:%M:%S') # convert to string
    amount = float( dict_temp['amount'])
    transaction_id = purchase(dict_temp,transaction_id)

    ######
    network_list = get_network(D, user_id)
    network_history= get_network_history(network_list, date_purchase,T)
    network_history =  sorted( network_history,key=operator.itemgetter(1), reverse=True)
    past_T_transactions = [item[0] for item in network_history]
    T_records = past_T_transactions[:T]
    mean,sd = calcualtion(T_records)
    anomaly_threshold = mean + (3*sd)
    if amount > anomaly_threshold:
        dict_temp_o['event_type'] = dict_temp['event_type']
        dict_temp_o['timestamp'] = dict_temp['timestamp']
        dict_temp_o['id'] = dict_temp['id']
        dict_temp_o['amount'] = dict_temp['amount']
        dict_temp_o['mean'] = "%.2f" % round(mean, 2)
        dict_temp_o['sd'] = "%.2f" % round(sd, 2)
        with open(output_file_path, 'a') as fp:
            fp.write('\n')
            json.dump(dict_temp_o, fp)
            fp.close()
    return None


'''
this is the function that will handle the stream of data, its similar the other other function with
minor modification's.
'''

def read_every_line_stream(transaction_id,stream_log):
    temp = 'not-empty'

    while temp !='':
        temp  = str(stream_log.readline())
        temp = str(temp)
        if len(temp)<10:
            break
        json_acceptable_string = temp.replace("'", "\"")
        dict_temp = json.loads(json_acceptable_string)
        event_type = rise_event (dict_temp)
        if event_type == 'b':
            befriend(dict_temp)
        if event_type == 'u':
            unfriend(dict_temp)
        if event_type == 'p':
            transaction_id +=1
            purchase_stream(dict_temp,transaction_id)
    return None

read_every_line_stream(transaction_id,stream_log)

########################################################################################################################

'''
END
'''
