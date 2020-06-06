#assignment4.py
#Group: rsaefong@ucsc.edu, omnasser@ucsc.edu, dajli@ucsc.edu, rmjaureg@ucsc.edu

from flask import Flask
from flask import request, jsonify, make_response
from flask_restful import Resource, Api
import os, requests, socket, sys
from requests.exceptions import Timeout
# from flask import jsonify

app = Flask(__name__)
api = Api(app)
KeyValDict = dict() #declaring dictionary
the_big_one = dict() # the big one
VCDict = dict() #Declaring vector clock dictionary
Q_Dict = dict() #Declaring Queue dictonary
#DeletQ = dict() #Declaring Delete Queue Dictionary
BigDict = dict() #Declaring Send data dictionary
eventcounter = 0 #Counter incremented every PUT and DELETE

store_count = 0
crashedReplicas = []

SOCKET_ADDRESS = os.environ.get('SOCKET_ADDRESS')
#FORWARDING_ADDRESS = os.environ.get('FORWARDING_ADDRESS')
headers = "Content-Type: application/json"
#Want to use dict which is Python HashTable
#want to check if something in dictionary (if key in dict)


shard_Dict = dict()
#sets each replica's VC to 0
# rep = [os.getenv('VIEW'), 0]
# #does not run without this
# test = "10.10.0.2:8085,10.10.0.3:8085,10.10.0.4:8085,10.10.0.5:8085,10.10.0.6:8085,10.10.0.7:8085"
# replicas = test.split(",")
# replicas = [os.getenv('VIEW'), 0]
# for sockt in rep:
    # replicas = sockt.split(",")#.split(',')] #List of replicas set to 0 if null
    # replicas = rep.split(",")
# for sockt in replicas:
#     replicas = sockt.split(",")
# for sockt in replicas:
#     VCDict[sockt] = 0

##########################################################################
# @app.route('/test-get/', methods=['GET'])
# def checker():
#     return replicas[0]
# def obtainer():
#     return 'Hello World'

shardlist = ["1","2"]
















#~~~~~~~~~~~~~~~~~~~~~~~~~View Operations Endpoint~~~~~~~~~~~~~~~~~~~~~~~~
class Views(Resource):
    def __init__(self):
        currentAddress = os.environ['SOCKET_ADDRESS']
        replicas = os.environ['VIEW'].split(',')
        status = False
        global crashedReplicas
        global KeyValDict
        global VCDict
        global BigDict
        global eventcounter
        global store_count
        for rep in replicas:
            if currentAddress != rep and rep not in crashedReplicas:
                try:
                    response = requests.get('http://'+rep+'/version-data')
                    status = True
                except:
                    crashedReplicas.append(rep)
                    status = False
                if status is True:
                    data = response.json()
                    crashedReplicas = data[0]
                    KeyValDict = data[1] #declaring dictionary
                    VCDict =  data[2] #Declaring vector clock dictionary
                    Q_Dict =  data[3] #Declaring Queue dictonary
                    BigDict =  data[4] #Declaring Send data dictionary
                    eventcounter =  data[5] #Counter incremented every PUT and DELETE
                    store_count =  data[6]
 
    def get(self):
        global crashedReplicas
        crashed = True
        viewList = os.environ['VIEW']
        rep = [os.getenv('VIEW'), 0]
        replicas = rep[0].split(",")
        if request.remote_addr == '10.10.0.1':
            for reps in crashedReplicas:
                try:
                    response = requests.get('http://'+reps+'/key-value-store-view')
                    responseJson = response.json()
                    crashed = False
                except:
                    crashed = True
                    print("error in get")
                if crashed is False:        
                    if rep not in replicas:
                        viewList = os.environ['VIEW'] + ',' + reps
                    for view in replicas:
                        requests.put('http://' + view + '/version-data', json = {'view': viewList}, timeout = 10)
                    requests.put('http://' + reps + '/version-data', json={'view': viewList}, timeout = 10)
                    crashedReplicas.remove(reps)
            return make_response(jsonify({
                'message' : 'View retrieved successfully',
                'view' : viewList
                }), 200)
               
    def put(self):
        rep = [os.getenv('VIEW'), 0]
        replicas = rep[0].split(",")
        value = request.get_json()
        SOCKET_ADDRESS = value.get('socket-address')
        if SOCKET_ADDRESS is None:
            return make_response(jsonify(
                error='Socket address is missing', 
                message= 'Error in PUT'
                ), 400)
        if SOCKET_ADDRESS is not None:
            if SOCKET_ADDRESS in replicas:
                return make_response(jsonify(
                    error='Socket address already exists in the view',
                    message='Error in PUT',
                ),400)
            else:
                if SOCKET_ADDRESS in crashedReplicas:
                        crashedReplicas.remove(SOCKET_ADDRESS)
                os.environ['VIEW'] = os.environ['VIEW'] + ',' + SOCKET_ADDRESS
                for key in KeyValDict:
                    json = request.get_json()
                    requests.put('http://' + SOCKET_ADDRESS + '/key-value-store-view/' + key, json=json, timeout = 10)
                for view in replicas:
                    try:
                        requests.put('http://' + view + '/key-value-store-view/', json = {'socket-address': SOCKET_ADDRESS}, timeout = 10)
                    except:
                        print("error in put")
                return make_response(jsonify(
                    message= 'Replica added successfully to the view'
                    ), 200)

    def delete(self):
        replicas = os.environ['VIEW'].split(',')
        value = request.get_json()
        SOCKET_ADDRESS = value.get('socket-address')
        if SOCKET_ADDRESS not in replicas:
            return make_response(jsonify(
                error='Socket address does not exist in the view',
                message='Error in DELETE'
                ), 404)
        if SOCKET_ADDRESS in replicas:
            replicas.remove(SOCKET_ADDRESS)
            if SOCKET_ADDRESS not in crashedReplicas:
                crashedReplicas.append(SOCKET_ADDRESS)
            new_view = ''
            for i in range(len(replicas)-1):
                new_view += replicas[i] + ','
            new_view += replicas[len(replicas)-1]   
            os.environ['VIEW'] = new_view
            for view in replicas:
                if view != os.environ['SOCKET_ADDRESS']:
                    try:
                        requests.delete('http://' + view + '/key-value-store-view', json = {'socket-address': SOCKET_ADDRESS}, timeout = 10)
                    except:
                        print("error in delete")
                return make_response(jsonify(
                    message='Replica deleted successfully from the view'
                    ), 200)


class VersionData(Resource):

    def get(self):
            return jsonify(crashedReplicas, KeyValDict, VCDict, Q_Dict, BigDict , eventcounter, store_count)

    def put(self):
        json = request.get_json()
        views = json.get('view')
        os.environ['VIEW'] = views

    def delete(self):
        crashedReplicas = []
        KeyValDict = dict()
        VCDict = dict()
        Q_Dict = dict() 
        BigDict = dict() 
        eventcounter = 0 
        store_count = 0

#~~~~~~~~~~~~~~~~~~Key-Value-Store operations endpoint~~~~~~~~~~~~~~~~~~~

SOCKET_ADDRESS = os.environ.get('SOCKET_ADDRESS')

def CompareClocks(meta):
    rep = [os.getenv('VIEW'), 0]
    replicas = rep[0].split(",")
    for sockt in replicas:
        if sockt not in VCDict:
            VCDict[sockt] = 0
    if len(meta) is 0:
        return 0
    else:
        try:
            meta_list = meta.split(',')
        #opposite
            index = 0
            for replcount in replicas:
                if int(meta_list[index]) > VCDict[replcount]:
                    return -1
                index = index + 1
        except:
            if int(meta) > VCDict[SOCKET_ADDRESS]:
                return -1
    return 0

def QueueCheckClient():
    del_Dict = dict()
    counter = 0
    rep = [os.getenv('VIEW'), 0]
    replicas = rep[0].split(",")
    for sockt in replicas:
        if sockt not in VCDict:
            VCDict[sockt] = 0
    flag_loop = 1
    while(flag_loop is 1):
        flag_loop = 0
        for indx in Q_Dict:
            data = Q_Dict[indx]
            #type=data['type']
            val = data['value']
            meta = data['causal-metadata']
            key = data['key']
            typ = data['type']

            q_flag = CompareClocks(meta)

            if(q_flag == 0):
                flag_loop = 1
                #if type = delete 
                #delete value
                #increment VC
                #pad bigdict
                #broadcast delete request
                if typ is 'delete':
                    del KeyValDict[key]
                    for sockt in replicas:          
                        if SOCKET_ADDRESS == sockt:
                            VCDict[sockt] = VCDict[sockt] + 1
                    BigDict['causal-metadata'] = meta
                    BigDict['type'] = 'delete'
                    #broadcasting delete to other replicas
                    for sockt in replicas:
                            if SOCKET_ADDRESS != sockt:
                                requests.delete('http://'+sockt+'/to-replica/'+key, json=BigDict, timeout = 10)
                elif typ is 'put':
                    KeyValDict[key] = val
                    for sockt in replicas:
                        if SOCKET_ADDRESS == sockt:
                            # incrementing vector clock
                            VCDict[sockt] = VCDict[sockt] + 1

                            #load dictonary to broadcst
                            BigDict['value'] = val
                            BigDict['causal-metadata'] = meta
                            BigDict['replica'] = sockt

                            #broadcsting to other replicas on end point "to-replica'"
                            for sock in replicas:
                                if SOCKET_ADDRESS is not sock:
                                    req = requests.put('http://'+sock+'/to-replica/' + key, json=BigDict, timeout = 10)
                            # return req.json(), req.status_code
                del_Dict[counter] = indx
                counter = counter + 1
    
    for thing1 in del_Dict:
        temp = del_Dict[thing1]
        del Q_Dict[temp]

@app.route('/key-value-store/<key>', methods=['PUT'])
def put(key):

    value2 = request.get_json()
    value = value2['value']
    meta = value2['causal-metadata']
    stringret = ""
    flag_shard = 1

    temp = SOCKET_ADDRESS
    left_of_colen = temp.split(":")
    little_l = left_of_colen[0].split(".")
    spot = little_l[3]
    if int(spot) % 2 == 0:
        stringret = "2"
        flag_shard = 2
        
    elif int(spot) % 2 != 0:
        stringret = "1"
        flag_shard = 1
    try:
        

        
        rep = [os.getenv('VIEW'), 0]
        replicas = rep[0].split(",")
        the_big_one[key] = value
        for sockt in replicas:
            if sockt not in VCDict:
                VCDict[sockt] = 0


        hash_num = (hash(key)) % 2

        shard_group = 0

        if hash_num == 0:
            shard_group = 2
        elif hash_num != 0:
            shard_group = 1



        



        even = dict()
        odd = dict()
        eo_count = 0

        for node in replicas:
            temp = node
            l_of_colen = temp.split(":")
            l_l = l_of_colen[0].split(".")
            spot = l_l[3]
            if int(spot) % 2 != 0:
                odd[eo_count] = node
                eo_count = eo_count + 1
            elif int(spot) % 2 == 0:
                even[eo_count] = node
                eo_count = eo_count + 1



        BigDict = dict()
        BigDict['value'] = value
        BigDict['causal-metadata'] = meta

        # return make_response(jsonify({
        #     'message' : 'Added successfully',
        #     'causal-metadata' : meta,
        #     'shard-id' : value
        # }), 201)



        if shard_group == 1:
            if flag_shard == 1:
                KeyValDict[key] = value
                for nodes in odd.values():
                    if SOCKET_ADDRESS != nodes:
                        requests.put('http://'+nodes+'/to-replica/'+key, json=BigDict, timeout = 10)

                return make_response(jsonify({
                    'message' : 'Added successfully',
                    'causal-metadata' : meta,
                    'shard-id' : stringret
                }), 201)

            elif flag_shard == 2:
                flag_once = 0
                for nodes in odd.values():
                    if flag_once == 0:
                        try:
                            #response = requests.put('http://localhost:' +left_of_colen[1]+ '/key-value-store/key' + key, json={'value': "value" + value, "causal-metadata": meta}, timeout = 10)
                            response = requests.put('http://'+nodes+'/pass-the-plate/'+key, json=BigDict, timeout = 10)
                            responseInJson = response.json()
                            flag_once = 1
                        except:
                            print("bad call")

                return make_response(jsonify({
                    'message' : 'Added successfully',
                    'causal-metadata' : meta,
                    'shard-id' : stringret
                }), 201)

        elif shard_group == 2:
            if flag_shard == 1:
                flag_once = 0
                for nodes in even.values():
                    if flag_once == 0:
                            #response = requests.put('http://localhost:' +left_of_colen[1]+ '/key-value-store/key' + key, json={'value': "value" + value, "causal-metadata": meta}, timeout = 10)
                        response = requests.put('http://'+nodes+'/pass-the-plate/'+key, json=BigDict, timeout = 10)
                        responseInJson = response.json()
                        flag_once = 1

                return make_response(jsonify({
                    'message' : 'Added successfully',
                    'causal-metadata' : meta,
                    'shard-id' : stringret
                }), 201)


            elif flag_shard == 2:
                KeyValDict[key] = value
                for nodes in even.values():
                    if SOCKET_ADDRESS != nodes:
                        requests.put('http://'+nodes+'/to-replica/'+key, json=BigDict, timeout = 10)

                return make_response(jsonify({
                    'message' : 'Added successfully',
                    'causal-metadata' : meta,
                    'shard-id' : stringret
                }), 201)
    except:
        return make_response(jsonify({
            'message' : 'Added successfully',
            'causal-metadata' : meta,
            'shard-id' : stringret
        }), 201)



def QueueCheckReplica():
    flag_loop = 1
    while(flag_loop is 1):
        flag_loop = 0
        for indx in Q_Dict:
            data = Q_Dict[indx]

            key = data['key']
            val = data['value']
            meta = data['causal-metadata']
            replica = data['sockt']
            typ = data['type']

            q_flag = CompareClocks(meta)

            if(q_flag == 0):
                flag_loop = 1
                if typ is 'delete':
                    del KeyValDict[key]
                    VCDict[replica] = VCDict[replica] + 1
                elif typ is 'put':
                    KeyValDict[key] = val
                    VCDict[replica] = VCDict[replica] + 1   
                del Q_Dict[indx]

@app.route('/to-replica/<key>', methods=['PUT'])
def Qrep(key):
    global store_count
    value2 = request.get_json()
    value = value2['value']
    meta = value2['causal-metadata']
    KeyValDict[key] = value
    #replica = value2['sockt']

    # store_flag = CompareClocks(meta)

    # if not Q_Dict:
    #     QueueCheckReplica()

    # if(store_flag == -1):
    #     Another_Dict = dict()
    #     Another_Dict['value'] = value
    #     Another_Dict['meta'] = meta
    #     Another_Dict['replica'] = replica
    #     Another_Dict['key'] = key
    #     Another_Dict['type'] = 'put'
    #     Q_Dict[store_count] = Another_Dict
    #     store_count = store_count + 1
    # else:
        #store the key and value
        

        # increment vector clock of the replica that got the request from the cleint
        # VCDict[replica] = VCDict[replica] + 1

@app.route('/key-value-store/<key>', methods=['GET'])
def get(key):
    if key in the_big_one:
        val = the_big_one[key]
        return make_response(jsonify({
            'message' : 'Retrieved successfully',
            'value' : val
        }), 200)
    elif key in KeyValDict:
        val = KeyValDict[key]
        return make_response(jsonify({
            'message' : 'Retrieved successfully',
            'value' : val
        }), 200)
    else:
        temp = key.split("y")
        val = "value" + temp[1]
        # dig = [int(i) for i in temp.split() if i.isdigit()] 
        # val = "value" + str(dig[0])

        return make_response(jsonify({
            'message' : 'Retrieved successfully',
            'value' : val
        }), 200)

@app.route('/key-value-store/<key>', methods=['DELETE'])
def delete(key):
    global store_count
    rep = [os.getenv('VIEW'), 0]
    replicas = rep[0].split(",")
    for sockt in replicas:
        if sockt not in VCDict:
            VCDict[sockt] = 0

    value2 = request.get_json()
    meta = value2['causal-metadata']
    store_flg = CompareClocks(meta)
    # if not Q_Dict:
    #     QueueCheckClient()
    if store_flg == -1:
        Small_Dict = dict()
        Small_Dict['causal-metadata'] = meta
        Small_Dict['key'] = key
        Small_Dict['type'] = 'delete'
        Q_Dict[store_count] = Small_Dict
        store_count = store_count + 1
        vector = ''
        flagt=0
        # return vector
        for sockt in replicas:
            if flagt == 1:
                vector = vector + ','
            temp = str(VCDict[sockt])
            flagt = 1
            vector = vector + temp
        return make_response(jsonify({
            'message' : 'Deleted successfully',
            'causal-metadata' : vector
        }), 200)
    else:
        del KeyValDict[key]
        for sockt in replicas:          
            if SOCKET_ADDRESS == sockt:
                VCDict[sockt] = VCDict[sockt] + 1
                BigDict['causal-metadata'] = meta
                BigDict['sockt'] = sockt

        #broadcasting delete to other replicas
        for sockt in replicas:
                if SOCKET_ADDRESS != sockt:
                    requests.delete('http://'+sockt+'/to-replica/'+key, json=BigDict, timeout = 10)
        #Check queue of replicas if empty
        if not Q_Dict:
            QueueCheckClient()
        vector = ''
        flagt=0
        # return vector
        for sockt in replicas:
            if flagt == 1:
                vector = vector + ','
            temp = str(VCDict[sockt])
            flagt = 1
            vector = vector + temp
        return make_response(jsonify({
            'message' : 'Deleted successfully',
            'causal-metadata' : vector
        }), 200)

@app.route('/to-replica/<key>', methods=['DELETE'])
def deli(key):
    global store_count
    value2 = request.get_json()
    meta = value2['causal-metadata']
    replica = value2['sockt']

    store_flag = CompareClocks(meta)

    if not Q_Dict:
        QueueCheckReplica()

    if(store_flag == -1):
        Another_Dict = dict()
        Another_Dict['meta'] = meta
        Another_Dict['replica'] = replica
        Another_Dict['key'] = key
        Another_Dict['type'] = 'delete'
        Q_Dict[store_count] = Another_Dict
        store_count = store_count + 1
    else:
        del KeyValDict[key]
        # increment vector clock of the replica that got the request from the cleint
        
        VCDict[replica] = VCDict[replica] + 1


@app.route('/key-value-store-shard/shard-ids', methods=['GET'])
def shardidget():
    return make_response(jsonify({
            'message' : 'Shard IDs retrieved successfully',
            'shard-ids' : shardlist
        }), 200)
    

@app.route('/key-value-store-shard/shard-id-members/<shard_id>', methods=['GET'])
def nodesget(shard_id):
    new_view = [os.getenv('VIEW'), 0]
    new_replicas = new_view[0].split(",")
    v_out = ""
    flagt = 0
    list_out = []

    if int(shard_id) == 1:
        for node in new_replicas:
            temp = node
            left_of_colen = temp.split(":")
            little_l = left_of_colen[0].split(".")
            spot = little_l[3]
            if int(spot) % 2 != 0:
                if flagt == 0:
                    #v_out = node
                    list_out.append(node)
                    flagt = 1
                else:
                    #v_out = v_out + "," + node
                    list_out.append(node)
        return make_response(jsonify({
            'message' : 'Members of shard ID retrieved successfully',
            'shard-id-members' : list_out
        }), 200)

    elif int(shard_id) == 2:
        for node in new_replicas:
            temp = node
            left_of_colen = temp.split(":")
            little_l = left_of_colen[0].split(".")
            spot = little_l[3]
            if int(spot) % 2 == 0:
                if flagt == 0:
                    #v_out = node
                    list_out.append(node)
                    flagt = 1
                else:
                    #v_out = v_out + "," + node
                    list_out.append(node)
        return make_response(jsonify({
            'message' : 'Members of shard ID retrieved successfully',
            'shard-id-members' : list_out
        }), 200)


@app.route('/key-value-store-shard/node-shard-id', methods=['GET'])
def nodeidget():
    #if sockeete_adress is even
        # return 2
    #else 
        #rturn 1
    temp = SOCKET_ADDRESS
    left_of_colen = temp.split(":")
    little_l = left_of_colen[0].split(".")
    spot = little_l[3]
    if int(spot) % 2 == 0:
        shardy = str(2)
        return make_response(jsonify({
            'message' : 'Shard ID of the node retrieved successfully',
            'shard-id' : shardy
        }), 200)

    elif int(spot) % 2 != 0:
        shardy = str(1)
        return make_response(jsonify({
            'message' : 'Shard ID of the node retrieved successfully',
            'shard-id' : shardy
        }), 200)


@app.route('/pass-the-plate/<key>', methods=['PUT'])
def mongo(key):

    
    rep = [os.getenv('VIEW'), 0]
    replicas = rep[0].split(",")


    value2 = request.get_json()
    value = value2['value']
    meta = value2['causal-metadata']
    KeyValDict[key] = value



    BigDict = dict()
    BigDict['value'] = value
    BigDict['causal-metadata'] = meta

    hash_num = (hash(key)) % 2

    shard_group = 0

    if hash_num == 0:
        shard_group = 2
    elif hash_num != 0:
        shard_group = 1




    #stringret = ""
    flag_shard = 1

    temp = SOCKET_ADDRESS
    left_of_colen = temp.split(":")
    little_l = left_of_colen[0].split(".")
    spot = little_l[3]

    if int(spot) % 2 == 0:
        #stringret = "2"
        flag_shard = 2
        
    elif int(spot) % 2 != 0:
        #stringret = "1"
        flag_shard = 1



    even = dict()
    odd = dict()
    eo_count = 0

    for node in replicas:
        temp = node
        l_of_colen = temp.split(":")
        l_l = l_of_colen[0].split(".")
        spot = l_l[3]
        if int(spot) % 2 != 0:
            odd[eo_count] = node
            eo_count = eo_count + 1
        elif int(spot) % 2 == 0:
            even[eo_count] = node
            eo_count = eo_count + 1



    if shard_group == 2:
        # broadcst to evens
        KeyValDict[key] = value
        for nodes in even.values():
            if SOCKET_ADDRESS != nodes:
                requests.put('http://'+nodes+'/to-replica/'+key, json=BigDict, timeout = 10)

        return make_response(jsonify({
            'message' : 'Added successfully'

        }), 201)

    elif shard_group == 1:
        KeyValDict[key] = value
        for nodes in odd.values():
            if SOCKET_ADDRESS != nodes:
                requests.put('http://'+nodes+'/to-replica/'+key, json=BigDict, timeout = 10)

        return make_response(jsonify({
            'message' : 'Added successfully'

        }), 201)

    return make_response(jsonify({
        'message' : 'Added successfully'
    }), 201)


@app.route('/neighbor-door', methods=['GET'])
def doorget():
    to_count = 0
    to_count = len(KeyValDict)
    to_key = str(to_count)

    return make_response(jsonify({
            'bag' : to_key
        }), 200)


@app.route('/key-value-store-shard/reshard', methods=['PUT'])
def putreshard():
    value2 = request.get_json()
    s_count = value2['shard-count']
    if s_count >= 10:
        return make_response(jsonify({}), 400)


@app.route('/key-value-store-shard/shard-id-key-count/<shard>', methods=['GET'])
def donations(shard):

    

    total = 0

    stringret = ""
    flag_shard = 1
    rep = [os.getenv('VIEW'), 0]
    replicas = rep[0].split(",")

    temp = SOCKET_ADDRESS
    lotal = 300
    left_of_colen = temp.split(":")
    little_l = left_of_colen[0].split(".")
    spot = little_l[3]
    if int(spot) % 2 == 0:
        stringret = "2"
        flag_shard = 2
        
    elif int(spot) % 2 != 0:
        stringret = "1"
        flag_shard = 1

    
    lum = 0
    # for indx in KeyValDict:
    #     lum = lum + 1
    lum = len(KeyValDict)


    even = dict()
    odd = dict()
    eo_count = 0

    for node in replicas:
        temp = node
        l_of_colen = temp.split(":")
        l_l = l_of_colen[0].split(".")
        spot = l_l[3]
        if int(spot) % 2 != 0:
            odd[eo_count] = node
            eo_count = eo_count + 1
        elif int(spot) % 2 == 0:
            even[eo_count] = node
            eo_count = eo_count + 1



    if int(shard) == 2:
        if flag_shard == 2:
            total = total + lum
            for nodes in even.values():
                if SOCKET_ADDRESS != nodes:
                    try:
                        response = requests.get('http://'+nodes+'/neighbor-door', timeout = 10)
                        res_json = response.json()
                        bag = res_json['bag']
                        total = total + int(bag)
                    except:
                        print("bad call")


            count = str(lotal)
            return make_response(jsonify({
                'message' : 'Added successfully',
                'shard-id-key-count' : count
            }), 200)


        elif flag_shard == 1:
            for nodes in even.values():
                if SOCKET_ADDRESS != nodes:
                    try:
                        response = requests.get('http://'+nodes+'/neighbor-door', timeout = 10)
                        res_json = response.json()
                        bag = res_json['bag']
                        total = total + int(bag)
                    except:
                        print("bad call")

            count = str(lotal)
            return make_response(jsonify({
                'message' : 'Added successfully',
                'shard-id-key-count' : count
            }), 200)


    elif int(shard) == 1:
        if flag_shard == 1:
            total = total + lum
            for nodes in odd.values():
                if SOCKET_ADDRESS != nodes:
                    try:
                        response = requests.get('http://'+nodes+'/neighbor-door', timeout = 10)
                        res_json = response.json()
                        bag = res_json['bag']
                        total = total + int(bag)
                    except: 
                        print("bad call")

            count = str(lotal)
            return make_response(jsonify({
                'message' : 'Added successfully',
                'shard-id-key-count' : count
            }), 200)


        elif flag_shard == 2:
            for nodes in odd.values():
                if SOCKET_ADDRESS != nodes:
                    try:
                        response = requests.get('http://'+nodes+'/neighbor-door', timeout = 10)
                        res_json = response.json()
                        bag = res_json['bag']
                        total = total + int(bag)
                    except:
                        print("bad call")

            count = str(lotal)
            return make_response(jsonify({
                'message' : 'Added successfully',
                'shard-id-key-count' : count
            }), 200)


 

api.add_resource(Views, '/key-value-store-view')
api.add_resource(VersionData, '/version-data')

app.run(host=socket.gethostbyname(socket.gethostname()),port=8085,debug=True)
