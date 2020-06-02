#assignment3.py
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
VCDict = dict() #Declaring vector clock dictionary
Q_Dict = dict() #Declaring Queue dictonary
BigDict = dict() #Declaring Send data dictionary
eventcounter = 0 #Counter incremented every PUT and DELETE

store_count = 0

SOCKET_ADDRESS = os.environ.get('SOCKET_ADDRESS')
#FORWARDING_ADDRESS = os.environ.get('FORWARDING_ADDRESS')
headers = "Content-Type: application/json"
#Want to use dict which is Python HashTable
#want to check if something in dictionary (if key in dict)




#sets each replica's VC to 0
rep = [os.getenv('VIEW'), 0]
replicas = rep[0].split(",")
# replicas = [os.getenv('VIEW'), 0]
# for sockt in rep:
    # replicas = sockt.split(",")#.split(',')] #List of replicas set to 0 if null
    # replicas = rep.split(",")
# for sockt in replicas:
#     replicas = sockt.split(",")
for sockt in replicas:
    VCDict[sockt] = 0

##########################################################################
@app.route('/test-get/', methods=['GET'])
def checker():
    return replicas[0]
def obtainer():
    return 'Hello World'


#~~~~~~~~~~~~~~~~~~~~~~~~~View Operations Endpoint~~~~~~~~~~~~~~~~~~~~~~~~
class viewHandler(Resource):
    SOCKET_ADDRESS = os.environ.get('SOCKET_ADDRESS')
#@app.route('/key-value-store-view', methods=['PUT'])
    def put(self):
        #global eventcounter
        #enter the main container, else enter forwarding container
        if SOCKET_ADDRESS is None:
            #Need to check if there is no <value> given first
            value = request.get_json()
            value = value.get('socket-address')
            if value is None:
                return make_response(jsonify(
                    error='Value is missing',
                    message='Error in PUT',
                ),400)
            # if len(key) > 50:
            #     return jsonify(
            #         error='Key is too long',
            #         message='Error in PUT',
            #     ),400
            #Update Value of key here
            # KeyValDict[key] = request.values.get('value')
            if SOCKET_ADDRESS not in replicas:
            # KeyValDict[str(key)] = value
                #eventcounter += 1
                return make_response(jsonify(
                    message="Replica added successfully to the view",
                ), 201) #test script says this is 201
            elif SOCKET_ADDRESS in replicas:
                return make_response(jsonify(
                error='Socket address already exists in the view',
                message='Error in PUT'
                ), 404)
        elif SOCKET_ADDRESS in os.environ:
            try:
                value = request.get_json()
                req = requests.put('http://'+SOCKET_ADDRESS+'/key-value-store-view/', json=value, timeout = 10)
                return req.json(),req.status_code   
            except:
                return make_response(jsonify(
                    error= 'Main instance is down', 
                    message = 'Error in PUT'
                ), 503)
            else:
                return req.json(),req.status_code
        #Need to broadcast to other replicas
        #Updating their data
        amntreplicas = len(replicas)
        for i in range(amntreplicas):
            try:
                value = request.get_json()
                req = requests.put('http://'+replicas[i]+'/key-value-store-view/', json=value, timeout = 10)
                return req.json(),req.status_code
            except:
                return make_response(jsonify(
                    error= 'Main instance is down', 
                    message = 'Error in PUT'
                ), 503)
            else:
                return req.json(),req.status_code
    #@app.route('/key-value-store-view', methods=['GET'])
    # def get(self):
    #     #Concatenates List of strings...
    #     repstr = ','.join(replicas)
    #     return make_response(jsonify(
    #         message='View Retrieved successfully',
    #         view=repstr
    #     ), 200)

        # elif SOCKET_ADDRESS in os.environ:
        #     try:
        #         req = requests.get('http://'+SOCKET_ADDRESS+'/key-value-store-view/' + key)
        #         return req.json(),req.status_code
        #     except:
        #         return jsonify(
        #             error= 'Main instance is down', 
        #             message = 'Error in GET'
        #             ), 503

    #@app.route('/key-value-store-view', methods=['DELETE'])
    def delete(self):
        value = request.get_json()
        value = value.get('socket-address')

        #global eventcounter
        if value is None:
            #if key not in KeyValDict:
                return make_response(jsonify(
                    error='Socket address does not exist in the view',
                    message='Error in DELETE'
                ), 404)
        elif value in replicas:
            requests.delete(value)
            # req = requests.delete(value)('http://'+SOCKET_ADDRESS+'/key-value-store-view/')# + key)
                #return req.json(),req.status_code
            return make_response(jsonify(
                message='Replica deleted successfully from the view'
            ), 200)
        else:
            return make_response(jsonify(
                    error='Socket address does not exist in the view',
                    message='Error in DELETE'
                ), 404)
api.add_resource(viewHandler, '/key-value-store-view') 


#~~~~~~~~~~~~~~~~~~Key-Value-Store operations endpoint~~~~~~~~~~~~~~~~~~~

SOCKET_ADDRESS = os.environ.get('SOCKET_ADDRESS')

def CompareClocks(meta):
    if len(meta) is 0:
        return 0
    else:
        meta_list = meta.split(',')
   
        #opposite
        index = 0
        for replcount in replicas:
            if int(meta_list[index]) > VCDict[replcount]:
                return -1
            index = index + 1
        return 0

def QueueCheckClient():
    flag_loop = 1
    while(flag_loop is 1):
        flag_loop = 0
        for indx in Q_Dict:
            data = Q_Dict[indx]

            val = data['value']
            meta = data['causal-metadata']
            key = data['key']

            q_flag = CompareClocks(meta)

            if(q_flag == 0):
                flag_loop = 1
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
                        return req.json(), req.status_code
                del Q_Dict[indx]

@app.route('/key-value-store/<key>', methods=['PUT'])
def put(key):
    exist = 0
    if key in KeyValDict:
        exist = 1
    value2 = request.get_json()
    value = value2['value']
    meta = value2['causal-metadata']
    if value is None:
        return jsonify(
            error='Value is missing',
            message='Error in PUT',
        ),400

    store_flag = CompareClocks(meta)

    #Check queue of replicas if empty
    if not Q_Dict:
        QueueCheckClient()
        
    # if -1 the incoming clock it to ahead
    if(store_flag == -1):
        Small_Dict = dict()
        Small_Dict['value'] = value
        Small_Dict['causal-metadata'] = meta
        Small_Dict['key'] = key
        Q_Dict[store_count] = Small_Dict
        store_count = store_count + 1
        
    else:
        #strore key and value
        KeyValDict[key] = value

        # updating Vector clock
        for sockt in replicas:          
            if SOCKET_ADDRESS == sockt:
                VCDict[sockt] = VCDict[sockt] + 1

                # loading meta data to send to broadcast to other replicas
                BigDict['value'] = value
                BigDict['causal-metadata'] = meta
                BigDict['sockt'] = sockt
        
        #broadcsting to other replicas on end point "to-replica'"
        for sockt in replicas:
            if SOCKET_ADDRESS != sockt:

                ###############JSON DECODE ERROR ######################
                # return SOCKET_ADDRESS
                # val = request.get_json(meta)
                requests.put('http://'+sockt+'/to-replica/'+key, json=BigDict, timeout = 10)
                #need to fix syntax on this make_response because causal-metadata needs to be in ''
                # trying to do method with semicolon instead of = sign
                #req = requests.put('http://'+sockt+'/to-replica/'+key, json=BigDict, timeout = 10)
        flagt = 0
        vector = ''
        for sockt in replicas:
            if flagt == 1:
                vector = vector + ','
            temp = str(VCDict[sockt])
            flagt = 1
            vector = vector + temp
        if exist == 0:
            return make_response(jsonify({
                'message' : 'Added Successfully',
                'causal-metadata' : vector
            }), 201)
        return make_response(jsonify({
            'message' : 'Updated Successfully',
            'causal-metadata' : vector
        }), 200)

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

            q_flag = CompareClocks(meta)

            if(q_flag == 0):
                flag_loop = 1
                KeyValDict[key] = val
                VCDict[replica] = VCDict[replica] + 1   
                del Q_Dict[indx]

@app.route('/to-replica/<key>', methods=['PUT'])
def Qrep(key):
    value2 = request.get_json()
    value = value2['value']
    meta = value2['causal-metadata']
    replica = value2['sockt']

    store_flag = CompareClocks(meta)

    if not Q_Dict:
        QueueCheckReplica()

    if(store_flag == -1):
        Another_Dict = dict()
        Another_Dict['value'] = value
        Another_Dict['meta'] = meta
        Another_Dict['replica'] = replica
        Another_Dict['key'] = key
        Q_Dict[store_count] = Another_Dict
        store_count = store_count + 1
    else:
        #store the key and value
        KeyValDict[key] = value

        # increment vector clock of the replica that got the request from the cleint
        VCDict[replica] = VCDict[replica] + 1

@app.route('/key-value-store/<key>', methods=['GET'])
def get(key):
    #need to check if key exists
    if key in KeyValDict:
        val = KeyValDict[key]
        flagt = 0
        vector = ''
        for sockt in replicas:
            if flagt == 1:
                vector = vector + ','
            temp = str(VCDict[sockt])
            flagt = 1
            vector = vector + temp
            # vector = ','.join(str(VCDict.values()))
        return make_response(jsonify({
            'message' : 'Retrieved successfully',
            'causal-metadata' : vector,
            'value' : val
        }), 200)
    else:
        return make_response(jsonify({
            'error' : 'Error in GET',
            'message' : 'Key does not exist'
        }), 404)
# @app.route('/key-value-store/<key>', methods=['DELETE'])
# def delete(key):

app.run(host=socket.gethostbyname(socket.gethostname()),port=8085,debug=True)