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
    VCDict[str(sockt)] = 0

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
    def get(self):
        #Concatenates List of strings...
        repstr = ','.join(replicas)
        return make_response(jsonify(
            message='View Retrieved successfully',
            view=repstr
        ), 200)

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

#Need to keep causal consistency using causal metadata
#Vector clocks are recommended as causal metadata
#class kvsHandler(Resource):
SOCKET_ADDRESS = os.environ.get('SOCKET_ADDRESS')

def CompareClocks(meta):
    if len(meta) is 0:
        return 0
    else:
        meta_list = meta.split(',')
        for idx in range(0, len(meta_list)):
            if (meta_list[idx]) == '':
                return 0
            meta_list[idx] = int(meta_list[idx])
        # map_obj = map(int, meta_list)
        # int_clock_client = list(map_obj)
        #opposite
        index = 0
        for replcount in replicas:
            if meta_list[index] > VCDict[replcount]:
                return -1
            index = index + 1
        return 0

def QueueCheckClient():
    flag_loop = 1
    while(flag_loop is 1):
        flag_loop = 0
        for key in Q_Dict:
            data = Q_Dict[key]

        #    ////////////////////////////////////
            # val = request.get_json()
            val = data['value']
            # meta is the vector clock vector clock
            meta = data['causal-metadata']
        #   //////////////////////////////////////////////

            q_flag = CompareClocks(meta)

            if(q_flag == 0):
                flag_loop = 1
                KeyValDict[str(key)] = val
                for sockt in replicas:
                    if SOCKET_ADDRESS == sockt:
                        # incrementing vector clock
                        VCDict[sockt] = VCDict[sockt] + 1

                        BigDict[str(val)] = val
                        BigDict[str(meta)] = meta
                        BigDict[str(sockt)] = sockt

                        #broadcsting to other replicas on end point "to-replica'"
                        for sock in replicas:
                            if SOCKET_ADDRESS is not sock:
                                req = requests.put('http://'+sock+'/to-replica/' + key, json=BigDict, timeout = 10)
                                return req.json(), req.status_code
                del Q_Dict[key]

    # go through the queue
    # compare stored VC vs current vector clock
    # if VCDict >= StoredVC gto all values:
    #     storedVC will store its key
    #     then broadcast
    #     delete the stored VC from the Queue
    #     increase VCDict ++

@app.route('/key-value-store/<key>', methods=['PUT'])
def put(key):
    value2 = request.get_json()
    value = value2['value']
    # value = data['value']
    if value is None:
        return jsonify(
            error='Value is missing',
            message='Error in PUT',
        ),400
    # meta is the vector clock vector clock
    # meta2 = request.get_json()
    meta = value2['causal-metadata']
    # meta = data['causal-metadata']
    # return meta
#        /////////////////////////////////////////

#    object = request.json('value')

#      ////////////////////////////////////////
    store_flag = CompareClocks(meta)

    #Check queue of replicas if empty
    if not Q_Dict:
        QueueCheckClient()
        
    if(store_flag == -1):
        # We queue the Value here
        # add dict like object to Q_object
        # DataDict = dict()
        # DataDict[str(value)] = value
        # DataDict[str(meta)] = meta
        # Q_Dict[key] = DataDict#object
        Small_Dict = dict()
        Small_Dict['value'] = value
        Small_Dict['causal-metadata'] = meta
        Q_Dict[key] = Small_Dict
        
    else:
        KeyValDict[str(key)] = value

    # updating VC
    for sockt in replicas:
    #    rep_list=sockt.split(",")
        if SOCKET_ADDRESS == sockt:
            # incrementing vector clock
            VCDict[sockt] = VCDict[sockt] + 1
            # loading meta data
            BigDict[str(value)] = value
            BigDict[str(meta)] = meta
            BigDict[str(sockt)] = sockt
    #    return rep_list[0]

    #broadcsting to other replicas on end point "to-replica'"

    for sockt in replicas:
        if SOCKET_ADDRESS is not sockt:
            req = requests.put('http://'+sockt+'/to-replica/' + key, json=BigDict, timeout = 10)
            return req.json(), req.status_code

# This funtion is from Replica to Replica
#class broadcaster(Resource):
def QueueCheckReplica():
    flag_loop = 1
    while(flag_loop is 1):
        flag_loop = 0
        for key in Q_Dict:
            data = Q_Dict[key]

#            ////////////////////////////////////
            val = data['value']
            # meta is the vector clock vector clock
            meta = data['causal-metadata']
            replica = data['sockt']
#            //////////////////////////////////////////////

            q_flag = CompareClocks(meta)

            if(q_flag == 0):
                flag_loop = 1
                KeyValDict[str(key)] = val
                VCDict[replica] = VCDict[replica] + 1   
                del Q_Dict[key]
@app.route('/to-replica/<key>', methods=['PUT'])
def Qrep(key):
    # value = request.get_json('value')
    # meta = request.get_json('meta')
    # replica = request.get_json('sockt')
    # data = request.get_json()
    value2 = request.get_json()
    value = value2['value']
    meta = value2['causal-metadata']
    replica = value2['sockt']

    store_flag = CompareClocks(meta)

#    /////////////////////////////////////////

    #object = request.json()

#    ////////////////////////////////////////

    if not Q_Dict:
        QueueCheckReplica()

    if(store_flag == -1):
        # DataDict = dict()
        # DataDict[str(value)] = value
        # DataDict[str(meta)] = meta
        # DataDict[str(replica)] = replica
        # Q_Dict[key] = DataDict
        Another_Dict = dict()
        Another_Dict['value'] = value
        Another_Dict['meta'] = meta
        Another_Dict['replica'] = replica
        Q_Dict[key] = Another_Dict
    else:
        KeyValDict[str(key)] = value
        # increment vector clock of the replica that got the request from the cleint
        VCDict[replica] = VCDict[replica] + 1
        



#**************************************************************************************************************#

#~~~~~~~~~~~~~~~~~~ old: Key-Value operations endpoint~~~~~~~~~~~~~~~~~~~

#Need to keep causal consistency using causal metadata
# class kvsHandler(Resource):
#     SOCKET_ADDRESS = os.environ.get('SOCKET_ADDRESS')
#     #@app.route('/key-value-store', methods=['PUT'])
#     def put(self, key):

#         #Get The FORWARDING ADDRESS to determine which container running
#         global eventcounter
#         #enter the main container, else enter forwarding container
#         if SOCKET_ADDRESS is None:
#             #Need to check if there is no <value> given first
#             value = request.get_json()
#             value = value.get('value')
#             metadat = request.get_json()
#             metadat = metadat.get('causal-metadata')
#             if value is None:
#                 return make_response(jsonify(
#                     error='Value is missing',
#                     message='Error in PUT',
#                 ),400)
#             if len(key) > 50:
#                 return make_response(jsonify(
#                     error='Key is too long',
#                     message='Error in PUT',
#                 ),400)
#             #Update Value of key here
#             # KeyValDict[key] = request.values.get('value')
#             if key not in KeyValDict:
#                 KeyValDict[str(key)] = value
#                 eventcounter = eventcounter + 1
#                 return make_response(jsonify({
#                     'message' : 'Added successfully',
#                     'causal-metadata' : 'V'+eventcounter
#                 }), 201) #test script says this is 201
#             elif key in KeyValDict:
#                 return make_response(jsonify({
#                     'message' : 'Added successfully',
#                     'causal-metadata' : 'V'+eventcounter
#                 }), 201)
#         elif 'SOCKET_ADDRESS' in os.environ:
#             try:
#                 value = request.get_json()
#                 req = requests.put('http://'+SOCKET_ADDRESS+'/key-value-store/' + key, json=value, timeout = 10)
#                 return req.json(),req.status_code
#             except:
#                 return make_response(jsonify(
#                     error= 'Main instance is down', 
#                     message = 'Error in PUT'
#                     ), 503)
#             else:
#                 return req.json(),req.status_code
#     #@app.route('/key-value-store', methods=['GET'])
#     def get(self, key):
#         if SOCKET_ADDRESS is None:
#             if key not in KeyValDict:
#                 return make_response(jsonify(
#                     doesExist=False,
#                     error='Key does not exist',
#                     message='Error in GET'
#                 ), 404)
#             elif key in KeyValDict:
#                 value = KeyValDict[str(key)]
#                 return make_response(jsonify({
#                     'message' : 'Retrieved successfully',
#                     'causal-metadata' : 'V'+eventcounter,
#                     'value' : value
#                 }), 200)
#         elif 'SOCKET_ADDRESS' in os.environ:
#             try:
#                 req = requests.get('http://'+SOCKET_ADDRESS+'/key-value-store/' + key)
#                 return req.json(),req.status_code
#             except:
#                 return make_response(jsonify(
#                     error= 'Main instance is down', 
#                     message = 'Error in GET'
#                     ), 503)

#     #@app.route('/key-value-store', methods=['DELETE'])
#     def delete(self, key):
#         global eventcounter
#         if SOCKET_ADDRESS is None:
#             if key not in KeyValDict:
#                 return make_response(jsonify(
#                     doesExist=False,
#                     error='Key does not exist',
#                     message='Error in DELETE'
#                 ), 404)
#             elif key in KeyValDict:
#                 del KeyValDict[str(key)]
#                 eventcounter = eventcounter + 1
#                 return make_response(jsonify(
#                     doesExist=True,
#                     message='Deleted successfully',
#                 ), 200)
#         elif 'SOCKET_ADDRESS' in os.environ:
#             try:
#                 req = requests.delete('http://'+SOCKET_ADDRESS+'/key-value-store/' + key)
#                 return req.json(),req.status_code
#             except:
#                 return make_response(jsonify(
#                     error= 'Main instance is down', 
#                     message = 'Error in DELETE'
#                     ), 503)
#api.add_resource(kvsHandler, '/key-value-store/', '/key-value-store/<key>')
app.run(host=socket.gethostbyname(socket.gethostname()),port=8085,debug=True)