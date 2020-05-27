<<<<<<< HEAD
#assignment3.py
#Group: rsaefong@ucsc.edu, omnasser@ucsc.edu, dajli@ucsc.edu, rmjaureg@ucsc.edu

from flask import Flask
from flask import request, jsonify, make_response
from flask_restful import Resource, Api
import os, requests, socket
from requests.exceptions import Timeout
# from flask import jsonify

app = Flask(__name__)
api = Api(app)
KeyValDict = dict() #declaring dictionary
eventcounter = 0 #Counter incremented every PUT and DELETE
replicas = [os.environ['VIEW']]#.split(',')] #List of replicas
SOCKET_ADDRESS = os.environ.get('SOCKET_ADDRESS')
#FORWARDING_ADDRESS = os.environ.get('FORWARDING_ADDRESS')
headers = "Content-Type: application/json"
#Want to use dict which is Python HashTable
#want to check if something in dictionary (if key in dict)

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


#~~~~~~~~~~~~~~~~~~Key-Value operations endpoint~~~~~~~~~~~~~~~~~~~

#Need to keep causal consistency using causal metadata
class kvsHandler(Resource):
    SOCKET_ADDRESS = os.environ.get('SOCKET_ADDRESS')
    #@app.route('/key-value-store', methods=['PUT'])
    def put(self, key):

        #Get The FORWARDING ADDRESS to determine which container running
        global eventcounter
        #enter the main container, else enter forwarding container
        if SOCKET_ADDRESS is None:
            #Need to check if there is no <value> given first
            value = request.get_json()
            value = value.get('value')
            metadat = request.get_json()
            metadat = metadat.get('causal-metadata')
            if value is None:
                return make_response(jsonify(
                    error='Value is missing',
                    message='Error in PUT',
                ),400)
            if len(key) > 50:
                return make_response(jsonify(
                    error='Key is too long',
                    message='Error in PUT',
                ),400)
            #Update Value of key here
            # KeyValDict[key] = request.values.get('value')
            if key not in KeyValDict:
                KeyValDict[str(key)] = value
                eventcounter = eventcounter + 1
                return make_response(jsonify({
                    'message' : 'Added successfully',
                    'causal-metadata' : 'V'+eventcounter
                }), 201) #test script says this is 201
            elif key in KeyValDict:
                return make_response(jsonify({
                    'message' : 'Added successfully',
                    'causal-metadata' : 'V'+eventcounter
                }), 201)
        elif 'SOCKET_ADDRESS' in os.environ:
            try:
                value = request.get_json()
                req = requests.put('http://'+SOCKET_ADDRESS+'/key-value-store/' + key, json=value, timeout = 10)
                return req.json(),req.status_code
            except:
                return make_response(jsonify(
                    error= 'Main instance is down', 
                    message = 'Error in PUT'
                    ), 503)
            else:
                return req.json(),req.status_code
    #@app.route('/key-value-store', methods=['GET'])
    def get(self, key):
        if SOCKET_ADDRESS is None:
            if key not in KeyValDict:
                return make_response(jsonify(
                    doesExist=False,
                    error='Key does not exist',
                    message='Error in GET'
                ), 404)
            elif key in KeyValDict:
                value = KeyValDict[str(key)]
                return make_response(jsonify({
                    'message' : 'Retrieved successfully',
                    'causal-metadata' : 'V'+eventcounter,
                    'value' : value
                }), 200)
        elif 'SOCKET_ADDRESS' in os.environ:
            try:
                req = requests.get('http://'+SOCKET_ADDRESS+'/key-value-store/' + key)
                return req.json(),req.status_code
            except:
                return make_response(jsonify(
                    error= 'Main instance is down', 
                    message = 'Error in GET'
                    ), 503)

    #@app.route('/key-value-store', methods=['DELETE'])
    def delete(self, key):
        global eventcounter
        if SOCKET_ADDRESS is None:
            if key not in KeyValDict:
                return make_response(jsonify(
                    doesExist=False,
                    error='Key does not exist',
                    message='Error in DELETE'
                ), 404)
            elif key in KeyValDict:
                del KeyValDict[str(key)]
                eventcounter = eventcounter + 1
                return make_response(jsonify(
                    doesExist=True,
                    message='Deleted successfully',
                ), 200)
        elif 'SOCKET_ADDRESS' in os.environ:
            try:
                req = requests.delete('http://'+SOCKET_ADDRESS+'/key-value-store/' + key)
                return req.json(),req.status_code
            except:
                return make_response(jsonify(
                    error= 'Main instance is down', 
                    message = 'Error in DELETE'
                    ), 503)
api.add_resource(kvsHandler, '/key-value-store/', '/key-value-store/<key>')
app.run(host=socket.gethostbyname(socket.gethostname()),port=8085,debug=True)
=======

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
        try:
            value = request.get_json()
            req = requests.put('http://'+SOCKET_ADDRESS+'/key-value-store-view/', json=value, timeout = 10)
            return req.json(),req.status_code   
        except:
            return make_response(jsonify(
                error= 'Put request failed', 
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
                    error= 'Put request failed', 
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
        value = request.get_json
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


#**************************************************************************************************************#


#~~~~~~~~~~~~~~~~~~Key-Value operations endpoint~~~~~~~~~~~~~~~~~~~

#Need to keep causal consistency using causal metadata
#Vector clocks are recommended as causal metadata
class kvsHandler(Resource):
    SOCKET_ADDRESS = os.environ.get('SOCKET_ADDRESS')

    @app.route('/key-value-store/<key>', methods=['PUT'])

    def put(self, key):
        value = requests.get_json('value')
        # meta is the vector clock vector clock
        meta = requests.get_json('causal-metadata')

        store_flag = CompareClocks(meta)

        if(store_flag == -1):
            # we queue the vale here
        else:
            if value is None:
                return jsonify(
                    error='Value is missing',
                    message='Error in PUT',
                ),400
            KeyValDict[str(key)] = value

        # do we quque do we broadcast or do we wait for the key to be added to the dictonary?

        # gettting current replica index
        for sockt in replicas:
            if SOCKET_ADDRESS == replicas[sockt]
                found_num= sockt
                # incremenign vector clock
                VC[sockt] = VC[sockt] + 1

        # loading meta data
        BigDict[str(value)] = value
        BigDict[str(meta)] = meta
        BigDict[str(replica)] = found_num

        #broadcsting to other replicas on end point "to-replica'"
        for sockt in replicas:
            if SOCKET_ADDRESS is not replicas[sockt]
                req = requests.put('http://'+replicas[sockt]+'/to-replica/' + key, json=BigDict, timeout = 10)


    def CompareClocks(meta) 
        meta_list = meta.split(',')
        map_obj = map(int, meta_list)
        int_clock_client = list(map_obj)

        #opposite
        for replcount in replicas:
            if int_clock_client[replcount] > VC[replcount]
            return -1
        return 0


    @app.route('/to-replica/<key>', methods=['PUT'])

    def put(self, key):
        value = requests.get_json('value')
        meta = requests.get_json('meta')
        replica = requests.get_json('replica')

        store_flag = CompareClocks(meta)

        if(store_flag == -1):
            # we queue the vale
        else:
                KeyValDict[str(key)] = value

            # increment vector clcok of the replica that got the request from the cleint
            VC[replica] = VC[replica] + 1

            # for rep in replicas
            #     if replica = replicas[rep]
            #         VC[rep] = VC[rep] + 1
            

    def CheckVector():
        flag_loop = 0
        if BigDict:
            while(flag = 0):
                for value in BigDict.values()   2 1 3 4 5    0
                    value.split(';')
                    VC_Old = value[1]
                    VC_Old.split(',')
                    flag = 0

                    for rep in replicas:
                        if VC_Old[rep] > VC[rep]
                            flag = flag + 1

                    if flag = 1
                        KeyValDict[str(key)] = value
                        for sockt in replicas
                            if SOCKET_ADDRESS = replicas[sockt]
                                VC[sockt] = VC[sockt] + 1
        else:
            return


#**************************************************************************************************************#

























# class kvsHandler(Resource):
#     SOCKET_ADDRESS = os.environ.get('SOCKET_ADDRESS')

#     @app.route('/key-value-store/<key>', methods=['PUT'])
#     #@app.route('/key-value-store', methods=['PUT'])
#     def put(self, key):
#         value = request.get_json
#         value = value.get('value')
#         if value is None:
#             return jsonify(
#                 error='Value is missing',
#                 message='Error in PUT',
#             ),400
#         if len(key) > 50:
#             return jsonify(
#                 error='Key is too long',
#                 message='Error in PUT',
#             ),400
#         if key not in KeyValDict:
#             KeyValDict[str(key)] = value
#             #return jsonify(
#              #   message='Added successfully',
#               #  replaced=False,
#             #), 201 #test script says this is 201
#         elif key in KeyValDict:
#             KeyValDict[str(key)] = value
#             #return jsonify(
#             #message='Updated successfully',
#             #replaced=True,
#             #), 200
#         process = 0
#         for sockt in replicas:
#             if SOCKET_ADDRESS == replicas[sockt]:
#                 process = sockt
#                 VC[sockt] = VC[sockt] + 1
#         amntreplicas = len(replicas)
#         for i in range(amntreplicas):
#             if SOCKET_ADDRESS is not replicas[i]:
#                 try:
#                     str(VC).strip('[]')
#                     # value = requests.get_json('value')
#                     # VC = requests.get_json('VC')
#                     # process = requests.get_json('process')

#                     new_val = value + ";" + VC + ";" + process
#                     new_val = requests.get_json('new_val')

#                     req = requests.put('http://'+replicas[i]+'/to-replica/' + key, json=new_val, timeout = 10)
#                     #req = requests.put('http://'+replicas[i]+'/key-value-store-view/', json=value, timeout = 10)
#                     #return req.json(),req.status_code
#                 except:
#                     return make_response(jsonify(
#                         error= 'Put request failed', 
#                         message = 'Error in PUT'
#                     ), 503)



#     # recived message from cleint
#     # decode and get the key-value 
#     # add key to own dict
#     # increment vector clock
#     # prepeae vector clock in metadata
#      #@app.route('/to-replica/<vector-clock>/<key-value>', methods=['PUT'])


#     @app.route('/to-replica/<key>', methods=['PUT'])
#     def put(self, key):
#         new_val = request.get_json
#         new_val = value.get('new_val')
#         # tempVC = request.get_json
#         # tempVC = value.get('VC')
#         # process = request.get_json
#         # process = value.get('process')
#         new_val.split(';')
#         value = new_val[0]
#         tempVC = new_val[1]
#         process = new_val[2]

#         arrived_clock = tempVC.split(',')


#         # check the queue for paet request and compare that vector clock with arrived clock 
#         CheckVector()


#         flag = 0

#         for rep in replicas:
#             if arrived_clock[rep] > VC[rep]:
#                 flag = flag + 1

#         if flag is 1:
#             KeyValDict[str(key)] = value
#             VC[int(process)] = VC[int(process)] + 1
#             #update vector clock
#         else:
#             new_value = value + ";" + tempVC + ";" + process
#             BigDict[str(key)] = new_value

#     def CheckVector():
#         flag_loop = 1
#         if BigDict:
#             while(flag_loop is 1):
#                 flag_loop = 0
#                 for value in BigDict.values(): 
#                     tempVal = value   
#                     value.split(';')
#                     Bench_VC = value[1]
#                     Bench_VC.split(',')
#                     flag = 0

#                     for rep in replicas:
#                         if Bench_VC[rep] > VC[rep]:
#                             flag = flag + 1

#                     if flag is 1:
#                         for kay in BigDict.keys():
#                             if BigDict[str(kay)] is tempVal:
#                                 KeyValDict[str(kay)] = value[0]
#                                 del BigDict[str(kay)]

#                         VC[int(value[2])] = VC[int(value[2])] + 1
#                         flag_loop = 1



