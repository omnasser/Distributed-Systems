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
#Vector clocks are recommended as causal metadata
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
#Vector Clock array of event integers
#Gets total count of replicas in replicas
replcount = 0
for sockt in replicas:
    replcount = replcount + 1
VC = [replcount]
BigDict = dict()
VCFROM = [replcount]
#Initializes VC's to 0
for replcount in replicas:
    VC[replcount] = 0
    VCFROM[replcount] = 0
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
    #~~~~~~~~~~~~~~~~~~Key-Value operations endpoint~~~~~~~~~~~~~~~~~~~

    #Need to keep causal consistency using causal metadata
    #Vector clocks are recommended as causal metadata
class kvsHandler(Resource):
    SOCKET_ADDRESS = os.environ.get('SOCKET_ADDRESS')

    @app.route('/key-value-store/<key>', methods=['PUT'])
    #@app.route('/key-value-store', methods=['PUT'])
    def put(self, key):
        value = request.get_json
        value = value.get('value')
        if value is None:
            return jsonify(
                error='Value is missing',
                message='Error in PUT',
            ),400
        if len(key) > 50:
            return jsonify(
                error='Key is too long',
                message='Error in PUT',
            ),400
        if key not in KeyValDict:
            KeyValDict[str(key)] = value
            #return jsonify(
             #   message='Added successfully',
              #  replaced=False,
            #), 201 #test script says this is 201
        elif key in KeyValDict:
            KeyValDict[str(key)] = value
            #return jsonify(
            #message='Updated successfully',
            #replaced=True,
            #), 200
        for sockt in replicas
            if SOCKET_ADDRESS = replicas[sockt]
                VC[sockt] = VC[sockt] + 1
        amntreplicas = len(replicas)
        for i in range(amntreplicas):
            if SOCKET_ADDRESS is not replicas[i]
                try:
                    str(VC).strip('[]')
                    value = requests.get_json('value')
                    VC = requests.get_json('VC')
                    req = requests.put('http://'+replicas[i]+'/to-replica/' + key, json=value, json=VC, timeout = 10)
                    #req = requests.put('http://'+replicas[i]+'/key-value-store-view/', json=value, timeout = 10)
                    #return req.json(),req.status_code
                except:
                    return make_response(jsonify(
                        error= 'Put request failed', 
                        message = 'Error in PUT'
                    ), 503)



    # recived message from cleint
    # decode and get the key-value 
    # add key to own dict
    # increment vector clock
    # prepeae vector clock in metadata
     #@app.route('/to-replica/<vector-clock>/<key-value>', methods=['PUT'])


    @app.route('/to-replica/<key>', methods=['PUT'])
    def put(self, key):
        value = request.get_json
        value = value.get('value')
        tempVC = request.get_json
        tempVC = value.get('VC')

        arrived_clock = tempVC.split(',')


        # check the queue for paet request and compare that vector clock with arrived clock 



        flag = 0

        for rep in replicas:
            if arrived_clock[rep] > VC[rep]
                flag = flag + 1

        if flag = 1
            KeyValDict[str(key)] = value
            #update vector clock
        else

            new_value = value + ";" + tempVC
            BigDict[str(key)] = new_value

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

    #recive meg from replica
    # decode and get vecotor clock and key-value
    # compare vecotor clocks / increase own vecotor clock 
    # if less
    #   add value to dic
    # else 
    #   queue
    # send okay







        #Get The FORWARDING ADDRESS to determine which container running
        global eventcounter
        value = request.get_json()
        value = value.get('value')
        metadat = request.get_json()
        metadat = metadat.get('causal-metadata')
        #enter the main container, else enter forwarding container
        if SOCKET_ADDRESS is None:
            #Need to check if there is no <value> given first
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
        for sockt in VC:
            if VC[sockt] >= VCFROM[sockt]
        for sockt in replicas
            if SOCKET_ADDRESS = replicas[sockt]
                VC[sockt] = VC[sockt] + 1
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
            #only put if causally consistent
        try:
            value = request.get_json()
            req = requests.put('http://'+SOCKET_ADDRESS+'/key-value-store/' + key, json=value, timeout = 10)
            return req.json(),req.status_code

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
        try:
            req = requests.get('http://'+SOCKET_ADDRESS+'/key-value-store/' + key)
            return req.json(),req.status_code
        except:
            return make_response(jsonify(
                error= 'Get request failed', 
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
        try:
            req = requests.delete('http://'+SOCKET_ADDRESS+'/key-value-store/' + key)
            return req.json(),req.status_code
        except:
            return make_response(jsonify(
                error= 'Put request failed', 
                message = 'Error in DELETE'
            ), 503)
api.add_resource(kvsHandler, '/key-value-store/', '/key-value-store/<key>')
app.run(host=socket.gethostbyname(socket.gethostname()),port=8085,debug=True)
>>>>>>> 7f4ff3ed54fbaac4b403d2eff4338672791dbffe
