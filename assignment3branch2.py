#assignment3.py
#Group: rsaefong@ucsc.edu, omnasser@ucsc.edu, dajli@ucsc.edu, rmjaureg@ucsc.edu

from flask import Flask
from flask import request, jsonify, make_response
import os, requests, socket
from requests.exceptions import Timeout
# from flask import jsonify

app = Flask(__name__)
KeyValDict = dict() #declaring dictionary
eventcounter = 0 #Counter incremented every PUT and DELETE
replicas = [os.environ['VIEW']]#.split(',')] #List of replicas
#FORWARDING_ADDRESS = os.environ.get('FORWARDING_ADDRESS')
SOCKET_ADDRESS = os.environ.get('SOCKET_ADDRESS')
headers = "Content-Type: application/json"
#Want to use dict which is Python HashTable
#want to check if something in dictionary (if key in dict)

#jsonify objects required
#~~~~~~~~~~~~~~~~~~~~~~~~~TEST ENDPOINT FOR CONNECTION ISSUE~~~~~~~~~~~~~~~~~
@app.route('/test-value', methods=['GET'])
def test(key):
    return jsonify(
        message='Seems to be working'
    ),200
#~~~~~~~~~~~~~~~~~~~~~~~~~View Operations Endpoint~~~~~~~~~~~~~~~~~~~~~~~~

@app.route('/key-value-store-view', methods=['PUT'])
def kvsvPUT():
    #global eventcounter
    #enter the main container, else enter forwarding container
    if SOCKET_ADDRESS is None:
        #Need to check if there is no <value> given first
        value = request.json
        value = value.get('socket-address')
        if value is None:
            return jsonify(
                error='Value is missing',
                message='Error in PUT',
            ),400
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
            return jsonify(
                message="Replica added successfully to the view",
            ), 201 #test script says this is 201
        elif SOCKET_ADDRESS in replicas:
             return jsonify(
             error='Socket address already exists in the view',
             message='Error in PUT'
             ), 404
    elif SOCKET_ADDRESS in os.environ:
        try:
            value = request.get_json()
            req = requests.put('http://'+SOCKET_ADDRESS+'/key-value-store-view/' + key, json=value, timeout = 10)
            return req.json(),req.status_code   
        except:
            return jsonify(
                error= 'Main instance is down', 
                message = 'Error in PUT'
            ), 503
        else:
            return req.json(),req.status_code
    #Need to broadcast to other replicas
    #Updating their data
    amntreplicas = len(replicas)
    for i in range(amntreplicas):
        try:
            value = request.get_json()
            req = requests.put('http://'+replicas[i]+'/key-value-store-view/' + key, json=value, timeout = 10)
            return req.json(),req.status_code
        except:
            return jsonify(
                error= 'Main instance is down', 
                message = 'Error in PUT'
            ), 503
        else:
            return req.json(),req.status_code
    
@app.route('/key-value-store-view', methods=['GET'])
def kvsvGET():
    #Concatenates List of strings...
    repstr = ','.join(replicas)
   # if SOCKET_ADDRESS is None:
        # if key not in KeyValDict:
        #     return jsonify(
        #         error='Key does not exist',
        #         message='Error in GET'
        #     ), 404
        # elif key in KeyValDict:
            # value = KeyValDict[str(key)]
    return jsonify(
        message='View Retrieved successfully',
        view=repstr
    ), 200

    # elif SOCKET_ADDRESS in os.environ:
    #     try:
    #         req = requests.get('http://'+SOCKET_ADDRESS+'/key-value-store-view/' + key)
    #         return req.json(),req.status_code
    #     except:
    #         return jsonify(
    #             error= 'Main instance is down', 
    #             message = 'Error in GET'
    #             ), 503

@app.route('/key-value-store-view', methods=['DELETE'])
def kvsvDELETE():
    value = request.json
    value = value.get('socket-address')

    #global eventcounter
    if value is None:
        #if key not in KeyValDict:
            return jsonify(
                error='Socket address does not exist in the view',
                message='Error in DELETE'
            ), 404
    elif value in replicas:
        requests.delete(value)
           # req = requests.delete(value)('http://'+SOCKET_ADDRESS+'/key-value-store-view/')# + key)
            #return req.json(),req.status_code
        return jsonify(
            message='Replica deleted successfully from the view'
        ), 200
    else:
        return jsonify(
                error='Socket address does not exist in the view',
                message='Error in DELETE'
            ), 404
#~~~~~~~~~~~~~~~~~~Key-Value operations endpoint~~~~~~~~~~~~~~~~~~~

#Need to keep causal consistency using causal metadata
#Vector clocks are recommended as causal metadata
@app.route('/key-value-store', methods=['PUT'])
def keyvalstorePUT(key):

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
            return jsonify(
                error='Value is missing',
                message='Error in PUT',
            ),400
        if len(key) > 50:
            return jsonify(
                error='Key is too long',
                message='Error in PUT',
            ),400
        #Update Value of key here
        # KeyValDict[key] = request.values.get('value')
        if key not in KeyValDict:
            KeyValDict[str(key)] = value
            eventcounter = eventcounter + 1
            return jsonify({
                'message' : 'Added successfully',
                'causal-metadata' : 'V'+eventcounter
            }), 201 #test script says this is 201
        elif key in KeyValDict:
            return jsonify({
                'message' : 'Added successfully',
                'causal-metadata' : 'V'+eventcounter
            }), 201
    elif 'SOCKET_ADDRESS' in os.environ:
        try:
            value = request.get_json()
            req = requests.put('http://'+SOCKET_ADDRESS+'/key-value-store/' + key, json=value, timeout = 10)
            return req.json(),req.status_code   
        except:
            return jsonify(
                error= 'Main instance is down', 
                message = 'Error in PUT'
                ), 503
        else:
            return req.json(),req.status_code
@app.route('/key-value-store', methods=['GET'])
def keyvalstoreGET(key):
    if SOCKET_ADDRESS is None:
        if key not in KeyValDict:
            return jsonify(
                doesExist=False,
                error='Key does not exist',
                message='Error in GET'
            ), 404
        elif key in KeyValDict:
            value = KeyValDict[str(key)]
            return jsonify({
                'message' : 'Retrieved successfully',
                'causal-metadata' : 'V'+eventcounter,
                'value' : value
            }), 200
    elif 'SOCKET_ADDRESS' in os.environ:
        try:
            req = requests.get('http://'+SOCKET_ADDRESS+'/key-value-store/' + key)
            return req.json(),req.status_code
        except:
            return jsonify(
                error= 'Main instance is down', 
                message = 'Error in GET'
                ), 503

@app.route('/key-value-store', methods=['DELETE'])
def keyvalstoreDELETE(key):
    global eventcounter
    if SOCKET_ADDRESS is None:
        if key not in KeyValDict:
            return jsonify(
                doesExist=False,
                error='Key does not exist',
                message='Error in DELETE'
            ), 404
        elif key in KeyValDict:
            del KeyValDict[str(key)]
            eventcounter = eventcounter + 1
            return jsonify(
                doesExist=True,
                message='Deleted successfully',
            ), 200
    elif 'SOCKET_ADDRESS' in os.environ:
        try:
            req = requests.delete('http://'+SOCKET_ADDRESS+'/key-value-store/' + key)
            return req.json(),req.status_code
        except:
            return jsonify(
                error= 'Main instance is down', 
                message = 'Error in DELETE'
                ), 503
app.run(host=socket.gethostbyname(socket.gethostname()),port=8085,debug=True)
