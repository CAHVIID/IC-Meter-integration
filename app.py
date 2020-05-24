############################################################################
#
# HTTP to Azure Event hub bridge
#
# 16-04-2020 1.0 Initial version    Christian Anker Hviid cah@byg.dtu.dk
# Inspired by ATEA script by Bjarke Bolding Rasmussen BBRA@atea.dk
#
# Integration bridge from IC-Meter cloud to Azure Event hub 
#
# Help sources
# http://www.ic-meter.com/dk/offentligt-api/
#
############################################################################

scriptversion = 1.0

import requests, time, json
import schedule
from datetime import datetime
from azure.eventhub import EventHubClient, EventData

cfg = json.load(open('config.json'))

# Function to save and reuse Auth-token
def saveToken(token):
    try:
        json.dump(token, open('session.tmp', 'w'))
        print('Token successfully stored')
    except Exception as e:
        raise e   

def loadToken():
    token = {}
    try:
       token = json.load(open('session.tmp'))
       return token
    except Exception as e:
        raise e

def auth():
	URL = cfg['baseURL'] + 'icm/oauth/token'
	header = {
		'Content-Type' : 'application/x-www-form-urlencoded'
	}
	body = 'client_id=trusted-client&grant_type=password&scope=read&username='+cfg['username']+'&password='+cfg['password']
	r = requests.post(URL, headers=header, data=body)
	saveToken(r.text)

def getData(buildingId,token):
	URL = cfg['baseURL'] + '/icm/api/buildings/2.0/building/indoor/'+ buildingId +'?period=10-min&resolution=minute&access_token='+token
	r = requests.get(URL)
	return r.json()

############ print

def logPrint(log):
    print(str(datetime.utcnow()) + ' UTC: ' + str(log))
     
############ numeric validation function

def is_numeric(n):
    try:
        float(n)
    except ValueError:
        return False
    else:
        return 


def sendPackage(data):
    allUnits = []
    for unit in data['units']:
        newUnit = {
			'deviceid' : unit['boxId'],
            'devicename' : unit['name'],
            'devicetype' : 'IC-Meter',
			'attributes' : {
                "location": data['name'],
                'latitude': data['latitude'],
                'longitude': data['longitude'],
				},
            'telemetry' : [],
		}
        for measure in unit['indoorMeasurements']:
            tele = {'ts': round(datetime.timestamp(datetime.strptime(measure['time'],"%Y-%m-%dT%H:%M:%SZ"))),
                    'values' : {},
					}
            for key in measure.keys():
                tele['values'][key] = measure[key]
                
            newUnit['telemetry'].append(tele)
        allUnits.append(newUnit)
    
    sendToEventHub(allUnits)

############ Send payload to Azure Event Hub function

def sendToEventHub(data):
    try:
        client = EventHubClient(
            cfg['EventHubURL'], debug=False, username=cfg['EventHubPolicyName'], password=cfg['EventHubPrimaryKey'])
        sender = client.add_sender(partition="0")
        client.run()
        try:
            count = 0
            for payload in data:
                sender.send(EventData(json.dumps(payload)))
                #logPrint("Payload sent: " + json.dumps(payload))
                count += 1
        except:
            logPrint('Send to Eventhub failed')
            raise
        finally:
            logPrint(str(count) + ' payloads sent')
            data.clear()
            client.stop()
    except KeyboardInterrupt:
        pass

def timedAuth():
	pass

def timedDataTransfer():
	token = json.loads(loadToken())
	data = getData(cfg['buildingId'],token['access_token'])
	sendPackage(data)

# Scheduled auth (eventually tweak to read token expiration)
schedule.every().day.do(auth)
# Scheduled data management - read data by GET to send to eventhub
schedule.every(int(cfg['EventHubBatchTimer'])).seconds.do(timedDataTransfer)
#schedule.every(10).minutes.do(timedDataTransfer)

auth()	#Authorize once before first run

# header
logPrint('Azure EventHub: ' + cfg['EventHubURL'])
logPrint('Configured to send payloads every ' + cfg['EventHubBatchTimer'] + ' seconds')

while True:
    schedule.run_pending()
    time.sleep(int(cfg['EventHubBatchTimer']))
    #time.sleep(1)