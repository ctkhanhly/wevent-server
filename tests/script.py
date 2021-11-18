import requests
import json

host = ''
API_KEY = ''

def testUpdateVote(url):
    plan_id = ''
    event_id = ''
    user_id = ''

    body = {
        'update_type' : 'vote',
        'plan_id' : plan_id,
        'event_id' : event_id,
        'user_id' : user_id
    }
    
    response = requests.put(
        url,
        data=json.dumps(body)),
        headers=getHeaders()
    )

    return response

def testUpdateAddEvent(url):
    plan_id = ''
    event_id = ''

    body = {
        'update_type' : 'add_event',
        'plan_id' : plan_id,
        'event_id' : event_id
    }
    
    response = requests.put(
        url,
        data=json.dumps(body)),
        headers=getHeaders()
    )

    return response

def testUpdateAddFriend(url):
    plan_id = ''
    user_id = ''

    body = {
        'update_type' : 'add_friend',
        'plan_id' : plan_id,
        'user_id' : user_id
    }
    
    response = requests.put(
        url,
        data=json.dumps(body)),
        headers=getHeaders()
    )

    return response

def testUpdateManualTrigger(url):
    plan_id = ''
    event_id = ''

    body = {
        'update_type' : 'manual_trigger',
        'plan_id' : plan_id,
        'event_id' : event_id
    }
    
    response = requests.put(
        url,
        data=json.dumps(body)),
        headers=getHeaders()
    )

    return response


def getHeaders():
    return {
        'Authorization': f'Bearer {API_KEY}'
    }

def testUpdatePlan():
    url = host + '/updatePlan'

    response = testUpdateVote(url)
    print('======================Test Update Plan======================')
    print('======================Test Update Vote======================')
    print(json.dumps(response, indent=4, sort_keys=True))
    print('============================================')

    print('======================Test Update Add Event======================')
    response = testUpdateAddEvent(url)
    print(json.dumps(response, indent=4, sort_keys=True))
    print('============================================')

    print('======================Test Update Add Friend======================')
    response = testUpdateAddFriend(url)
    print(json.dumps(response, indent=4, sort_keys=True))
    print('============================================')

    print('======================Test Update Manual Trigger======================')
    response = testUpdateManualTrigger(url)
    print(json.dumps(response, indent=4, sort_keys=True))
    print('============================================')


def testSearchEvents():
    url = host + '/searchEvents'
    params = {}
    neighborhood = ''
    start = ''
    category = ''

    params['neighborhood'] = neighborhood
    params['start'] = start
    params['category'] = category

    response = requests.get(
        url,
        params=params,
        headers=getHeaders()
    )

    print('======================Test Search Events======================')
    print(json.dumps(response, indent=4, sort_keys=True))
    print('============================================')

def testCreatePlan():
    url = host + '/createPlan'
    name = ''
    start = ''
    trigger_option = ''
    host_id = ''

    body = {
        'name' : name,
        'start': start,
        'trigger_option': trigger_option,
        'host_id': host_id
    }

    response = requests.get(
        url,
        data=json.dumps(body),
        headers=getHeaders()
    )

    print('======================Test Search Events======================')
    print(json.dumps(response, indent=4, sort_keys=True))
    print('============================================')


if __name__ == "__main__":
    testCreatePlan()
    testSearchEvents()
    testUpdatePlan()