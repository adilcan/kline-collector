import redis
import json

r = redis.Redis(
    host='127.0.0.1',
    port=6379, 
    password='insert password here')

def checkObject():
    k = json.loads(r.get('1502942400000').decode('utf-8'))
    print(k)
    

checkObject()
