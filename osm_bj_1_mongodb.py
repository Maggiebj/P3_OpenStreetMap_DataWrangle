#!/usr/bin/env python
# -*- coding: utf-8 -*-


from pprint import pprint
from pymongo import MongoClient
import pymongo
import json
db_name='osm'

client = MongoClient('localhost:27017')
db = client[db_name]
print db.beijingfull.find().count()

##correct address:city to BEIJING if it not a correct city name
element=db.beijingfull.find({'$and':[{'address.city':{'$ne':'beijing'}},{'address.city':{
'$ne':'Beijing'}},{'address.city':{'$exists':1}},{'address.city':{'$ne':'北京'}}
,{'address.city':{'$ne':'北京市'}}]},{'_id':0,'id':1,'address.city':1})
print 'address.city need to correct:\n'
for e in element:
	pprint (e)
db.beijingfull.update({'$and':[{'address.city':{'$ne':'beijing'}},{'address.city':{
'$ne':'Beijing'}},{'address.city':{'$exists':1}},{'address.city':{'$ne':'北京'}}
,{'address.city':{'$ne':'北京市'}}]},{'$set':{'address.city':'BEIJING'}},multi=True)
print "address:city have been corrected to 'BEIJING'."



##correct address:postcode from 100XX to 1000XX
postcode=db.beijingfull.find({'address.postcode':{'$regex':'^[0-9]{0,5}$'}},{'address.postcode':1,'id':1})
for p in postcode:
	print "id{} postcode {} not correct".format(p['id'],p['address'])
	if p['address']['postcode'].find('100')>=0:
		p['address']['postcode']=p['address']['postcode'].replace('100','1000')
		db.beijingfull.save(p)
		print 'id{} postcode is corrected to {}'.format(p['id'],p['address'])
	

##correct color value with hex code
element=db.beijingfull.find({'roof:colour':'red'},{'_id':0,'id':1,'roof:colour':1})
for e in element:
	pprint (e)
db.beijingfull.update({'roof:colour':'red'},{'$set':{'roof:colour':'#ff0000'}},multi=True)
print "roof:colour red have been corrected."
element=db.beijingfull.find({'roof:colour':'orange'},{'_id':0,'id':1,'roof:colour':1})
for e in element:
	pprint (e)
db.beijingfull.update({'roof:colour':'orange'},{'$set':{'roof:colour':'#ffa500'}},multi=True)
print "roof:colour orange have been corrected."
element=db.beijingfull.find({'roof:colour':'green'},{'_id':0,'id':1,'roof:colour':1})
for e in element:
	pprint (e)
db.beijingfull.update({'roof:colour':'green'},{'$set':{'roof:colour':'#00ff00'}},multi=True)
print "roof:colour green have been corrected."
element=db.beijingfull.find({'roof:colour':'blue'},{'_id':0,'id':1,'roof:colour':1})
for e in element:
	pprint (e)
db.beijingfull.update({'roof:colour':'blue'},{'$set':{'roof:colour':'#0000ff'}},multi=True)
print "roof:colour blue have been corrected."
element=db.beijingfull.find({'roof:colour':'brown'},{'_id':0,'id':1,'roof:colour':1})
for e in element:
	pprint (e)
db.beijingfull.update({'roof:colour':'brown'},{'$set':{'roof:colour':'#a52a2a'}},multi=True)
print "roof:colour brown have been corrected."
element=db.beijingfull.find({'roof:colour':'black'},{'_id':0,'id':1,'roof:colour':1})
for e in element:
	pprint (e)
db.beijingfull.update({'roof:colour':'black'},{'$set':{'roof:colour':'#000000'}},multi=True)
print "roof:colour black have been corrected."
element=db.beijingfull.find({'roof:colour':'white'},{'_id':0,'id':1,'roof:colour':1})
for e in element:
	pprint (e)
db.beijingfull.update({'roof:colour':'white'},{'$set':{'roof:colour':'#ffffff'}},multi=True)
print "roof:colour white have been corrected."
element=db.beijingfull.find({'$or':[{'roof:colour':'grey'},{'roof:colour':'gray'}]},{'_id':0,'id':1,'roof:colour':1})
for e in element:
	pprint (e)
db.beijingfull.update({'$or':[{'roof:colour':'grey'},{'roof:colour':'gray'}]},{'$set':{'roof:colour':'#808080'}},multi=True)
print "roof:colour grey and gray have been corrected."

print "Number of nodes:\n",db.beijingfull.find({'type':'node'}).count()
print "Number of ways:\n",db.beijingfull.find({'type':'way'}).count()
print "Number of unique users:\n",len(db.beijingfull.distinct('created.uid'))
print "Top 10 contributing users:\n"
pipeline=[
{'$group':{'_id':{'uid':'$created.uid','name':'$created.user'},'count':{'$sum':1}}},
{'$sort':{'count':-1}},
{'$limit':10}]
top10user=db.beijingfull.aggregate(pipeline)
for t in top10user:
	print (t)
print "Number of users appearing least"
pipeline=[
{'$group':{'_id':{'uid':'$created.uid','user':'$created.user'},'count':{'$sum':1}}},
{'$group':{'_id':'$count','num_users':{'$sum':1}}},
{'$sort':{'_id':1}},
{'$limit':1}
]
num1post=db.beijingfull.aggregate(pipeline)
for t in num1post:
	print (t)

print "top 10 appearing amenities:"
pipeline=[
{"$match":{"amenity":{"$exists":1}}}, 
{"$group":{"_id":"$amenity","count":{"$sum":1}}}, 
{"$sort":{"count":-1}}, 
{"$limit":10}
]
amenities=db.beijingfull.aggregate(pipeline)
for t in amenities:
	print (t)

print "most popular cuisines:"
pipeline=[
{"$match":{"amenity":{"$exists":1}, "amenity":"restaurant",'cuisine':{'$exists':1}}},
{"$group":{"_id":"$cuisine", "count":{"$sum":1}}},
{"$sort":{"count":-1}},
{"$limit":5}
]
cuisine=db.beijingfull.aggregate(pipeline)
for t in cuisine:
	print t

##find how many years the data have been created.	
from sets import Set	
year=Set([])
element=db.beijingfull.find({'created.timestamp':{'$exists':1}})
for e in element:
	
	year.add(e['created']['timestamp'][0:e['created']['timestamp'].find('-')])
print year
print '{} total docs from 2007'.format(db.beijingfull.find().count())
print '{} docs created in 2016'.format(db.beijingfull.find({'created.timestamp':{'$gte':'2016-01-01T00:00:00Z'}}).count())
print '{} docs created in 2015'.format(db.beijingfull.find({'created.timestamp':{'$gte':'2015-01-01T00:00:00Z','$lte':'2015-12-31T23:59:59Z'}}).count())
