zarkov
======

Zarkov is a Lightweight Map-Reduce Framework


Zarkov is an event logger
======
Over the past few weeks I've been working on a service in Python that I'm calling, in the tradition of naming projects after characters in Flash Gordon, Zarkov. So what exactly is Zarkov? Well, Zarkov is many things (and may grow to more):

*Zarkov is an event logger
*Zarkov is a lightweight map-reduce framework
*Zarkov is an aggregation service
*Zarkov is a webservice
In the next few posts, I'll be going over each of the components of Zarkov and how they work together. Today, I'll focus on Zarkov as an event logger.



Technologies
======
So there are just a few prerequisite technologies you should know something about before working with Zarkov. I'll give a brief overview of these here.

*ZeroMQ: ZeroMQ is used for Zarkov's wire and buffering protocol all over the place. Generally you'll use PUSH sockets to send data and events to Zarkov, and REQ sockets to talk to the Zarkov map-reduce router.
*MongoDB: Zarkov uses MongoDB to store events and aggregates, so you should have a MongoDB server handy if you'll be doing anything with Zarkov. We also use Ming, an object-document mapper developed at SourceForge, to do most of our interfacing with MongoDB.
*Gevent: Internally, Zarkov uses gevent's "green threads" to keep things nice and lightweight. If you're just using Zarkov, you probably don't need to know a lot about gevent, but if you start hacking on the source code, it's all over the place (as well as special ZeroMQ and Ming adapters for gevent). So it's probably good to have at least a passing familiarity.
