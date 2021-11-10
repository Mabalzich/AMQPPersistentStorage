# -*- coding: utf-8 -*-
"""
Created on Sat Mar 20 14:20:04 2021
"""

import pika
import pymongo
import sys, time

def main():

    if(sys.argv[1] != "-rip" or sys.argv[3] != "-rport"):
        sys.stderr.write("Incorrect Command Line Arguments\n")

    #connecting to the channel
    username = 'root'
    password = 'toor'
    host = sys.argv[2] #should be localhost
    port = sys.argv[4] #should be 5672

    credentials = pika.PlainCredentials(username, password)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host, port, '/', credentials))
    channel = connection.channel()
    print('[Ctrl 01] – Connecting to RabbitMQ instance on ' + host + ' with port ' + str(port))
    
    #variables
    places = ['Squires', 'Goodwin', 'Library']
    subjects = ['Food','Meetings','Rooms','Classrooms','Auditorium','Noise','Seating','Wishes']

    for place in places:
        channel.exchange_declare(exchange=place, exchange_type='direct')

    for subject in subjects:
        channel.queue_declare(queue=subject)

    channel.queue_bind(exchange=places[0], queue=subjects[0], routing_key=subjects[0])
    print('[Ctrl 02] – Initialized Exchanges and Queues: %s:%s' % (places[0], subjects[0]))

    channel.queue_bind(exchange=places[0], queue=subjects[1], routing_key=subjects[1])
    print('[Ctrl 02] – Initialized Exchanges and Queues: %s:%s' % (places[0], subjects[1]))

    channel.queue_bind(exchange=places[0], queue=subjects[2], routing_key=subjects[2])
    print('[Ctrl 02] – Initialized Exchanges and Queues: %s:%s' % (places[0], subjects[2]))

    channel.queue_bind(exchange=places[1], queue=subjects[3], routing_key=subjects[3])
    print('[Ctrl 02] – Initialized Exchanges and Queues: %s:%s' % (places[1], subjects[3]))

    channel.queue_bind(exchange=places[1], queue=subjects[4], routing_key=subjects[4])
    print('[Ctrl 02] – Initialized Exchanges and Queues: %s:%s' % (places[1], subjects[4]))

    channel.queue_bind(exchange=places[2], queue=subjects[5], routing_key=subjects[5])
    print('[Ctrl 02] – Initialized Exchanges and Queues: %s:%s' % (places[2], subjects[5]))

    channel.queue_bind(exchange=places[2], queue=subjects[6], routing_key=subjects[6])
    print('[Ctrl 02] – Initialized Exchanges and Queues: %s:%s' % (places[2], subjects[6]))

    channel.queue_bind(exchange=places[2], queue=subjects[7], routing_key=subjects[7])
    print('[Ctrl 02] – Initialized Exchanges and Queues: %s:%s' % (places[2], subjects[7]))

    #create mongo db instance
    db = pymongo.MongoClient().test
    print('[Ctrl 03] – Initialized MongoDB datastore')
    
    #get command
    while(True):
        cmd = input('[Ctrl 04] –> Enter a command:  <ENTER YOUR PRODUCE / CONSUME / EXIT COMMAND> ')
        if cmd == 'exit':
            print('[Ctrl 08] – Exiting')
            #close connection
            connection.close()
            sys.exit(0)
        elif cmd[0] == 'p':
            place = cmd.split(':')[1].split('+')[0]
            if place not in places:
                print('Invalid place (Note: it is case sensitive).\n')
                continue
            subject = cmd.split('+')[1].split(' \"')[0]
            if subject not in subjects:
                print('Invalid subject (Note: it is case sensitive).\n')
                continue
            message = cmd.split('\"')[1]
                
            mongoDBInsertion(db, cmd[0], place, subject, message)
            
            #produce message
            channel.exchange_declare(exchange=place, exchange_type='direct') #to ensure
            channel.basic_publish(exchange=place,routing_key=subject,body=message)
            print('[Ctrl 06] – Produced message "%s" on %s:%s' % (message, place, subject))
            continue
        elif cmd[0] == 'c':
            place = cmd.split(':')[1].split('+')[0]
            if place not in places:
                print('Invalid place (Note: it is case sensitive).\n')
                continue
            subject = cmd.split('+')[1]
            if subject not in subjects:
                print('Invalid subject (Note: it is case sensitive).\n')
                continue

            mongoDBInsertion(db, cmd[0], place, subject, None)
            
            #consume message
            channel.queue_bind(exchange=place,queue=subject,routing_key=subject)
            response = channel.basic_get(queue=subject, auto_ack=False)

            basicGetOk, basicProperties, message = response

            if message is not None and basicGetOk is not None:
                print('[Ctrl 07] – Consumed message “%s” on <%s:%s>' % (message.decode("utf-8"), basicGetOk.exchange, basicGetOk.routing_key))

            continue
        else:
            print('Please enter a valid command\n')

def mongoDBInsertion(db, cmd, place, subject, msg):

    msgID = '17' + '$' + str(time.time())

    if msg is not None:
        command = {'Action': cmd[0], 'Place': place, 'MsgID': msgID, 'Subject': subject, 'Message': msg}
    else:
        command = {'Action': cmd[0], 'Place': place, 'MsgID': msgID, 'Subject': subject}

    db.utilization.insert_one(command)
    print('[Ctrl 05] – Inserted command into MongoDB: ' + str(command))
    
if __name__ == '__main__':
    main()