# AMQPPersistentStorage
Learned to use the AMQP messaging protocol with a persistent storage aspect.

Created an AMQP server with multiple exchanges and multiple queues. The program takes input from the terminal and will publish or consume a message accordingly. Messages being published are routed to queues via the place and subject in their description. Similarly, messages from queues are consumed based off of users' input.
All actions are inserted into the MongDB database and since the storage is persistent, this data will be saved in the database even after program has stopped execution.
