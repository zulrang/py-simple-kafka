from messages import Consumer


consumer = Consumer('python-test')
for msg in consumer.listen():
    print(msg)
