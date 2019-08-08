from messages import Message, flush


for x in range(10):
    msg = Message('python-test', name='producer', version='0.9', data={'value': x})
    msg.send()

flush()
