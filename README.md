# distributed_systems
distributed systems homeworks


1. For the first iteration, the delay = 2 seconds. This can be seen in the logs of the secondary_1.

2. For the first iteration, the delay is random value from such list delay_time= [30,60,90,120]. This is an emulation to show that for each message the delay may be different, and the messages are stored in a different order than they were sent, but as a result, when all are processed, the order is correct

3. For emulating inaccesibility of secondary node, in 3rd iteration, command: /bin/sh -c "sleep 300 && python secondary_2.py" is using, so secondary_2 is inaccesible for 5 minutes after master and secondary_1 started, but anyway correctly recieve messages. Depending on the w parameter, the client is blocked until it receives a responses. When w = 1, the client is available for writing immediately

3.1 Moved additional features for the third iteration into a separate folder because heartbeat logging is cluttering the logs. Both 'Heartbeats' and 'Quorum append' are implemented into the logic. command: /bin/sh -c "sleep 120 && python secondary_2.py" - I reduced it to 2 minutes to wait less time for the status - healthy