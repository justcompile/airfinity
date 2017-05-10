from __future__ import print_function
import os
from my_events.consumers import (
    AlphaConsumer,
    BetaConsumer,
    GammaConsumer,
)

consumer_map = {
    'alpha': AlphaConsumer('alpha'),
    'beta': BetaConsumer('beta'),
    'gamma': GammaConsumer('gamma')
}


topic = os.environ['TOPIC']

consumer = consumer_map[os.environ['TOPIC']]

print ('Start consuming: ', topic)
consumer.run()
