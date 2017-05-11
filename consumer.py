from __future__ import print_function
from multiprocessing import Process
from my_events.consumers import (
    AlphaConsumer,
    BetaConsumer,
    GammaConsumer,
)

if __name__ == '__main__':
    consumers = [
        AlphaConsumer('alpha'),
        BetaConsumer('beta'),
        GammaConsumer('gamma'),
    ]

    processes = [
        Process(target=consumer.run) for consumer in consumers
    ]

    try:
        for p in processes:
            p.start()

        for p in processes:
            p.join() # waits indefinitiely unless the process ends
    except KeyboardInterrupt:
        for p in processes:
            p.terminate()
            
        print('Bye!')
