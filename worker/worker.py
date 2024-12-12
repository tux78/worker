import logging
# Init logging
logging.basicConfig(level = logging.INFO, format = '%(asctime)s [%(process)s] [%(levelname)s] %(name)s: %(message)s', datefmt = '[%Y-%m-%d %H:%M:%S %z]')
logger = logging.getLogger('Worker')

import asyncio
import argparse
import platform, json, traceback
from requests import Session

import websockets
import base64
from enum import Enum
from typing import NamedTuple

import parser

# Init variables
services = {}

#########################
# Helper classes
#########################
class ProcessType(Enum):
    PRODUCER = 'Producer'
    CONSUMER = 'Consumer'

#########################
# Broker class (worker)
#########################
class Broker:
    def __init__(self) -> None:
        self.connections = set()

    async def publish(self, message: str) -> None:
        for connection in self.connections:
            await connection.put(message)

    async def subscribe(self):
        connection = asyncio.Queue()
        self.connections.add(connection)
        try:
            while True:
                yield await connection.get()
        finally:
            self.connections.remove(connection)

msg_broker = Broker()
Message = NamedTuple("Message", [("topic", str), ("key", str), ("value", str)])

########################################
# Service
########################################
async def startService(process, code, params, type):

    def create_process(process, code):
        try:
            moduleContent = base64.b64decode(code)
            exec(moduleContent, globals())
        except Exception as e:
            logger.debug(e)
        return globals()[process]

    serviceName = str(asyncio.current_task().get_name())
    logger.info(f'Started service {serviceName}...')
    try:
        if type == ProcessType.CONSUMER.value:
            async with create_process(process, code)(**params) as msgProcessing:
                try:
                    async for msg in msgProcessing:
                        logger.debug('Message received...')
                        result = await parsing.parseMessage(msg)
                        logger.debug('Message parsed...')
                        if parsing.getTemplateState(result['metadata.uid']):
                            await msg_broker.publish(result)
                        logger.debug('Message published...')
                        services[serviceName]['events'] += 1
                except Exception as e:
                    logger.error(e)
                    pass
        elif type == ProcessType.PRODUCER.value:
            p = create_process(process, code)(**params)
            async for msg in msg_broker.subscribe():
                await p.publish(msg)
                logger.debug('Message published...')
                services[serviceName]['events'] += 1
            del p
    except OSError as e:
        logger.info(f'Cannot start service {serviceName}: ' + e.strerror)
    except Exception as e:
        logger.error(e)
        logger.error(''.join(traceback.format_exc()))
    finally:
        services[serviceName].update({'status': 'stopped'})
        await manager['broker'].put(get_status(True))
        logger.info(f'Stopped service {serviceName}...')

########################################
# Watchdog
########################################
async def watcher(tg):

    async def consumer(websocket):
        try:
            async for message in websocket:
                event = json.loads(message)
                match event.pop('type'):
                    case 'mapping':
                        action = event.pop('action')
                        if action == 'update':
                            parsing.updateTemplate(event)
                        elif action == 'delete':
                            parsing.deleteTemplate(event['sample_id'])
                        else:
                            logger.debug('Unknown action ' + str(action) + ' has been issued.')
                    case 'service':
                        action = event.pop('action')
                        serviceName = event.pop('serviceName')
                        serviceContent = event.pop('serviceContent')
                        if action == 'start'and not serviceName in [task.get_name() for task in tg._tasks]:
                            logger.debug('Starting service ' + serviceName + '...')
                            services.setdefault(serviceName, {'type': serviceContent['serviceType'], 'events': 0, 'status': 'starting'})
                            tg.create_task(
                                startService(
                                    process = serviceContent['serviceProcess'],
                                    code = serviceContent['serviceCode'],
                                    params = serviceContent['serviceParams'],
                                    type = serviceContent['serviceType']
                                ),
                                name = serviceName
                            )
                            services[serviceName].update({'status': 'running'})
                        elif action == 'stop' and serviceName in [task.get_name() for task in tg._tasks]:
                            logger.debug('Stopping service ' + serviceName + '...')
                            services[serviceName].update({'status': 'stopped'})
                            for t in tg._tasks:
                                if serviceName == t.get_name():
                                    t.cancel()
                                    logger.info('Service ' + serviceName + ' stopped...')
                        else:
                            logger.debug('Unknown action ' + str(action) + ' has been issued.')
                        # Update Manager with new status
                        await manager['broker'].put(get_status(True))
                    case 'worker':
                        action = event.pop('action')
                        if action == 'stop':
                            logger.debug('Stopping watchdog...')
                            logger.debug('Reason: ' + event.get('reason', 'unknown'))
                            for task in tg._tasks:
                                task.cancel()
                            logger.info('Stopped...')
                        elif action == 'getstatus':
                            logger.debug('Sending status update...')
                            await manager['broker'].put(get_status(True))
                        else:
                            logger.debug('Unknown action ' + str(action) + ' has been issued.')
        except (websockets.ConnectionClosedError, ConnectionRefusedError):
            raise asyncio.CancelledError

    async def producer(websocket):
        try:
            while True:
                message = await manager['broker'].get()
                await websocket.send(json.dumps(message))
        except (websockets.ConnectionClosedError, ConnectionRefusedError):
            raise asyncio.CancelledError

    logger.info('Started node ' + platform.node() + '...')
    logger.debug('Connecting to manager...')
    while True:
        try:
            async with websockets.connect(manager['url']) as websocket:
                await websocket.send(json.dumps({
                    'type': 'init',
                    'node': platform.node(),
                    'appID': manager['appID']
                }))

                logger.info('Connected to manager awaiting messages...')
                consumer_task = asyncio.create_task(consumer(websocket), name = "consumer")
                producer_task = asyncio.create_task(producer(websocket), name = "producer")
                done, pending = await asyncio.wait(
                    [consumer_task, producer_task],
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in pending:
                    task.cancel()
        except Exception as e:
            logger.error(e)
            logger.error('Failed to connect to manager. Retrying in 3 seconds...')
            await asyncio.sleep(3)

########################################
# Main routine
########################################
async def main():
    asyncio.current_task().set_name('Worker')
    logger.debug(f'Starting on node {platform.node()}...')

    async with asyncio.TaskGroup() as tg:
        tg.create_task(watcher(tg), name = "Watchdog")
    
    logger.info(f'Stopped on node {platform.node()}...')


# Helper functions
def cmd_args():
    parser = argparse.ArgumentParser()
    # Parse args from console
    parser.add_argument(
        '-a', '--appID',
        required = True,
        help = '(required) Application/Customer Identifier')
    parser.add_argument(
        '-m', '--manager',
        default = 'manager.vaudebe.net',
        help = 'Manager FQDN plus optional port (default: %(default)s)')
    parser.add_argument(
        '-i', '--insecure',
        default = 'wss://',
        const = 'ws://',
        dest = 'schema',
        action = 'store_const',
        help = 'Use insecure manager connection')
    parser.add_argument(
        '-c', '--disable-certificate-check',
        action = 'store_true',
        help = 'Disable certificate validation')
    parser.add_argument(
        '-d', '--debug',
        action = 'store_true',
        help = 'Enable Debbuging')
    return vars(parser.parse_args())

def get_status(worker_running):
    return {
        "type": "status",
        "status": "running" if worker_running else "stopping",
        "services": {k: {"status": v['status'], "events": v['events'], "type": v['type']} for k, v in services.items()}
    }

# Init routine
if __name__ == "__main__":

    cmd = cmd_args()

    logging.basicConfig(format='[%(asctime)s] [%(levelname)s] %(taskName)s: %(message)s')
    logger = logging.getLogger('Worker')
    if cmd['debug']:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    manager = {
        'url': cmd['schema'] + cmd['manager'] + '/api/v1/ws/workers',
        'appID': cmd['appID'],
        'disableCertificateCheck': cmd['disable_certificate_check'],
        'broker': asyncio.Queue()
    }

    parsing = parser.Parser(manager['broker'])

    asyncio.run(main(), debug = cmd['debug'])
    logger.info('Shutting down')
