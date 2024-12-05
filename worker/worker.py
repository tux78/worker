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
# Api class
#########################
class Api:

    def __init__(self, api_host = 'manager', api_port = '8000', appID = None):
        self.api = f'https://{api_host}:{api_port}/api/v1'
        self.wss = f'wss://{api_host}:{api_port}/api/v1/ws/workers'
        self.session = Session()
        logger.debug('Setting up manager session...')
        result = self.session.post('https://' + api_host + ':' + api_port + '/', json = {'logonType': 'worker', 'appID': appID}, verify = False)
        if result.status_code == 403:
            logger.error('Authentication to manager failed: please verify appID')
            self.session = None
        else:
            logger.debug('Authentication to manager successful.')

    async def get(self, endpoint):
        if self.session:
            result = self.session.get(self.api + endpoint, verify=False)
            return result.json()
        else:
            logger.debug('No connection to manager')
            raise ConnectionError

    async def post(self, endpoint, content):
        if self.session:
            result = self.session.post(self.api + endpoint, json = content, verify = False)
            return result.json()
        else:
            logger.debug('No connection to manager')
            raise ConnectionError

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

broker = Broker()
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
                            await broker.publish(result)
                        logger.debug('Message published...')
                        services[serviceName]['events'] += 1
                except:
                    pass
        elif type == ProcessType.PRODUCER.value:
            p = create_process(process, code)(**params)
            async for msg in broker.subscribe():
                await p.publish(msg)
                logger.debug('Message published...')
                services[serviceName]['events'] += 1
            del p
    except OSError as e:
        logger.info(f'Cannot start service {serviceName}: ' + e.strerror)
    except Exception as e:
        logger.info(''.join(traceback.format_exc()))
    finally:
        services[serviceName].update({'status': 'stopped'})
        logger.info(f'Stopped service {serviceName}...')

########################################
# Watchdog
########################################
async def watcher(tg):
    logger.info('Started on node ' + platform.node() + '...')
    worker_running = True
    import ssl
    sslCTX = ssl.SSLContext(protocol=ssl.PROTOCOL_TLS_CLIENT)
    sslCTX.check_hostname = False
    sslCTX.verify_mode = ssl.CERT_NONE

    while worker_running:
        try:
            logger.debug('Connecting to manager...')
            async with websockets.connect(api.wss, ssl = sslCTX) as websocket:
                appID = await websocket.recv()
                logger.info(appID)
                logger.info('Connected to manager awaiting messages...')
                await websocket.send(json.dumps(get_status(worker_running)))
                async for message in websocket:
                    node, cmd, data = (v for v in json.loads(message).values())
                    if node and node != platform.node():
                        continue
                    match cmd:
                        case 'controlSample':
                            action = data.pop('action')
                            if action == 'update':
                                parsing.updateTemplate(data)
                            elif action == 'delete':
                                parsing.deleteTemplate(data)
                            else:
                                logger.debug('Unknown action ' + str(action) + ' has been issued.')
                        case 'controlService':
                            action = data.pop('command')
                            svc = data.pop('service')
                            if action == 'start'and not svc in [task.get_name() for task in tg._tasks]:
                                logger.debug('Starting service ' + svc + '...')
                                services.setdefault(svc, {'type': data['serviceType'], 'events': 0, 'status': 'running'})
                                tg.create_task(startService(process = data['serviceProcess'], code = data['serviceCode'], params = data['serviceParams'], type = data['serviceType']), name = svc)
                            elif action == 'stop' and svc in [task.get_name() for task in tg._tasks]:
                                logger.debug('Stopping service ' + svc + '...')
                                services[svc].update({'status': 'stopped'})
                                for t in tg._tasks:
                                    if svc == t.get_name():
                                        t.cancel()
                                        logger.info('Service ' + svc + ' stopped...')
                            else:
                                logger.debug('Unknown action ' + str(action) + ' has been issued.')
                        case 'controlWorker':
                            action = data.pop('command')
                            if action == 'stop':
                                logger.debug('Stopping watchdog...')
                                for task in tg._tasks:
                                    task.cancel()
                                logger.info('Stopped...')
                                worker_running = False
                            else:
                                logger.debug('Unknown action ' + str(action) + ' has been issued.')
                    logger.debug('Sending status update...')
                    await websocket.send(json.dumps(get_status(worker_running)))
        except (websockets.ConnectionClosedError, ConnectionRefusedError):
            logger.error('Connection Error...retrying')
            await asyncio.sleep(3)
        except (websockets.exceptions.InvalidStatusCode):
            logger.error('Connection Error (InvalidStatusCode)...')
            worker_running = False

########################################
# Main routine
########################################
async def main():
    asyncio.current_task().set_name('Worker')
    logger.debug(f'Starting on node {platform.node()}...')

    if api.session:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(watcher(tg), name = "Watchdog")
    else:
        logger.debug('No connection to manager. Aborting...')
    logger.info(f'Stopped on node {platform.node()}...')


# Helper functions
def cmd_args(args = None):
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--process', nargs='+', action='append', metavar=('NAME TYPE', 'additional arguments'))
    # Parse args from manager
    if args:
        return vars(parser.parse_args(args))
    # Parse args from console
    parser.add_argument('-m', '--manager', required=True, nargs=3, metavar=('IP/FQDN', 'PORT', 'appID'))
    return vars(parser.parse_args())

def get_status(worker_running):
    return {
        "node": platform.node(),
        "status": "running" if worker_running else "stopping",
        "services": {k: {"status": v['status'], "events": v['events'], "type": v['type']} for k, v in services.items()}
    }

# Init routine
if __name__ == "__main__":

    cmd = cmd_args()
    manager = cmd['manager']
#    for name, processType, *args in cmd['process']:
#        add_service(name, processType, {arg.split("=")[0]: arg.split("=")[1] for arg in args})

    api_log_level = logging.DEBUG
    logging.basicConfig(format='[%(asctime)s] [%(levelname)s] %(taskName)s: %(message)s')
    logger = logging.getLogger('Worker')
    if api_log_level:
        logger.setLevel(api_log_level)

    api = Api(*manager)
    parsing = parser.Parser(api)

    asyncio.run(main(), debug = (api_log_level == logging.DEBUG))
    logger.info('Shutting down')
