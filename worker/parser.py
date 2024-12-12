import logging
logger = logging.getLogger('Worker.Parser')

import time, asyncio

from tokenizer import Tokenizer

class Parser:

    def __init__(self, manager):
        self.manager = manager
        self.templates = {}
        # support functions
        self.tokenizer = None

    def updateTemplate(self, data):
        self.templates.update(data)

    def deleteTemplate(self, sample_id):
        # TODO: deletion of templates (samples) not properly working
        # probably full restart/reset of TemplateMiner required?
        if sample_id in self.templates:
            self.templates.pop(sample_id)

    def getTemplateState(self, sample):
        return self.templates.get(sample, {'active': True})['active']

    async def getTokenizer(self):
        #TODO: review instantiation of Tokenizer class//implement using parameters
        if not self.tokenizer:
            self.tokenizer = Tokenizer()
        return self.tokenizer

    async def parseMessage(self, msg):
        if 'raw_data' in msg:
            payload = msg['raw_data']
            process = msg.get('metadata.log_provider', 'GENERIC')
            status = None

            # tokenize sample
            change_type, sample_id, current_template, previous_sample_id, params = (await self.getTokenizer()).tokenize(process, payload)
           
            # Upload sample to manager
            if change_type != "none" or (sample_id not in self.templates):
                # self.templates[str(sample_id)] = await self.api.post('/samples/' + sample_id, {
                await self.manager.put({
                    'type': 'update',
                    'id': sample_id,
                    'process': process,
                    'change_type': previous_sample_id if change_type == 'cluster_template_changed' else change_type,
                    'template': current_template,
                    'raw': payload,
                    'tokens': [{'value': o if o else v, 'type': t} for v, t, o in params] if params else {}
                })

            # Wait for Mapping Update
            while not self.templates.get(sample_id, None):
                await asyncio.sleep(1)

            # Prepare return value
            msg.update({
                "category_uid": self.templates[sample_id]['mappings'].get('category.uid', '-'),
                "class_uid": self.templates[sample_id]['mappings'].get('class.uid', '-'),
                "metadata.uid": sample_id,
                "metadata.version": self.templates[sample_id].get('version', {'value': '0.0.0'})['value'],
                "metadata.processed_time": time.time()
            })

            msg.update({self.templates[sample_id]['mappings'].get(str(i), "unmapped." + str(i)): v for i, (v, t, o) in enumerate(params)})

        return msg
