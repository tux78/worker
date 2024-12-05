import logging
logger = logging.getLogger('Worker.Parser')

import time, jsonpickle

from tokenizer import Tokenizer, MinerConfig

class Parser:

    def __init__(self, api):
        self.api = api
        self.templates = {}
        # support functions
        self.tokenizer = None

    def updateTemplate(self, data):
        self.templates.update(data)

    def deleteTemplate(self, data):
        # TODO: deletion of templates (samples) not properly working
        # probably full restart/reset of TemplateMiner required?
        if data['sample_id'] in self.templates:
            self.templates.pop(data['sample_id'])

    def getTemplateState(self, sample):
        return self.templates.get(sample, {'active': True})['active']

    async def getTokenizer(self):
        #TODO: review instantiation of Tokenizer class//implement using MinerConfig
        if not self.tokenizer:
            self.config : MinerConfig = jsonpickle.decode((await self.api.get('/workers/drain'))['content'].replace('service.', 'tokenizer.'), keys = True)
            self.tokenizer = Tokenizer()
        return self.tokenizer

    async def parseMessage(self, msg):
        if 'raw_data' in msg:
            payload = msg['raw_data']
            process = msg.get('metadata.log_provider', 'GENERIC')

            # tokenize sample
            change_type, sample_id, current_template, previous_sample_id, params = (await self.getTokenizer()).tokenize(process, payload)
           

            # Upload sample to manager
            if change_type != "none" or (sample_id not in self.templates):
                self.templates[str(sample_id)] = await self.api.post('/samples/' + sample_id, {
                    'process': process,
                    'change_type': previous_sample_id if change_type == 'cluster_template_changed' else change_type,
                    'template': current_template,
                    'raw': payload,
                    'tokens': [{'value': o if o else v, 'type': t} for v, t, o in params] if params else {}
                })

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
