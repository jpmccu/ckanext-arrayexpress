from ckan.plugins.core import SingletonPlugin, implements
from ckanext.harvest.interfaces import IHarvester
import logging
import urllib2
from lxml import html, etree
import datetime
import urllib

class ArrayExpressHarvester(SingletonPlugin):
    '''
    An ArrayExpress Harvester.
    '''
    implements(IHarvester)

    QUERY_STRING=""
    API_URL="http://www.ebi.ac.uk/arrayexpress/xml/v2/experiments"
    AE_FIELDS = set([
        'accession',
        'array',
        'ef',
        'efv',
        'expdesign',
        'exptype',
        'gxa',
        'pmid',
        'sa',
        'species',
        'expandfo',
        'directsub',
        'assaycount',
        'efocount',
        'samplecount',
        'sacount',
        'rawcount',
        'fgemcount',
        'miamescore',
        'date',
        ])

    def configure(self, config):
        if 'arrayexpress.api.url' in config:
            API_URL=config['arrayexpress.api.url']
            log.debug("Setting ArrayExpress API URL to "+API_URL)
        fields = []
        for field in AE_FIELDS:
            key = 'arrayexpress.'+field
            if key in config:
                value = config[key]
                if " " in value:
                    value = '"'+value+'"'
                fields.append(field+"="+value)
        if len(fields) > 0:
            if "?" not in API_URL:
                API_URL = API_URL+"?"
            else:
                API_URL = API_URL + "&"
            API_URL = API_URL+"&".join(fields)
        log.debug("URI is "+API_URL)

    def validate_config(self,config):
        if not config:
            return config

        try:
            config_obj = json.loads(config)

            if 'default_tags' in config_obj:
                if not isinstance(config_obj['default_tags'],list):
                    raise ValueError('default_tags must be a list')

            if 'default_groups' in config_obj:
                if not isinstance(config_obj['default_groups'],list):
                    raise ValueError('default_groups must be a list')

                # Check if default groups exist
                context = {'model':model,'user':c.user}
                for group_name in config_obj['default_groups']:
                    try:
                        group = get_action('group_show')(context,{'id':group_name})
                    except NotFound,e:
                        raise ValueError('Default group not found')

            if 'default_extras' in config_obj:
                if not isinstance(config_obj['default_extras'],dict):
                    raise ValueError('default_extras must be a dictionary')

            if 'user' in config_obj:
                # Check if user exists
                context = {'model':model,'user':c.user}
                try:
                    user = get_action('user_show')(context,{'id':config_obj.get('user')})
                except NotFound,e:
                    raise ValueError('User not found')

            for key in ('read_only','force_all'):
                if key in config_obj:
                    if not isinstance(config_obj[key],bool):
                        raise ValueError('%s must be boolean' % key)
            if 'params' in config_obj:
                for param in config_obj['params']:
                    if param not in AE_FIELDS:
                        raise ValueError('%s must be a valid ArrayExpress API parameter.' % key)

        except ValueError,e:
            raise e

        return config

    def info(self):
        return {
            'name': 'arrayexpress',
            'title': 'ArrayExpress',
            'description': 'Harvester for the ArrayExpress database of functional genomics experiments.'
            }

    def getParams(self):
        return "&".join([k+"="+urllib.urlencode(self.params[k]) for k in self.params.keys()])

    def gather_stage(self,harvest_job):
        log.debug('In ArrayExpressHarvester.gather_stage(%s)' % harvest_job.source.url)
        # Get feed contents
        self._set_config(harvest_job.source.config)
        previous_job = Session.query(HarvestJob) \
                        .filter(HarvestJob.source==harvest_job.source) \
                        .filter(HarvestJob.gather_finished!=None) \
                        .filter(HarvestJob.id!=harvest_job.id) \
                        .order_by(HarvestJob.gather_finished.desc()) \
                        .limit(1).first()

        baseURL = harvest_job.source.url
        if (previous_job and not previous_job.gather_errors
            and not len(previous_job.objects) == 0):
            if not self.config.get('force_all',False):
                last_time = harvest_job.gather_started.isoformat()
                today = format(datetime.date.today())
                self.params['date'] = '['+last_time+' '+today+']'
        url = baseURL + "?" + self.getParams()
        
        doc = etree.parse(url)
        ids = []
        for accessionElements in doc.findall('//experiment/accession'):
            accession = link_element.text.strip()
            id = sha1(accession).hexdigest()
            obj = HarvestObject(guid=id, job=harvest_job, content=accession)
            log.debug("ArrayExpress accession: "+accession)
            #obj.save()
            
            #ids.append(obj.id)
        return ids

    def fetch_stage(self,harvest_object):
        log.debug('In ArrayExpressHarvester.fetch_stage')

    def import_stage(self,harvest_object):
        log.debug('In ArrayExpressHarvester.import_stage')
        return None

    def _set_config(self,config_str):
        if config_str:
            self.config = json.loads(config_str)
        else:
            self.config = {}
        if params in self.config:
            self.params = self.config['params']
        else:
            self.params = {}
