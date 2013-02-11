from ckan.plugins.core import SingletonPlugin, implements
from ckanext.harvest.interfaces import IHarvester
from ckan import model
from ckan.model import Session, Package
from ckan.logic import ValidationError, NotFound, get_action
from ckan.logic.schema import default_related_schema
from ckan.lib.helpers import json
import urllib2
from lxml import html, etree
import datetime
import urllib
import re
from ckanext.harvest.model import HarvestJob, HarvestObject, HarvestGatherError, HarvestObjectError
from ckan.lib.base import c
import traceback, sys

from ckanext.harvest.harvesters.base import HarvesterBase



import logging
log = logging.getLogger(__name__)

class ArrayExpressHarvester(HarvesterBase):
    '''
    An ArrayExpress Harvester.
    '''

    QUERY_STRING=""
    API_URL="http://www.ebi.ac.uk/arrayexpress/xml/v2/experiments"
    AE_FIELDS = set([
        'keywords',
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

    def _get_content(self, url):
        http_request = urllib2.Request(url = url)

        try:
            api_key = self.config.get('api_key',None)
            if api_key:
                http_request.add_header('Authorization',api_key)
            http_response = urllib2.urlopen(http_request)

            return http_response.read()
        except Exception, e:
            raise e
    
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
                log.debug(config_obj['params'].keys())
                for param in config_obj['params'].keys():
                    log.debug(param)
                    if param not in self.AE_FIELDS:
                        raise ValueError('%s must be a valid ArrayExpress API parameter.' % param)

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
        print self.params
        return "&".join([k+"="+self.params[k]
                         for k in self.params.keys() if self.params[k] != None])

    def gather_stage(self,harvest_job):
        log.debug('In ArrayExpressHarvester.gather_stage(%s)' % harvest_job.source.url)
        # Get feed contents
        self._set_config(harvest_job.source.config)
        
        #previous_job = Session.query(HarvestJob) \
        #                .filter(HarvestJob.source==harvest_job.source) \
        #                .filter(HarvestJob.gather_finished!=None) \
        #                .filter(HarvestJob.id!=harvest_job.id) \
        #                .order_by(HarvestJob.gather_finished.desc()) \
        #                .limit(1).first()

        baseURL = harvest_job.source.url+"/xml/v2/experiments"
        #if (previous_job and not previous_job.gather_errors
        #    and not len(previous_job.objects) == 0):
        #    if not self.config.get('force_all',False):
        #        last_time = harvest_job.gather_started.isoformat()
        #        today = format(datetime.date.today())
        #        self.params['date'] = '['+last_time+' '+today+']'
        url = baseURL + "?" + self.getParams()

        print "Fetching from "+url
        doc = etree.parse(url)
        ids = []
        for accessionElement in doc.findall('//experiment/accession'):
            accession = accessionElement.text.strip()
            obj = HarvestObject(guid=accession, job=harvest_job, content=accession)
            print "ArrayExpress accession: "+accession
            obj.save()
            
            ids.append(obj.id)
        print ids
        return ids

    def fetch_stage(self,harvest_object):
        log.debug('In ArrayExpressHarvester.fetch_stage (%s)' % harvest_object.id)
        self._set_config(harvest_object.job.source.config)
        accession = harvest_object.content
        experimentURL = harvest_object.job.source.url+"/json/v2/experiments/"+accession
        filesURL = harvest_object.job.source.url+"/json/v2/files/"+accession
        content = None
        try:
            experimentStr = self._get_content(experimentURL)
            filesStr = self._get_content(filesURL)
            experiments = json.loads(experimentStr)
            files = json.loads(filesStr)
            experiment = experiments['experiments']['experiment']
            experiment['files'] = files['files']['experiment']['file']
            content = json.dumps(experiment)
        except Exception, e:
            self._save_object_error('Unable to get content for accession: %s: %r' %
                                    (accession, e), harvest_object)
            return None
        harvest_object.content = content
        harvest_object.save()
        return True

    def _collapse_notes(self, desc):
        result = []
        if desc == None:
            return ""
        for l in desc:
            if type(l) == dict:
                for k in l.keys():
                    result.append(l[k]['$'])
            else:
                result.append(l)
        return ''.join(result).encode('ascii','ignore')

    def _get_author(self,experiment):
        if type(experiment['provider']) == dict:
            return {
                'author':experiment['provider']['contact'],
                'author_email':experiment['provider']['email']
                }
        else:
            for provider in experiment['provider']:
                if provider['role'] == 'investigator':
                    return {
                        'author':provider['contact'],
                        'author_email':provider['email']
                        }
        return {}
                    
    def _get_maintainer(self,experiment):
        if type(experiment['provider']) == dict:
            return {
                'maintainer':experiment['provider']['contact'],
                'maintainer_email':experiment['provider']['email']
                }
        else:
            for provider in experiment['provider']:
                if provider['role'] == 'submitter':
                    return {
                        'maintainer':provider['contact'],
                        'maintainer_email':provider['email']
                        }
        return {}
    
    def import_stage(self,harvest_object):
        log.debug('In ArrayExpressHarvester.import_stage')
        if not harvest_object:
            log.error('No harvest object received')
            return False

        if harvest_object.content is None:
            self._save_object_error('Empty content for object %s' % harvest_object.id,
                    harvest_object, 'Import')
            return False

        self._set_config(harvest_object.job.source.config)

        try:
            experiment = json.loads(harvest_object.content)
            accession  = experiment['accession']
            packageID = re.sub("[^a-z0-9_-]+","_", accession.lower())
            author = self._get_author(experiment)
            maintainer = self._get_maintainer(experiment)
            dataset = {
                'id':harvest_object.guid,
                'name':packageID,
                'title':experiment['name'].encode('ascii','ignore'),
                'url': harvest_object.job.source.url+"/experiments/"+accession,
                'notes':self._collapse_notes(experiment['description']['text']),
                'state':'active',
                'license':'Other (Open)',
                'license_title':'Other (Open)',
                'license_id':'other-open',
                'tags':[],
                'resources':[],
                'extras':{
                    "ArrayExpress Accession":accession
                    },
                'related':[]
                }
            dataset.update(self._get_author(experiment))
            dataset.update(self._get_maintainer(experiment))
            if 'species' in experiment:
                dataset['tags'].append(experiment['species'])
            if 'experimentdesign' in experiment:
                if type(experiment['experimentdesign']) == list:
                    dataset['tags'].extend(experiment['experimentdesign'])
                else:
                    dataset['tags'].append(experiment['experimentdesign'])
            if 'experimentaltype' in experiment:
                if type(experiment['experimenttype']) == list:
                    dataset['tags'].extend(experiment['experimenttype'])
                else:
                    dataset['tags'].append(experiment['experimenttype'])
                    #if 'miamescores' in experiment:
                    #for key in experiment['miamescores'].keys():
                    #dataset['extras']['miame_'+key] = str(experiment['miamescores'][key])
            related = []
            if 'bibliography' in experiment and experiment['bibliography'] != None:
                bib = experiment['bibliography']
                if type(bib) != list:
                    bib = [bib]
                for b in bib:
                    if 'title' not in b:
                        continue
                    article = {
                        'type':'Paper',
                        'title': b['title'],
                        'dataset_id':dataset['id']
                        }
                    if 'accession' in b:
                        article['url'] = "http://www.ncbi.nlm.nih.gov/pubmed/"+str(b['accession'])
                    if 'doi' in b:
                        article['url'] = "http://dx.doi.org/"+b['doi']

                    if 'authors' in b:
                        article['description'] = 'By: '+b['authors']
                    related.append(article)
            for f in experiment['files']:
                resource = {
                    'url':f['url'],
                    'name':f['name'],
                    'type':'file',
                    'format':f['kind'],
                    'size':f['size'],
                    'last_modified':f['lastmodified']
                    }
                dataset['resources'].append(resource)
            status =  self._create_or_update_package(dataset,harvest_object)
            print status
            related = [self._create_or_update_related(r,dataset) for r in related]
            #dataset['related'] = related
            #status =  self._create_or_update_package(dataset,harvest_object)
            
        except ValidationError,e:
            self._save_object_error('Invalid package with GUID %s: %r' % (harvest_object.guid, e.error_dict),
                                    harvest_object, 'Import')
        except Exception, e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback)
            print e
            self._save_object_error('%r' % e,harvest_object,'Import')

    def _create_or_update_related(self,related,package_dict):
        #print package_dict
        # Check API version
        if self.config:
            api_version = self.config.get('api_version','2')
            #TODO: use site user when available
            user_name = self.config.get('user',u'harvest')
        else:
            api_version = '2'
            user_name = u'harvest'
        schema = default_related_schema()
        context = {
            'model': model,
            'session': Session,
            'user': user_name,
            'api_version': api_version,
            'schema': schema,
        }
        old_related = dict([(x['url'], x['id']) for x in
        get_action("related_list")(context,{"id":package_dict["id"]})])
        print "Existing:", old_related
        if related['url'] in old_related:
            related['id'] = old_related[related['url']]
            return get_action('related_update')(context, related)
        else:
            return get_action('related_create')(context, related)

    def _set_config(self,config_str):
        if config_str:
            self.config = json.loads(config_str)
        else:
            self.config = {}
        if 'params' in self.config:
            self.params = self.config['params']
        else:
            self.params = {}
