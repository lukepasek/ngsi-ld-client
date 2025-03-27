import requests
from keycloak import KeycloakOpenID
import json
from datetime import datetime, timedelta
import time

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# None = 'https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.8.jsonld'

COLOR_JSON = True

if COLOR_JSON:
    from pygments import highlight
    from pygments.lexers import JsonLexer
    from pygments.formatters import TerminalFormatter

class ContextBrokerClient:

    def __init__(self, base_url=None, tenant=None, add_tenant_to_path=False, keycloak_url=None, keycloak_realm=None, client_id=None, client_secret_key=None, token_grant_type="client_credentials") -> None:
        if base_url.endswith('/ngsi-ld/v1/'):
            base_url = base_url[0:-len('/ngsi-ld/v1/')]
        elif base_url.endswith('/'):
            base_url = base_url[0:-1]
        self.base_url = base_url
        self.tenant = tenant
        self.add_tenant_to_path = add_tenant_to_path

        self.session = requests.Session()

        self.token_expire = datetime.now()
        self.token = None
        if keycloak_url:
            self.keycloak_openid = KeycloakOpenID(server_url=keycloak_url, client_id=client_id, client_secret_key=client_secret_key, realm_name=keycloak_realm, verify=False)
            self.token_grant_type = token_grant_type
        else:
            self.keycloak_openid = None 

    def _get_token_token(self, get_token=None) -> str:
        if get_token:
            return get_token()
        now = datetime.now()
        if not self.token or now >= self.token_expire:    
            self.token = self.keycloak_openid.token(grant_type=self.token_grant_type)
            self.token_expire =  now + timedelta(seconds=int(self.token['expires_in'])-5)
            print ("  🔑  got new token at", now, "token expires at", self.token_expire, "(", int(self.token['expires_in']), "- 5 sec)")
        return self.token['access_token']

    def _build_headers(self, get_token=None, tenant=None, context=None, accept='application/json', extra_headers=None) -> dict:
        headers = {
            'Accept': accept,
        }
        if get_token:
            headers['Authorization'] = 'Bearer '+get_token()
        elif self.keycloak_openid:
            headers['Authorization'] = 'Bearer '+self._get_token_token()
        if tenant and not self.add_tenant_to_path:
            headers['NGSILD-Tenant'] = tenant
        if extra_headers:
            headers.update(extra_headers)
        if context:
            headers['Content-Type'] = 'application/json'
            headers['Link'] = '<'+context+'>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"'
        else:
            headers['Content-Type'] = 'application/ld+json'
        return headers

    def _build_url(self, url:str, tenant:str) -> str:
        if url.startswith('http://') or url.startswith('https://'):
            return url
        complete_url = self.base_url
        if self.add_tenant_to_path:
            complete_url += '/'+tenant
        if not url.startswith('/ngsi-ld/v1/'):
            complete_url += '/ngsi-ld/v1/' + url
        else:
            complete_url += url
        return complete_url
    
    def _print_json_data(self, json_data):
        if COLOR_JSON and len(json_data)<1024:
            print(highlight(json.dumps( json_data, indent=2), JsonLexer(), TerminalFormatter()))
        else:
            print(json.dumps( json_data, indent=2))

    def get(self, url:str, get_token=None, tenant:str=None, context:str=None, accept:str='application/ld+json', extra_headers=None, print_response:bool=True, print_request_headers:bool=False, retry:int=0, verbose=True):
        if not tenant:
            tenant = self.tenant
        if not retry:
            retry = 1
        url = self._build_url(url, tenant)

        if verbose:
            print('🔁 GET', url, 'tenant:', tenant)
        headers = self._build_headers(get_token, tenant, context, accept, extra_headers)
        if verbose and print_request_headers:
            print('--> request headers: ', headers)

        response = self.session.get(url, headers=headers)

        if verbose:
            print('<--', response.status_code, response.reason, response.elapsed.total_seconds())
        # print('<--', response.headers)

        json_data = response.json()
        if response.status_code == 200:
            # json_data = response.json()
            if print_response:
                self._print_json_data(json_data)
            return (json_data, response.content, response.headers)
        else:
            if verbose:
                print('<--', response.headers)
                if json_data:
                    self._print_json_data(json_data)
                else:
                    print('<--', response.content)

        return (None, response.content, response.headers)
    
    def post(self, url=None, get_token=None, tenant=None, context=None, accept='application/json', data=None, extra_headers=None, print_request_headers=False, retry:int=0, verbose=True):
        if not tenant:
            tenant = self.tenant
        if not retry:
            retry = 1
        url = self._build_url(url, tenant)

        if isinstance(data, bytearray):
            binary_data = data
        else:
            binary_data = json.dumps(data).encode(encoding='utf-8')

        if verbose:
            print('🔁 POST', url, 'tenant:', tenant, "size:", len(binary_data), 'bytes')
        headers = self._build_headers(get_token, tenant, context, accept, extra_headers)
        if verbose and print_request_headers:
            print('--> request headers: ', headers)

        resp_status = 0
        retry_cnt = 0

        while not (resp_status>=200 and resp_status<300) and retry>retry_cnt:
            retry_cnt += 1
            if get_token or self.keycloak_openid:
                headers['Authorization'] = 'Bearer '+ self._get_token_token(get_token)
            response = self.session.post(
                url,
                headers = headers,
                data = binary_data
            )
            resp_status = response.status_code
            if verbose:
                print('<--', response.status_code, response.reason, response.elapsed.total_seconds(), 'sec')
            if (resp_status>=200 and resp_status<300) or resp_status == 404:
                return (True, response.status_code, response.headers, None)
            else:
                last_resp_status = response.status_code
                last_resp_headers = response.headers
                last_resp_content = response.content
                if verbose:
                    print('<--', response.headers)
                    json_data = response.json
                    if json_data:
                        self._print_json_data(json_data)
                    else:
                        print('<--', response.content)
                if retry>retry_cnt:
                    delay = retry_cnt*5
                    if verbose:
                        print("🚨 retrying in", delay, "sec...")
                    time.sleep(retry_cnt*5)
        return (False, last_resp_status, last_resp_headers, last_resp_content)

    def put(self, url=None, get_token=None, tenant=None, context=None, accept='application/json', data=None, extra_headers=None, print_request_headers=False, retry:int=0, retry_delay=5, verbose=True):
        if not tenant:
            tenant = self.tenant
        if not url:
            url = self._build_url(url, tenant)
        if not retry:
            retry = 1

        print('🔁 PUT', url, 'tenant:', tenant)
        headers = self._build_headers(get_token, tenant, context, accept, extra_headers)
        if print_request_headers:
            print("--> request headers: ", headers)

        resp_status = 0
        retry_cnt = 0
        while not (resp_status>=200 and resp_status<300) and retry>retry_cnt:
            retry_cnt += 1
            if get_token or self.keycloak_openid:
                headers['Authorization'] = 'Bearer '+ self._get_token_token(get_token)
            response = self.session.put(
                url,
                headers = headers,
                data = data
            )
            resp_status = response.status_code
            print('<--', response.status_code, response.reason, response.elapsed.total_seconds(), 'sec')
            if (resp_status>=200 and resp_status<300) or resp_status == 404:
                return (True, response.status_code, response.headers, None)
            else:
                last_resp_status = response.status_code
                last_resp_headers = response.headers
                last_resp_content = response.content
                if verbose:
                    print('<--', response.headers)
                    json_data = response.json
                    if json_data:
                        self._print_json_data(json_data)
                    else:
                        print('<--', response.content)
                if retry>retry_cnt:
                    delay = retry_cnt*5
                    print("🚨 retrying in", delay, "sec...")
                    time.sleep(retry_cnt*retry_delay)
        return (False, last_resp_status, last_resp_headers, last_resp_content)
    
    def patch(self, url=None, get_token=None, tenant=None, context=None, accept='application/json', data=None, extra_headers=None, print_request_headers=False, retry:int=0):
        if not tenant:
            tenant = self.tenant
        if not url:
            url = self._build_url(url, tenant)
        if not retry:
            retry = 1

        print('🔁 PATCH', url, 'tenant:', tenant)
        headers = self._build_headers(get_token, tenant, context, accept, extra_headers)
        if print_request_headers:
            print("--> request headers: ", headers)

        resp_status = 0
        retry_cnt = 0
        while not (resp_status>=200 and resp_status<300) and retry>retry_cnt:
            retry_cnt += 1
            if get_token or self.keycloak_openid:
                headers['Authorization'] = 'Bearer '+ self._get_token_token(get_token)
            response = self.session.patch(
                url,
                headers = headers,
                data = data
            )
            resp_status = response.status_code
            print('<--', response.status_code, response.reason, response.elapsed.total_seconds(), 'sec')
            if (resp_status>=200 and resp_status<300) or resp_status == 404:
                return (True, response.status_code, response.headers, None)
            else:
                last_resp_status = response.status_code
                last_resp_headers = response.headers
                last_resp_content = response.content
                print('<--', response.headers)
                json_data = response.json
                if json_data:
                    self._print_json_data(json_data)
                else:
                    print('<--', response.content)
                if retry>retry_cnt:
                    delay = retry_cnt*5
                    print("🚨 retrying in", delay, "sec...")
                    time.sleep(retry_cnt*5)
        return (False, last_resp_status, last_resp_headers, last_resp_content)
    
    def delete(self, url=None, get_token=None, tenant=None, context=None, accept='application/json', extra_headers=None, print_request_headers=False, retry:int=0):
        if not tenant:
            tenant = self.tenant
        if not retry:
            retry = 1
        url = self._build_url(url, tenant)


        print('🔁 DELETE', url, 'tenant:', tenant)
        headers = self._build_headers(get_token, tenant, context, accept, extra_headers)
        if print_request_headers:
            print("--> request headers: ", headers)

        resp_status = 0
        retry_cnt = 0
        while not ((resp_status>=200 and resp_status<300) or resp_status == 404) and retry>retry_cnt:
            retry_cnt += 1
            if get_token or self.keycloak_openid:
                headers['Authorization'] = 'Bearer '+ self._get_token_token(get_token)
            response = self.session.delete(
                url,
                headers = headers,
            )
            resp_status = response.status_code
            print('<--', response.status_code, response.reason, response.elapsed.total_seconds(), 'sec')
            if (resp_status>=200 and resp_status<300) or resp_status == 404:
                return (True, response.status_code, response.headers, None)
            else:
                last_resp_status = response.status_code
                last_resp_headers = response.headers
                last_resp_content = response.content
                print('<--', response.headers)
                json_data = response.json
                if json_data:
                    self._print_json_data(json_data)
                else:
                    print('<--', response.content)
                if retry>retry_cnt:
                    delay = retry_cnt*5
                    print("🚨 retrying in", delay, "sec...")
                    time.sleep(retry_cnt*5)
        return (False, last_resp_status, last_resp_headers, last_resp_content)

    def ql_download_temporal_entities(tenant, entities=None, types=None):

        if not entities:
            if not types:
                types_data, _, _ = get_scorpio(SRC_SCORPIO_BASE_URL+'/ngsi-ld/v1/types', get_src_token_token, tenant=tenant, print_response=True)
                if types_data:
                    tenant_types = types_data['typeList']
            else:
                tenant_types = types

            entities = []

            for entity_type in tenant_types:
                if entity_type not in ['AirQualityObserved']:
                    entities_data, data, _ = get_scorpio(SRC_SCORPIO_BASE_URL+'/ngsi-ld/v1/entities?type='+entity_type, get_src_token_token, tenant=tenant, print_response=True)
                    with open("type_"+entity_type+'.json', "wb") as binary_file:
                        binary_file.write(data)
                    for e in entities_data:
                        entities.append(e['id'])
                        

            for entity_id in entities:
                entity_data, _, response_headers = get_scorpio(SRC_SCORPIO_BASE_URL+'/ngsi-ld/v1/entities/'+entity_id, get_src_token_token, tenant=tenant, print_response=True)

                if 'id' in entity_data and entity_data['id'] == entity_id:
                    link = response_headers.get('Link')
                    entity_template = entity_data
                    entity_type = entity_template['type']

                    ql_fetch_end = False
                    batch_size = 10000
                    batch_offset = 0
                    record_count = 0
                    while not ql_fetch_end:
                        temporal_data, data, response_headers = get_scorpio(SRC_QL_BASE_URL+'/v2/entities/'+entity_id+"?last_n="+str(batch_size)+"&offset="+str(batch_offset), get_src_token_token, tenant=tenant, print_response=False)
                        if not temporal_data:
                            break
                        count = len(temporal_data['index'])
                        with open("ql_data_"+entity_id.replace(':', '_')+"-"+str(batch_offset)+'-'+str(batch_offset+count)+'.json', "wb") as binary_file:
                            binary_file.write(data)
                        record_count += count
                        batch_offset += batch_size
                        print(count, record_count)
                        if count<batch_size:
                            ql_fetch_end = True
                    continue

    def get_types(self, context=None, print_response=False):
        json_data, _, _ = self.get('types', context=context, print_response=print_response)
        return json_data

    def get_entities_by_type(self, type_name, context=None, sdm_model=None, attrs=None, print_response=False):
        request_path = 'entities?type='+type_name
        if attrs:
            if isinstance(attrs, str):
                request_path += '&attrs='+attrs
            elif isinstance(attrs, list):
                request_path += '&attrs='+','.join(attrs)
        if not context and sdm_model:
            context = self.sdm_model_to_context(sdm_model)
        json_data, _, _ = self.get(request_path, context=context, print_response=print_response)
        return json_data

    def get_entity(self, entity_id, context=None, sdm_model=None, attrs=None, print_response=False):
        request_path = 'entities/'+entity_id
        if attrs:
            if isinstance(attrs, str):
                request_path += '?attrs='+attrs
            elif isinstance(attrs, list):
                request_path += '?attrs='+','.join(attrs)
        
        if not context and sdm_model:
            context = self.sdm_model_to_context(sdm_model)
        json_data, _, _ = self.get(request_path, context=context, print_response=print_response)
        return json_data

    def get_temporal_entities_by_type(self, type_name, context=None, sdm_model=None, attrs=None, last_n=1000, format='concise', from_time=None, to_time=None, query_params=None, print_response=False):
        request_path = 'temporal/entities?type='+type_name+'&lastN='+str(last_n)
        if format:
            request_path += '&format='+format
        if attrs:
            if isinstance(attrs, str):
                request_path += '&attrs='+attrs
            elif isinstance(attrs, list):
                request_path += '&attrs='+','.join(attrs)
        if query_params:
            for qpk, qpv in query_params.items():
                request_path += '&'+qpk+'='+qpv
        if from_time and to_time:
            request_path += '&timerel=between'+'&timeAt='+from_time+'&endTimeAt='+to_time
        elif from_time: 
            request_path += '&timerel=after'+'&timeAt='+from_time
        elif to_time:
            request_path += '&timerel=before'+'&timeAt='+to_time
        if not context and sdm_model:
            context = self.sdm_model_to_context(sdm_model)
        if not context and sdm_model:
            context = self.sdm_model_to_context(sdm_model)
        json_data, _, _ = self.get(request_path, context=context, print_response=print_response)
        return json_data

    def get_temporal_entity(self, entity_id, context=None, sdm_model=None, attrs=None, last_n=1000, format='concise', from_time=None, to_time=None, query_params=None, print_response=False):
        request_path = 'temporal/entities/'+entity_id+'?lastN='+str(last_n)
        if format:
            request_path += '&format='+format
        if attrs:
            if isinstance(attrs, str):
                request_path += '&attrs='+attrs
            elif isinstance(attrs, list):
                request_path += '&attrs='+','.join(attrs)
        if query_params:
            for qpk, qpv in query_params.items():
                request_path += '&'+qpk+'='+qpv
        if from_time and to_time:
            request_path += '&timerel=between'+'&timeAt='+from_time+'&endTimeAt='+to_time
        elif from_time: 
            request_path += '&timerel=after'+'&timeAt='+from_time
        elif to_time:
            request_path += '&timerel=before'+'&timeAt='+to_time
        if not context and sdm_model:
            context = self.sdm_model_to_context(sdm_model)
        json_data, _, _ = self.get(request_path, context=context, print_response=print_response)
        return json_data

    def sdm_type_to_context(self, type_name):
        if type_name.startswith('https://smartdatamodels.org/'):
            model_name, short_type_name = type_name.replace('https://smartdatamodels.org/', '').split('/', 2)
            return (short_type_name, 'https://raw.githubusercontent.com/smart-data-models/'+model_name+'/master/context.jsonld')
        return (type_name, None)
    
    def sdm_model_to_context(self, model_name):
        return 'https://raw.githubusercontent.com/smart-data-models/dataModel.'+model_name+'/master/context.jsonld'

