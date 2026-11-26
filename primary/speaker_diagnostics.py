"""Fixed read-operation and transport metadata; no HTTP headers, URLs or XML."""
REASONS=frozenset(('http_transport','http_deadline','http_encoding','http_body_limit',
 'xml_size_or_encoding','invalid_xml','xml_complexity','response_field','soap_rejected',
 'soap_response_shape','peer_identity','topology_membership','topology_uuid',
 'topology_coordinator','topology_boot_sequence','rendering_state'))
ENUMS={
 'speaker_peer':frozenset(('target','coordinator')),
 'speaker_service':frozenset(('DeviceDescription','ZoneGroupTopology','AVTransport','RenderingControl')),
 'speaker_read_action':frozenset(('GetDeviceDescription','GetZoneGroupState','GetMediaInfo',
   'GetTransportInfo','GetVolume','GetMute')),
 'speaker_http_phase':frozenset(('request','response_headers','response_body')),
 'speaker_transport_reason':frozenset(('timeout','connection_refused','connection_reset',
   'remote_disconnected','http_protocol','os_error'))}

def safe_record(value):
    result={}
    if type(value)is not dict:return result
    for key,choices in ENUMS.items():
        item=value.get(key)
        if type(item)is str and item in choices:result[key]=item
    reason=value.get('speaker_read_reason')
    if type(reason)is str and reason in REASONS:result['speaker_read_reason']=reason
    status=value.get('speaker_http_status')
    if type(status)is int and 100<=status<=599:result['speaker_http_status']=status
    return result

def read_failure(error,operation):
    record=safe_record(operation)
    record.update(safe_record(getattr(error,'http_diagnostics',None)))
    code=error.args[0] if len(error.args)==1 else None
    if type(code)is str and code in REASONS:record['speaker_read_reason']=code
    return record

def diagnostic_fields(error):
    result=safe_record(getattr(error,'read_diagnostics',None))
    value=getattr(error,'readback_diagnostics',None)
    if type(value)is dict:
        count=value.get('readback_failed_reads')
        if type(count)is int and 0<=count<=100000:result['readback_failed_reads']=count
        records=value.get('readback_read_failures')
        if type(records)is list and len(records)<=2:
            result['readback_read_failures']=[safe_record(row) for row in records if type(row)is dict]
    return result
