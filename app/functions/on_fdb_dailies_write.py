from app import init_gcp_logger


def on_fdb_dailies_write(event, context):
    """Triggered by a change to a Firestore document.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    gcp_logger = init_gcp_logger()
    gcp_logger.info('on_fdb_dailies_write()')
    event_id = context.event_id
    event_type = context.event_type
    resource = context.resource

    resource = 'projects/firefire-gcp-1/databases/(default)/documents/test_sites/6408091979/dailies/20190331'

    doc_path = resource.split('/documents/')[1]
    day_id = doc_path.split('/')[-1]

    gcp_logger.info(
        'event_id=%s, event_type=%s, resource=%s, context=%s, event=%s, day_id=%s', event_id, event_type, resource, context, event, day_id)

    return ('', 200)
