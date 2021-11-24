from tap_campaign_monitor.streams.base import ChildStream

import singer
LOGGER = singer.get_logger()

class CampaignSegmentsStream(ChildStream):
    KEY_PROPERTIES = ['CampaignID', 'ListID', 'SegmentID', 'Title']

    TABLE = 'campaign_segments'
    REQUIRES = ['campaigns']

    def get_parent_id(self, parent):
        return parent.get('CampaignID')

    def incorporate_parent_id(self, obj, parent):
        obj['CampaignID'] = self.get_parent_id(parent)
        return obj

    def get_api_path_for_child(self, parent):
        return (
            '/campaigns/{}/listsandsegments.json'
            .format(self.get_parent_id(parent)))

    def get_stream_data(self, result):
        return self.transform_record(result)

    def sync_data(self, parent=None):
        if parent is None:
            raise RuntimeError('Cannot sync a subobject of null!')

        table = self.TABLE
        url = (
            'https://api.createsend.com/api/v3.2{api_path}'.format(
                api_path=self.get_api_path_for_child(parent)))

        response = self.client.make_request(url, self.API_METHOD)

        if 'Segments' in response:
            # Take the first Segment in the object
            if len(response['Segments']) > 0:
                res_segment = {'SegmentID': response['Segments'][0]['SegmentID'], 
                               'ListID': response['Segments'][0]['ListID'],
                               'Title': response['Segments'][0]['Title']
                              }
                result = {"Results": [res_segment]}
            else:
                result = {"Results": []}

        data = self.get_stream_data(result)

        with singer.metrics.record_counter(endpoint=table) as counter:
            singer.write_records(
                table,
                [self.incorporate_parent_id(data, parent)])

            counter.increment()