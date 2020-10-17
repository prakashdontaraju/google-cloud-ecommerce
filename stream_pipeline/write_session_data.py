import logging
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions



column_names = ['event_time','event_type','product_id','category_id','category_code','brand','price',
        'user_id','user_session','hour','category','sub_category','product','product_details']

stream_data_schema = {
    'fields': [{'name': 'event_time', 'type': 'DATETIME', 'mode': 'REQUIRED'},
    {'name': 'event_type', 'type': 'STRING', 'mode': 'REQUIRED'},
     {'name': 'product_id', 'type': 'STRING', 'mode': 'REQUIRED'},
     {'name': 'category_id', 'type': 'STRING', 'mode': 'REQUIRED'},
     {'name': 'category_code', 'type': 'STRING', 'mode': 'REQUIRED'},
     {'name': 'brand', 'type': 'STRING', 'mode': 'REQUIRED'},
     {'name': 'price', 'type': 'FLOAT64', 'mode': 'REQUIRED'},
     {'name': 'user_id', 'type': 'STRING', 'mode': 'REQUIRED'},
     {'name': 'user_session', 'type': 'STRING', 'mode': 'REQUIRED'},
     {'name': 'hour', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
     {'name': 'category', 'type': 'STRING', 'mode': 'NULLABLE'},
     {'name': 'sub_category', 'type': 'STRING', 'mode': 'NULLABLE'},
     {'name': 'product', 'type': 'STRING', 'mode': 'NULLABLE'},
     {'name': 'product_details', 'type': 'STRING', 'mode': 'NULLABLE'}
     ]
}



def getTableSpec(dataset, project):
    logging.info('Creating a BigQuery Table Spec')
    table_name = 'stream_data'
    table_spec = '{0}:{1}.{2}'.format(project, dataset, table_name)
    logging.info('Table Spec for WriteToBigQuery {0}'.format(table_spec))

    return table_spec



class TransformMessagesIntoDictionary(beam.DoFn):

    def change_message_formatting(self, record):
        time_part = record[1].split('+')[0]
        timestamp = record[0] + ' ' + time_part
        # To maintain as string
        record[0] = timestamp
        # remove + seperator and fractional digits (second precision)
        record.remove(record[1])
        # Change price to float
        record[6] = float(record[6])

        # Get Hour from event_time Datetime variable
        record.append(int(record[0][11:13]))

        # Split category code into details
        category_details = record[4].split('.')
        for detail in category_details:
            record.append(detail)

        return record


    def get_dictionary(self, record, column_names):
        record = [dict(zip(column_names, record))]
        return record


    def process(self, element, column_names):
        # logging.info('In Transform Messages into Dictionary')
        element = element.decode('utf-8')
        element = element.split(',')
        element = self.change_message_formatting(element)
        transformed_message = self.get_dictionary(element, column_names)
        return transformed_message



def run(table_spec, pipeline_args):
    # save_main_session can be set to true because some DoFn's rely on
    # globally imported modules.

    pipeline_options = PipelineOptions(
        project=pipeline_args.project, dataset=pipeline_args.dataset,
        runner=pipeline_args.runner, staging_location=pipeline_args.staging_location,
        temp_location=pipeline_args.temp_location, region=pipeline_args.region,
        save_main_session=False, streaming=True
    )

    with beam.Pipeline(options=pipeline_options) as pcoll:
        transformed = (
            pcoll
            | "Read PubSub Messages" >> beam.io.ReadFromPubSub(topic=pipeline_args.topic)
            | "Messages as Dictionary" >> beam.ParDo(TransformMessagesIntoDictionary(),
                        column_names=column_names)
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                table_spec,
                schema=stream_data_schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                )
            )



if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser(description='Write session stream to BigQuery')

    parser.add_argument(
        '--project',
        help='Example: --project $DEVSHELL_PROJECT_ID',
        required=True)

    parser.add_argument(
        '--dataset',
        help='Dataset of BigQuery table',
        required=True)
        
    parser.add_argument(
        '--topic',
        help='Path to Topic to publish messages'
        )

    parser.add_argument(
        '--runner',
        help='Example: --runner DataflowRunner',
        default='DataflowRunner')

    parser.add_argument(
        '--temp_location',
        help='Example: --staging_location gs://$PROJECT/tmp/',
        required=True)

    parser.add_argument(
        '--staging_location',
        help='Example: --staging_location gs://$PROJECT/staging/',
        required=True)

    parser.add_argument(
        '--region',
        help='Region of project'
        )
    

    pipeline_args = parser.parse_args()
    table_spec = getTableSpec(pipeline_args.dataset, pipeline_args.project)
    run(
        table_spec,
        pipeline_args
    )

    logging.info('Successfully Wrote All Streamed Messages to BigQuery')