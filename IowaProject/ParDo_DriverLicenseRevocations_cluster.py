import logging,os,datetime
import apache_beam as beam 
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

class NormalizeRev(beam.DoFn):
    def process(self, element, revocation_pcoll):
        record = element

        #Get county 
        county = record.get('COUNTY')
        if county != 'NON-RESIDENT':
           #Get 2014 revocations
           y14 = record.get('_2014_')
           if y14 != 'NA':
              #Remove comas and convert to integer
              vals = y14.split(',')
              if len(vals) > 1:
                 y14 =int(vals[0] + vals[1])
              else:
                  y14 = int(y14)
           #Substitute the NA to 0
           else: 
               y14 = 0

           #Get 2015 revocations
           y15 = record.get('_2015_')
           if y15 != 'NA':
              #Remove comas and convert to integer
              vals2 = y15.split(',')
              if len(vals2) > 1:
                 y15 = int(vals2[0] + vals2[1])
              else:
                  y15 = int(y15)
           #Substitute NA to 0
           else:
               y15 = 0
        
           new_record = {'County':county, '_2014_': y14, '_2015_':y15}
           return [new_record]

PROJECT_ID = os.environ['PROJECT_ID']
BUCKET = os.environ['BUCKET']
DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

#Run pipeline on Dataflow
options = {
        'runner': 'DataflowRunner',
        'job_name':'transform-license-revocations',
        'project': PROJECT_ID,
        'temp_location': BUCKET + '/temp',
        'staging_location': BUCKET + '/staging',
        'machine_type': 'n1-standard-8',
        'num_workers': 12
        }

opts = beam.pipeline.PipelineOptions(flags=[], **options)

with beam.Pipeline('DataflowRunner', options=opts) as p:

    #Create PCollection from BigQuery
    revocation_pcoll = p | 'Read Revocations' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM IowaCountiesRevocations.IowaCountiesRevocations'))

    #Apply a ParDo to the PCollection for Normalization
    normalizedRev_pcoll = revocation_pcoll | 'Normalize revocation' >> beam.ParDo(NormalizeRev(), beam.pvalue.AsList(revocation_pcoll))

    #Write PCollection to a log file
    normalizedRev_pcoll | 'Write to File' >> WriteToText('normalizedRevocations.txt')

    #Create Table in BigQuery 
    qualified_table_name = PROJECT_ID + ':IowaCountiesRevocations.ParDo_DriverLicenseRevocations'
    table_schema = 'County:STRING,_2014_:INTEGER,_2015_:INTEGER'

    normalizedRev_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                               schema=table_schema, 
                                                               create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
                                                               write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))

#logging.getLogger().setLevel(logging.ERROR)
