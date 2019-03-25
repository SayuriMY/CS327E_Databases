''' The goal of this pipeline is to apply ParDo to perform a series of modification to the IowaAlcoholSales dataset:
1)Remove null values
2)Make sure all county names are in uppercase.
3)Calculate the information per County'''

import logging,os,datetime
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

#DoFn to perform on each element in the input PCollection.
#This function makes all counties name uppercase.
class NormalizeCounty(beam.DoFn):
   def process(self, element):
       record = element

       #Get county name and volume sole from record
       county = record.get('County')

       #Convert county name to uppercase.
       county = county.upper()

       #Update county value in record
       record['County'] = county

       return[record]

PROJECT_ID = os.environ['PROJECT_ID']
BUCKET = os.environ['BUCKET']
DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

#Run pipeline on Dataflow
options = {
        'runner': 'DataflowRunner',
        'job_name':'transform-alcohol-sales14',
        'project': PROJECT_ID,
        'temp_location': BUCKET + '/temp',
        'staging_location': BUCKET + '/staging',
        'machine_type': 'n1-standard-8',
        'num_workers': 12
}

opts = beam.pipeline.PipelineOptions(flags=[], **options)

#construct a pipeline object and set configuration options
#(pipeline runner that will execute pipeline --> DirectRunner)
#(contains project ID: axial-module-216302)
with beam.Pipeline('DataflowRunner', options=opts) as p:

   #Create PCollection from Big Query dataset.
   sales14_pcoll = p | 'Read Sales14' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT County, round(sum(VolSold_L_),2) AS VolumeL, sum(BottlesSold) AS Bottles, round(sum(Sale_Dollars_),2) AS Sales, COUNT(Distinct StoreNum) AS NumStores FROM IowaAlcoholSales.2014Sales WHERE County IS NOT NULL GROUP BY County'))

   #Normaliza PCollection using ParDo.
   normalized_sales14_pcoll = sales14_pcoll | 'Normalizing County' >> beam.ParDo(NormalizeCounty())

   #Create table in BigQuery
   qualified_table_name = PROJECT_ID + ':IowaAlcoholSales.SummaryAlcoholSales14'
   table_schema = 'County:STRING,VolumeL:FLOAT,Bottles:INTEGER,Sales:FLOAT,NumStores:INTEGER'

   normalized_sales14_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name,
                                                                 schema=table_schema,
                                                                 create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                                 write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
logging.getLogger().setLevel(logging.ERROR)
