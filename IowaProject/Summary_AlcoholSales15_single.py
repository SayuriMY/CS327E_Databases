''' The goal of this pipeline is to apply ParDo to perform a series of modification to the IowaAlcoholSales dataset:
1)Remove null values
2)Make sure all county names are in uppercase.
3)Calculate the information per County'''

import logging
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

#Project Id is needed for bigquery data source, even with local execution.
options = {
        'project': 'axial-module-216302'
}

opts = beam.pipeline.PipelineOptions(flags=[], **options)

#construct a pipeline object and set configuration options
#(pipeline runner that will execute pipeline --> DirectRunner)
#(contains project ID: axial-module-216302)
with beam.Pipeline('DirectRunner', options=opts) as p:

   #Create PCollection from Big Query dataset.
   sales15_pcoll = p | 'Read Sales14' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT County, round(sum(VolumeSold_L_),2) AS VolumeL, sum(BottlesSold) AS Bottles, round(sum(Sale_Dollars_),2) AS Sales, COUNT(Distinct StoreNum) AS NumStores FROM IowaAlcoholSales.2015Sales WHERE County IS NOT NULL GROUP BY County'))

   #Normaliza PCollection using ParDo.
   normalized_sales15_pcoll = sales15_pcoll | 'Normalizing County' >> beam.ParDo(NormalizeCounty())

   #Create table in BigQuery
   qualified_table_name = 'axial-module-216302:IowaAlcoholSales.SummaryAlcoholSales15'
   table_schema = 'County:STRING,VolumeL:FLOAT,Bottles:INTEGER,Sales:FLOAT,NumStores:INTEGER'

   normalized_sales15_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name,
                                                                 schema=table_schema,
                                                                 create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                                 write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))

logging.getLogger().setLevel(logging.ERROR)
