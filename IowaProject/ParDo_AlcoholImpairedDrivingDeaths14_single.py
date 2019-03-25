import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

class FormatTable(beam.DoFn):
   def process(self, element):
      record = element
      
      #Get values from record
      county = record.get('County')
      impD = record.get('Alcohol_Impaired_Driving_Deaths')
      deaths = record.get('Driving_Deaths')
      percentage = record.get('Alcohol_Impaired__percentage_')
      z = record.get('Z_Score')
      
      county = county.upper()
      
      new_record = {'County':county,'ImpDeaths':impD,'DrivingDeaths':deaths,'ImpDeathsP':percentage,'ZScore':z}

      return[new_record]

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
   deaths_pcoll = p | 'Read Deaths14' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM IowaIAlcoholImpairedDrivingDeaths.2014Deaths WHERE County IS NOT NULL'))  
 
   #Use ParDo to filter PCollection and select counties where impaired deaths higher than 10%. 
   formated_pcoll = deaths_pcoll | beam.ParDo(FormatTable())

   # Write PCollection to a log file
   formated_pcoll | 'Write to File 1' >> WriteToText('deaths14.txt')
   
   #Create table in BigQuery
   qualified_table_name = 'axial-module-216302:beam.ParDo_AlcoholImpairedDrivingDeaths14'
   table_schema = 'County:STRING,ImpDeaths:INTEGER,DrivingDeaths:INTEGER,ImpDeathsP:INTEGER,ZScore:FLOAT'

   formated_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name,
                                                                 schema=table_schema, 
                                                                 create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
                                                                 write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
logging.getLogger().setLevel(logging.ERROR)
