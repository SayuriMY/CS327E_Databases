import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

#DoFn to perform on each element in the input PCollection.
class FilterByPercentageImpairedDeaths(beam.DoFn):
   def process(self, element):
      record = element 

      #Get alcohol imapired percentage for that record
      percentage_deaths = record.get('Alcohol_Impaired__percentage_')

      #Select record with percentage_deaths higher or equal to 10%. 
      if percentage_deaths >= 10:
          #Get county
          county = record.get('County')

          #Get total impaired deaths
          impaired_deaths = record.get('Alcohol_Impaired_Driving_Deaths')
          
          #Make list
          new_record = {'County': county,'Alcohol_Impaired_Driving_Deaths': impaired_deaths,'Alcohol_Impaired__percentage_' : percentage_deaths}
          
          return [new_record]

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
   deaths14_pcoll = p | 'Read Deaths14' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM IowaIAlcoholImpairedDrivingDeaths.2014Deaths'))  
 
   #Use ParDo to filter PCollection and select counties where impaired deaths higher than 10%. 
   filteredCounties_pcoll = deaths14_pcoll | beam.ParDo(FilterByPercentageImpairedDeaths())

   # Write PCollection to a log file
   filteredCounties_pcoll | 'Write to File 1' >> WriteToText('deaths14.txt')
   
   #Create table in BigQuery
   qualified_table_name = 'axial-module-216302:beam.ParDo'
   table_schema = 'County:STRING,Alcohol_Impaired_Driving_Deaths:INTEGER,Alcohol_Impaired__percentage_:INTEGER'

   filteredCounties_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name,
                                                                 schema=table_schema, 
                                                                 create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
                                                                 write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))

logging.getLogger().setLevel(logging.ERROR)
