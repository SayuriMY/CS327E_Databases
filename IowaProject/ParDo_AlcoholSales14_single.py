''' The goal of this pipeline is to apply ParDo to perform a series of modification to the IowaAlcoholSales dataset:
1)Substitute the null value with the correct county using the StoreNum. 
2)Make sure all county names are in uppercase. 
3)Calculate the total volume sold per county.
4)Calculate the total number of stores per county.'''

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

#Make sales tuples
class SalesTuples(beam.DoFn):
    def process(self, element):
       record = element

       #Get County name and volume of alcohol sold from record
       county = record.get('County')
       volume = record.get('VolSold')

       #Make tuple
       sale_tuple = (county, volume)

       return[sale_tuple]

#Make number stores tuple
class StoresTuples(beam.DoFn):
    def process(self, element):
        record = element

        #Get county name and number of stores from record
        county = record.get('County')
        stores = record.get('NumStores')

        #Make tuple
        store_tuple = (county, stores)

        return[store_tuple]

#Edit grouped Pcollection
class FormatPcoll(beam.DoFn):
    def process(self, element):
        record = element
        
        #Get county, volume and number of stores
        county = record[0]
        info = record[1]
        volume = info.get('VolSold')[0]
        stores = info.get('NumStores')[0]
        
        #Make new record
        new_record = {'County': county, 'VolSold': volume, 'NumStores': stores}
         
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
   sales14_pcoll = p | 'Read Sales14' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT County, round(sum(VolSold_L_),2) as VolSold, FROM IowaAlcoholSales.2014Sales WHERE County IS NOT NULL GROUP BY County'))  
   
   stores14_pcoll = p | 'Read Stores14' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT County, COUNT(distinct StoreNum) as NumStores FROM IowaAlcoholSales.2014Sales WHERE County IS NOT NULL GROUP BY County'))

   #Normaliza PCollection using ParDo.
   normalized_sales14_pcoll = sales14_pcoll | 'Normalizing County' >> beam.ParDo(NormalizeCounty())

   normalized_stores14_pcoll = stores14_pcoll | 'Normalizing County 2' >> beam.ParDo(NormalizeCounty())

   #Make tuples from PCollection using PArDo.
   sales = normalized_sales14_pcoll | 'Making sales tuples' >> beam.ParDo(SalesTuples())
   stores = normalized_stores14_pcoll | ' Making stores tuples' >> beam.ParDo(StoresTuples())

   #Use CoGroupByKey to combine both PCollections 
   grouped_pcoll = ({'VolSold':sales, 'NumStores':stores} | 'Combine sales and stores 14' >> beam.CoGroupByKey())

   #Use ParDo to format grouped_pcoll 
   formated_pcoll = grouped_pcoll | 'Formatting PCollection' >> beam.ParDo(FormatPcoll())

   #Write PCollection in txt file.
   #grouped_pcoll | 'Write to File ' >> WriteToText('county_sales14.txt')
   #sales | 'Write to File' >> WriteToText('sales.txt')
   #stores | 'Write to File2' >> WriteToText('vol.txt')

   #Create table in BigQuery
   qualified_table_name = 'axial-module-216302:beam.ParDo_AlcoholSales14_single'
   table_schema = 'County:STRING,VolSold:FLOAT,NumStores:INTEGER'

   formated_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name,
                                                                 schema=table_schema, 
                                                                 create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, 
                                                                 write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))

logging.getLogger().setLevel(logging.ERROR)
