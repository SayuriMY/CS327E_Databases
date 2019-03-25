--Query to get which stores and their location(county) sold more than 50,000 dlls of
--alcohol in 2014 and ordered by sale.
SELECT s2014.County, s2014.VolSold_L_, s2014.Sale_Dollars_, s2014.StoreNum
FROM IowaAlcoholSales.2014Sales s2014
WHERE Sale_Dollars_ > 50000
ORDER BY Sale_Dollars_ DESC;

--Query to get which stores and their location(county) sold more than 50,000 dlls of
--alcohol in 2015 and ordered by sale. R: only in polk a store sold more than 50000dlls, the interesting result
--is the same store appeared in the data a couple of times.
SELECT s2015.County, s2015.VolumeSold_L_, s2015.Sale_Dollars_, s2015.StoreNum
FROM IowaAlcoholSales.2015Sales s2015
WHERE Sale_Dollars_ > 50000
ORDER BY Sale_Dollars_ DESC;

--Get the store name, address, and county of store with highest number of sales in 2015
--by using the store number collected from the previous query. This was ordered by Name and Address.
SELECT s2015.StoreName, s2015.Address, s2015.County
FROM IowaAlcoholSales.2015Stores s2015
WHERE s2015.StoreNum = 2633
ORDER BY s2015.StoreName, s2015.Address ;

--Get Store number and address of stores with a null county from 2014Stores data table.
--Ordered by name and address
SELECT s2014.StoreName, s2014.Address, s2014.StoreNum
FROM IowaAlcoholSales.2014Stores s2014
WHERE  s2014.County IS NULL
ORDER BY  s2014.StoreName, s2014.Address ;

--The _2014_ and _2015_ years of the IowaCountiesRevocation table contain a 'NA'
--values which are making the datatype 'string'. By finding them, they can be replaced by
--a numerical value to edit the data later on. The results are ordered by County
SELECT rev.County
FROM IowaCountiesRevocations.IowaCountiesRevocations rev
WHERE  rev._2014_ = 'NA' OR rev._2015_ = 'NA'
ORDER BY  rev.County ;

--Query to get the County with an amount of alcohol impaired deaths higher than 30
--and driving deaths higher than a 100 in 2014. R: Polk County, which is the same county
--with highest number of alcohol sales in 2015. The results are ordered by County
SELECT d2014.County, d2014.Alcohol_Impaired_Driving_Deaths, d2014.Driving_Deaths
FROM IowaIAlcoholImpairedDrivingDeaths.2014Deaths d2014
WHERE  d2014.Alcohol_Impaired_Driving_Deaths > 30 AND d2014.Driving_Deaths > 100
ORDER BY  d2014.County ;

--Query to get the County with an amount of alcohol impaired deaths higher than 30
--and driving deaths higher than a 100 in 2015. Without counting the County with value 'null'
--The results are ordered by County
SELECT d2015.County, d2015.Alcohol_Impaired_Driving_Deaths, d2015.Driving_Deaths
FROM IowaIAlcoholImpairedDrivingDeaths.2015Deaths d2015
WHERE  d2015.Alcohol_Impaired_Driving_Deaths > 30 AND d2015.Driving_Deaths > 100 AND d2015.County Is NOT NULL
ORDER BY  d2015.County ;
