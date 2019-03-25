--This query gives the itemnum sold, storenum, the sales in dlls, store name, and zipcode in descending order by sale. 
SELECT s2014.StoreNum, s2014.ItemNum, ROUND(s2014.Sale_Dollars_,1) as SaleDlls, st2014.StoreName, st2014.ZipCode
FROM [IowaAlcoholSales.2014Sales] s2014 LEFT OUTER JOIN [IowaAlcoholSales.2014Stores] st2014
ON s2014.StoreNum = st2014.StoreNum
ORDER BY s2014.Sale_Dollars_ DESC

--This query gives the itemnum sold, storenum, the sales in dlls, store name, and zipcode in descending order by zipcode
SELECT s2015.StoreNum, s2015.ItemNum, ROUND(s2015.Sale_Dollars_,1) as SaleDlls, st2015.StoreName, st2015.ZipCode
FROM [IowaAlcoholSales.2015Sales] s2015 LEFT OUTER JOIN [IowaAlcoholSales.2015Stores] st2015
ON s2015.StoreNum = st2015.StoreNum
ORDER BY st2015.ZipCode DESC

--This query shows the total sales from the different zipcodes and counties. 
SELECT COALESCE(s2014.County,'Unassigned') AS COUNTY2014, st2014.ZipCode, ROUND(SUM(s2014.Sale_Dollars_),2) AS TOTALSALES
FROM [IowaAlcoholSales.2014Sales] s2014 LEFT JOIN [IowaAlcoholSales.2014Stores] st2014
ON s2014.StoreNum = st2014.StoreNum
GROUP BY COUNTY2014, st2014.ZipCode
ORDER BY COUNTY2014, st2014.ZipCode

--This query was used to create a view of the total volume of alcohol (L) and total sales (dlls) in Stores in Iowa in 2014.
--https://cloud.google.com/bigquery/docs/views
SELECT S2014.StoreNum, ROUND(SUM(s2014.VolSold_L_),2) as TotalVol, ROUND(SUM(s2014.Sale_Dollars_),2) as TotalSales
FROM [IowaAlcoholSales.2014Sales] s2014
GROUP BY s2014.StoreNum
ORDER BY s2014.StoreNum

--This query was used to create a view of the total volume of alcohol (L) and total sales (dlls) in Stores in Iowa in 2015.
--https://cloud.google.com/bigquery/docs/views
SELECT S2015.StoreNum, ROUND(SUM(s2015.VolumeSold_L_),2) as TotalVol, ROUND(SUM(s2015.Sale_Dollars_),2) as TotalSales
FROM [IowaAlcoholSales.2015Sales] s2015
GROUP BY s2015.StoreNum
ORDER BY s2015.StoreNum

--This query has two Joins. The first, selects the stores with lower volume of alcohol sold in 2015 than 2014, but who still made a higher 
--total sales number (Dlls). While the second join, selects those stores in the 2014 store table to get the name of the store selected on the
--previous join. These stores were order in descending order according the total sales. 
SELECT s2014.StoreNum, st2014.StoreName ,s2014.TotalVol, s2014.TotalSales
FROM [IowaAlcoholSales.SalesList2014] s2014 JOIN [IowaAlcoholSales.SalesList2015] s2015 ON s2014.StoreNum = s2015.StoreNum
LEFT OUTER JOIN [IowaAlcoholSales.2014Stores] st2014 ON s2014.StoreNum = st2014.StoreNum
WHERE s2015.TotalSales > s2014.TotalSales AND s2015.TotalVol < s2014.TotalVol
ORDER BY s2014.TotalSales DESC

--This query select those stores who's Total sales were less than 10,000 dlls in 2014 and 2015. It gives the storenum, store name, totalvol,
--and total sales. 
SELECT s2014.StoreNum, st2014.StoreName ,s2014.TotalVol, s2014.TotalSales
FROM [IowaAlcoholSales.SalesList2014] s2014 JOIN [IowaAlcoholSales.SalesList2015] s2015 ON s2014.StoreNum = s2015.StoreNum
LEFT OUTER JOIN [IowaAlcoholSales.2014Stores] st2014 ON s2014.StoreNum = st2014.StoreNum
WHERE (s2014.TotalSales < 10000 AND s2015.TotalSales < 10000)
ORDER BY s2014.TotalSales DESC
