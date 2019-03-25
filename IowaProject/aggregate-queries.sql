--*****USED FOR DATA VIS*****
--From the IowaAlcoholSales dataset and  2015Sales table, the query counts the number of stores per County and the total volume of alcohol sold and total sales per County. 
--The table is ordered by Total Sales in descending order
SELECT COALESCE(s2015.County,'Unassigned') AS COUNTY2015, COUNT(s2015.County) as CountyStores, ROUND(SUM(s2015.VolumeSold_L_),2) as TotalVol, ROUND(SUM(s2015.Sale_Dollars_),2) as TotalSales
FROM IowaAlcoholSales.2015Sales s2015
GROUP BY COUNTY2015
ORDER BY TotalSales DESC

--*****USED FOR DATA VIS*****
--From the IowaAlcoholSales dataset and  2014Sales table, the query counts the number of stores per County and the total volume of alcohol sold and total sales per County. 
--The table is ordered by Total Sales in descending order
SELECT COALESCE(s2014.County,'Unassigned') AS COUNTY2014, COUNT(s2014.County) as CountyStores, ROUND(SUM(s2014.VolSold_L_),2) as TotalVol, ROUND(SUM(s2014.Sale_Dollars_),2) as TotalSales
FROM IowaAlcoholSales.2014Sales s2014
GROUP BY COUNTY2014
ORDER BY TotalSales DESC

----*****USED FOR DATA VIS*****
--From the IowaAlcoholSales dataset using the 2015 Sales and Stores tables, the query calculates the Total, Bottom, Average and Top Sales From each store in their zip code
--The table is ordered by TotalSales in descending order
SELECT
  ZipCode,
  st.StoreName,
  ROUND(SUM(Sale_Dollars_),2) AS TotalSales,
  ROUND(MIN(Sale_Dollars_),2) AS BottomSales,
  ROUND(AVG(Sale_Dollars_),2) AS AverageSales,
  ROUND(MAX(Sale_Dollars_),2) AS TopSales,
  COUNT(st.StoreNum) AS NumOfStore
FROM [IowaAlcoholSales.2015Sales] s LEFT JOIN [IowaAlcoholSales.2015Stores] st
ON s.StoreNum = st.StoreNum
GROUP BY ZipCode, st.StoreName
ORDER BY TotalSales DESC;

--From the IowaAlcoholSales dataset and  2014Sales table, the query gives the min and max volume of alcohol sold per county in 2014. 
--The table is ordered by county
SELECT COALESCE(s2014.County,'Unassigned') AS COUNTY2014, MAX(s2014.VolSold_L_) as MaxVol, MIN(s2014.VolSold_L_) as MinVol
FROM IowaAlcoholSales.2014Sales s2014
GROUP BY COUNTY2014
ORDER BY COUNTY2014

--From the IowaAlcoholSales dataset and 2015Stores table, the query gives the zipcode with number of stores when the number of stores
--is higher than 15.This is ordered by zipcode in ascending order
SELECT s2015.ZipCode AS ZipCode2015, COUNT(s2015.StoreNum) as Stores
FROM IowaAlcoholSales.2015Stores s2015
GROUP BY ZipCode2015
HAVING COUNT(s2015.StoreNum) > 15
ORDER BY ZipCode2015

