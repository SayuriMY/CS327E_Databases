--This Query Shows stores that have sales above the average amount of sales from 2014
SELECT s2014.StoreNum, ROUND(s2014.Sale_Dollars_,2) as Sales, st2014.StoreName, st2014.ZipCode 
FROM 
    (SELECT StoreNum, Sale_Dollars_ FROM IowaAlcoholSales.2014Sales WHERE Sale_Dollars_ >
      (SELECT AVG(Sale_Dollars_) AS Avg FROM IowaAlcoholSales.2014Sales))
      s2014 LEFT OUTER JOIN IowaAlcoholSales.2014Stores st2014
ON s2014.StoreNum = st2014.StoreNum
ORDER BY s2014.Sale_Dollars_ DESC

--Stores that have TOTAL sales above the average TOTAL sales from 2014
SELECT s2014.StoreNum, ROUND(s2014.TotalSales,2) as Sales, st2014.StoreName, st2014.ZipCode 
FROM 
    (SELECT StoreNum, TotalSales FROM `axial-module-216302.IowaAlcoholSales.SalesList2014` WHERE TotalSales >
      (SELECT AVG(TotalSales) AS Avg FROM `axial-module-216302.IowaAlcoholSales.SalesList2014`))
      s2014 LEFT OUTER JOIN `axial-module-216302.IowaAlcoholSales.2014Stores` st2014
ON s2014.StoreNum = st2014.StoreNum
ORDER BY s2014.TotalSales DESC
       
--This Query determines the which year a Store Sold more and the difference between the 2 years
SELECT s2014.StoreNum,
    CASE WHEN SUM(s2014.TotalSales) > SUM(s2015.TotalSales) THEN 2014 ELSE 2015 END AS HigherSales,
    CASE WHEN SUM(s2014.TotalSales) > SUM(s2015.TotalSales) THEN ROUND(SUM(s2014.TotalSales)-SUM(s2015.TotalSales),2) ELSE ROUND(SUM(s2015.TotalSales)-SUM(s2014.TotalSales),2) END AS Difference
FROM (SELECT s2014.StoreNum, ROUND(SUM(s2014.Sale_Dollars_),2) as TotalSales FROM `axial-module-216302.IowaAlcoholSales.2014Sales` as s2014 GROUP BY s2014.StoreNum) s2014 
      JOIN 
     (SELECT s2015.StoreNum, ROUND(SUM(s2015.Sale_Dollars_),2) as TotalSales FROM `axial-module-216302.IowaAlcoholSales.2015Sales` as s2015 GROUP BY s2015.StoreNum) s2015 
       ON s2014.StoreNum = s2015.StoreNum
LEFT OUTER JOIN `axial-module-216302.IowaAlcoholSales.2014Stores` st2014 ON s2014.StoreNum = st2014.StoreNum
GROUP BY s2014.StoreNum
       
--This Query Shows the stores in 2014 that have higher sales than the AVG of sales in 2015 and lower Total Volume than 2015's AVG
SELECT s2014.StoreNum, st2014.StoreName ,s2014.TotalVol, s2014.TotalSales
FROM `axial-module-216302.IowaAlcoholSales.SalesList2014` s2014 LEFT OUTER JOIN `axial-module-216302.IowaAlcoholSales.2014Stores` st2014 ON s2014.StoreNum = st2014.StoreNum
WHERE (SELECT AVG(TotalSales) FROM `axial-module-216302.IowaAlcoholSales.SalesList2015`) > s2014.TotalSales 
    AND (SELECT AVG(TotalVol) FROM `axial-module-216302.IowaAlcoholSales.SalesList2015`) < s2014.TotalVol
ORDER BY s2014.TotalSales DESC
       
-- This query shows the information of Stores that exist in 2015, but not in 2014. 
SELECT * 
FROM `IowaAlcoholSales.2015Stores` 
WHERE StoreNum NOT IN
(SELECT StoreNum FROM `IowaAlcoholSales.2014Stores`)
ORDER BY storeNum

-- This query shows the information of Stores that existed in 2014, but not in 2015. 
SELECT * 
FROM `IowaAlcoholSales.2014Stores` 
WHERE StoreNum NOT IN
(SELECT StoreNum FROM `IowaAlcoholSales.2015Stores`)
ORDER BY storeNum

--Query showing the store with a sale higher than avg total sales of 2014.
SELECT s2014.StoreNum, ROUND(s2014.Sale_Dollars_,2) as Sales, s2014.VolSold_L_ ,st2014.StoreName, st2014.ZipCode 
FROM 
    (SELECT StoreNum, Sale_Dollars_, VolSold_L_ FROM `axial-module-216302.IowaAlcoholSales.2014Sales` WHERE Sale_Dollars_ >
      (SELECT AVG(TotalSales) AS Avg FROM `axial-module-216302.IowaAlcoholSales.SalesList2014`))
      s2014 LEFT OUTER JOIN `axial-module-216302.IowaAlcoholSales.2014Stores` st2014
ON s2014.StoreNum = st2014.StoreNum
ORDER BY s2014.Sale_Dollars_ DESC

 --Query showing all items sold by store from previous query
Select *
FROM `IowaAlcoholSales.2014Sales` 
WHERE StoreNum =
(SELECT StoreNum FROM `IowaAlcoholSales.Store2014HSAvg`)
ORDER BY Sale_Dollars_ DESC
