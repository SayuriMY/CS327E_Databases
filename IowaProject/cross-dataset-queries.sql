--Show volume of alcohol sold per store, number of stores, number of revocation and alcohol impaired deaths
--of counties were alcohol impaired deaths is more then 35% in 2014.
create view IowaAlcoholSales.Counties35PImpairedDeaths14 AS
select sd14.county, ImpDeathsP, VolPerStore, NumStores, _2014_ as Revocations
from (Select d14.county, ImpDeathsP, VolPerStore, NumStores
from (Select county, ImpDeathsP
from `axial-module-216302.IowaIAlcoholImpairedDrivingDeaths.AlcoholImpairedDrivingDeaths14`
where ZScore >= 1.13) d14 LEFT JOIN (select county, round((VolumeL/NumStores),2) as VolPerStore, NumStores
from `axial-module-216302.IowaAlcoholSales.SummaryAlcoholSales14`) s14
on d14.county = s14.county) sd14 LEFT JOIN `axial-module-216302.IowaCountiesRevocations.DriverLicenseRevocations` r14
on sd14.county = r14.county

--Show volume of alcohol sold per store, number of stores, number of revocation and alcohol impaired deaths
--of counties were alcohol impaired deaths is more then 35% in 2014.
create view IowaAlcoholSales.Counties35PImpairedDeaths15 AS
select sd15.county, ImpDeathsP, VolPerStore, NumStores, _2015_ as Revocations
from (Select d15.county, ImpDeathsP, VolPerStore, NumStores
from (Select county, ImpDeathsP
from `axial-module-216302.IowaIAlcoholImpairedDrivingDeaths.AlcoholImpairedDrivingDeaths15`
where ZScore >= 0.9) d15 LEFT JOIN (select county, round((VolumeL/NumStores),2) as VolPerStore, NumStores
from `axial-module-216302.IowaAlcoholSales.SummaryAlcoholSales15`) s15
on d15.county = s15.county) sd15 LEFT JOIN `axial-module-216302.IowaCountiesRevocations.DriverLicenseRevocations` r15
on sd15.county = r15.county

--Query to calculate if there is a correlation between the volume of alcohol sold per store and Alcohol Impaired Driving Deaths in 2014.
--Rho was 0.0607.
SELECT round(( 1 - ( (6*sum(d2))/ (count(county) * ((count(county)*count(county))-1)))),4) as rho
FROM (SELECT vol_rank.county, Volume_rank, Imp_Deaths_rank, (vol_rank.Volume_rank - death_rank.Imp_Deaths_rank) as d, (vol_rank.Volume_rank - death_rank.Imp_Deaths_rank)*(vol_rank.Volume_rank - death_rank.Imp_Deaths_rank) as d2
FROM (SELECT county, row_number() over() as Volume_rank
FROM (select county, (VolumeL/NumStores) as VolPerStore from `IowaAlcoholSales.SummaryAlcoholSales14` order by VolPerStore)) vol_rank
LEFT JOIN (SELECT county, row_number() over() as Imp_Deaths_rank
FROM (select county, ImpDeathsP from `IowaIAlcoholImpairedDrivingDeaths.ParDo_AlcoholImpairedDrivingDeaths14` order by ImpDeathsP)) death_rank
ON vol_rank.county = death_rank.county)

--Query to create a scatter plot between alcohol impaired deaths and alcohol sales in 2014.
Create view IowaAlcoholSales.CorrelationImpDeaths_Sales AS
select s14.county, round((VolumeL/NumStores),2) as VolPerStore, d14.ImpDeathsP
from `axial-module-216302.IowaAlcoholSales.SummaryAlcoholSales14` s14
LEFT JOIN `axial-module-216302.IowaIAlcoholImpairedDrivingDeaths.AlcoholImpairedDrivingDeaths14` d14
on s14.county = d14.county
order by VolPerStore

--Query to calculate if there is a correlation between the volume of alcohol sold per store and Alcohol Impaired Driving Deaths in 2015.
--Rho was 0.0274.
SELECT round(( 1 - ( (6*sum(d2))/ (count(county) * ((count(county)*count(county))-1)))),4) as rho
FROM (SELECT vol_rank.county, Volume_rank, Imp_Deaths_rank, (vol_rank.Volume_rank - death_rank.Imp_Deaths_rank) as d, (vol_rank.Volume_rank - death_rank.Imp_Deaths_rank)*(vol_rank.Volume_rank - death_rank.Imp_Deaths_rank) as d2
FROM (SELECT county, row_number() over() as Volume_rank
FROM (select county, (VolumeL/NumStores) as VolPerStore from `IowaAlcoholSales.SummaryAlcoholSales15` order by VolPerStore)) vol_rank
LEFT JOIN (SELECT county, row_number() over() as Imp_Deaths_rank
FROM (select county, ImpDeathsP from `IowaIAlcoholImpairedDrivingDeaths.ParDo_AlcoholImpairedDrivingDeaths15` order by ImpDeathsP)) death_rank
ON vol_rank.county = death_rank.county)

--Query to create a scatter plot between alcohol impaired deaths and alcohol sales in 2015.
Create view IowaAlcoholSales.CorrelationImpDeaths_Sales15 AS
select s15.county, round((VolumeL/NumStores),2) as VolPerStore, d15.ImpDeathsP
from `axial-module-216302.IowaAlcoholSales.SummaryAlcoholSales15` s15
LEFT JOIN `axial-module-216302.IowaIAlcoholImpairedDrivingDeaths.AlcoholImpairedDrivingDeaths15` d15
on s15.county = d15.county
order by VolPerStore

--Query to calculate if there is a correlation between the Driver License Revocations and Alcohol Sales in 2014.
--Rho was 0.8654.
SELECT round(( 1 - ( (6*sum(d2))/ (count(county) * ((count(county)*count(county))-1)))),4) as rho
FROM (SELECT rev_rank.county, revocation_rank, Volume_rank, (rev_rank.revocation_rank - vol_rank.Volume_rank) as d, (rev_rank.revocation_rank - vol_rank.Volume_rank)*(rev_rank.revocation_rank - vol_rank.Volume_rank) as d2
FROM (SELECT county, row_number() over() as revocation_rank
FROM (select county, _2014_ as revocations from `IowaCountiesRevocations.DriverLicenseRevocations` order by revocations)) rev_rank
LEFT JOIN  (SELECT county, row_number() over() as Volume_rank
FROM (select county, VolumeL from `IowaAlcoholSales.SummaryAlcoholSales14` order by VolumeL)) vol_rank
ON rev_rank.county = vol_rank.county)

--Query to create a scatter plot between driver license revocations and alcohol sales in 2014.
Create view IowaAlcoholSales.CorrelationRev_Sales14 AS
select s14.county, VolumeL, _2014_
from `axial-module-216302.IowaAlcoholSales.SummaryAlcoholSales14` s14
LEFT JOIN `axial-module-216302.IowaCountiesRevocations.DriverLicenseRevocations`  r14
on s14.county = r14.county
order by VolumeL

--Query to calculate if there is a correlation between the Driver License Revocations and Alcohol Sales in 2015.
--Rho was 0.8621.
SELECT round(( 1 - ( (6*sum(d2))/ (count(county) * ((count(county)*count(county))-1)))),4) as rho
FROM (SELECT rev_rank.county, revocation_rank, Volume_rank, (rev_rank.revocation_rank - vol_rank.Volume_rank) as d, (rev_rank.revocation_rank - vol_rank.Volume_rank)*(rev_rank.revocation_rank - vol_rank.Volume_rank) as d2
FROM (SELECT county, row_number() over() as revocation_rank
FROM (select county, _2015_ as revocations from `IowaCountiesRevocations.DriverLicenseRevocations` order by revocations)) rev_rank
LEFT JOIN  (SELECT county, row_number() over() as Volume_rank
FROM (select county, VolumeL from `IowaAlcoholSales.SummaryAlcoholSales15` order by VolumeL)) vol_rank
ON rev_rank.county = vol_rank.county)

--Query to create a scatter plot between driver license revocations and alcohol sales in 2015.
Create view IowaAlcoholSales.CorrelationRev_Sales15 AS
select s15.county, VolumeL, _2015_
from `axial-module-216302.IowaAlcoholSales.SummaryAlcoholSales15` s15 LEFT JOIN `axial-module-216302.IowaCountiesRevocations.DriverLicenseRevocations`  r15
on s15.county = r15.county
order by VolumeL

--Query utilized to create the histograms of Alcohol Impaired Driving Deaths Dataset
SELECT * FROM (SELECT count(ImpDeathsP) as frequency, bucket
FROM (SELECT * FROM (SELECT ImpDeathsP, CASE WHEN ImpDeathsP >=  0 AND ImpDeathsP < 5 THEN 10
                        WHEN ImpDeathsP >= 5 AND ImpDeathsP < 10 THEN 11
                        WHEN ImpDeathsP >= 10 AND ImpDeathsP < 15 THEN 12
                        WHEN ImpDeathsP >= 15 AND ImpDeathsP < 20 THEN 13
                        WHEN ImpDeathsP >= 20 AND ImpDeathsP < 25 THEN 14
                        WHEN ImpDeathsP >= 25 AND ImpDeathsP < 30 THEN 15
                        WHEN ImpDeathsP >= 30 AND ImpDeathsP < 35 THEN 16
                        WHEN ImpDeathsP >= 35 AND ImpDeathsP < 40 THEN 17
                        WHEN ImpDeathsP >= 40 AND ImpDeathsP < 45 THEN 18
                        WHEN ImpDeathsP >= 45 AND ImpDeathsP < 50 THEN 19
                        WHEN ImpDeathsP >= 50 AND ImpDeathsP < 55 THEN 20
                        WHEN ImpDeathsP >= 55 AND ImpDeathsP < 60 THEN 21
                        WHEN ImpDeathsP >= 60 AND ImpDeathsP < 101 THEN 22
                        ELSE -1 END as bucket
FROM [IowaIAlcoholImpairedDrivingDeaths.AlcoholImpairedDrivingDeaths14])) Group by bucket) hist14 LEFT JOIN
(SELECT count(ImpDeathsP) as frequency, bucket FROM
(SELECT * FROM (SELECT ImpDeathsP, CASE WHEN ImpDeathsP >=  0 AND ImpDeathsP < 5 THEN 10
                        WHEN ImpDeathsP >= 5 AND ImpDeathsP < 10 THEN 11
                        WHEN ImpDeathsP >= 10 AND ImpDeathsP < 15 THEN 12
                        WHEN ImpDeathsP >= 15 AND ImpDeathsP < 20 THEN 13
                        WHEN ImpDeathsP >= 20 AND ImpDeathsP < 25 THEN 14
                        WHEN ImpDeathsP >= 25 AND ImpDeathsP < 30 THEN 15
                        WHEN ImpDeathsP >= 30 AND ImpDeathsP < 35 THEN 16
                        WHEN ImpDeathsP >= 35 AND ImpDeathsP < 40 THEN 17
                        WHEN ImpDeathsP >= 40 AND ImpDeathsP < 45 THEN 18
                        WHEN ImpDeathsP >= 45 AND ImpDeathsP < 50 THEN 19
                        WHEN ImpDeathsP >= 50 AND ImpDeathsP < 55 THEN 20
                        WHEN ImpDeathsP >= 55 AND ImpDeathsP < 60 THEN 21
                        WHEN ImpDeathsP >= 60 AND ImpDeathsP < 101 THEN 22
                        ELSE -1 END as bucket
FROM [IowaIAlcoholImpairedDrivingDeaths.AlcoholImpairedDrivingDeaths15])) Group by bucket) hist15 ON hist14.bucket = hist15.bucket

--Query utilized to create histogram of alcohol sales in 2014.
select count(VolumeL) as frequency ,integer(((bucket-10)*500)) as bar
from (Select VolumeL , (INTEGER((VolumeL/NumStores) /500) + 10) as bucket
from IowaAlcoholSales.SummaryAlcoholSales14 )
group by bar
order by bar

--Query utilized to create histogram of alcohol sales in 2015.
select count(VolumeL) as frequency ,integer(((bucket-10)*500)) as bar
from (Select VolumeL , (INTEGER((VolumeL/NumStores)/500) + 10) as bucket
from IowaAlcoholSales.SummaryAlcoholSales15  )
group by bar
order by bar

--Query utilized to create histogram of driver license revocations in 2014
select count( _2014_) as frequency ,((bucket-10)*100) as bar
from (Select _2014_ , (INTEGER(_2014_/100) + 10) as bucket
from IowaCountiesRevocations.DriverLicenseRevocations )
group by bar

--Query utilized to create histogram of driver license revocations in 2015
select count( _2015_) as frequency ,((bucket-10)*100) as bar
from (Select _2015_ , (INTEGER(_2015_/100) + 10) as bucket
from IowaCountiesRevocations.DriverLicenseRevocations )
group by bar
