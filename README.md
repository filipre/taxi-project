# Taxi Project

This Spark project finds out which NY taxi trips have potential return trips. Assume someone takes a cab Taxi1 from A to B and another cab Taxi2 from B to A. Following conditions qualifies a "return trip":

- Someone takes Taxi2 after Taxi1
- Taxi1's dropoff location and Taxi2's pickup location are not furhter than 100m away.
- Taxi2's dropoff location and Taxi1's pickup location are not further than 100m away as well.
- Time between Taxi1's dropoff time and Taxi2's pickup time is not more than 8h.

## Result

Analyzing 10906858 taxi rides, 5964009 rides could be possible return trips. Although this number sounds huge, keep in mind that for one ride to location B, multiple return rides back to A are possible.  

## Dataset

You can download the dataset [here](http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml). I used the data from January, 2016.
