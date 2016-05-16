# Big-Data-Analysis-Rodent-Inspection-in-NYC 

<p>
    Rodents and other pests are often a big problem for businesses in New York City. Pest complaints often lead to poor Health Inspection grades, poor customer satisfaction, and poor food hygiene. The goal of this project is to track their locations in NYC neighboorhoods by using the New York City 311 complaint data and sorting it for rodent inspections. Once we find the neighborhoods that rodents often congregate, the project will focus on creating a heatmap of the most common locations for rodents in the city. Hopefully, when businesses move to other neighboorhoods, they will use this data to see whether or not the area is optimal due to high rodent populations. 

</p>


## Commands used to run this big data project:

### Command for Subway: <br>
./submit_Subway.sh subway.py /gws/classes/bdma/ccny/groups/8/311_Service_Requests_from_2010_to_Present.csv output output.txt 32

### Command for Building: <br>
./submit_Building.sh Mn_BuildingAge_Rat.py /gws/classes/bdma/ccny/groups/8/311_Service_Requests_from_2010_to_Present.csv outputMn outputMn.txt 22

### Command to mereg csv files:
cat *.csv >merge.csv

### statement to read multiple csv files into one Rdd:
Rdd = sc.textFile("Restautant_311/*.csv")   // Restautant_311 folder contains all the csv files


##Contributors:
Khadeeja Din <br>
Xue Wei Fan<br>
Davide Libi-Bourne<br>
John Settineri
