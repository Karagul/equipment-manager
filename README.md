# Equipment Manager

 This is a project is used to find the last known `status`, `event` or `location` of a container. 

## Data
 The data in this project is being loaded from csv files which are generated in Qlik.

 The way we retrieve the data is via an SQL script.

 The data is loaded everyday, once a day.

### Sources
 - `GOS`
 - `Greencat`
 - `Doris Iceland`
 - `Doris SMCL`

#### Collecion
 Once the data is loaded into csv files and placed in the respective drive it is collected by a Python script.

#### Processing
 Once the data has been read into a Python script it is processed by the same script.
 
 From here we are able to break-down the data and cross reference from each individual system.

 This allows us to find the most recent behaviour of a container.
