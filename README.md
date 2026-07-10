# Netflix_Azure_Data_Engineering_Project

Project overview :

step 1: ingesting data from github to azure datalake bronze layer using data factory
step 1.1 : Creating to linked services for connecting ADF with source(github) and Datalake.

step 2: creating data pipeline in ADF
step 2.1: use copy data from moving and transforming data

Step 2.2 : source : Create Dataset_github using parameterized url for accessing all csv files automatically from source
type :HTTP connection
base url: github raw url
url : advanced> get the dataset > add parameters
parameters: add file_name (String)
connection: add dynamic url >> /abhirupdas99/Azure_Data_Engineering_Project_End_to_End/refs/heads/main/RawData_AND_Notebooks/@{dataset().file_name}

step 2.3: Sink : Destination : Set as Datalake Gen2 (format: csv , linked servicename : netflixdatalakeconnection , browse: select bronze for the data to be stored ) we will feed the directory using parameters (@folder_name , @file_name )

step 2.3 add a parameter at the pipeline :
name: p_array
type :array
value :
[
{
"folder_name": "netflix_cast",
"file_name": "netflix_cast.csv"
},
{
"folder_name": "netflix_category",
"file_name": "netflix_category.csv"
},
{
"folder_name": "netflix_countries",
"file_name": "netflix_countries.csv"
},
{
"folder_name": "netflix_directors",
"file_name": "netflix_directors.csv"
}
]

(((Note:>>
we will directly send all contents of these 4 folders from github (source) to directly bronze layer via ADF . Whereas The Netflix_titles and it contents will be imported from raw layer to bronze layer through autoloader.)))

step 2.4>> create an iterative element:For-each loop for reading all the files from source
add the parameter created earlier

step 2.4 : For the for-each loop in the settings add the array created as parameter.

Step 2.5 : cut and paste the copy data pipeline inside the for-each loop

In Source : add parameter >> @item.file_name
In Destination/sink : add parameter>>
folder name : @item/folder_name
filename: @item/file_name

Step 2.6 : Create a data validation
This will ensure when the pipeline gets triggered.
in our case we want the pipelun to run when raw layer gets changed/affected.

settings>> add a new dataset ( datalake gen2 > csv ) . name it as DS_validation that will read contents from raw layer

Again Data_validation > settings > open > set the filename netflix_titles for validation in raw folder.
This will make sure that te data_validation is waiting for the netflix_title file to be present inside raw layer . Until it is there the data_validation will not be succeded.

step 3 : Add another activity as WebActivity
create a webactivity named : FilesMetadata . This will create a metadata for all the files.

> > Paste the url of the root folder where all data resides
> > Method : Get

Step 4: If we want to store all these json response in a variable , we can do the via setVariable in azure.

> > create a variable in pipeline :
> > variable name : metadatavar
> > type: String

> > create activity : setVariable
> > name :Metadatavariable
> > Settings :

               name > the variable we created
               value>  @string(activity('GithubMetadata').output)
               basically  the output from the metadata of the files (webactivity) are collected as a object , in order to read them in the set varable  we store it inside  a string , hence we are putting the output inside : @string()

once step 4 completed , connect all 4 activities and run debug to test the pipeline.

==========================================================================================================================================================
Creating Databricks service>>>

step 5:

in azure portal create databricks service:

1. rg : Netflix_project1_rg
2. workspace name: Netflix_ADB_Abhirup
3. region : East US
4. pricing tier: Premium ( Needed to use unity catalog)
5. Managed RG name: RG_Managed_Netflixprojectgroup

create databricks .

==========================================================================================================================================================
Step 6:

create resource: Access connectors for databricks
this helps to connect databricks with datalake .

Step 7: azure portal > go to storage account > IAM > add role assignment > storage blob data contributor >> next >> managed identity > select members > access connector for azure databricks > select Access_Connector_Netflix >> Review+Assign

Step 8 : go to rg >> access connector >> copy Resource ID (/subscriptions/39b82d3b-e1ee-4ec3-b926-7d8e86b30892/resourceGroups/Netflix_project1_rg/providers/Microsoft.Databricks/accessConnectors/Access_connector_Netflix)
without this resource id databricks wont be able to access datalake

now go to >> https://accounts.azuredatabricks.net
click on catalog >> create metastore
metastore name >> Netflix_Unity_Metastore
region> canadacentral
adls path >> metastore@netflixabhidatalake.dfs.core.windows.net/
access connector id >> /subscriptions/39b82d3b-e1ee-4ec3-b926-7d8e86b30892/resourceGroups/Netflix_project1_rg/providers/Microsoft.Databricks/accessConnectors/Access_connector_Netflix

then click create

After creation go to catalogs>> select metastore>> workspaces>> add workspace >> Netflix_ADB_Abhirup

Step 9: edit metastore admin>> select www.dasabhirup365@gmail.com

==========================================================================================================================================================
Come back to Databricks Workspace >>

step 10 : Create catalog > new catalog>
catalog name : Netflix Catalog

> > add all users

Step 11: Connect databricks workspace to datalake storage layers (bronze, gold, silver)

> > go to databricks workspace >> External data

1. create credential:
   credential name :Abhirup_creds
   connector id: /subscriptions/39b82d3b-e1ee-4ec3-b926-7d8e86b30892/resourceGroups/Netflix_project1_rg/providers/Microsoft.Databricks/accessConnectors/Access_connector_Netflix

2. Create External locations:

Raw:
name : Raw-ext
url : abfss://raw@netflixabhidatalake.dfs.core.windows.net/

Bronze :
name : Bronze-ext
url : abfss://bronze@netflixabhidatalake.dfs.core.windows.net

silver:
name : Silver-ext
url : abfss://silver@netflixabhidatalake.dfs.core.windows.net

Gold:
name : Gold-ext
url : abfss://gold@netflixabhidatalake.dfs.core.windows.net

==========================================================================================================================================================

               DATABRICKS  WORKSPACE

Step 11: Create Compute cluster for Databricks workspace

Step 12: create folder named Netflix_project in databricks workspace

Step 13 : create Notebook Autoloader in folder (Will be used to incrementally load data in databricks workspace)

==========================================================================================================================================================
After the successful execution of the notebook , there will be a checkpoint folder in silver , which will work as a checkpoint for incremental data loading.

==========================================================================================================================================================
Step 14: Create a notebook in databricks named: Silver_2

In this notebook we create 2 widgets named as : sourcefolder & targetfolder

this helps to create data transformation of all files from source folder( lets's say bronze/ netflix_director ) >>>> to destination folder (silver/netflix_director)

==========================================================================================================================================================
Step 15 : create a lookup notebook >> 3_lookupNotebook

In this notebook we create an array of the bronze layer directory structure and will pass as parameters to another notebook Silver_2.

Ex: for our case the array is like :
files = [
{
"sourcefolder" : "netflix_directors",
"targetfolder" : "netflix_directors"
},
{
"sourcefolder" : "netflix_cast",
"targetfolder" : "netflix_cast"
},
{
"sourcefolder" : "netflix_countries",
"targetfolder" : "netflix_countries"
},
{
"sourcefolder" : "netflix_category",
"targetfolder" : "netflix_category"
},
]

now create dbutils.jobs.taskValues.set() in the same notebook.

> > This is a Databricks Jobs feature that allows us to pass data between tasks in a multi-task job pipeline by passing the key,value pair .
> > here the key can be a random_name and value should be the name of the array

therefore the full job utility looks like:

dbutils.jobs.taskValues.set(key = "any_name", value = files)

==========================================================================================================================================================

Step 16 : CREATION OF DATABRICKS JOBS

create a databricks job :
create >> job

        Task 1 >>>>>>>>>>
                   task type>> Notebook
                   path >> path of notebook : 3_lookup_notebook
                   compute >> select cluster
                   create task

       Task 2 >>>>>>>>>
                  task type>> Notebook
                   path >> path of notebook : 2_Silver notebook
                   compute >> select cluster
                   create task

**\*\***IMP**\***
here the silver notebook will accept sourcefolder and destination folder as parameters in a loop

      to do that >> select loop over icon for task2

           task type>> for-each loop
           Inputs>> {{tasks.Lookup_Notebooks.values.task_array}}

           here we are passing the key of 3_lookup_notebook so that silver_2 receives all sorce and destination directories in a loop

Now in the inner task , set the parameters as :
Sourcefolder :{{input.sourcefolder}}
targetfolder :{{input.targetfolder}}

once done >> Run the Job

==========================================================================================================================================================

Step 17: create Notebook 4_Silver , where it will pickup netflix_titles in delta format from bronze layer and then will go through a lot of transformation and will be finally stored in Silver layer in delta format.

==========================================================================================================================================================

Step 18: Running a specific databricks notebook on a scheduled week/day / particular time.

create Notebook 5_lookupnotebook >>
This notebook will get the weeday value (lets say 7) from task and then pass as parameter.

create a databricks Job >>>>

Task 1 : taskname :Weekday_lookup_from_notebook
type : Notebook
path : path of the notebook "5_lookupnotebook"
parameters :
key :weekday
value: {{job.start_time.iso_weekday}}

create task 1

Task 2:
create an If-else condition

         taskname: Ifweekday
         condition: {{tasks.Weekday_lookup_from_notebook.values.weekoutput}} == "value" (7 in our case)
         (weekoutput is the key for 5_lookupnotebook)

create task 2

If True >>>

Task 3 :

        taskname : SilverMasterdata
        type: Notebook
        path : path of 4_silver notebook

If False >>>

create a notebook : 6_falsenotebook

Task 4 :

      taskname : run_false_notebook
        type: Notebook
        path : path of 6_falsenotebook
        depends on: Ifweekday(false)

Run the Job . Now we have successfully scheduled the job

==========================================================================================================================================================

Now we have all data in delta format in Silver Layer

Step 18:

For transportation from silver layer to gold layer we have created a delta pipeline at azure databricks workspace . This pipeline is responsible for transporting all data from silver layer to gold layer of Unity Catalog.

A notebook is created for tasks to be followed at the delta pipeline
the notebook is : 7_DLT_Notebook

Once the delta pipeline runs then we have all tranformed data at the gold layer at Unity Catalog . This data now can be further sent to external visulization tools like tableau or powerbi using the connection file generated through azure marketplace.

How the gold layer reaches Power BI
Once the DLT pipeline promotes data from Silver to Gold inside Unity Catalog, the gold tables sit as governed Delta tables, queryable through a Databricks SQL Warehouse. The "connection file generated through Azure Marketplace" your README mentions is a .pbids file — Databricks' Partner Connect integration for Power BI generates this automatically. It's a small shortcut file that pre-fills the SQL Warehouse's server hostname, HTTP path, and target catalog/schema, so opening it in Power BI Desktop launches straight into the connector's authentication prompt instead of making you hunt down those values manually.
By default, that connector brings the gold tables in as DirectQuery, not Import — every visual queries the warehouse live rather than pulling a static copy into Power BI's own engine. That keeps the dashboard synced to the gold layer without a refresh schedule, but it's also where the real constraints show up: once a report is built on a DirectQuery source you can't just flip it to Import, Top N-style visual filters aren't available the way they are in Import mode, and calculated columns are effectively off the table since they'd need to be materialized by re-querying the source on every interaction. That's why the workarounds were COUNTROWS()-based DAX measures standing in for calculated columns, and "Basic" filtering standing in for Top N wherever a dynamic ranked filter wasn't available.
One thing worth confirming: the guide you shared describes building the model from five local CSV files via Get Data → Text/CSV — that's an Import-mode workflow, not DirectQuery. That reads like a separate, earlier version of this dashboard rather than the one wired live to the gold layer. Is the CSV-based guide an earlier prototype, or did the final dashboard also end up pulling from CSV exports instead of DirectQuery? Worth knowing for sure before it comes up in an interview answer, since the mechanics genuinely differ. Either way, the design decisions below — star schema, bridge tables, DAX layer, page structure — hold regardless of which connectivity mode is actually delivering the data, so here's the full spec:

1. Data audit
   Titles is the fact table — 6,236 rows, 13 columns. Four supporting tables are bridge/dimension tables: cast (44,311 rows), category/genres (13,670 rows), country (7,179 rows), directors (4,852 rows) — each many-to-many against Titles, since one show can have several cast members, genres, countries, or directors.
   Issues found and fixed:

\_rescued_data — 100% null, dropped entirely
One corrupted row — release_year reading 80119194 and type reading "1944" (a clear column-shift in that record), dropped
rating — "TV" had silently overwritten 4,981 real rating values; relabeled to "Unknown" since the true rating wasn't recoverable
date_added — stored as text, converted to a proper Date type, decomposed into year_added / month_added / month_name
show_id — text in Titles, integer in every bridge table; cast to Whole Number everywhere so joins actually resolve
duration_minutes — populated for TV Shows too in the raw data; nulled out there since TV Shows should read off duration_seasons instead

2. Power Query transformations
   Load all 5 tables, rename the queries (Titles, Cast, Category, Country, Directors), apply the fixes above to Titles, and confirm show_id is Whole Number on the four bridge tables — they were otherwise already clean, no other transforms needed.
3. Data model — star schema
   Titles is the fact table; Cast, Category, Country, Directors are dimensions, each joined 1:Many to Titles on show_id. All four relationships use single-direction cross-filtering (Titles → bridge tables) — with genuinely many-to-many fields like cast or genre, bidirectional filtering would create ambiguity and double-count rows.
4. DAX layer
   A dedicated \_Measures table holds:

Core KPIs: Total Titles, Total Movies, Total TV Shows, Movie % of catalog, Avg Movie Duration, Avg TV Show Seasons
Time intelligence: YoY Growth % (current-year count vs. DATEADD(..., -1, YEAR))
Ranking: Top Genre by Count via TOPN over the Category table

5. Dashboard pages

Executive Overview — KPI cards for total titles and the movie/TV split, a donut chart for that split, a Top-10-genre bar chart, a year-over-year line chart by content type, and a type slicer
Content Deep Dive — a filled map of titles by country, a genre treemap, a ratings-by-type clustered bar, a country × type matrix, and a genre slicer
Cast & Directors — Top-15 bar charts for cast and directors, a cast word cloud, a drill-through title table, and country/type slicers
Time & Trends — an area chart of additions by year and type, a by-month column chart, a runtime-vs-release-year scatter plot, a YoY growth KPI card, and a year-range slicer

6. Theme
   A custom "Netflix Dark" theme — #141414 canvas, #E50914 primary red for chart series 1/KPI cards/headings, #2F2F2F visual card backgrounds, #B81D24 as a secondary accent, white text, #999999 for secondary labels. Borders off, data labels on, gridlines set to a subtle #333333, slicers synced across all four pages via View → Sync Slicers.
7. Publish
   Publish to a Power BI workspace, set up refresh (scheduled import refresh if the source is Import mode — DirectQuery stays live without one), and share via link, Teams/SharePoint embed, or exported PNG/PDF.
