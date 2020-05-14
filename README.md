## Phases of work:
1. dataset research
- find interesting dataset
- find/brainstorm complementary datasets (I want to enrich the data to complicate the data eng)
- decide the use case
2. database design
- most likely a dimension/fact table design, relational DB not likely since my use case is probably analytical
3. creating the relevant dimension tables
- create prototype end use case
4. setup airflow
- submit to udacity for review
5. expand use case development
- want to integrate ML further

## Candidate Dataset
Seattle Public Libraries Physical Title Checkout (106M rows to date, daily updated) found in: https://data.seattle.gov/Community/Checkouts-By-Title-Physical-Items-/5src-czff
1. ID
2. CheckoutYear
3. BibNumber (FK to dataset (a)?)
4. ItemBarcode
5. ItemType
6. Collection
7. CallNumber
8. ItemTitle
9. Subjects
10. CheckoutDateTime  

Can be supplemented by other datasets from Seattle Public Library:
- Seattle Library Books Collection Dataset (a), 44.4M rows from https://data.seattle.gov/Community/Library-Collection-Inventory/6vkj-f5xf   
 Provides additional information about the book being loaned
1. BibNum (PK)
2. Title
3. Author
4. ISBN
5. PublicationYear
7. Publisher
8. Subjects
9. ItemType
10. ItemCollection
11. FloatingItem
12. ItemLocation
13. ReportDate
14. ItemCount

- Seattle Library Integrated Library System (ILS) Data Dictionary (b), 580 rows from https://data.seattle.gov/Community/Integrated-Library-System-ILS-Data-Dictionary/pbt3-ytbc  
Provides some additional information about some of the categories used. This table cannot be used as is, will probably need to break it down into smaller tables because it appears that col `Code` appear into multiple columns such as ItemCollection, Collection
1. Code
2. Description
3. Code Type
4. Format Group
5. Format Subgroup
6. Category Group
7. Category Subgroup
8. Age Group

- Additional Datasets that can be sourced externally:
NYT Bestsellers Dataset, sample can be found at: https://www.kaggle.com/cmenca/new-york-times-hardcover-fiction-best-sellers  (put on hold, there is too much data to crunch at the moment)


## Boundary Setting  
There is too much data to ingest, unless I use a paid Spark cluster to crunch the data. Since this project could take quite a while, I will cut down the number of rows to build a database to minimise the running cost of this project, especially so when I intend to investigate more on the machine learning aspect.  
Looking at the inventory dataset and data dictionary, I see that the physical loans of the books can be split by library branches. There are 35 branch codes in the data dictionary. I will pick a branch that doesnt have too heavy a volume and analyse the volume of transactions.  

## Use Case  
Given the borrowing records of a branch, what are the popular genres that are in demand?  
Do a dashboard for librarians to assess the state of library stocks.

(Advanced use case utilizing ML):
Predict the demand for books to aid in the future purchase decision making.




## Project Thoughts
I really wanted to use the HDB Resale dataset from data.gov.sg, but taking into account the total number of rows (~850K rows in all to date), I could not find enough supplementary data to make up for 1M rows of data as required in the rubric.
Possible datasets to supplement that was considered:
1. Singapore bank interest rate from MAS (monthly data)
2. Query points of interest for each HDB block from onemap.sg (with 10K HDB blocks, there will not be enough data to make up for 140K rows - how many businesses and busstops (~5000) do we have? Not that many!) 
3. Sale of private housing (Not enough data rows, <50K )