class socrata_class():
    select_codes = """code,description,code_type,format_group,format_subgroup,category_group,category_subgroup,age_group"""
    
    code_df=code_df[["code", "description","code_type","format_group","format_subgroup","category_group","category_subgroup","age_group"]]

    select_inventory = """bibnum, title,isbn, publicationyear, publisher, itemtype, itemcollection, floatingitem, itemlocation, reportdate, itemcount, author, subjects"""

    inventory_df = inventory_df[["bibnum","title","isbn", "publicationyear", "publisher", "itemtype", "itemcollection", "floatingitem", "itemlocation", "reportdate", "itemcount", "author", "subjects"]]

    select_loans = """id, checkoutyear,bibnumber, itembarcode, itemtype,collection, callnumber, itemtitle, subjects, checkoutdatetime"""

    loans_df=loans_df[["id", "checkoutyear","bibnumber", "itembarcode", "itemtype","collection", "callnumber", "itemtitle", "subjects", "checkoutdatetime"]]

class staging_tables():
    create_inventory_stg = """
    create table if not exists staging_inventory(
        bibnum bigint,
        title varchar(max),
        isbn varchar(max),
        publicationyear varchar(500),
        publisher varchar(500),
        itemtype varchar(500),
        itemcollection varchar(500),
        floatingitem varchar(500),
        itemlocation varchar(500),
        reportdate text,
        itemcount int,
        author varchar(500),
        subjects varchar(max))
    """

    create_loans_stg = """
    create table if not exists staging_loans(
        id text,
        checkoutyear int,
        bibnumber bigint,
        itembarcode bigint,
        itemtype text,
        collection text,
        callnumber text,
        itemtitle varchar(max),
        subjects varchar(max),
        checkoutdatetime text )
    """

    create_code_staging = """
    create table if not exists staging_codes(
        code varchar(10), 
        description text,
        code_type text,
        format_group text,
        format_subgroup varchar(50),
        category_group varchar(20),
        category_subgroup text,
        age_group varchar(10))
    """

    copy_stg_inventory = """
    copy staging_inventory
    from 's3://seattle-test/{}.csv' 
    iam_role 'arn:aws:iam::351134467134:role/redshift_access_s3'
    csv delimiter '|' ignoreheader 1
    """

    copy_stg_loans = """
    copy staging_loans
    from 's3://seattle-test/{}.csv' 
    iam_role 'arn:aws:iam::351134467134:role/redshift_access_s3'
    csv delimiter '|' ignoreheader 1
    """

    copy_stg_code = """
    copy staging_codes
    from 's3://seattle-test/{}.csv'
    iam_role 'arn:aws:iam::351134467134:role/redshift_access_s3'
    csv delimiter '|' ignoreheader 1
    """
    ##must be created from inventory: df of only 1 column, subjects, then remove duplicate, explode the string column. upload.
    create_staging_exploded_subjects = """
    create table if not exists staging_exploded_subjects(
        subjects_string varchar(max),
        subject varchar(500))
    """

    copy_stg_exploded_subjects= """
    copy staging_exploded_subjects
    from 's3://seattle-test/{}.csv'
    iam_role 'arn:aws:iam::351134467134:role/redshift_access_s3'
    csv delimiter '|' ignoreheader 1
    """

class dim_tables():
    create_time_dim = """"
    create table if not exists dim_time(
        time_key timestamp PRIMARY KEY,
        quarter_of_year int not null,
        day_of_week int not null,
        day int not null,
        month int not null,
        year int not null)
    """"
    insert_time_dim = """
    insert into dim_time(
    select
        time_key,
        extract(quarter from time_key) as quarter,
        extract(dayofweek from time_key) as day_of_week,
        extract(day from time_key) as day,
        extract(month from time_key) as month,
        extract(year from time_key) as year
    from 
        (select 
            distinct to_date(substring(checkoutdatetime,1,10), 'YYYY-MM-DD') as time_key
        from staging_loans)
    )"""

    create_dim_collection = """
    create table if not exists dim_collections(
        code varchar(10),
        item_type varchar(50))
    """

    insert_dim_collection = """
    insert into dim_collections(
    select 
        code, 
        category_group
    from staging_codes 
    where category_group!='')
    """

    create_dim_subject = """
    create table if not exists dim_subject(
        subject varchar(500)
        subject_id int identity(1,1))
    """

    insert_dim_subject = """
    insert into dim_subject
    (select 
        distinct subject 
    from staging_exploded_subjects)
    """

    create_dim_books = """
    create table if not exists dim_books(
        bibnum bigint PRIMARY KEY,
        title varchar(max),
        publisher varchar(500),
        author varchar(500))
    """

    insert_dim_books = """
    insert into dim_books
    with grouped_table as (
        select 
            *, 
            row_number() over ( partition by bibnum order by itemcount) as num
        from staging_inventory)    
    select 
        bibnum,
        title,
        publisher,
        author
    from grouped_table
    where num=1
    """

class fact():
    #['id', 'time_key', 'itemtype', 'collection', 'bibnumber', 'subject_group']
    create_fact_loans = """
    create table if not exists fact_loans(
        id varchar(50) PRIMARY KEY,
        time_key timestamp,
        itemtype varchar(10),
        collection varchar(10),
        bibnumber bigint,
        subject_group_id varchar(32)
    )
    """

    insert_fact_loans = """    
    with unique_books_db as (
        select
            distinct 
                bibnum,
                subjects 
        from staging_inventory
    )
    insert into fact_loans (
    select 
        sl.id, 
        to_date(substring(sl.checkoutdatetime,1,10), 'YYYY-MM-DD') as time_key, 
        sl.itemtype, 
        sl.collection, 
        sl.bibnumber, 
        md5(udb.subjects)
    from staging_loans sl left outer join 
    unique_books_db udb on sl.bibnumber = udb.bibnum
    )
    """

class bridge_link():
    create_bridge_subjects = """
    create table if not exists bridge_subject(
        subject_group_id varchar(32),
        subject_id int,
        subjects_string varchar(max)
    )
    """
    insert_bridge_subjects="""
    insert into bridge_subject
    with subjects_cte as(
        select 
            subjects_string,
            md5(subjects_string) as hash_value
    from
        (select 
            distinct subjects_string
        from staging_exploded_subjects)
    )
    select 
        cte.hash_value as subject_group_id,
        ds.subject_id,
        cte.subjects_string
    from
        ((staging_exploded_subjects ses left outer join subjects_cte cte
        on ses.subjects_string=cte.subjects_string) 
        left outer join dim_subject ds 
        on ses.subject = ds.subject)
    """

# """"
# parse_datetime
# select checkoutdatetime, 
# (substring(checkoutdatetime,1,10) ||' ' || substring(checkoutdatetime, 12,12))::timestamp, 
# to_date(substring(checkoutdatetime,1,10), 'YYYY-MM-DD') 
# from loans
# """



# #how to select distinct rows based on bibnumber:
# """
# with grouped as (select *, row_number() over (partition by bibnum order by itemcount) as rank from inventory_staging)
# select * from grouped where rank=1
# """

# create_book_dim = """
# create table if not exists dim_book

# """

def tidy_split_new(df, column, new_column, sep='|', keep=False):
    """
    Split the values of a column and expand so the new DataFrame has one split
    value per row. Filters rows where the column is missing.

    Params
    ------
    df : pandas.DataFrame
        dataframe with the column to split and expand
    column : str
        the column to split and expand
    sep : str
        the string used to split the column's values
    keep : bool
        whether to retain the presplit value as it's own row

    Returns
    -------
    pandas.DataFrame
        Returns a dataframe with the same columns as `df`.
    """
    indexes = list()
    new_values = list()
    df = df.dropna(subset=[column])
    for i, presplit in enumerate(df[column].astype(str)):
        values = presplit.split(sep)
        if keep and len(values) > 1:
            indexes.append(i)
            new_values.append(presplit)
        for value in values:
            indexes.append(i)
            new_values.append(value)
    new_df = df.iloc[indexes, :].copy()
    new_df[new_column] = new_values
    return new_df

import json
import pandas as pd
from sodapy import Socrata
from IPython.core.display import display, HTML
from IPython.core.interactiveshell import InteractiveShell
InteractiveShell.ast_node_interactivity = "all"
display(HTML("<style>.container { width:100% !important; }</style>"))
pd.set_option('display.max_colwidth', None)

with open('credentials.json') as f:
    data = json.load(f)
    socrates_id = data['socrates_id']
    socrates_key = data['socrates_api_key']
    app_token = data['app_token']
    
inventory_db = "6vkj-f5xf"
loans_db = "5src-czff"
codes_db = "pbt3-ytbc"
client = Socrata("data.seattle.gov", app_token, username=socrates_id,password=socrates_key, timeout=100)

# code_response = client.get(codes_db, limit=50000, select="code, description,code_type,format_group,format_subgroup,category_group,category_subgroup,age_group")
code_response = client.get(codes_db)
codes_df = pd.DataFrame.from_records(code_response)
codes_df=codes_df[["code","description","code_type","format_group","format_subgroup","category_group","category_subgroup","age_group"]]
codes_df.to_csv('codes_db.csv', index=False, sep="|")

count_inventory_response = client.get(inventory_db, select="count(*)", where = 'reportdate > "2020-04-29T00:00:00.000"')
target_count = int(count_inventory_response[0]['count'])

counter=0
keep_records=[]
limit=50000
while counter<target_count:
    inventory_results = client.get(inventory_db, limit=limit, offset=counter, where='reportdate > "2020-04-29T00:00:00.000"')
    temp = pd.DataFrame.from_records(inventory_results)
    temp = temp[["bibnum","title","isbn", "publicationyear", "publisher", "itemtype", "itemcollection", "floatingitem", "itemlocation", "reportdate", "itemcount", "author", "subjects"]]    
    keep_records.append(temp)
    counter+=limit
    print("{} downloaded".format(counter))
inventory_df = pd.concat(keep_records)
inventory_df.to_csv('may_inventory.csv', index=False, sep="|")

subjects = inventory_df['subjects'].unique()
subjects_df = pd.DataFrame({'subjects':subjects})
subjects_df = tidy_split_new(subjects_df, 'subjects', 'subject', sep=',', keep=False)
subjects_df.to_csv("subjects_exploded.csv", sep='|', index=False)

# loans_results = client.get(loans_db,select="id, checkoutyear,bibnumber, itembarcode, itemtype,collection, callnumber, itemtitle, subjects, checkoutdatetime", where = 'checkoutdatetime > "2020-04-30T23:59:00.00"')
loans_results = client.get(loans_db, where = 'checkoutdatetime > "2020-04-30T23:59:00.00"')
loans_df = pd.DataFrame.from_records(loans_results)
loans_df = loans_df[["id", "checkoutyear","bibnumber", "itembarcode", "itemtype","collection", "callnumber", "itemtitle", "subjects", "checkoutdatetime"]]
loans_df.to_csv("may_loans.csv", sep='|', index=False)

