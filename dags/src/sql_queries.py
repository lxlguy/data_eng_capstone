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
    create_exploded_subjects_stg = """
    create table if not exists staging_exploded_subjects(
        subjects_string varchar(max),
        subject varchar(500))
    """

class dim_tables():
    create_time_dim = """
    create table if not exists dim_time(
        time_key timestamp PRIMARY KEY,
        quarter_of_year int not null,
        day_of_week int not null,
        day int not null,
        month int not null,
        year int not null)
    """
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
        code varchar(10) PRIMARY KEY,
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
        subject varchar(500),
        subject_id int identity(1,1) PRIMARY KEY)
    """

    insert_dim_subject = """
    insert into dim_subject
    with distinct_table as (select 
        distinct subject 
    from staging_exploded_subjects)
    select dt.subject 
    from distinct_table dt left outer join dim_subject ds
    on dt.subject = ds.subject
    where ds.subject is null
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
        gt.bibnum,
        gt.title,
        gt.publisher,
        gt.author
    from grouped_table gt left outer join dim_books db
        on gt.bibnum = db.bibnum
    where num=1 and db.bibnum is null
    """
class fact():

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
    insert into fact_loans (
    with grouped_table as (
        select * from
            (select 
            *, 
            row_number() over ( partition by bibnum order by itemcount) as num
            from staging_inventory)
        where num=1)
    select 
        sl.id, 
        to_date(substring(sl.checkoutdatetime,1,10), 'YYYY-MM-DD') as time_key, 
        sl.itemtype, 
        sl.collection, 
        sl.bibnumber, 
        md5(gt.subjects)
    from staging_loans sl left outer join grouped_table gt
    on sl.bibnumber = gt.bibnum
    where gt.bibnum is not null
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
    select 
        md5(ses.subjects_string),
        ds.subject_id,
        ses.subjects_string
    from
        (staging_exploded_subjects ses left outer join dim_subject ds 
        on ses.subject = ds.subject) left outer join bridge_subject bs
        on md5(ses.subjects_string) = bs.subject_group_id
        where bs.subject_group_id is null
    """
