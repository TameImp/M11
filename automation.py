import pandas as pd
#  this currently causing an issue - from sqlalchemy import create_engine


local_df = pd.read_csv('/Users/rancemw6/Library/CloudStorage/OneDrive-ImmediateMedia/Documents/Advanced Data Fellowship/L6/M11 - Engineering/Assignments/Assignment 1/IMDB-Movie-Data-Local.csv')
pg_df = pd.read_csv('/Users/rancemw6/Library/CloudStorage/OneDrive-ImmediateMedia/Documents/Advanced Data Fellowship/L6/M11 - Engineering/Assignments/Assignment 1/IMDB-Movie-Data-Postgres.csv')
s3_df = pd.read_csv("/Users/rancemw6/Library/CloudStorage/OneDrive-ImmediateMedia/Documents/Advanced Data Fellowship/L6/M11 - Engineering/Assignments/Assignment 1/IMDB-Movie-Data-S3.csv")

pg_df = pg_df.iloc[: , 1:]

pg_df .rename(columns = {'Title.1' : 'Title'}, inplace = True)

# print(local_df.info)
# print(pg_df.info)
# print(s3_df.info)

def duplicate_clean(pg, s3_df, local_df):
    local_df.at[239,'Title']='The Host (2013)'
    pg_df.at[240,'Title']='The Host (2013)'
    s3_df.at[239,'Title']='The Host (2013)'
    local_df.at[632,'Title']='The Host (2006)'
    pg_df.at[632,'Title']='The Host (2006)'
    s3_df.at[632,'Title']='The Host (2006)'

duplicate_clean(pg_df, s3_df, local_df)

# def merge_data_func(pg_df, s3_df, local_df, merge_key):
#     """
#     Transform data by merging the 3 dataframe
#     Input: None 
#     Output: Merged data as a dataframe
#     """
#     admission_data = pg_df.\
#                         merge(s3_df,on=merge_key).\
#                         merge(local_df,on=merge_key)
#     return admission_data

# transformed_data = merge_data_func(pg_df, s3_df, local_df, 'Title')

# print(transformed_data)

transformed_data = pd.read_csv('/Users/rancemw6/Library/CloudStorage/OneDrive-ImmediateMedia/Documents/Advanced Data Fellowship/L6/M11 - Engineering/Assignments/Assignment 1/IMDB-Movie-Data.csv')

report = transformed_data.groupby('Genre')['Revenue (Millions)'].sum()
report_df = pd.DataFrame(report)

report_df = report_df.sort_values('Revenue (Millions)', ascending=False)

def createList(n):
    lst = []
    for i in range(n+1):
        lst.append(i)
    return(lst)
rank_list = (createList(len(report_df.index))) 

rank_list.remove(0)

report_df['Rank'] = rank_list

print(report_df)