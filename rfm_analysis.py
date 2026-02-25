import pandas as pd
from sqlalchemy import create_engine


#mysql connection
host = 'localhost'
port = 3306
user = 'root'
password = 'rachita2004'
database = 'marketing_analytics'

engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')


#fetch dta

df = pd.read_sql("SELECT * FROM performance;", engine)

print(">> Sample Data from MySQL:")
print(df.head())


#rfm score function

def rfm_score(series, reverse=False):
    """
    Rank-based scoring from 1 to 5
    reverse=True → lower values = higher score (Recency)
    """
    ranks = series.rank(method="first", ascending=not reverse)
    scores = pd.qcut(ranks, 5, labels=[1,2,3,4,5])
    return scores.astype(int)


#rfm calculation

def compute_rfm(df, id_col):

    df['date'] = pd.to_datetime(df['date'])
    snapshot_date = df['date'].max() + pd.Timedelta(days=1)

    rfm = df.groupby(id_col).agg({
        'date': lambda x: (snapshot_date - x.max()).days,
        'performance_id': 'count',
        'revenue_generated': 'sum'
    }).reset_index()

    rfm.columns = [id_col,'Recency','Frequency','Monetary']

    #rfm scores
    rfm['R_Score'] = rfm_score(rfm['Recency'], reverse=True)
    rfm['F_Score'] = rfm_score(rfm['Frequency'])
    rfm['M_Score'] = rfm_score(rfm['Monetary'])

    #combined score
    rfm['RFM_Score'] = (
        rfm['R_Score'].astype(str) +
        rfm['F_Score'].astype(str) +
        rfm['M_Score'].astype(str)
    )

    return rfm


#segment function

def segment(row):
    if row['R_Score']>=4 and row['F_Score']>=4 and row['M_Score']>=4:
        return "Champions"
    elif row['F_Score']>=4 and row['M_Score']>=4:
        return "Loyal Customers"
    elif row['R_Score']>=4:
        return "Recent Customers"
    elif row['R_Score']<=2 and row['F_Score']>=3:
        return "At Risk"
    else:
        return "Others"


#run rfm

rfm_campaigns = compute_rfm(df,'campaign_id')

#apply segmentation
rfm_campaigns['Segment'] = rfm_campaigns.apply(segment, axis=1)

print("\n>> RFM Results:")
print(rfm_campaigns.head())

#save
rfm_campaigns.to_csv("rfm_campaigns.csv", index=False)

print("\n RFM analysis saved as rfm_campaigns.csv")
