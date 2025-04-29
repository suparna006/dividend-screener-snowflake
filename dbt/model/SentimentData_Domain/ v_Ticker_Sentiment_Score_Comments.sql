{{
  config(
    materialized = 'view'
     )
}}

With Src_Ticker_Sentiment_Score_Comments as
(
Select 
    ss.Ticker Ticker, 
    Concat(SS.Ticker , ' has a Sentiment Scrore of ', estimatedsentiment
    , ' - ' , 
    Case 
        when estimatedsentiment >= -1 and estimatedsentiment <= -0.5
        Then 'Bearish'
        when estimatedsentiment >= -0.49 and estimatedsentiment <= -0.1
        Then 'Somewhat-Bearish'
        when estimatedsentiment >= -0.09 and estimatedsentiment <= 0.09
        Then 'Neutral'
        when estimatedsentiment >= 0.1 and estimatedsentiment <= 0.49
        Then 'Somewhat Bullish'
        when estimatedsentiment >= 0.5 and estimatedsentiment <= 1
        Then 'Bullish'
    End 
    ,' with the number of sentiments for each categories as follows  '
    , 'Somewhat-Bullish :: ' ,  Nvl(sb.Sentiment_Count ,0)
    , ', Neutral ::', nvl(NT.Sentiment_Count, 0)
    , ', Bullish ::' , nvl(BL.Sentiment_Count ,0)
    ,  ', Somewhat-Bearish ::' , Nvl(sbr.Sentiment_Count ,0 )
    ,  ', Bearish ::' ,  Nvl(BR.Sentiment_Count ,0)
    )  AS Comments
from 
    (select Ticker, sum(relevance_score*OVERALL_SENTIMENT_score) /sum(relevance_score) as estimatedsentiment 
    from  {{ ref("SP500_SentimentData_Hist")}}  
    group by Ticker) SS
Left Join
    (select Ticker ,  overall_sentiment_label , count(1) Sentiment_Count
    from {{ ref("SP500_SentimentData_Hist")}}   where overall_sentiment_label='Somewhat-Bullish'
    group by Ticker , overall_sentiment_label ) SB
on ss.Ticker=Sb.ticker
Left Join 
    (select Ticker ,  overall_sentiment_label , count(1) Sentiment_Count
    from {{ ref("SP500_SentimentData_Hist")}}    where overall_sentiment_label='Neutral'
    group by Ticker , overall_sentiment_label ) NT
on sb.Ticker=nt.Ticker
Left Join
    (select Ticker ,  overall_sentiment_label , count(1) Sentiment_Count
    from {{ ref("SP500_SentimentData_Hist")}}    where overall_sentiment_label='Bullish'
    group by Ticker , overall_sentiment_label) BL
on sb.Ticker=BL.Ticker
Left JOIN
    (select Ticker ,  overall_sentiment_label , count(1) Sentiment_Count
    from {{ ref("SP500_SentimentData_Hist")}}    where overall_sentiment_label='Somewhat-Bearish'
    group by Ticker , overall_sentiment_label) SBR
on sb.Ticker=sbr.Ticker
Left join
    (select Ticker ,  overall_sentiment_label , count(1) Sentiment_Count
    from {{ ref("SP500_SentimentData_Hist")}}    where overall_sentiment_label='Bearish'
    group by Ticker , overall_sentiment_label) Br
on sb.Ticker=br.Ticker
)

Select * from Src_Ticker_Sentiment_Score_Comments











