

update  convertbond_publish_info
set type = '获得中国证监会同意注册批复的公告'
where id = '4d6274ce-de65-42a2-a5cc-19d340d7270f'



select
*,
DATEDIFF(a.t2,a.t1)
,DATEDIFF(a.t3,a.t2)
from (
	select secCode
	,secName
	,max(case when type = '审核委员会审核通过的公告' then Date(substring(publishTime,1,10)) end) as t1
	,min(case when type = '获得中国证监会同意注册批复的公告' then Date(substring(publishTime,1,10)) end) as t2
	,min(case when type in ('向不特定对象发行可转换公司债券募集说明书摘要') then Date(substring(publishTime,1,10)) end) as t3
	from  convertbond_publish_info
	where publishTime >'2021-01-01 00:00:00'
	group by secCode
	,secName
	order by t3 desc
)a 
where a.t3 >= '2021-12-24'

## 分析数据量
select count(1) from  (
	select secCode
	,secName
	,max(case when type = '审核委员会审核通过的公告' then Date(substring(publishTime,1,10)) end) as t1
	,min(case when type = '获得中国证监会同意注册批复的公告' then Date(substring(publishTime,1,10)) end) as t2
	,min(case when type in ('向不特定对象发行可转换公司债券募集说明书摘要') then Date(substring(publishTime,1,10)) end) as t3
	from  convertbond_publish_info
	where publishTime >'2021-01-01 00:00:00'
	group by secCode
	,secName
	order by t3 desc
)a 
where a.t3 >= '2021-12-24'


## 统计拿到批文后，到发布募集说明书分布情况。
select
round(DATEDIFF(a.t3,a.t2)/7,0)
,count(1)
from (
	select secCode
	,secName
	,max(case when type = '审核委员会审核通过的公告' then Date(substring(publishTime,1,10)) end) as t1
	,min(case when type = '获得中国证监会同意注册批复的公告' then Date(substring(publishTime,1,10)) end) as t2
	,min(case when type in ('向不特定对象发行可转换公司债券募集说明书摘要') then Date(substring(publishTime,1,10)) end) as t3
	from  convertbond_publish_info
	where publishTime >'2021-01-01 00:00:00'
	group by secCode
	,secName
	order by t3 desc
)a 
where a.t3 >= '2021-12-24'
group by round(DATEDIFF(a.t3,a.t2)/7,0)

## 分析占比

select
round(DATEDIFF(a.t3,a.t2)/7,0) as time
,count(1) as cnt
,round(count(1)/123,3) as cnt
from (
	select secCode
	,secName
	,max(case when type = '审核委员会审核通过的公告' then Date(substring(publishTime,1,10)) end) as t1
	,min(case when type = '获得中国证监会同意注册批复的公告' then Date(substring(publishTime,1,10)) end) as t2
	,min(case when type in ('向不特定对象发行可转换公司债券募集说明书摘要') then Date(substring(publishTime,1,10)) end) as t3
	from  convertbond_publish_info
	where publishTime >'2021-01-01 00:00:00'
	group by secCode
	,secName
	order by t3 desc
)a 
where a.t3 >= '2021-12-24'
group by round(DATEDIFF(a.t3,a.t2)/7,0)



## 分析沪市情况

select 
round(DATEDIFF(a.t3,a.t2)/7,0) as time
,count(1) as cnt
,round(count(1)/123,3) as cnt
from (
	select code
	,name
	,max(case when type = '审核委员会审核通过的公告' then Date(substring(see_date,1,10)) end) as t1
	,min(case when type = '获得中国证监会同意注册批复的公告' then Date(substring(see_date,1,10)) end) as t2
	,min(case when type in ('可转换公司债券募集说明书摘要') then Date(substring(see_date,1,10)) end) as t3
	from  sh_convertbond_publish_info
	group by code
	,name
) a 
where a.t2 >= '2022-01-01' and a.t3 is not null
group by round(DATEDIFF(a.t3,a.t2)/7,0)

order by t2