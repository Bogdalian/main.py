# -----------------------------------------------------------------------------------------------------------------------
QUERY_1_1 = '''
select accepted.mess1+ rejected.mess2 as All_mess, rejected.mess2 as Reject_mess from
(
select count (id) as mess1  from problems_petition pp 
where num=1 -- первичные
and "source" <>5 -- не 004
and accepted_at >= '2021-06-14'::date -- Приняты модератором
and accepted_at <'2021-06-21'::date
) Accepted,
(
select count (id) as mess2  from problems_petition pp 
where num=1 -- первичные
and status=2 -- статус Отклонено
and "source" <>5 -- не 004
and rejected_at >= 'date_start'::date
and rejected_at <'date_end'::date
and reject_person_id is not null -- отклонил модератор, а не сами удалили
) Rejected
''' # верно
# -----------------------------------------------------------------------------------------------------------------------
QUERY_2_1 = '''
select count(distinct (pp2.author_id))
from problems_petition pp, problems_problem pp2
where 
pp.problem_id = pp2.id  -- связь сообщений и проблем
and pp.created_at >= 'date_start'::date -- промежуток подачи первичных сообщений
and pp.created_at <'date_end'::date
and pp."source" <>5 -- не 004
and pp.num=number -- первичное сообщение
'''
QUERY_2_2 = '''
select count (distinct (pp2.author_id))
from problems_petition pp, problems_problem pp2, accounts_user au 
where 
pp.problem_id = pp2.id and -- связь сообщений и проблем
pp2.author_id =au.id -- связь заявителей по проблемам и их аккаунтов
and pp.created_at >= 'date_start'::date -- промежуток подачи первичных сообщений
and pp.created_at <'date_end'::date
and pp."source" <>5 -- не 004
and pp.num=number -- первичное сообщение
and au.esia_profile_updated_at is not null -- с ЕСИА
'''
# ----------------------------------------------------------------------------------------------------------------------
QUERY_3_1 = '''
select avg (EXTRACT(EPOCH FROM (pp.accepted_at - pp.created_at)::interval )/60) 
from problems_petition pp 
where num=1 -- первичные
and "source" <>5 -- не 004
and accepted_at >= 'date_start'::date -- Дата принятия модератором
and accepted_at <'date_end'::date
'''
QUERY_3_2 = '''
select avg (EXTRACT(EPOCH FROM (pp.rejected_at - pp.created_at)::interval )/60)
from problems_petition pp 
where num=1 -- первичные
and "source" <>5 -- не 004
and status=2 -- статус Отклонено
and rejected_at >= 'date_start'::date -- дата отклонения
and rejected_at <'date_end'::date
and reject_person_id is not null -- отклонил модератор, а не сами удалили
'''
#-----------------------------------------------------------------------------------------------------------------------
QUERY_4 = '''
select distinct(pp2.author_id) as author, count( pp2.id), au.esia_profile_updated_at as has_esia
from problems_petition pp, problems_problem pp2, accounts_user au 
where 
pp.problem_id = pp2.id-- связь сообщений и проблем
and pp2.author_id = au.id  -- связь заявителей и их аккаунтов
and pp.created_at >= 'date_start'::date -- выбранный день подачи сообщений
and pp.created_at <'date_end'::date
and pp."source" <>5 -- не 004
and pp.num=1 -- первичное сообщение
group by author, au.esia_profile_updated_at
having (count( pp2.id)) >=10
'''
# ----------------------------------------------------------------------------------------------------------------------
QUERY_1_1_add = '''select accepted.mess1+ rejected.mess2 as All_mess, rejected.mess2 as Reject_mess from -- принято 
(                                                                                                    -- сообщений всего
select count (id) as mess1  from problems_petition pp -- [0]:всего, [1]:отклоено, [0]-[1]:принято
where num=number 
and "source" <>5 -- не 004
and accepted_at >= 'date_start'::date -- Приняты модератором
and accepted_at <'date_end'::date
) Accepted,
(
select count (id) as mess2  from problems_petition pp 
where num=number 
and status=2 -- статус Отклонено
and "source" <>5 -- не 004
and rejected_at >= 'date_start'::date
and rejected_at <'date_end'::date -- '2021-06-14'
and reject_person_id is not null -- отклонил модератор, а не сами удалили
) Rejected
'''
#-----------------------------------------------------------------------------------------------------------------------
QUERY_1_2_add =""" select avg (EXTRACT(EPOCH FROM (pp.accepted_at - pp.created_at)::interval )/60) 
from problems_petition pp 
where num=number 
and "source" <>5 -- не 004
and accepted_at >= 'date_start'::date -- Дата принятия модератором
and accepted_at <'date_end'::date
"""
#-----------------------------------------------------------------------------------------------------------------------
QUERY_1_3_add = '''select avg (EXTRACT(EPOCH FROM (pp.rejected_at - pp.created_at)::interval )/60)
from problems_petition pp 
where num=number 
and "source" <>5 -- не 004
and status=2 -- статус Отклонено
and rejected_at >= 'date_start'::date -- дата отклонения
and rejected_at < 'date_end'::date
and reject_person_id is not null -- отклонил модератор, а не сами удалили
'''
# ----------------------------------------------------------------------------------------------------------------------
# Среднее время от создания до принятия/отклнения
QUERY_5 = '''
select avg (n) from
((select  (EXTRACT(EPOCH FROM (pp.accepted_at - pp.created_at)::interval )/60) as dt1
from problems_petition pp 
where num=number 
and "source" <>5 -- не 004
and accepted_at >= 'date_start'::date -- Дата принятия модератором
and accepted_at <'date_end'::date
)
union all
(select (EXTRACT(EPOCH FROM (pp.rejected_at - pp.created_at)::interval )/60) as dt2
from problems_petition pp 
where num=number 
and "source" <>5 -- не 004
and status=2 -- статус Отклонено
and rejected_at >= 'date_start'::date -- дата отклонения
and rejected_at <'date_end'::date
and reject_person_id is not null -- отклонил модератор, а не сами удалили
)) t(n)
'''
# ----------------------------------------------------------------------------------------------------------------------
# Разбиение проблем по тематикам
QUERRY_causes_deviations = '''
select p1.count_p1 from (
select count (id) as count_p1  from problems_petition pp 
where num=1 
and status=2 -- статус Отклонено
and "source" <>5 -- не 004
and rejected_at >= 'date_start'::date
and rejected_at <'date_end'::date
and reject_person_id is not null 
and reject_reason like 'Проблема отклонена в соответствии с пунктом causes_number Правил%')
p1
'''
