CREATE VIEW dqr.dqfailedresults
AS
select * from dqr.dqresults 
where rundatetime>
(select watermarkvalue from dqr.incremental_load_mappings 
where tablename='dqr.dqresults') and RuleStatus ='Fail'


SELECT * FROM dqr.dqfailedresults

select * from dqr.incremental_load_mappings 

update dqr.incremental_load_mappings set watermarkvalue ='2025-01-01'
where tablename = 'dqr.dqresults'
