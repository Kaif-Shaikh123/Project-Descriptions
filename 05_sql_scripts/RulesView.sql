CREATE VIEW dqr.Vw_Rules
--ALTER VIEW dqr.Vw_Rules
AS
SELECT R.*,C.dqrulename,C.dqruletype,O.dqobjectname FROM dqr.dqrules R JOIN dqr.dqchecks C on R.dqcheckid = C.dqcheckid
JOIN dqr.dqobjects O ON R.dqobjectid = O.dqobjectid
WHERE R.Active = 1 AND R.lastmodifieddate>(SELECT watermarkvalue FROM [dqr].[incremental_load_mappings]
WHERE tablename = 'dqr.dqrules')


SELECT * FROM dqr.Vw_Rules

SELECT * FROM dqr.Vw_Rules

DELETE FROM dqr.DQRules WHERE dqruleid>14