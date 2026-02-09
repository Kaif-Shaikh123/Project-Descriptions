INSERT INTO dqr.dqrules(dqruleid,	dqcheckid,	dqobjectid,	sourcelayer	,targetlayer,	dqattribute1,	dqattribute2,	sqlquery,	lastmodifieddate	,Active) VALUES (15,4,1,'bronze','NA','CostCenterNumber','','SELECT NullCount FROM(SELECT COUNT(*) AS NullCount
FROM costcenter
WHERE CostCenterNumber IS NULL) t WHERE NullCount>0;','2025-09-02 10:24:00',1);
INSERT INTO dqr.dqrules(dqruleid,	dqcheckid,	dqobjectid,	sourcelayer	,targetlayer,	dqattribute1,	dqattribute2,	sqlquery,	lastmodifieddate	,Active) VALUES (16,4,2,'bronze','NA','CurrencyId','','SELECT NullCount FROM(SELECT COUNT(*) AS NullCount
FROM currency
WHERE CurrencyId IS NULL) t WHERE NullCount>0;','2025-09-02 10:24:00',1);
INSERT INTO dqr.dqrules(dqruleid,	dqcheckid,	dqobjectid,	sourcelayer	,targetlayer,	dqattribute1,	dqattribute2,	sqlquery,	lastmodifieddate	,Active) VALUES (17,4,3,'bronze','NA','CustomerId','','SELECT NullCount FROM(SELECT COUNT(*) AS NullCount
FROM custtable
WHERE CustomerId IS NULL) t WHERE NullCount>0;','2025-09-02 10:24:00',1);
INSERT INTO dqr.dqrules(dqruleid,	dqcheckid,	dqobjectid,	sourcelayer	,targetlayer,	dqattribute1,	dqattribute2,	sqlquery,	lastmodifieddate	,Active) VALUES (18,4,4,'bronze','NA','FiscalPeriodName','','SELECT NullCount FROM(SELECT COUNT(*) AS NullCount
FROM fiscalperiod
WHERE FiscalPeriodName IS NULL) t WHERE NullCount>0;','2025-09-02 10:24:00',1);
INSERT INTO dqr.dqrules(dqruleid,	dqcheckid,	dqobjectid,	sourcelayer	,targetlayer,	dqattribute1,	dqattribute2,	sqlquery,	lastmodifieddate	,Active) VALUES (19,4,5,'bronze','NA','PartyId','','SELECT NullCount FROM(SELECT COUNT(*) AS NullCount
FROM parties
WHERE PartyId IS NULL) t WHERE NullCount>0;','2025-09-02 10:24:00',1);
INSERT INTO dqr.dqrules(dqruleid,	dqcheckid,	dqobjectid,	sourcelayer	,targetlayer,	dqattribute1,	dqattribute2,	sqlquery,	lastmodifieddate	,Active) VALUES (20,4,6,'bronze','NA','PartyNumber','','SELECT NullCount FROM(SELECT COUNT(*) AS NullCount
FROM partyaddress
WHERE PartyNumber IS NULL) t WHERE NullCount>0;','2025-09-02 10:24:00',1);
INSERT INTO dqr.dqrules(dqruleid,	dqcheckid,	dqobjectid,	sourcelayer	,targetlayer,	dqattribute1,	dqattribute2,	sqlquery,	lastmodifieddate	,Active) VALUES (21,4,7,'bronze','NA','PromotionId','','SELECT NullCount FROM(SELECT COUNT(*) AS NullCount
FROM promotable
WHERE PromotionId IS NULL) t WHERE NullCount>0;','2025-09-02 10:24:00',1);
INSERT INTO dqr.dqrules(dqruleid,	dqcheckid,	dqobjectid,	sourcelayer	,targetlayer,	dqattribute1,	dqattribute2,	sqlquery,	lastmodifieddate	,Active) VALUES (22,4,8,'bronze','NA','PoNumber,LineItem','','SELECT NullCount FROM(SELECT COUNT(*) AS NullCount
FROM purchaseorder
WHERE PoNumber,LineItem IS NULL) t WHERE NullCount>0;','2025-09-02 10:24:00',1);
INSERT INTO dqr.dqrules(dqruleid,	dqcheckid,	dqobjectid,	sourcelayer	,targetlayer,	dqattribute1,	dqattribute2,	sqlquery,	lastmodifieddate	,Active) VALUES (23,4,9,'bronze','NA','CategoryId','','SELECT NullCount FROM(SELECT COUNT(*) AS NullCount
FROM purchcategory
WHERE CategoryId IS NULL) t WHERE NullCount>0;','2025-09-02 10:24:00',1);
INSERT INTO dqr.dqrules(dqruleid,	dqcheckid,	dqobjectid,	sourcelayer	,targetlayer,	dqattribute1,	dqattribute2,	sqlquery,	lastmodifieddate	,Active) VALUES (24,4,10,'bronze','NA','ContractId','','SELECT NullCount FROM(SELECT COUNT(*) AS NullCount
FROM purchcontracts
WHERE ContractId IS NULL) t WHERE NullCount>0;','2025-09-02 10:24:00',1);
INSERT INTO dqr.dqrules(dqruleid,	dqcheckid,	dqobjectid,	sourcelayer	,targetlayer,	dqattribute1,	dqattribute2,	sqlquery,	lastmodifieddate	,Active) VALUES (25,4,11,'bronze','NA','ItemId','','SELECT NullCount FROM(SELECT COUNT(*) AS NullCount
FROM purchitem
WHERE ItemId IS NULL) t WHERE NullCount>0;','2025-09-02 10:24:00',1);
INSERT INTO dqr.dqrules(dqruleid,	dqcheckid,	dqobjectid,	sourcelayer	,targetlayer,	dqattribute1,	dqattribute2,	sqlquery,	lastmodifieddate	,Active) VALUES (26,4,12,'bronze','NA','SalesOrderNumber','','SELECT NullCount FROM(SELECT COUNT(*) AS NullCount
FROM salesorderline
WHERE SalesOrderNumber IS NULL) t WHERE NullCount>0;','2025-09-02 10:24:00',1);
INSERT INTO dqr.dqrules(dqruleid,	dqcheckid,	dqobjectid,	sourcelayer	,targetlayer,	dqattribute1,	dqattribute2,	sqlquery,	lastmodifieddate	,Active) VALUES (27,4,12,'bronze','NA','SalesOrderLine','','SELECT NullCount FROM(SELECT COUNT(*) AS NullCount
FROM salesorderline
WHERE SalesOrderLine IS NULL) t WHERE NullCount>0;','2025-09-02 10:24:00',1);
INSERT INTO dqr.dqrules(dqruleid,	dqcheckid,	dqobjectid,	sourcelayer	,targetlayer,	dqattribute1,	dqattribute2,	sqlquery,	lastmodifieddate	,Active) VALUES (28,4,13,'bronze','NA','VendId','','SELECT NullCount FROM(SELECT COUNT(*) AS NullCount
FROM vendtable
WHERE VendId IS NULL) t WHERE NullCount>0;','2025-09-02 10:24:00',1);
INSERT INTO dqr.dqrules(dqruleid,	dqcheckid,	dqobjectid,	sourcelayer	,targetlayer,	dqattribute1,	dqattribute2,	sqlquery,	lastmodifieddate	,Active) VALUES (29,4,14,'bronze','NA','WorkerID','','SELECT NullCount FROM(SELECT COUNT(*) AS NullCount
FROM workertable
WHERE WorkerID IS NULL) t WHERE NullCount>0;','2025-09-02 10:24:00',1);

INSERT INTO dqr.dqrules(dqruleid,	dqcheckid,	dqobjectid,	sourcelayer	,targetlayer,	dqattribute1,	dqattribute2,	sqlquery,	lastmodifieddate	,Active) VALUES (30,4,7,'bronze','NA','LastProcessedChange_DateTime','','SELECT NullCount FROM(SELECT COUNT(*) AS NullCount
FROM promotable
WHERE LastProcessedChange_DateTime IS NULL) t WHERE NullCount>0;','2025-09-02 10:24:00',1);


select * from dqr.dqrules

DELETE FROM dqr.DQRules WHERE dqruleid>14