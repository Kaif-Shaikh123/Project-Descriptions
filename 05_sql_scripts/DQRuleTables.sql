CREATE SCHEMA dqr

GO


CREATE TABLE dqr.dqchecks(
	dqcheckid int primary key,	
	dqrulename varchar(100),	
	dqruletype varchar(100),
	lastmodifieddate datetime2
)

GO


CREATE TABLE dqr.dqobjects(
	dqobjectid int primary key,	
	dqobjectname varchar(100),	
	active bit,
	lastmodifieddate datetime2
)

GO


CREATE TABLE dqr.dqrules(
	dqruleid INT PRIMARY KEY,
	dqcheckid INT FOREIGN KEY REFERENCES dqr.dqchecks(dqcheckid),
	dqobjectid INT FOREIGN KEY REFERENCES dqr.dqobjects(dqobjectid),
	sourcelayer	varchar(50),
	targetlayer	varchar(50),
	dqattribute1	varchar(50),
	dqattribute2	varchar(50),
	sqlquery	varchar(max),
	lastmodifieddate	datetime2,
	Active bit

)


SELECT * FROM dqr.dqrules

drop table dqr.dqrules