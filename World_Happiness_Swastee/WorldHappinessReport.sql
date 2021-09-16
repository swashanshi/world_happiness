IF (NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Country'))
BEGIN
	CREATE TABLE Country(
	CountryId INT PRIMARY KEY IDENTITY(1, 1),
	CountryName NVARCHAR(255) NOT NULL
	);
END

IF (NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'HappinessReportDetails'))
BEGIN
	CREATE TABLE HappinessReportDetails(
	CountryId INT REFERENCES Country,
	TrackingYear INT NOT NULL,
	HappinessScore DECIMAL (5, 4),
	Economy DECIMAL (5, 4),
	Family DECIMAL (5, 4),
	SocialSupport DECIMAL (5, 4),
	Health DECIMAL (5, 4),
	Freedom DECIMAL (5, 4),
	Trust DECIMAL (5, 4),
	Generosity DECIMAL (5, 4),
	DystopiaResidual DECIMAL (5, 4),
	PRIMARY KEY(CountryId, TrackingYear)
	);
END