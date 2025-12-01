CREATE TABLE ETL_ExecutionLog (
    LogID INT IDENTITY PRIMARY KEY,
    SourceName NVARCHAR(100),
    Partition NVARCHAR(100),
    LoadedAt DATETIME2 DEFAULT SYSDATETIME()
);

CREATE TABLE DataQualityIssues (
    IssueID INT IDENTITY PRIMARY KEY,
    SourceName NVARCHAR(100),
    IssueType NVARCHAR(100),
    Description NVARCHAR(500),
    CreatedAt DATETIME2 DEFAULT SYSDATETIME()
);

CREATE TABLE Reconciliation (
    ReconciliationID INT IDENTITY PRIMARY KEY,
    CustomerID INT NOT NULL,
    BillingPeriod DATE NOT NULL,
    BilledKWh DECIMAL(12,3),
    MeasuredKWh DECIMAL(12,3),
    Difference AS (MeasuredKWh - BilledKWh) PERSISTED,
    FOREIGN KEY (CustomerID) REFERENCES Customer(CustomerID)
);
