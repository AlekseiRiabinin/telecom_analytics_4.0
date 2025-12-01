CREATE TABLE Customer (
    CustomerID INT IDENTITY PRIMARY KEY,
    CustomerName NVARCHAR(150) NOT NULL,
    Email NVARCHAR(150),
    Phone NVARCHAR(50),
    Address NVARCHAR(250),
    RegionID INT NOT NULL,
    Segment NVARCHAR(50) CHECK (Segment IN ('Residential','Commercial','Industrial')),
    CreatedAt DATETIME2 DEFAULT SYSDATETIME(),
    FOREIGN KEY (RegionID) REFERENCES Region(RegionID)
);

CREATE TABLE Contract (
    ContractID INT IDENTITY PRIMARY KEY,
    CustomerID INT NOT NULL,
    StartDate DATE NOT NULL,
    EndDate DATE NULL,
    Status NVARCHAR(20) CHECK (Status IN ('Active','Expired','Suspended')),
    CreatedAt DATETIME2 DEFAULT SYSDATETIME(),
    FOREIGN KEY (CustomerID) REFERENCES Customer(CustomerID)
);

CREATE TABLE CustomerMeters (
    CustomerMeterID INT IDENTITY PRIMARY KEY,
    CustomerID INT NOT NULL,
    MeterID INT NOT NULL,
    StartDate DATE NOT NULL,
    EndDate DATE NULL,
    FOREIGN KEY (CustomerID) REFERENCES Customer(CustomerID),
    FOREIGN KEY (MeterID) REFERENCES Meters(MeterID)
);

CREATE TABLE BillingCycle (
    BillingCycleID INT IDENTITY PRIMARY KEY,
    CycleName NVARCHAR(100) NOT NULL,
    StartDay TINYINT CHECK (StartDay BETWEEN 1 AND 28),
    EndDay TINYINT CHECK (EndDay BETWEEN 1 AND 31),
    IsActive BIT DEFAULT 1
);
