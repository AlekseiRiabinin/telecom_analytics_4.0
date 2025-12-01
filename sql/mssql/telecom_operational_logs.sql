CREATE TABLE Outage (
    OutageID INT IDENTITY PRIMARY KEY,
    StartTime DATETIME2 NOT NULL,
    EndTime DATETIME2 NULL,
    RegionID INT NOT NULL,
    FOREIGN KEY (RegionID) REFERENCES Region(RegionID)
);

CREATE TABLE OutageRootCause (
    RootCauseID INT IDENTITY PRIMARY KEY,
    OutageID INT NOT NULL,
    Description NVARCHAR(200),
    FOREIGN KEY (OutageID) REFERENCES Outage(OutageID)
);

CREATE TABLE FieldServiceTickets (
    TicketID INT IDENTITY PRIMARY KEY,
    MeterID INT NOT NULL,
    CreatedAt DATETIME2 DEFAULT SYSDATETIME(),
    Description NVARCHAR(500),
    FOREIGN KEY (MeterID) REFERENCES Meters(MeterID)
);

CREATE TABLE EventSummary (
    EventID INT IDENTITY PRIMARY KEY,
    EventDate DATE NOT NULL,
    MeterID INT NOT NULL,
    Alerts INT,
    FOREIGN KEY (MeterID) REFERENCES Meters(MeterID)
);
