CREATE TABLE Meters (
    MeterID INT IDENTITY PRIMARY KEY,
    SerialNumber NVARCHAR(100) UNIQUE NOT NULL,
    Model NVARCHAR(100),
    Manufacturer NVARCHAR(100),
    FirmwareVersion NVARCHAR(50),
    InstalledAt DATETIME2,
    Status NVARCHAR(20) CHECK (Status IN ('Active','Inactive','Maintenance'))
);

CREATE TABLE InstallerLogs (
    InstallerLogID INT IDENTITY PRIMARY KEY,
    MeterID INT NOT NULL,
    ActionType NVARCHAR(50) CHECK (ActionType IN ('Install','Replace','Remove')),
    ActionDate DATETIME2 NOT NULL,
    Notes NVARCHAR(500),
    FOREIGN KEY (MeterID) REFERENCES Meters(MeterID)
);

CREATE TABLE MeterCommunicationStatus (
    StatusID INT IDENTITY PRIMARY KEY,
    MeterID INT NOT NULL,
    LastSeen DATETIME2,
    SignalStrength INT,
    Online BIT,
    FOREIGN KEY (MeterID) REFERENCES Meters(MeterID)
);
