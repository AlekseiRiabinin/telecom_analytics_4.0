CREATE TABLE Substations (
    SubstationID INT IDENTITY PRIMARY KEY,
    Name NVARCHAR(100),
    Location NVARCHAR(200)
);

CREATE TABLE Transformers (
    TransformerID INT IDENTITY PRIMARY KEY,
    SubstationID INT NOT NULL,
    TransformerName NVARCHAR(100),
    MaxLoadKW INT,
    FOREIGN KEY (SubstationID) REFERENCES Substations(SubstationID)
);

CREATE TABLE Feeders (
    FeederID INT IDENTITY PRIMARY KEY,
    TransformerID INT NOT NULL,
    FeederName NVARCHAR(100),
    FOREIGN KEY (TransformerID) REFERENCES Transformers(TransformerID)
);

CREATE TABLE MeterTopology (
    TopologyID INT IDENTITY PRIMARY KEY,
    MeterID INT NOT NULL,
    FeederID INT NOT NULL,
    FOREIGN KEY (MeterID) REFERENCES Meters(MeterID),
    FOREIGN KEY (FeederID) REFERENCES Feeders(FeederID)
);
