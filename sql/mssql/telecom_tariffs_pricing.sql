CREATE TABLE TariffCatalog (
    TariffID INT IDENTITY PRIMARY KEY,
    TariffName NVARCHAR(100) NOT NULL,
    RateClassID INT NOT NULL,
    IsActive BIT DEFAULT 1,
    FOREIGN KEY (RateClassID) REFERENCES RateClasses(RateClassID)
);

CREATE TABLE TariffRates (
    TariffRateID INT IDENTITY PRIMARY KEY,
    TariffID INT NOT NULL,
    TOUCodeID INT NOT NULL,
    StartTime TIME NOT NULL,
    EndTime TIME NOT NULL,
    PricePerKWh DECIMAL(10,4) NOT NULL,
    FOREIGN KEY (TariffID) REFERENCES TariffCatalog(TariffID),
    FOREIGN KEY (TOUCodeID) REFERENCES TimeOfUseCodes(TOUCodeID)
);

CREATE TABLE TariffAssignments (
    AssignmentID INT IDENTITY PRIMARY KEY,
    ContractID INT NOT NULL,
    TariffID INT NOT NULL,
    EffectiveFrom DATE NOT NULL,
    EffectiveTo DATE NULL,
    FOREIGN KEY (ContractID) REFERENCES Contract(ContractID),
    FOREIGN KEY (TariffID) REFERENCES TariffCatalog(TariffID)
);
