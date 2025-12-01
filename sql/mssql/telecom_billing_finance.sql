CREATE TABLE Invoices (
    InvoiceID INT IDENTITY PRIMARY KEY,
    CustomerID INT NOT NULL,
    BillingCycleID INT NOT NULL,
    InvoiceDate DATE NOT NULL,
    TotalAmount DECIMAL(12,2) NOT NULL,
    Status NVARCHAR(20) CHECK (Status IN ('Pending','Paid','Overdue')),
    FOREIGN KEY (CustomerID) REFERENCES Customer(CustomerID),
    FOREIGN KEY (BillingCycleID) REFERENCES BillingCycle(BillingCycleID)
);

CREATE TABLE InvoiceLines (
    LineID INT IDENTITY PRIMARY KEY,
    InvoiceID INT NOT NULL,
    Description NVARCHAR(200),
    Quantity DECIMAL(12,3),
    UnitPrice DECIMAL(12,4),
    Amount AS (Quantity * UnitPrice) PERSISTED,
    FOREIGN KEY (InvoiceID) REFERENCES Invoices(InvoiceID)
);

CREATE TABLE Payments (
    PaymentID INT IDENTITY PRIMARY KEY,
    InvoiceID INT NOT NULL,
    Amount DECIMAL(12,2) NOT NULL,
    PaidAt DATETIME2 NOT NULL,
    Method NVARCHAR(50),
    FOREIGN KEY (InvoiceID) REFERENCES Invoices(InvoiceID)
);
