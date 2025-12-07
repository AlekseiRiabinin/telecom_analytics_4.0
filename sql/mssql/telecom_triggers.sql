-- Auto-close expired contracts
CREATE TRIGGER trg_AutoCloseContracts
ON Contract
AFTER INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE Contract
    SET Status = 'Expired'
    WHERE EndDate < CAST(GETDATE() AS DATE)
    AND Status = 'Active';
END;
GO


-- Prevent overlapping tariff assignments
CREATE TRIGGER trg_CheckTariffOverlap
ON TariffAssignments
AFTER INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    IF EXISTS (
        SELECT 1 FROM TariffAssignments t
        JOIN inserted i ON t.ContractID = i.ContractID
        WHERE t.AssignmentID <> i.AssignmentID
        AND (
            (i.EffectiveFrom BETWEEN t.EffectiveFrom AND t.EffectiveTo)
            OR (i.EffectiveTo BETWEEN t.EffectiveFrom AND t.EffectiveTo)
        )
    )
    BEGIN
        RAISERROR ('Tariff assignment date range overlaps existing assignment.', 16, 1);
        ROLLBACK TRANSACTION;
        RETURN;
    END;
END;
GO


-- Prevent deleting meters with active customers
CREATE TRIGGER trg_PreventMeterDelete
ON Meters
INSTEAD OF DELETE
AS
BEGIN
    IF EXISTS (
        SELECT 1
        FROM CustomerMeters cm
        JOIN deleted d ON cm.MeterID = d.MeterID
        WHERE cm.EndDate IS NULL
    )
    BEGIN
        RAISERROR ('Cannot delete meter with active customer assignment.', 16, 1);
        RETURN;
    END;

    DELETE FROM Meters
    WHERE MeterID IN (SELECT MeterID FROM deleted);
END;
GO


-- Automatically update communication health scoring
CREATE TRIGGER trg_ComputeMeterHealth
ON MeterCommunicationStatus
AFTER INSERT
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE m
    SET Status = CASE 
                    WHEN i.Online = 1 AND i.SignalStrength >= 80 THEN 'Active'
                    WHEN i.Online = 1 THEN 'Degraded'
                    ELSE 'Inactive'
                 END
    FROM Meters m
    JOIN inserted i ON m.MeterID = i.MeterID;
END;
GO


-- Simple AUDIT trigger template
CREATE TABLE AuditLog (
    AuditID BIGINT IDENTITY PRIMARY KEY,
    TableName NVARCHAR(128),
    Operation NVARCHAR(10),
    RowID NVARCHAR(200),
    ChangedAt DATETIME2 DEFAULT SYSDATETIME(),
    ChangedBy NVARCHAR(128) DEFAULT SYSTEM_USER
);
GO

CREATE TRIGGER trg_Audit_Customer
ON Customer
FOR INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO AuditLog(TableName, Operation, RowID)
    SELECT
        'Customer',
        CASE 
            WHEN EXISTS (SELECT 1 FROM inserted) AND EXISTS (SELECT 1 FROM deleted) THEN 'UPDATE'
            WHEN EXISTS (SELECT 1 FROM inserted) THEN 'INSERT'
            ELSE 'DELETE'
        END,
        COALESCE(CAST(i.CustomerID AS NVARCHAR(50)), CAST(d.CustomerID AS NVARCHAR(50)))
    FROM inserted i
    FULL OUTER JOIN deleted d ON i.CustomerID = d.CustomerID;
END;
GO


-- Validate invoice totals automatically
CREATE TRIGGER trg_UpdateInvoiceTotal
ON InvoiceLines
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE Invoices
    SET TotalAmount = dbo.fn_CalculateInvoiceTotal(Invoices.InvoiceID)
    WHERE InvoiceID IN (
        SELECT InvoiceID FROM inserted
        UNION
        SELECT InvoiceID FROM deleted
    );
END;
GO


-- Auto-generate invoice lines (template)
CREATE TRIGGER trg_AutoGenerateInvoiceLines
ON Invoices
AFTER INSERT
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO InvoiceLines (InvoiceID, Description, Quantity, UnitPrice)
    SELECT 
        i.InvoiceID,
        'Energy Consumption',
        0,  -- Spark/ETL fills this in later
        tr.PricePerKWh
    FROM inserted i
    CROSS APPLY (SELECT dbo.fn_GetActiveTariff(i.CustomerID)) t(TariffID)
    JOIN TariffRates tr ON tr.TariffID = t.TariffID
    WHERE tr.TOUCodeID = 1; -- sample
END;
GO
