-- Check if a customer has any active contract
CREATE FUNCTION fn_HasActiveContract (@CustomerID INT)
RETURNS BIT
AS
BEGIN
    DECLARE @Exists BIT = 0;

    IF EXISTS (
        SELECT 1 FROM Contract
        WHERE CustomerID = @CustomerID
        AND Status = 'Active'
        AND (EndDate IS NULL OR EndDate >= CAST(GETDATE() AS DATE))
    )
        SET @Exists = 1;

    RETURN @Exists;
END;
GO


-- Get the active tariff for a customer (scalar)
CREATE FUNCTION fn_GetActiveTariff (@CustomerID INT)
RETURNS INT
AS
BEGIN
    DECLARE @TariffID INT;

    SELECT TOP 1 @TariffID = t.TariffID
    FROM Contract c
    JOIN TariffAssignments t ON t.ContractID = c.ContractID
    WHERE c.CustomerID = @CustomerID
    AND c.Status = 'Active'
    AND t.EffectiveFrom <= CAST(GETDATE() AS DATE)
    AND (t.EffectiveTo IS NULL OR t.EffectiveTo >= CAST(GETDATE() AS DATE))
    ORDER BY t.EffectiveFrom DESC;

    RETURN @TariffID;
END;
GO


-- Get all active meters for a customer (table-valued)
CREATE FUNCTION fn_GetCustomerMeters (@CustomerID INT)
RETURNS TABLE
AS
RETURN (
    SELECT MeterID, StartDate, EndDate
    FROM CustomerMeters
    WHERE CustomerID = @CustomerID
    AND (EndDate IS NULL OR EndDate >= CAST(GETDATE() AS DATE))
);
GO


-- Get current communication status for a meter
CREATE FUNCTION fn_CurrentMeterStatus (@MeterID INT)
RETURNS TABLE
AS
RETURN (
    SELECT TOP 1 
        LastSeen, SignalStrength, Online
    FROM MeterCommunicationStatus
    WHERE MeterID = @MeterID
    ORDER BY LastSeen DESC
);
GO


-- Calculate invoice total using persisted computed columns
CREATE FUNCTION fn_CalculateInvoiceTotal (@InvoiceID INT)
RETURNS DECIMAL(12,2)
AS
BEGIN
    DECLARE @Total DECIMAL(12,2) = 0;

    SELECT @Total = SUM(Amount)
    FROM InvoiceLines
    WHERE InvoiceID = @InvoiceID;

    RETURN @Total;
END;
GO
