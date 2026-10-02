-- ============================================================================
-- SitePulse: Microsoft SQL Server / Azure SQL Analytical Star Schema DDL
-- Target Domain: Digital Analytics & Content Ops Reliability
-- Compatible: SQL Server 2019+, Azure SQL Database, Azure Synapse Analytics
-- ============================================================================

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'sitepulse')
BEGIN
    EXEC('CREATE SCHEMA sitepulse;');
END
GO

-- 1. DIMENSION: dim_page
IF OBJECT_ID('sitepulse.dim_page', 'U') IS NULL
BEGIN
    CREATE TABLE sitepulse.dim_page (
        page_sk VARCHAR(32) NOT NULL,
        url NVARCHAR(2048) NOT NULL,
        path NVARCHAR(512) NOT NULL,
        title NVARCHAR(512) NULL,
        content_category NVARCHAR(100) NULL,
        created_at DATETIME2 DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_dim_page PRIMARY KEY CLUSTERED (page_sk)
    );
END
GO

-- 2. DIMENSION: dim_device
IF OBJECT_ID('sitepulse.dim_device', 'U') IS NULL
BEGIN
    CREATE TABLE sitepulse.dim_device (
        device_sk VARCHAR(32) NOT NULL,
        device_type NVARCHAR(50) NOT NULL,
        os NVARCHAR(50) NOT NULL,
        browser NVARCHAR(50) NOT NULL,
        created_at DATETIME2 DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_dim_device PRIMARY KEY CLUSTERED (device_sk)
    );
END
GO

-- 3. DIMENSION: dim_campaign
IF OBJECT_ID('sitepulse.dim_campaign', 'U') IS NULL
BEGIN
    CREATE TABLE sitepulse.dim_campaign (
        campaign_sk VARCHAR(32) NOT NULL,
        utm_source NVARCHAR(100) NULL,
        utm_medium NVARCHAR(100) NULL,
        utm_campaign NVARCHAR(100) NULL,
        created_at DATETIME2 DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_dim_campaign PRIMARY KEY CLUSTERED (campaign_sk)
    );
END
GO

-- 4. FACT: fact_sessions (Clustered Columnstore Index for high-performance analytics)
IF OBJECT_ID('sitepulse.fact_sessions', 'U') IS NULL
BEGIN
    CREATE TABLE sitepulse.fact_sessions (
        session_sk VARCHAR(32) NOT NULL,
        session_id VARCHAR(64) NOT NULL,
        visitor_id VARCHAR(64) NOT NULL,
        session_start DATETIME2 NOT NULL,
        session_end DATETIME2 NOT NULL,
        duration_seconds BIGINT NULL,
        page_view_count BIGINT NOT NULL,
        has_cart INT NOT NULL DEFAULT 0,
        has_checkout INT NOT NULL DEFAULT 0,
        has_purchase INT NOT NULL DEFAULT 0,
        total_revenue DECIMAL(18, 2) NOT NULL DEFAULT 0.00,
        device_sk VARCHAR(32) NULL,
        campaign_sk VARCHAR(32) NULL,
        session_date DATE NOT NULL,
        CONSTRAINT PK_fact_sessions PRIMARY KEY NONCLUSTERED (session_sk)
    );

    CREATE CLUSTERED COLUMNSTORE INDEX CCI_fact_sessions
    ON sitepulse.fact_sessions;
END
GO

-- 5. FACT: fact_conversions
IF OBJECT_ID('sitepulse.fact_conversions', 'U') IS NULL
BEGIN
    CREATE TABLE sitepulse.fact_conversions (
        conversion_sk VARCHAR(32) NOT NULL,
        conversion_event_id VARCHAR(64) NOT NULL,
        session_sk VARCHAR(32) NOT NULL,
        session_id VARCHAR(64) NOT NULL,
        visitor_id VARCHAR(64) NOT NULL,
        conversion_time DATETIME2 NOT NULL,
        order_id NVARCHAR(100) NULL,
        revenue DECIMAL(18, 2) NOT NULL,
        items_count INT NULL,
        currency VARCHAR(10) NOT NULL DEFAULT 'USD',
        device_sk VARCHAR(32) NULL,
        campaign_sk VARCHAR(32) NULL,
        conversion_date DATE NOT NULL,
        CONSTRAINT PK_fact_conversions PRIMARY KEY CLUSTERED (conversion_sk)
    );

    CREATE NONCLUSTERED INDEX IX_fact_conversions_session
    ON sitepulse.fact_conversions (session_sk);
END
GO

-- 6. OPERATIONAL: incident_lifecycle
IF OBJECT_ID('sitepulse.incident_lifecycle', 'U') IS NULL
BEGIN
    CREATE TABLE sitepulse.incident_lifecycle (
        incident_id VARCHAR(32) NOT NULL,
        detected_at DATETIME2 NOT NULL,
        status NVARCHAR(50) NOT NULL, -- DETECTED, TRIAGED, INVESTIGATING, CLOSED
        severity NVARCHAR(20) NOT NULL, -- CRITICAL, HIGH, MEDIUM, LOW
        affected_metric NVARCHAR(100) NOT NULL,
        affected_segment NVARCHAR(100) NOT NULL,
        root_cause_classification NVARCHAR(200) NULL,
        business_impact_usd DECIMAL(18, 2) NULL,
        owner_team NVARCHAR(100) NULL,
        assigned_engineer NVARCHAR(100) NULL,
        action_items_json NVARCHAR(MAX) NULL,
        resolution_notes NVARCHAR(MAX) NULL,
        updated_at DATETIME2 NOT NULL,
        resolved_at DATETIME2 NULL,
        CONSTRAINT PK_incident_lifecycle PRIMARY KEY CLUSTERED (incident_id)
    );
END
GO
