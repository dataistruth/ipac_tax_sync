-- Master table existence + CT/CDC status check (199 tables from config/common/common_tables.json)
-- Set your database name below before running.

USE [iPC_2025_Dev7_15347];  -- <-- change this
GO

SET NOCOUNT ON;

DECLARE @schema SYSNAME = N'dbo';

-- Database-level flags
SELECT
    DB_NAME() AS database_name,
    d.is_cdc_enabled AS db_cdc_enabled,
    ct_db.retention_period AS ct_retention_minutes,
    ct_db.retention_period_units AS ct_retention_units
FROM sys.databases d
LEFT JOIN sys.change_tracking_databases ct_db ON ct_db.database_id = DB_ID();

IF OBJECT_ID('tempdb..#table_list') IS NOT NULL DROP TABLE #table_list;
CREATE TABLE #table_list (table_name SYSNAME NOT NULL PRIMARY KEY);

INSERT INTO #table_list (table_name) VALUES
    (N'AssetClassOverrideImportData'),
    (N'AtRiskInput_Snapshot'),
    (N'AtRiskPackage'),
    (N'BasisOverrideImportData'),
    (N'BookEffective_Snapshot'),
    (N'BookK1AdjustmentbyPartner'),
    (N'BoxJKLInput_Snapshot'),
    (N'BoxJKLLineItem'),
    (N'ContributionLineWithAttributes'),
    (N'CostPercentage_704c_Snapshot'),
    (N'CostPercentage_Snapshot'),
    (N'CostPercentageWACPeriod_Snapshot'),
    (N'CustomFootnoteInput'),
    (N'CustomFootnoteLineItem'),
    (N'CustomFootnotePackage'),
    (N'CustomImportData'),
    (N'CustomImportData_Snapshot'),
    (N'CustomImportDetail'),
    (N'CustomRegisterDetail'),
    (N'CustomRegisterTemplateDetails'),
    (N'CYAdjustmentInput_Snapshot'),
    (N'CYAdjustmentLineItem'),
    (N'DefaultAllocationRuleSetup'),
    (N'DF_EntityFeedTransactionMapping'),
    (N'DF_FinancialAlloc_Archive'),
    (N'DF_PCAP_Archive'),
    (N'ECIRate'),
    (N'Entity'),
    (N'EntityAllocationRule_Snapshot'),
    (N'EntityConfigurationImportData'),
    (N'EntityConfigurations'),
    (N'EntityLPOffset_SnapShot'),
    (N'EntityOffset_SnapShot'),
    (N'EntityPartnerRelationship'),
    (N'EntityRelationship'),
    (N'ENU_704cAllocationLogic'),
    (N'ENU_AdjustmentType'),
    (N'ENU_AllocationBy'),
    (N'ENU_AllocationLogic'),
    (N'ENU_AllocationPercentageType'),
    (N'ENU_AssetClass'),
    (N'ENU_AttributeType'),
    (N'ENU_CountryListImports'),
    (N'ENU_CustomAllocations'),
    (N'ENU_DataList'),
    (N'ENU_DF_DataList'),
    (N'ENU_EntityType'),
    (N'ENU_Event'),
    (N'ENU_FilingStatus'),
    (N'ENU_ForeignCorptypeofPFIC'),
    (N'ENU_GlobalMenuGroup'),
    (N'ENU_IncomeAttributeType'),
    (N'ENU_InvestmentType'),
    (N'Enu_K1K3ValidationRule'),
    (N'ENU_K1Status'),
    (N'ENU_K3Attribute'),
    (N'ENU_LevelType'),
    (N'ENU_LineType'),
    (N'ENU_MappingSource'),
    (N'ENU_PFICAddressType'),
    (N'ENU_PFICForeignCorp'),
    (N'ENU_ProcessEntityConfigurations'),
    (N'ENU_ProcessEntityConfigurationsType'),
    (N'ENU_RuleGroup'),
    (N'ENU_RuleType'),
    (N'ENU_SICCodes'),
    (N'ENU_StateDataList'),
    (N'ENU_TaxClass'),
    (N'ENU_TrialBalanceCategory'),
    (N'ENU_TrialBalanceSource'),
    (N'ENU_UnderlyingType'),
    (N'ExchangeRateOverride'),
    (N'FDAPRate'),
    (N'FinalFund'),
    (N'FinalFundLog'),
    (N'FootNoteTrialBalanceAdjustments_Snapshot'),
    (N'ForeignCurrencyAverageRate'),
    (N'ForeignCurrencyRate'),
    (N'Form199AInput_Snapshot'),
    (N'Form199ALineItem'),
    (N'Form199APackage'),
    (N'Form8865Input_Snapshot'),
    (N'Form8865LineItem'),
    (N'Form8865Package'),
    (N'Form8865SchInput_Snapshot'),
    (N'Form8865SchPackage'),
    (N'Form8886Entity'),
    (N'Form8886Input_Snapshot'),
    (N'Form8886LineItem'),
    (N'Form8886Package'),
    (N'Form926Entity'),
    (N'Form926Input_Snapshot'),
    (N'Form926LineItem'),
    (N'Form926Package'),
    (N'GlobalMenu'),
    (N'ImportErrorMessages'),
    (N'IncomeAttribute'),
    (N'IncomeAttribute_PreviousYear'),
    (N'IncomeAttributeRounding'),
    (N'K1GLineItem'),
    (N'K1GPartnerTypes'),
    (N'K1Input_Snapshot'),
    (N'K1LineItem'),
    (N'K1Package'),
    (N'K1UBTI_SNAPSHOT'),
    (N'K3MappedAllocableLines'),
    (N'Location'),
    (N'LockUnderlyingEntities'),
    (N'LookthroughAdjustments_Snapshot'),
    (N'LookthroughReclass_Snapshot'),
    (N'LookthroughReclassFNTieringBlocker_Snapshot'),
    (N'M1Adjustments_Snapshot'),
    (N'M3Rules'),
    (N'M3RulesMapping'),
    (N'Map_CustomReportLine'),
    (N'MAP_DerivedLines'),
    (N'Map_Form163J'),
    (N'MAP_ImportColumn'),
    (N'MAP_K1LineItemGroup'),
    (N'MAP_K1LineItemLineType'),
    (N'MAP_ParentChildK1GLineItem'),
    (N'MAP_SourceAttributeRelation'),
    (N'MAPDataRegister'),
    (N'MapDefaultAllocRuleToLineItem'),
    (N'MappingLineItem'),
    (N'MapRulesToUnderlyings'),
    (N'MapYearlyToK1'),
    (N'MasterChartOfAccounts'),
    (N'MasterImportEntityFeed'),
    (N'MergePartner_Snapshot'),
    (N'NonLookthroughEntities'),
    (N'ParentK1GLineItem'),
    (N'Partner_Snapshot'),
    (N'PE_EntityByInvestment'),
    (N'PFICAlertDetails'),
    (N'PFICFootNoteEntity'),
    (N'PFICFootnoteInput_Snapshot'),
    (N'PFICFootnoteLineItem'),
    (N'PFICFootnotePackage'),
    (N'PficForeignCorpClassificationInput'),
    (N'PFICK1Mapping'),
    (N'PFICUpdateAlert'),
    (N'PFICXmlOverrideInput'),
    (N'PFICXmlOverridePackage'),
    (N'Phase'),
    (N'ProcessEntityConfigurationsImport'),
    (N'QuarterDates'),
    (N'ReclassTrialBalanceAdjustments_Snapshot'),
    (N'ReportPackage'),
    (N'RoundingOverride_Snapshot'),
    (N'SidePocketDefinition'),
    (N'SM_AllowHighestGradualRate'),
    (N'SM_EntityLevelStateThresholds'),
    (N'SM_ExcessPartnerWithholding_Snapshot'),
    (N'SM_FederaltoStatePartnerTypeMapping'),
    (N'SM_K1PackageFileData'),
    (N'SM_LookthroughAdjustments_Snapshot'),
    (N'SM_LookthroughReclass_Snapshot'),
    (N'SM_PartnerComposite'),
    (N'SM_PartnerComposite_Snapshot'),
    (N'SM_PartnerWithholding_Snapshot'),
    (N'SM_Payment_Snapshot'),
    (N'SM_PaymentAllocationRules'),
    (N'SM_PYFiling'),
    (N'SM_PYFiling_Snapshot'),
    (N'SM_RoundingOverride'),
    (N'SM_SpecialAllocationCode'),
    (N'SM_SpecialAllocationInput_Snapshot'),
    (N'SM_SpecialAllocationPaymentLineItem'),
    (N'SM_StateCompositeConfiguration'),
    (N'SM_StateFilingHighPenaltyLogic'),
    (N'SM_StateFilingResidencyLogic'),
    (N'SM_StateLineAllocationRule_Snapshot'),
    (N'SM_StateLines'),
    (N'SM_StateTaxRates'),
    (N'SM_StateTaxRates_SubLineItems'),
    (N'SM_StateThresholds'),
    (N'SM_StateWorkspaceInput_Snapshot'),
    (N'SM_StopCompWHCalculation'),
    (N'SM_WHWaiverExemptRules'),
    (N'State_StateCode'),
    (N'StateWithholdingCompositeOverrideData_Snapshot'),
    (N'TagPercentage_Snapshot'),
    (N'TaxHoldbackPercentage'),
    (N'TaxPeriod'),
    (N'Tranche'),
    (N'TrancheSidePocket'),
    (N'TransactionLog'),
    (N'Transfers_Snapshot'),
    (N'TransfersInput_Snapshot'),
    (N'TrialBalanceAdjustments_SnapShot'),
    (N'TU_Entity'),
    (N'UBTIDFPercent_Snapshot'),
    (N'UBTILOC_Snapshot'),
    (N'Workflow'),
    (N'WorkflowChain'),
    (N'WorkflowStatus'),
    (N'Yearly_Snapshot'),
    (N'YearlyByInvestment');

-- Detail: all 199 master tables
SELECT
    tl.table_name,
    CASE WHEN t.object_id IS NOT NULL THEN 1 ELSE 0 END AS table_exists,
    CASE WHEN pk.object_id IS NOT NULL THEN 1 ELSE 0 END AS has_pk,
    CASE WHEN ct.object_id IS NOT NULL THEN 1 ELSE 0 END AS ct_enabled,
    CASE WHEN cdc_tab.source_object_id IS NOT NULL THEN 1 ELSE 0 END AS cdc_enabled,
    cdc_tab.capture_instance,
    CASE
        WHEN t.object_id IS NULL THEN 'MISSING'
        WHEN ct.object_id IS NOT NULL AND cdc_tab.source_object_id IS NOT NULL THEN 'CT+CDC'
        WHEN ct.object_id IS NOT NULL THEN 'CT'
        WHEN cdc_tab.source_object_id IS NOT NULL THEN 'CDC'
        ELSE 'NONE'
    END AS replication_status
FROM #table_list tl
LEFT JOIN sys.tables t
    ON t.object_id = OBJECT_ID(QUOTENAME(@schema) + '.' + QUOTENAME(tl.table_name))
LEFT JOIN (
    SELECT object_id FROM sys.indexes WHERE is_primary_key = 1
) pk ON pk.object_id = t.object_id
LEFT JOIN sys.change_tracking_tables ct ON ct.object_id = t.object_id
LEFT JOIN cdc.change_tables cdc_tab ON cdc_tab.source_object_id = t.object_id
ORDER BY tl.table_name;

-- Summary counts
SELECT replication_status, COUNT(*) AS table_count
FROM (
    SELECT
        CASE
            WHEN t.object_id IS NULL THEN 'MISSING'
            WHEN ct.object_id IS NOT NULL AND cdc_tab.source_object_id IS NOT NULL THEN 'CT+CDC'
            WHEN ct.object_id IS NOT NULL THEN 'CT'
            WHEN cdc_tab.source_object_id IS NOT NULL THEN 'CDC'
            ELSE 'NONE'
        END AS replication_status
    FROM #table_list tl
    LEFT JOIN sys.tables t
        ON t.object_id = OBJECT_ID(QUOTENAME(@schema) + '.' + QUOTENAME(tl.table_name))
    LEFT JOIN sys.change_tracking_tables ct ON ct.object_id = t.object_id
    LEFT JOIN cdc.change_tables cdc_tab ON cdc_tab.source_object_id = t.object_id
) s
GROUP BY replication_status
ORDER BY replication_status;

-- Problems only: missing table or neither CT nor CDC enabled
SELECT *
FROM (
    SELECT
        tl.table_name,
        CASE WHEN t.object_id IS NOT NULL THEN 1 ELSE 0 END AS table_exists,
        CASE WHEN pk.object_id IS NOT NULL THEN 1 ELSE 0 END AS has_pk,
        CASE WHEN ct.object_id IS NOT NULL THEN 1 ELSE 0 END AS ct_enabled,
        CASE WHEN cdc_tab.source_object_id IS NOT NULL THEN 1 ELSE 0 END AS cdc_enabled,
        cdc_tab.capture_instance,
        CASE
            WHEN t.object_id IS NULL THEN 'MISSING'
            WHEN ct.object_id IS NOT NULL AND cdc_tab.source_object_id IS NOT NULL THEN 'CT+CDC'
            WHEN ct.object_id IS NOT NULL THEN 'CT'
            WHEN cdc_tab.source_object_id IS NOT NULL THEN 'CDC'
            ELSE 'NONE'
        END AS replication_status
    FROM #table_list tl
    LEFT JOIN sys.tables t
        ON t.object_id = OBJECT_ID(QUOTENAME(@schema) + '.' + QUOTENAME(tl.table_name))
    LEFT JOIN (
        SELECT object_id FROM sys.indexes WHERE is_primary_key = 1
    ) pk ON pk.object_id = t.object_id
    LEFT JOIN sys.change_tracking_tables ct ON ct.object_id = t.object_id
    LEFT JOIN cdc.change_tables cdc_tab ON cdc_tab.source_object_id = t.object_id
) r
WHERE r.replication_status IN ('MISSING', 'NONE')
ORDER BY r.table_name;
GO
