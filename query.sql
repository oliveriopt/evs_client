DECLARE @sql NVARCHAR(MAX) = N'';

-- 1) Bases únicas
WITH input_rows AS (
    SELECT *
    FROM (VALUES
        (N'brkLTL', N'mr', N'MarginRule', N'FreightTermId', N'0'),
        (N'brkLTL', N'mr', N'MarginRule', N'MarginRuleId', N'1'),
        (N'XPOCustomer', N'crm', N'Address', N'AddressId', N'1')
        -- resto de filas
    ) v([Database],[Schema],[Table],[Column],[is_pk_flag])
)
SELECT @sql = STRING_AGG('
    SELECT DBName = ''' + [Database] + ''', SchemaPk = s.name, TablePk = t.name, ColumnPk = c.name
    FROM [' + [Database] + '].sys.tables t
    JOIN [' + [Database] + '].sys.schemas s ON s.schema_id = t.schema_id
    JOIN [' + [Database] + '].sys.key_constraints kc ON kc.parent_object_id = t.object_id AND kc.type = ''PK''
    JOIN [' + [Database] + '].sys.index_columns ic ON ic.object_id = t.object_id AND ic.index_id = kc.unique_index_id
    JOIN [' + [Database] + '].sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
', ' UNION ALL ')
FROM (SELECT DISTINCT [Database] FROM input_rows) dbs;

SET @sql = '
WITH input_rows AS (
    SELECT *
    FROM (VALUES
        (N''brkLTL'', N''mr'', N''MarginRule'', N''FreightTermId'', N''0''),
        (N''brkLTL'', N''mr'', N''MarginRule'', N''MarginRuleId'', N''1''),
        (N''XPOCustomer'', N''crm'', N''Address'', N''AddressId'', N''1'')
    ) v([Database],[Schema],[Table],[Column],[is_pk_flag])
),
rows_0 AS (
    SELECT [Database],[Schema],[Table],[Column]
    FROM input_rows
    WHERE is_pk_flag = N''0''
),
pk_catalog AS (
' + @sql + '
)
SELECT * FROM pk_catalog;
';

EXEC sp_executesql @sql;
